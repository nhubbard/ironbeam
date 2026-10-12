//! Direct-only lowering and stateless fusion.
use ironbeam_core::{Error, Graph, Operation, Registry};

/// A physical planning choice made while lowering the selected Direct output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanDecision {
    PrunedNodes {
        keys: Vec<String>,
    },
    MaterializationBarrier {
        key: String,
        semantic_id: String,
    },
    FusedStatelessRoute {
        source_key: String,
        operations: Vec<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanSummary {
    pub source_key: String,
    pub output_key: String,
    pub operation_count: usize,
    pub fused_stages: usize,
    pub batch_limit: usize,
    pub source_len_hint: Option<usize>,
    pub estimated_partitions: Option<usize>,
    pub graph_fingerprint: String,
    pub routes: usize,
    pub materialized: bool,
    pub decisions: Vec<PlanDecision>,
}

impl PlanSummary {
    /// Describe the selected Direct plan without opening any source.
    #[must_use]
    pub fn explain(&self) -> String {
        let mut description = format!(
            "Direct plan: {} -> {}\nGraph: {}\nExecution: {}\nOperations: {}\nFused stages: {}\nSource routes: {}\nBatch limit: {}\nSource length hint: {}\nEstimated partitions: {}",
            self.source_key,
            self.output_key,
            self.graph_fingerprint,
            if self.materialized {
                "materialized"
            } else {
                "streaming"
            },
            self.operation_count,
            self.fused_stages,
            self.routes,
            self.batch_limit,
            self.source_len_hint
                .map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
            self.estimated_partitions
                .map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
        );
        description.push_str("\nDecisions:");
        if self.decisions.is_empty() {
            description.push_str(" none");
        }
        for decision in &self.decisions {
            match decision {
                PlanDecision::PrunedNodes { keys } => {
                    description
                        .push_str(&format!("\n  Pruned unselected nodes: {}", keys.join(", ")));
                }
                PlanDecision::MaterializationBarrier { key, semantic_id } => {
                    description.push_str(&format!(
                        "\n  Materialization barrier: {key} ({semantic_id})"
                    ));
                }
                PlanDecision::FusedStatelessRoute {
                    source_key,
                    operations,
                } => {
                    description.push_str(&format!(
                        "\n  Fused stateless route from {source_key}: {}",
                        operations.join(" -> ")
                    ));
                }
            }
        }
        description
    }
}

pub(crate) struct Route {
    pub(crate) source_key: String,
    pub(crate) operations: Vec<String>,
}
pub(crate) struct PhysicalPlan {
    pub(crate) summary: PlanSummary,
    pub(crate) routes: Vec<Route>,
    pub(crate) materialized: bool,
}

#[allow(clippy::too_many_lines)]
pub(crate) fn lower(
    graph: &Graph,
    registry: &Registry,
    output_key: &str,
    batch_limit: usize,
) -> Result<PhysicalPlan, Error> {
    let output = graph
        .node(output_key)
        .ok_or_else(|| Error::graph(output_key, "output node missing"))?;
    if !matches!(output.operation, Operation::Output | Operation::Sink { .. }) {
        return Err(Error::graph(
            output_key,
            "requested terminal is not an output",
        ));
    }
    let mut selected = std::collections::BTreeSet::new();
    collect_dependencies(graph, output_key, &mut selected)?;
    let mut decisions = Vec::new();
    let mut pruned = graph
        .nodes
        .iter()
        .filter(|node| !selected.contains(&node.key))
        .map(|node| node.key.clone())
        .collect::<Vec<_>>();
    if !pruned.is_empty() {
        pruned.sort();
        decisions.push(PlanDecision::PrunedNodes { keys: pruned });
    }
    for key in &selected {
        if let Some(node) = graph.node(key)
            && requires_materialization(&node.operation)
        {
            decisions.push(PlanDecision::MaterializationBarrier {
                key: key.clone(),
                semantic_id: node.semantic_id.clone(),
            });
        }
    }
    let materialized = decisions
        .iter()
        .any(|decision| matches!(decision, PlanDecision::MaterializationBarrier { .. }));
    if materialized {
        let sources: Vec<_> = selected
            .iter()
            .filter_map(|key| {
                graph
                    .node(key)
                    .filter(|node| matches!(node.operation, Operation::Source { .. }))
            })
            .collect();
        let source_key = sources
            .first()
            .ok_or_else(|| Error::graph(output_key, "plan has no source"))?
            .key
            .clone();
        return Ok(PhysicalPlan {
            summary: PlanSummary {
                source_key,
                output_key: output_key.into(),
                operation_count: selected.len().saturating_sub(sources.len() + 1),
                fused_stages: selected
                    .iter()
                    .filter(|key| {
                        graph.node(key).is_some_and(|node| {
                            matches!(
                                node.operation,
                                Operation::Map
                                    | Operation::Filter
                                    | Operation::FlatMap
                                    | Operation::AssignTimestamp
                                    | Operation::FixedWindow
                            )
                        })
                    })
                    .count(),
                batch_limit,
                source_len_hint: sources
                    .iter()
                    .map(|node| registry.source_len_hint(&node.key))
                    .sum(),
                estimated_partitions: sources
                    .iter()
                    .map(|node| {
                        registry
                            .source_len_hint(&node.key)
                            .map(|len| len.div_ceil(batch_limit))
                    })
                    .sum(),
                graph_fingerprint: graph.fingerprint()?,
                routes: sources.len(),
                materialized: true,
                decisions,
            },
            routes: Vec::new(),
            materialized: true,
        });
    }
    let mut routes = Vec::new();
    lower_routes(graph, output_key, Vec::new(), &mut routes)?;
    let source_key = routes
        .first()
        .ok_or_else(|| Error::graph(output_key, "plan has no source"))?
        .source_key
        .clone();
    let source_len_hint = routes
        .iter()
        .map(|route| registry.source_len_hint(&route.source_key))
        .sum();
    let operation_count = routes
        .iter()
        .flat_map(|route| route.operations.iter())
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    let summary = PlanSummary {
        source_key,
        output_key: output_key.into(),
        operation_count,
        fused_stages: routes
            .iter()
            .filter(|route| !route.operations.is_empty())
            .count(),
        batch_limit,
        source_len_hint,
        estimated_partitions: routes
            .iter()
            .map(|route| {
                registry
                    .source_len_hint(&route.source_key)
                    .map(|len| len.div_ceil(batch_limit))
            })
            .sum(),
        graph_fingerprint: graph.fingerprint()?,
        routes: routes.len(),
        materialized: false,
        decisions: {
            decisions.extend(
                routes
                    .iter()
                    .filter(|route| route.operations.len() >= 2)
                    .map(|route| PlanDecision::FusedStatelessRoute {
                        source_key: route.source_key.clone(),
                        operations: route.operations.clone(),
                    }),
            );
            decisions
        },
    };
    Ok(PhysicalPlan {
        summary,
        routes,
        materialized: false,
    })
}

fn requires_materialization(operation: &Operation) -> bool {
    matches!(
        operation,
        Operation::GroupByKey
            | Operation::Reshuffle
            | Operation::WaitOn
            | Operation::CoGroupByKey
            | Operation::JoinInner
            | Operation::JoinLeft
            | Operation::JoinRight
            | Operation::JoinFull
            | Operation::SideInputMap
            | Operation::GlobalCombine
            | Operation::FoldGlobal
            | Operation::FoldPerKey
            | Operation::FoldOrderedGlobal
            | Operation::FoldOrderedPerKey
            | Operation::BatchMapValues { .. }
    )
}

fn collect_dependencies(
    graph: &Graph,
    key: &str,
    selected: &mut std::collections::BTreeSet<String>,
) -> Result<(), Error> {
    if !selected.insert(key.into()) {
        return Ok(());
    }
    let node = graph
        .node(key)
        .ok_or_else(|| Error::graph(key, "missing dependency"))?;
    if let Some(parent) = &node.input {
        collect_dependencies(graph, parent, selected)?;
    }
    for edge in &node.additional_inputs {
        collect_dependencies(graph, &edge.key, selected)?;
    }
    Ok(())
}

fn lower_routes(
    graph: &Graph,
    key: &str,
    suffix: Vec<String>,
    routes: &mut Vec<Route>,
) -> Result<(), Error> {
    let node = graph
        .node(key)
        .ok_or_else(|| Error::graph(key, "missing parent node"))?;
    match &node.operation {
        Operation::Source { .. } => {
            let mut operations = suffix;
            operations.reverse();
            routes.push(Route {
                source_key: node.key.clone(),
                operations,
            });
        }
        Operation::Output
        | Operation::Sink { .. }
        | Operation::Map
        | Operation::Filter
        | Operation::FlatMap
        | Operation::AssignTimestamp
        | Operation::FixedWindow => {
            let mut suffix = suffix;
            if !matches!(node.operation, Operation::Output | Operation::Sink { .. }) {
                suffix.push(node.key.clone());
            }
            let parent = node
                .input
                .as_deref()
                .ok_or_else(|| Error::graph(key, "missing parent"))?;
            lower_routes(graph, parent, suffix, routes)?;
        }
        Operation::Flatten => {
            let parent = node
                .input
                .as_deref()
                .ok_or_else(|| Error::graph(key, "missing parent"))?;
            lower_routes(graph, parent, suffix.clone(), routes)?;
            for edge in &node.additional_inputs {
                lower_routes(graph, &edge.key, suffix.clone(), routes)?;
            }
        }
        _ => return Err(Error::unsupported(key, "direct", "lowering")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironbeam_core::{ErrorKind, Pipeline};

    #[test]
    fn lowering_checks_terminal_and_chain_shape() {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![1_i32])
            .unwrap()
            .map_named("map", |x| x + 1)
            .unwrap()
            .output("out")
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        assert_eq!(
            lower(&graph, &registry, "missing", 1).err().unwrap().kind,
            ErrorKind::Graph
        );
        assert_eq!(
            lower(&graph, &registry, "source", 1).err().unwrap().kind,
            ErrorKind::Graph
        );
        let mut broken = graph.clone();
        broken
            .nodes
            .iter_mut()
            .find(|n| n.key == "out")
            .unwrap()
            .input = None;
        assert_eq!(
            lower(&broken, &registry, "out", 1).err().unwrap().kind,
            ErrorKind::Graph
        );
        let mut broken = graph.clone();
        broken
            .nodes
            .iter_mut()
            .find(|n| n.key == "out")
            .unwrap()
            .input = Some("absent".into());
        assert_eq!(
            lower(&broken, &registry, "out", 1).err().unwrap().kind,
            ErrorKind::Graph
        );
        let mut broken = graph;
        broken
            .nodes
            .iter_mut()
            .find(|n| n.key == "map")
            .unwrap()
            .operation = Operation::GlobalCombine;
        assert!(lower(&broken, &registry, "out", 1).unwrap().materialized);
    }

    #[test]
    fn explain_reports_only_decisions_the_selected_plan_makes() {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
        source.map_named("unused", |value| value + 10).unwrap();
        source
            .map_named("double", |value| value * 2)
            .unwrap()
            .filter_named("even", |value| value % 2 == 0)
            .unwrap()
            .output("out")
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        let summary = lower(&graph, &registry, "out", 1).unwrap().summary;
        assert!(!summary.materialized);
        assert_eq!(
            summary.decisions,
            vec![
                PlanDecision::PrunedNodes {
                    keys: vec!["unused".into()],
                },
                PlanDecision::FusedStatelessRoute {
                    source_key: "source".into(),
                    operations: vec!["double".into(), "even".into()],
                },
            ]
        );
        let explanation = summary.explain();
        assert!(explanation.contains("Pruned unselected nodes: unused"));
        assert!(explanation.contains("Fused stateless route from source: double -> even"));
        assert!(!explanation.contains("Materialization barrier"));
    }

    #[test]
    fn source_only_plan_reports_no_physical_decisions() {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![1_i32, 2])
            .unwrap()
            .output("out")
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        let summary = lower(&graph, &registry, "out", 2).unwrap().summary;
        assert!(summary.decisions.is_empty());
        assert!(summary.explain().contains("Decisions: none"));
    }

    #[test]
    fn explain_identifies_the_materializing_operation() {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![(1_i32, 2_i32)])
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .output("out")
            .unwrap();
        let (graph, registry) = pipeline.snapshot().unwrap();
        let summary = lower(&graph, &registry, "out", 2).unwrap().summary;
        assert!(summary.materialized);
        assert!(summary.decisions.iter().any(|decision| {
            matches!(decision, PlanDecision::MaterializationBarrier { key, .. } if key == "group")
        }));
        assert!(summary.explain().contains("Materialization barrier: group"));
    }
}
