//! Direct-only lowering and stateless fusion.
use ironbeam_core::{Error, Graph, Operation, Registry};
use std::fmt::Write as _;

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
                    write!(
                        description,
                        "\n  Pruned unselected nodes: {}",
                        keys.join(", ")
                    )
                    .expect("writing to String cannot fail");
                }
                PlanDecision::MaterializationBarrier { key, semantic_id } => {
                    write!(
                        description,
                        "\n  Materialization barrier: {key} ({semantic_id})"
                    )
                    .expect("writing to String cannot fail");
                }
                PlanDecision::FusedStatelessRoute {
                    source_key,
                    operations,
                } => {
                    write!(
                        description,
                        "\n  Fused stateless route from {source_key}: {}",
                        operations.join(" -> ")
                    )
                    .expect("writing to String cannot fail");
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

pub(crate) fn lower(
    graph: &Graph,
    registry: &Registry,
    output_key: &str,
    batch_limit: usize,
) -> Result<PhysicalPlan, Error> {
    lower_inner(graph, registry, output_key, batch_limit, false)
}

pub(crate) fn lower_stream(
    graph: &Graph,
    registry: &Registry,
    output_key: &str,
    batch_limit: usize,
) -> Result<PhysicalPlan, Error> {
    lower_inner(graph, registry, output_key, batch_limit, true)
}

#[allow(clippy::too_many_lines)]
fn lower_inner(
    graph: &Graph,
    registry: &Registry,
    output_key: &str,
    batch_limit: usize,
    stream_shard_boundary: bool,
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
            && !(stream_shard_boundary
                && matches!(node.operation, Operation::Reshuffle)
                && node.semantic_id == "ironbeam-builtin/stream-shard-boundary")
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
                                    | Operation::SlidingWindow
                                    | Operation::SessionWindow
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
    lower_routes(
        graph,
        output_key,
        Vec::new(),
        &mut routes,
        stream_shard_boundary,
    )?;
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

const fn requires_materialization(operation: &Operation) -> bool {
    matches!(
        operation,
        Operation::GroupByKey
            | Operation::Reshuffle
            | Operation::WaitOn
            | Operation::WaitOnWindowed
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
    stream_shard_boundary: bool,
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
        | Operation::StatefulParDo { .. }
        | Operation::FixedWindow
        | Operation::SlidingWindow
        | Operation::SessionWindow => {
            let mut suffix = suffix;
            if !matches!(node.operation, Operation::Output | Operation::Sink { .. }) {
                suffix.push(node.key.clone());
            }
            let parent = node
                .input
                .as_deref()
                .ok_or_else(|| Error::graph(key, "missing parent"))?;
            lower_routes(graph, parent, suffix, routes, stream_shard_boundary)?;
        }
        Operation::Reshuffle
            if stream_shard_boundary
                && node.semantic_id == "ironbeam-builtin/stream-shard-boundary" =>
        {
            let parent = node
                .input
                .as_deref()
                .ok_or_else(|| Error::graph(key, "missing parent"))?;
            lower_routes(graph, parent, suffix, routes, stream_shard_boundary)?;
        }
        Operation::Flatten => {
            let parent = node
                .input
                .as_deref()
                .ok_or_else(|| Error::graph(key, "missing parent"))?;
            lower_routes(graph, parent, suffix.clone(), routes, stream_shard_boundary)?;
            for edge in &node.additional_inputs {
                lower_routes(
                    graph,
                    &edge.key,
                    suffix.clone(),
                    routes,
                    stream_shard_boundary,
                )?;
            }
        }
        _ => return Err(Error::unsupported(key, "direct", "lowering")),
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/planner__tests.rs"]
mod tests;
