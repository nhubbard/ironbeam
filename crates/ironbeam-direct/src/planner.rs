//! Direct-only lowering and stateless fusion.
use ironbeam_core::{Error, Graph, Operation, Registry};

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
}

pub(crate) struct Route {
    pub(crate) source_key: String,
    pub(crate) operations: Vec<String>,
}
pub(crate) struct PhysicalPlan {
    pub(crate) summary: PlanSummary,
    pub(crate) routes: Vec<Route>,
    pub(crate) source_digest: Option<String>,
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
    let materialized = selected.iter().any(|key| {
        graph.node(key).is_some_and(|node| {
            matches!(
                node.operation,
                Operation::GroupByKey
                    | Operation::JoinInner
                    | Operation::JoinLeft
                    | Operation::JoinRight
                    | Operation::JoinFull
                    | Operation::SideInputMap
                    | Operation::GlobalCombine
            )
        })
    });
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
                                Operation::Map | Operation::Filter | Operation::FlatMap
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
            },
            routes: Vec::new(),
            source_digest: None,
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
        source_key: source_key.clone(),
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
    };
    Ok(PhysicalPlan {
        summary,
        source_digest: (routes.len() == 1)
            .then(|| {
                registry
                    .source_config_digest(&source_key)
                    .map(str::to_owned)
            })
            .flatten(),
        routes,
        materialized: false,
    })
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
        | Operation::FlatMap => {
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
}
