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
}

pub(crate) struct PhysicalPlan {
    pub(crate) summary: PlanSummary,
    pub(crate) operations: Vec<String>,
    pub(crate) source_digest: Option<String>,
}

pub(crate) fn lower(
    graph: &Graph,
    registry: &Registry,
    output_key: &str,
    batch_limit: usize,
) -> Result<PhysicalPlan, Error> {
    let output = graph
        .node(output_key)
        .ok_or_else(|| Error::graph(output_key, "output node missing"))?;
    if !matches!(output.operation, Operation::Output) {
        return Err(Error::graph(
            output_key,
            "requested terminal is not an output",
        ));
    }
    let mut operations = Vec::new();
    let mut current = output;
    let source_key = loop {
        match current.operation {
            Operation::Source { .. } => break current.key.clone(),
            Operation::Map | Operation::Filter => operations.push(current.key.clone()),
            Operation::Output => {}
            _ => return Err(Error::unsupported(&current.key, "direct", "lowering")),
        }
        let parent = current
            .input
            .as_ref()
            .ok_or_else(|| Error::graph(&current.key, "missing parent"))?;
        current = graph
            .node(parent)
            .ok_or_else(|| Error::graph(parent, "missing parent node"))?;
    };
    operations.reverse();
    let source_len_hint = registry.source_len_hint(&source_key);
    let summary = PlanSummary {
        source_key: source_key.clone(),
        output_key: output_key.into(),
        operation_count: operations.len(),
        fused_stages: usize::from(!operations.is_empty()),
        batch_limit,
        source_len_hint,
        estimated_partitions: source_len_hint.map(|len| len.div_ceil(batch_limit)),
        graph_fingerprint: graph.fingerprint()?,
    };
    Ok(PhysicalPlan {
        summary,
        operations,
        source_digest: registry
            .source_config_digest(&source_key)
            .map(str::to_owned),
    })
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
        assert!(matches!(
            lower(&broken, &registry, "out", 1).err().unwrap().kind,
            ErrorKind::UnsupportedCapability { .. }
        ));
    }
}
