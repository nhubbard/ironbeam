//! Test-only Dataflow lowering prototype; no public Runner trait or cloud execution.
use ironbeam_core::{Capabilities, Graph, Operation, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};

fn lower_dataflow_preview(graph: &Graph) -> Result<Vec<String>, ironbeam_core::Error> {
    graph.validate_for("dataflow-preview", &Capabilities::default())?;
    let mut labels = Vec::new();
    for node in &graph.nodes {
        let transform = match node.operation {
            Operation::Source { .. } => "Impulse/materialized-source",
            Operation::Map | Operation::Filter => "ParDo",
            Operation::Output => "ResultExport",
            _ => {
                return Err(ironbeam_core::Error::unsupported(
                    &node.key,
                    "dataflow-preview",
                    "lowering",
                ));
            }
        };
        labels.push(format!("{}:{transform}", node.key));
    }
    Ok(labels)
}

#[test]
fn same_descriptor_lowers_to_direct_and_dataflow_preview() {
    let p = Pipeline::default();
    let source = p.from_vec("input", vec![1_i32, 2, 3, 4]).unwrap();
    let output = source
        .map_named("double/v1", |x| x * 2)
        .unwrap()
        .filter_named("even/v1", |x| x % 2 == 0)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = output.snapshot().unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [2, 4, 6, 8]
    );
    assert_eq!(
        lower_dataflow_preview(&graph).unwrap(),
        [
            "input:Impulse/materialized-source",
            "double/v1:ParDo",
            "even/v1:ParDo",
            "result:ResultExport",
        ]
    );
    assert!(graph.fingerprint().unwrap().starts_with("sha256-v1:"));
}
