// Unit tests for src/multi_output.rs.
use super::{prepare, prepare_sinks};
use ironbeam_core::{ErrorKind, Pipeline};

#[test]
fn pair_planner_rejects_terminals_with_the_wrong_operation() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let left = source.output("left").unwrap();
    let right = source
        .map_named("right-map", |value| value + 1)
        .unwrap()
        .output("right")
        .unwrap();
    let (graph, registry) = left.snapshot().unwrap();
    assert_eq!(
        prepare(&graph, &registry, "source", right.key())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        prepare_sinks(&graph, &registry, left.key(), right.key())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
}
