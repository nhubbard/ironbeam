use ironbeam_core::{ErrorKind, Pipeline, Version};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::PartitionExt;

#[test]
fn partition_routes_each_value_to_exactly_one_branch() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", (0_i32..9).collect()).unwrap();
    let outputs = input
        .partition_named("classify", "part", 3, |value| value.rem_euclid(3) as usize)
        .unwrap();
    assert_eq!(outputs.len(), 3);
    let merged = outputs[0]
        .flatten_named("merge", &outputs[1..])
        .unwrap()
        .output("result")
        .unwrap();
    let mut values = DirectRunner
        .run_collect(&merged, &JobConfig::default())
        .unwrap()
        .0;
    values.sort_unstable();
    assert_eq!(values, (0_i32..9).collect::<Vec<_>>());
    let branch = outputs[1].output("second-output").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&branch, &JobConfig::default())
            .unwrap()
            .0,
        vec![1, 4, 7]
    );
}

#[test]
fn partition_rejects_invalid_counts_and_indices() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![1_i32]).unwrap();
    assert_eq!(
        input
            .partition_named("none", "part", 0, |_| 0)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        input
            .partition_named("empty-prefix", "", 2, |_| 0)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    let bad = input
        .partition_named("bad-classify", "bad-part", 2, |_| 2)
        .unwrap()[0]
        .output("bad-output")
        .unwrap();
    let error = DirectRunner
        .run_collect(&bad, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
}

#[test]
fn versioned_partition_changes_graph_identity() {
    let graph_for = |minor| {
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", vec![1_i32]).unwrap();
        input
            .partition_with_semantic_named(
                "classify",
                "part",
                2,
                "tests/partition",
                Version { major: 1, minor },
                |value| value.rem_euclid(2) as usize,
            )
            .unwrap();
        pipeline.snapshot().unwrap().0.fingerprint().unwrap()
    };
    assert_ne!(graph_for(0), graph_for(1));
}

#[test]
fn partition_of_empty_input_has_empty_outputs() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("input", Vec::<i32>::new()).unwrap();
    let branches = source
        .partition_named("classify", "part", 2, |_| 0)
        .unwrap();
    for (index, branch) in branches.into_iter().enumerate() {
        let output = branch.output(&format!("output-{index}")).unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .unwrap()
                .0,
            Vec::<i32>::new()
        );
    }
}
