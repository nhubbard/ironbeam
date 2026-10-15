use ironbeam_core::{BagStateMutation, Pipeline, Version};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn bag_state_append_replace_and_clear_run_through_direct() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "pairs",
            vec![(1_i32, 10_i32), (1, 20), (1, -1), (1, 30), (2, 7)],
        )
        .unwrap()
        .stateful_bag_map_named::<i32, i32>(
            "bag",
            "tests/direct-bag-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let count = i32::try_from(prior.len()).unwrap();
                let mutation = match value {
                    -1 => BagStateMutation::Replace(vec![99]),
                    value => BagStateMutation::Append(vec![value]),
                };
                Ok((mutation, count))
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    let rows = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(rows, [(1, 0), (1, 1), (1, 2), (1, 1), (2, 0)]);
}
