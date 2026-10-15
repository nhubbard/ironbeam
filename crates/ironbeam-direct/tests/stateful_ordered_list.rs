use ironbeam_core::{OrderedListStateMutation, Pipeline, Version};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn ordered_list_state_runs_per_key_through_direct() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "pairs",
            vec![(1_i32, 20_i32), (1, 10), (2, 30), (1, 10), (1, -1), (1, 15)],
        )
        .unwrap()
        .stateful_ordered_list_map_named::<i32, Vec<i32>>(
            "ordered",
            "tests/direct-ordered-list-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let seen = prior.iter().map(|(_, item)| *item).collect();
                let mutation = if value == -1 {
                    OrderedListStateMutation::ClearRange {
                        start_ms: 10,
                        end_ms: 11,
                    }
                } else {
                    OrderedListStateMutation::Append(i64::from(value), value)
                };
                Ok((mutation, seen))
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    let rows = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(
        rows,
        [
            (1, vec![]),
            (1, vec![20]),
            (2, vec![]),
            (1, vec![10, 20]),
            (1, vec![10, 10, 20]),
            (1, vec![20]),
        ]
    );
}
