use ironbeam_core::{Pipeline, SetStateMutation, Version};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn set_state_deduplicates_and_mutates_through_direct() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "pairs",
            vec![
                (1_i32, 10_i32),
                (1, 10),
                (1, 20),
                (2, 10),
                (1, -1),
                (1, 10),
                (1, -2),
                (1, 10),
            ],
        )
        .unwrap()
        .stateful_set_map_named::<i32, (i32, i32)>(
            "set",
            "tests/direct-set-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    i32::from(prior.contains(&value)),
                    i32::try_from(prior.len()).unwrap(),
                );
                let mutation = match value {
                    -1 => SetStateMutation::Clear,
                    -2 => SetStateMutation::Replace(vec![10, 10, 30]),
                    value => SetStateMutation::Insert(value),
                };
                Ok((mutation, observation))
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
            (1, (0, 0)),
            (1, (1, 1)),
            (1, (0, 1)),
            (2, (0, 0)),
            (1, (0, 2)),
            (1, (0, 0)),
            (1, (0, 1)),
            (1, (1, 2)),
        ]
    );
}
