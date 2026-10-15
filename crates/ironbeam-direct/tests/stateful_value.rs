use ironbeam_core::{Pipeline, Version};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn bounded_value_state_is_scoped_by_key_and_resets_between_runs() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32), (1, 3), (2, 4), (1, 5)])
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "running",
            "example/running-total",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let total = prior.unwrap_or(0) + value;
                Ok((Some(total), total))
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let expected = [(1, 2), (1, 5), (2, 4), (1, 10)];
    for _ in 0..2 {
        let (actual, _) = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap();
        assert_eq!(actual, expected);
    }
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_stateful_value("running"));
    assert!(matches!(
        graph.node("running").unwrap().operation,
        ironbeam_core::Operation::StatefulParDo { .. }
    ));
}

#[test]
fn bounded_value_state_can_clear_a_cell() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32), (1, 0), (1, 3)])
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "running",
            "example/clear-running-total",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                if value == 0 {
                    Ok((None, 0))
                } else {
                    let total = prior.unwrap_or(0) + value;
                    Ok((Some(total), total))
                }
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (actual, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(actual, [(1, 2), (1, 0), (1, 3)]);
}

#[test]
fn bounded_value_state_is_isolated_across_fixed_windows() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32), (1, 12), (1, 3)])
        .unwrap()
        .assign_timestamps_named("time", |(_, value)| i64::from(*value) * 1_000_000)
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "running",
            "example/windowed-running-total",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let total = prior.unwrap_or(0) + value;
                Ok((Some(total), total))
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (actual, _) = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                parallelism: 1,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(actual, [(1, 2), (1, 12), (1, 5)]);
}
