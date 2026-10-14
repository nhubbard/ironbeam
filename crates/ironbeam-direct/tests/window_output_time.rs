use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn fixed_window_group_uses_end_of_window_output_timestamp() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "values",
            vec![
                (2_000_000_i64, ("a".to_owned(), 2_i32)),
                (7_000_000, ("a".to_owned(), 3)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("timestamps")
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .end_of_window_output_time_named("output-policy")
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_timestamps_named("reify")
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(rows, vec![(9_000_000, ("a".to_owned(), vec![2, 3]))]);
}

#[test]
fn sliding_window_groups_use_each_interval_end() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("values", vec![(6_000_000_i64, ("a".to_owned(), 2_i32))])
        .unwrap()
        .timestamps_from_pairs_named("timestamps")
        .unwrap()
        .sliding_windows("windows", 10_000_000, 5_000_000, 0)
        .unwrap()
        .end_of_window_output_time_named("output-policy")
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_timestamps_named("reify")
        .unwrap()
        .with_window_named("interval")
        .unwrap()
        .output("result")
        .unwrap();
    let (mut rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    rows.sort_by_key(|row| row.0.start_ns);
    assert_eq!(rows.len(), 2);
    assert_eq!((rows[0].0.start_ns, rows[0].1.0), (0, 9_000_000));
    assert_eq!((rows[1].0.start_ns, rows[1].1.0), (5_000_000, 14_000_000));
}
