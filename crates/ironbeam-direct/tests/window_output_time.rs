use ironbeam_core::{Pipeline, WindowOutputTime, cogroup_by_key_2_named};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn global_group_uses_beam_end_or_latest_output_timestamp() {
    for (policy, expected) in [
        (WindowOutputTime::EndOfWindow, i64::MAX),
        (WindowOutputTime::LatestInPane, 7_000_000),
    ] {
        let pipeline = Pipeline::default();
        let keyed = pipeline
            .from_vec(
                "events",
                vec![(7_000_000_i64, (1_i32, 7_i32)), (2_000_000, (1, 2))],
            )
            .unwrap()
            .timestamps_from_pairs_named("timestamps")
            .unwrap();
        let keyed = match policy {
            WindowOutputTime::EndOfWindow => keyed.end_of_window_output_time_named("policy"),
            WindowOutputTime::LatestInPane => keyed.latest_in_pane_output_time_named("policy"),
            WindowOutputTime::EarliestInPane => unreachable!(),
        }
        .unwrap();
        let output = keyed
            .group_by_key_named("group")
            .unwrap()
            .reify_timestamps_named("group-time")
            .unwrap()
            .output("result")
            .unwrap();
        let rows = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, expected);
        assert_eq!(rows[0].1.0, 1);
        assert_eq!(rows[0].1.1.len(), 2);
    }
}

#[test]
fn bounded_session_join_uses_merged_output_time_policy() {
    for (policy, expected_time) in [
        (WindowOutputTime::EndOfWindow, 12_000_000),
        (WindowOutputTime::LatestInPane, 8_000_000),
    ] {
        let pipeline = Pipeline::default();
        let left = pipeline
            .from_vec(
                "left",
                vec![
                    (0_i64, ("a".to_owned(), 1_i32)),
                    (8_000_000, ("a".to_owned(), 2)),
                ],
            )
            .unwrap()
            .timestamps_from_pairs_named("left-time")
            .unwrap()
            .session_windows_named("left-window", 5_000_000)
            .unwrap();
        let right = pipeline
            .from_vec("right", vec![(4_000_000_i64, ("a".to_owned(), 10_i32))])
            .unwrap()
            .timestamps_from_pairs_named("right-time")
            .unwrap()
            .session_windows_named("right-window", 5_000_000)
            .unwrap();
        let apply = |values: ironbeam_core::PCollection<(String, i32)>, key| match policy {
            WindowOutputTime::EndOfWindow => values.end_of_window_output_time_named(key),
            WindowOutputTime::LatestInPane => values.latest_in_pane_output_time_named(key),
            WindowOutputTime::EarliestInPane => unreachable!(),
        };
        let left = apply(left, "left-policy").unwrap();
        let right = apply(right, "right-policy").unwrap();
        let output = left
            .join_inner_named("join", &right)
            .unwrap()
            .reify_timestamps_named("timestamp")
            .unwrap()
            .with_window_named("bounds")
            .unwrap()
            .output("result")
            .unwrap();
        let mut rows = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0;
        rows.sort_by_key(|(_, (_, (_, (left, _))))| *left);
        assert_eq!(rows.len(), 2);
        for (window, (time, (key, (left, right)))) in rows {
            assert_eq!((window.start_ns, window.end_ns), (0, 13_000_000));
            assert_eq!((time, key, right), (expected_time, "a".to_owned(), 10));
            assert!((1..=2).contains(&left));
        }
    }
}

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

#[test]
fn latest_in_pane_uses_maximum_event_time_independent_of_arrival_order() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "values",
            vec![
                (7_000_000_i64, ("a".to_owned(), 7_i32)),
                (2_000_000, ("a".to_owned(), 2)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("timestamps")
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .latest_in_pane_output_time_named("output-policy")
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
    assert_eq!(rows, vec![(7_000_000, ("a".to_owned(), vec![7, 2]))]);
}

#[test]
fn latest_in_pane_is_retained_by_accumulating_early_and_on_time_panes() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "values",
            vec![
                (7_000_000_i64, ("a".to_owned(), 7_i32)),
                (2_000_000, ("a".to_owned(), 2)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("timestamps")
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .latest_in_pane_output_time_named("output-policy")
        .unwrap()
        .accumulating_count_trigger_named("trigger", 1)
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
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row.0 == 7_000_000));
}

#[test]
fn heterogeneous_cogroup_uses_the_declared_output_timestamp() {
    for (policy, expected) in [
        (WindowOutputTime::EndOfWindow, 9_000_000),
        (WindowOutputTime::LatestInPane, 7_000_000),
    ] {
        let pipeline = Pipeline::default();
        let left = pipeline
            .from_vec("left", vec![(2_000_000_i64, (1_i32, 2_i32))])
            .unwrap()
            .timestamps_from_pairs_named("left-time")
            .unwrap()
            .fixed_windows("left-window", 10_000_000)
            .unwrap();
        let right = pipeline
            .from_vec("right", vec![(7_000_000_i64, (1_i32, "right".to_owned()))])
            .unwrap()
            .timestamps_from_pairs_named("right-time")
            .unwrap()
            .fixed_windows("right-window", 10_000_000)
            .unwrap();
        let (left, right) = match policy {
            WindowOutputTime::EndOfWindow => (
                left.end_of_window_output_time_named("left-policy").unwrap(),
                right
                    .end_of_window_output_time_named("right-policy")
                    .unwrap(),
            ),
            WindowOutputTime::LatestInPane => (
                left.latest_in_pane_output_time_named("left-policy")
                    .unwrap(),
                right
                    .latest_in_pane_output_time_named("right-policy")
                    .unwrap(),
            ),
            WindowOutputTime::EarliestInPane => unreachable!(),
        };
        let output = cogroup_by_key_2_named("group", &left, &right)
            .unwrap()
            .reify_timestamps_named("reify")
            .unwrap()
            .output("result")
            .unwrap();
        let (rows, _) = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap();
        assert_eq!(
            rows,
            vec![(expected, (1, (vec![2], vec!["right".to_owned()])))]
        );
    }
}

#[test]
fn join_output_time_uses_the_whole_key_window() {
    for (policy, expected) in [
        (WindowOutputTime::EndOfWindow, 9_000_000),
        (WindowOutputTime::LatestInPane, 7_000_000),
    ] {
        let pipeline = Pipeline::default();
        let left = pipeline
            .from_vec(
                "left",
                vec![(2_000_000_i64, (1_i32, 2_i32)), (4_000_000, (1, 4))],
            )
            .unwrap()
            .timestamps_from_pairs_named("left-time")
            .unwrap()
            .fixed_windows("left-window", 10_000_000)
            .unwrap();
        let right = pipeline
            .from_vec("right", vec![(7_000_000_i64, (1_i32, 7_i32))])
            .unwrap()
            .timestamps_from_pairs_named("right-time")
            .unwrap()
            .fixed_windows("right-window", 10_000_000)
            .unwrap();
        let (left, right) = match policy {
            WindowOutputTime::EndOfWindow => (
                left.end_of_window_output_time_named("left-policy").unwrap(),
                right
                    .end_of_window_output_time_named("right-policy")
                    .unwrap(),
            ),
            WindowOutputTime::LatestInPane => (
                left.latest_in_pane_output_time_named("left-policy")
                    .unwrap(),
                right
                    .latest_in_pane_output_time_named("right-policy")
                    .unwrap(),
            ),
            WindowOutputTime::EarliestInPane => unreachable!(),
        };
        let output = left
            .join_inner_named("join", &right)
            .unwrap()
            .reify_timestamps_named("reify")
            .unwrap()
            .output("result")
            .unwrap();
        let (mut rows, _) = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap();
        rows.sort_unstable();
        assert_eq!(rows, vec![(expected, (1, (2, 7))), (expected, (1, (4, 7)))]);
    }
}

#[test]
fn global_and_keyed_combines_follow_window_output_time() {
    use ironbeam_core::CombineFn;
    struct Sum;
    impl CombineFn<i32, i32, i32> for Sum {
        fn create(&self) -> i32 {
            0
        }
        fn add_input(&self, total: &mut i32, value: i32) {
            *total += value;
        }
        fn merge(&self, total: &mut i32, other: i32) {
            *total += other;
        }
        fn finish(&self, total: i32) -> i32 {
            total
        }
    }
    for (policy, expected) in [
        (WindowOutputTime::EarliestInPane, 2_000_000),
        (WindowOutputTime::EndOfWindow, 9_000_000),
        (WindowOutputTime::LatestInPane, 7_000_000),
    ] {
        let pipeline = Pipeline::default();
        let keyed = pipeline
            .from_vec(
                "values",
                vec![(7_000_000_i64, (1_i32, 7_i32)), (2_000_000, (1, 2))],
            )
            .unwrap()
            .timestamps_from_pairs_named("timestamps")
            .unwrap()
            .fixed_windows("windows", 10_000_000)
            .unwrap();
        let keyed = match policy {
            WindowOutputTime::EarliestInPane => keyed,
            WindowOutputTime::EndOfWindow => {
                keyed.end_of_window_output_time_named("policy").unwrap()
            }
            WindowOutputTime::LatestInPane => {
                keyed.latest_in_pane_output_time_named("policy").unwrap()
            }
        };
        let values = keyed.map_named("strip-key", |(_, value)| value).unwrap();
        let vector = values
            .combine_globally_named("vector", |rows| rows.into_iter().sum::<i32>())
            .unwrap()
            .reify_timestamps_named("vector-time")
            .unwrap()
            .output("vector-out")
            .unwrap();
        let lifted = values
            .combine_globally_lifted_named("lifted", Sum)
            .unwrap()
            .reify_timestamps_named("lifted-time")
            .unwrap()
            .output("lifted-out")
            .unwrap();
        let keyed = keyed
            .combine_values_lifted_named("keyed", Sum)
            .unwrap()
            .reify_timestamps_named("keyed-time")
            .unwrap()
            .output("keyed-out")
            .unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&vector, &JobConfig::default())
                .unwrap()
                .0,
            vec![(expected, 9)]
        );
        assert_eq!(
            DirectRunner
                .run_collect(&lifted, &JobConfig::default())
                .unwrap()
                .0,
            vec![(expected, 9)]
        );
        assert_eq!(
            DirectRunner
                .run_collect(&keyed, &JobConfig::default())
                .unwrap()
                .0,
            vec![(expected, (1, 9))]
        );
    }
}
