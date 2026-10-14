use ironbeam_core::{Pipeline, cogroup_by_key_3_named};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};

#[test]
fn explicit_session_assignment_merges_when_grouped_later() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "events",
            vec![
                (0_i64, ("a".to_owned(), 1_i32)),
                (8_000_000, ("a".to_owned(), 2)),
                (4_000_000, ("a".to_owned(), 3)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("event-time")
        .unwrap()
        .session_windows_named("session-assignment", 5_000_000)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .with_window_named("bounds")
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!((rows[0].0.start_ns, rows[0].0.end_ns), (0, 13_000_000));
    assert_eq!(rows[0].1.0, "a");
    let mut values = rows[0].1.1.clone();
    values.sort_unstable();
    assert_eq!(values, [1, 2, 3]);
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the four join kinds on the same keyed session fixture.
fn bounded_join_merges_session_intervals_across_both_inputs() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec(
            "left-events",
            vec![
                (0_i64, ("a".to_owned(), 1_i32)),
                (8_000_000, ("a".to_owned(), 2)),
                (30_000_000, ("b".to_owned(), 3)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("left-time")
        .unwrap()
        .session_windows_named("left-sessions", 5_000_000)
        .unwrap();
    let right = pipeline
        .from_vec(
            "right-events",
            vec![
                (4_000_000_i64, ("a".to_owned(), 10_i32)),
                (20_000_000, ("a".to_owned(), 11)),
                (31_000_000, ("b".to_owned(), 12)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("right-time")
        .unwrap()
        .session_windows_named("right-sessions", 5_000_000)
        .unwrap();
    let output = left
        .join_inner_named("session-join", &right)
        .unwrap()
        .with_window_named("bounds")
        .unwrap()
        .map_named("rows", |(window, (key, (left, right)))| {
            format!(
                "{key}:{left}:{right}:{}..{}",
                window.start_ns, window.end_ns
            )
        })
        .unwrap()
        .output("result")
        .unwrap();
    let (mut rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "a:1:10:0..13000000",
            "a:2:10:0..13000000",
            "b:3:12:30000000..36000000",
        ]
    );

    let left_output = left
        .join_left_named("session-left-join", &right)
        .unwrap()
        .with_window_named("left-bounds")
        .unwrap()
        .map_named("left-rows", |(window, (key, (left, right)))| {
            format!(
                "{key}:{left}:{right:?}:{}..{}",
                window.start_ns, window.end_ns
            )
        })
        .unwrap()
        .output("left-result")
        .unwrap();
    let (mut left_rows, _) = DirectRunner
        .run_collect(&left_output, &JobConfig::default())
        .unwrap();
    left_rows.sort_unstable();
    assert_eq!(left_rows.len(), 3);
    assert!(left_rows.iter().all(|row| row.contains(":Some(")));

    let right_output = left
        .join_right_named("session-right-join", &right)
        .unwrap()
        .with_window_named("right-bounds")
        .unwrap()
        .map_named("right-rows", |(window, (key, (left, right)))| {
            format!(
                "{key}:{left:?}:{right}:{}..{}",
                window.start_ns, window.end_ns
            )
        })
        .unwrap()
        .output("right-result")
        .unwrap();
    let (mut right_rows, _) = DirectRunner
        .run_collect(&right_output, &JobConfig::default())
        .unwrap();
    right_rows.sort_unstable();
    assert_eq!(right_rows.len(), 4);
    assert!(right_rows.contains(&"a:None:11:20000000..25000000".to_owned()));

    let full_output = left
        .join_full_named("session-full-join", &right)
        .unwrap()
        .with_window_named("full-bounds")
        .unwrap()
        .map_named("full-rows", |(window, (key, (left, right)))| {
            format!(
                "{key}:{left:?}:{right:?}:{}..{}",
                window.start_ns, window.end_ns
            )
        })
        .unwrap()
        .output("full-result")
        .unwrap();
    let (mut full_rows, _) = DirectRunner
        .run_collect(&full_output, &JobConfig::default())
        .unwrap();
    full_rows.sort_unstable();
    assert_eq!(full_rows.len(), 4);
    assert!(full_rows.contains(&"a:None:Some(11):20000000..25000000".to_owned()));
}

#[test]
fn bounded_three_input_cogroup_merges_sessions_across_inputs() {
    let pipeline = Pipeline::default();
    let first = pipeline
        .from_vec(
            "first",
            vec![(0_i64, ("a".to_owned(), 1_i32)), (8, ("a".to_owned(), 2))],
        )
        .unwrap()
        .timestamps_from_pairs_named("first-time")
        .unwrap()
        .session_windows_named("first-session", 5)
        .unwrap();
    let second = pipeline
        .from_vec(
            "second",
            vec![
                (4_i64, ("a".to_owned(), "bridge".to_owned())),
                (20, ("a".to_owned(), "isolated".to_owned())),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("second-time")
        .unwrap()
        .session_windows_named("second-session", 5)
        .unwrap();
    let third = pipeline
        .from_vec(
            "third",
            vec![
                (12_i64, ("a".to_owned(), true)),
                (30, ("b".to_owned(), true)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("third-time")
        .unwrap()
        .session_windows_named("third-session", 5)
        .unwrap();
    let output = cogroup_by_key_3_named("session-cogroup", &first, &second, &third)
        .unwrap()
        .with_window_named("bounds")
        .unwrap()
        .map_named(
            "rows",
            |(window, (key, (mut first, mut second, mut third)))| {
                first.sort_unstable();
                second.sort_unstable();
                third.sort_unstable();
                format!(
                    "{key}:{first:?}:{second:?}:{third:?}:{}..{}",
                    window.start_ns, window.end_ns
                )
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (mut rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "a:[1, 2]:[\"bridge\"]:[true]:0..17",
            "a:[]:[\"isolated\"]:[]:20..25",
            "b:[]:[]:[true]:30..35",
        ]
    );
}

#[test]
fn keyed_sessions_merge_bridge_and_restore_completed_checkpoint() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "events",
            vec![
                (0_i64, ("a".to_owned(), 1_i32)),
                (8_000_000, ("a".to_owned(), 2)),
                (4_000_000, ("a".to_owned(), 3)),
                (13_000_000, ("a".to_owned(), 4)),
                (4_000_000, ("b".to_owned(), 5)),
                (-4_000_000, ("c".to_owned(), 6)),
                (-1_000_000, ("c".to_owned(), 7)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("event-time")
        .unwrap()
        .group_by_session_named("sessions", 5_000_000)
        .unwrap()
        .reify_timestamps_named("session-time")
        .unwrap()
        .with_window_named("session-bounds")
        .unwrap()
        .map_named("rows", |(window, (time, (key, mut values)))| {
            values.sort_unstable();
            format!(
                "{key}|{values:?}|{time}|{}|{}",
                window.start_ns, window.end_ns
            )
        })
        .unwrap()
        .output("result")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 2,
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            binary_build_id: "session-checkpoint-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let expected = vec![
        "a|[1, 2, 3, 4]|0|0|18000000".to_owned(),
        "b|[5]|4000000|4000000|9000000".to_owned(),
        "c|[6, 7]|-4000000|-4000000|4000000".to_owned(),
    ];
    for _ in 0..2 {
        let (mut rows, _) = DirectRunner.run_collect(&output, &config).unwrap();
        rows.sort_unstable();
        assert_eq!(rows, expected);
    }
}

#[test]
fn sliding_windows_assign_two_overlapping_intervals_across_zero() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "events",
            vec![
                (-1_000_000_i64, ("a".to_owned(), 1_i32)),
                (0, ("a".to_owned(), 2)),
                (1_000_000, ("a".to_owned(), 3)),
                (4_000_000, ("a".to_owned(), 4)),
                (7_000_000, ("a".to_owned(), 5)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("event-time")
        .unwrap()
        .sliding_windows("sliding", 4_000_000, 2_000_000, 0)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .reify_timestamps_named("group-time")
        .unwrap()
        .with_window_named("bounds")
        .unwrap()
        .map_named("rows", |(window, (time, (key, mut values)))| {
            values.sort_unstable();
            format!(
                "{key}|{values:?}|{time}|{}|{}",
                window.start_ns, window.end_ns
            )
        })
        .unwrap()
        .output("result")
        .unwrap();
    let (mut rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "a|[1, 2, 3]|-1000000|-2000000|2000000",
            "a|[1]|-1000000|-4000000|0",
            "a|[2, 3]|0|0|4000000",
            "a|[4, 5]|4000000|4000000|8000000",
            "a|[4]|4000000|2000000|6000000",
            "a|[5]|7000000|6000000|10000000",
        ]
    );
}
