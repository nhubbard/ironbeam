use ironbeam_core::Pipeline;
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
