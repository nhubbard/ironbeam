use ironbeam_core::{Pipeline, WindowTrigger};

fn triggered_fingerprint(count: u64) -> String {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .early_count_trigger_named("count-trigger", count)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("groups").unwrap().time.trigger,
        WindowTrigger::EarlyElementCount { count }
    );
    graph.fingerprint().unwrap()
}

#[test]
fn early_count_is_validated_and_changes_graph_identity() {
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("source", vec![1_i32]).unwrap();
    assert!(values.early_count_trigger_named("global", 1).is_ok());
    let windowed = values.fixed_windows("window", 10_000_000).unwrap();
    assert!(windowed.early_count_trigger_named("zero", 0).is_err());
    assert!(
        windowed
            .early_count_trigger_named("overflow", i32::MAX as u64 + 1)
            .is_err()
    );
    assert_ne!(triggered_fingerprint(1), triggered_fingerprint(2));
}

#[test]
fn count_or_processing_time_batch_policy_validates_and_identifies_duration() {
    let make = |delay_ns| {
        let pipeline = Pipeline::default();
        pipeline
            .test_stream_source(
                "source",
                vec![ironbeam_core::TestStreamEvent::Elements(vec![(
                    (1_i32, 2_i32),
                    1_000_000,
                )])],
            )
            .unwrap()
            .group_into_batches_with_max_buffering_duration_named("batches", 2, delay_ns)
            .unwrap();
        let (graph, _) = pipeline.snapshot().unwrap();
        assert_eq!(
            graph.node("batches/group").unwrap().time.trigger,
            WindowTrigger::EarlyCountOrProcessingTime { count: 2, delay_ns }
        );
        graph.fingerprint().unwrap()
    };
    assert_ne!(make(5_000_000), make(6_000_000));
    let accumulating = Pipeline::default();
    accumulating
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .accumulating_count_or_processing_time_trigger_named("trigger", 2, 5_000_000)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (graph, _) = accumulating.snapshot().unwrap();
    assert_eq!(
        graph.node("group").unwrap().time.trigger,
        WindowTrigger::EarlyCountOrProcessingTimeAccumulating {
            count: 2,
            delay_ns: 5_000_000,
        }
    );
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    for delay_ns in [0, -1_000_000, 1] {
        assert!(
            source
                .group_into_batches_with_max_buffering_duration_named("invalid", 2, delay_ns)
                .is_err()
        );
    }
    assert!(
        source
            .group_into_batches_with_max_buffering_duration_named("empty", 0, 5_000_000)
            .is_err()
    );
    assert!(
        source
            .group_into_batches_with_max_buffering_duration_named(
                "overflow",
                i32::MAX as usize + 1,
                5_000_000,
            )
            .is_err()
    );
    assert!(
        source
            .accumulating_count_or_processing_time_trigger_named("zero-count", 0, 5_000_000)
            .is_err()
    );
    assert!(
        source
            .accumulating_count_or_processing_time_trigger_named("bad-delay", 2, 1)
            .is_err()
    );
}

#[test]
fn processing_time_trigger_validates_delay_and_changes_graph_identity() {
    let make = |delay_ns, accumulating| {
        let pipeline = Pipeline::default();
        let source = pipeline
            .test_stream_source(
                "source",
                vec![ironbeam_core::TestStreamEvent::Elements(vec![(
                    (1_i32, 2_i32),
                    1_000_000,
                )])],
            )
            .unwrap()
            .fixed_windows("window", 10_000_000)
            .unwrap();
        let triggered = if accumulating {
            source.accumulating_processing_time_trigger_named("trigger", delay_ns)
        } else {
            source.early_processing_time_trigger_named("trigger", delay_ns)
        }
        .unwrap();
        triggered.group_by_key_named("group").unwrap();
        let (graph, _) = pipeline.snapshot().unwrap();
        (
            graph.fingerprint().unwrap(),
            graph.node("group").unwrap().time.trigger,
        )
    };
    assert_eq!(
        make(5_000_000, false).1,
        WindowTrigger::EarlyProcessingTime {
            delay_ns: 5_000_000
        }
    );
    assert_eq!(
        make(5_000_000, true).1,
        WindowTrigger::EarlyProcessingTimeAccumulating {
            delay_ns: 5_000_000
        }
    );
    assert_ne!(make(5_000_000, false).0, make(6_000_000, false).0);
    assert_ne!(make(5_000_000, false).0, make(5_000_000, true).0);
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("values", vec![1_i32]).unwrap();
    assert!(
        source
            .early_processing_time_trigger_named("zero", 0)
            .is_err()
    );
    assert!(
        source
            .early_processing_time_trigger_named("fraction", 1)
            .is_err()
    );
    assert!(
        source
            .early_processing_time_trigger_named("negative", -1_000_000)
            .is_err()
    );
    let global = pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32)])
        .unwrap()
        .early_processing_time_trigger_named("global-timer", 5_000_000)
        .unwrap();
    global.group_by_key_named("global-group").unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("global-group").unwrap().time.trigger,
        WindowTrigger::EarlyProcessingTime {
            delay_ns: 5_000_000
        }
    );
    let sessions = source.session_windows_named("sessions", 5_000_000).unwrap();
    assert!(
        sessions
            .early_processing_time_trigger_named("session-timer", 5_000_000)
            .is_err()
    );
    let offset = pipeline
        .from_vec("offset-pairs", vec![(1_i32, 3_i32)])
        .unwrap()
        .fixed_windows_with_offset("offset", 10_000_000, 2_000_000)
        .unwrap()
        .allowed_lateness_named("offset-lateness", 1_000_000)
        .unwrap();
    let offset = offset
        .early_processing_time_trigger_named("offset-timer", 5_000_000)
        .unwrap();
    offset.group_by_key_named("offset-group").unwrap();
    pipeline.snapshot().unwrap();
}

#[test]
fn unbounded_global_group_requires_a_finite_count_trigger() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .test_stream_source(
            "values",
            vec![ironbeam_core::TestStreamEvent::Elements(vec![(
                ("a".to_owned(), 2_i32),
                0,
            )])],
        )
        .unwrap();
    assert!(values.group_by_key_named("untriggered").is_err());
    let grouped = values
        .early_count_trigger_named("trigger", 2)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node(grouped.key()).unwrap().time.window,
        ironbeam_core::Window::Global
    );
    assert_eq!(
        graph.node(grouped.key()).unwrap().time.trigger,
        WindowTrigger::EarlyElementCount { count: 2 }
    );
}

#[test]
fn default_trigger_keeps_legacy_time_json_shape() {
    let time = ironbeam_core::Time::default();
    let json = serde_json::to_string(&time).unwrap();
    assert!(!json.contains("trigger"));
    assert_eq!(
        serde_json::from_str::<ironbeam_core::Time>(&json).unwrap(),
        time
    );
}

#[test]
fn accumulating_count_has_distinct_graph_identity() {
    let pipeline = Pipeline::default();
    let grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .accumulating_count_trigger_named("count-trigger", 1)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node(grouped.key()).unwrap().time.trigger,
        WindowTrigger::EarlyElementCountAccumulating { count: 1 }
    );
    assert_ne!(graph.fingerprint().unwrap(), triggered_fingerprint(1));
}

#[test]
fn late_count_requires_lateness_and_changes_graph_identity() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap();
    let windowed = values.fixed_windows("windows", 10_000_000).unwrap();
    assert!(windowed.late_count_trigger_named("no-lateness", 2).is_err());
    let late = windowed
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap();
    assert!(late.late_count_trigger_named("zero", 0).is_err());
    assert!(
        late.late_count_trigger_named("overflow", i32::MAX as u64 + 1)
            .is_err()
    );
    let triggered = late.late_count_trigger_named("late-count", 2).unwrap();
    triggered
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("groups").unwrap().time.trigger,
        WindowTrigger::LateElementCount { count: 2 }
    );
    let triggered_fingerprint = graph.fingerprint().unwrap();

    let baseline = Pipeline::default();
    baseline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    assert_ne!(
        triggered_fingerprint,
        baseline.snapshot().unwrap().0.fingerprint().unwrap()
    );
}

#[test]
fn accumulating_late_count_has_distinct_graph_identity() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .accumulating_late_count_trigger_named("late-count", 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("groups").unwrap().time.trigger,
        WindowTrigger::LateElementCountAccumulating { count: 2 }
    );

    let discarding = Pipeline::default();
    discarding
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .late_count_trigger_named("late-count", 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    assert_ne!(
        graph.fingerprint().unwrap(),
        discarding.snapshot().unwrap().0.fingerprint().unwrap()
    );
}

#[test]
fn combined_count_trigger_requires_lateness_and_records_both_thresholds() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap();
    let windowed = values.fixed_windows("windows", 10_000_000).unwrap();
    assert!(
        windowed
            .early_and_late_count_trigger_named("no-lateness", 1, 2)
            .is_err()
    );
    let late = windowed
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap();
    assert!(
        late.early_and_late_count_trigger_named("zero-early", 0, 2)
            .is_err()
    );
    assert!(
        late.early_and_late_count_trigger_named("zero-late", 1, 0)
            .is_err()
    );
    let triggered = late
        .early_and_late_count_trigger_named("combined", 1, 2)
        .unwrap();
    triggered
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("groups").unwrap().time.trigger,
        WindowTrigger::EarlyAndLateElementCount {
            early_count: 1,
            late_count: 2
        }
    );
}

#[test]
fn accumulating_combined_count_has_distinct_identity_and_validates_thresholds() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap();
    let windowed = values.fixed_windows("windows", 10_000_000).unwrap();
    assert!(
        windowed
            .accumulating_early_and_late_count_trigger_named("no-lateness", 1, 2)
            .is_err()
    );
    let late = windowed
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap();
    assert!(
        late.accumulating_early_and_late_count_trigger_named("zero-early", 0, 2)
            .is_err()
    );
    assert!(
        late.accumulating_early_and_late_count_trigger_named("zero-late", 1, 0)
            .is_err()
    );
    late.accumulating_early_and_late_count_trigger_named("combined", 1, 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let trigger = graph.node("groups").unwrap().time.trigger;
    assert_eq!(
        trigger,
        WindowTrigger::EarlyAndLateElementCountAccumulating {
            early_count: 1,
            late_count: 2
        }
    );
    assert_eq!(trigger.early_count(), Some(1));
    assert_eq!(trigger.late_count(), Some(2));
    assert!(trigger.is_accumulating());

    let discarding = Pipeline::default();
    discarding
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .early_and_late_count_trigger_named("combined", 1, 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    assert_ne!(
        graph.fingerprint().unwrap(),
        discarding.snapshot().unwrap().0.fingerprint().unwrap()
    );
}
