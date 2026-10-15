use ironbeam_core::{
    Element, Envelope, ErrorKind, PaneInfo, Pipeline, TestStreamEvent, Version, Window,
    WindowInterval,
};

struct UncodedState;

impl Element for UncodedState {
    const TYPE_ID: &'static str = "tests/uncoded-state";
    const CODER_ID: &'static str = "tests/uncoded-state";
}

#[test]
fn value_state_registration_rejects_unsupported_window_and_coder_contracts() {
    let pipeline = Pipeline::default();
    let stream = pipeline
        .test_stream_source(
            "stream",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap();
    let session = stream
        .session_windows_named("sessions", 10_000_000)
        .unwrap();
    assert!(
        session
            .stateful_value_map_named::<i32, i32>(
                "session-state",
                "tests/session-value-state",
                Version { major: 1, minor: 0 },
                |_, value, _| Ok((Some(value), value)),
            )
            .is_err()
    );
    assert!(
        stream
            .stateful_value_map_named::<UncodedState, i32>(
                "uncoded-state",
                "tests/uncoded-value-state",
                Version { major: 1, minor: 0 },
                |_, value, _| Ok((None, value)),
            )
            .is_err()
    );
}

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "count",
            "tests/checkpointed-value-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), count))
            },
        )
        .unwrap();
    pipeline
}

fn apply(registry: &ironbeam_core::Registry, key: i32) -> i32 {
    let output = registry
        .apply_many(
            "count",
            Envelope {
                value: Box::new((key, 1_i32)),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None,
                pane: PaneInfo::NO_FIRING,
            },
        )
        .unwrap();
    let [row] = output.try_into().ok().unwrap();
    row.value.downcast::<(i32, i32)>().unwrap().1
}

#[test]
fn keyed_cells_survive_coded_snapshot_and_restore() {
    let pipeline = pipeline();
    let (_, first) = pipeline.snapshot().unwrap();
    assert_eq!(apply(&first, 1), 1);
    assert_eq!(apply(&first, 1), 2);
    assert_eq!(apply(&first, 2), 1);
    let before_expiration = first.checkpoint_stateful_value("count").unwrap();
    first.expire_stateful_value("count", Some(100)).unwrap();
    assert_eq!(
        first.checkpoint_stateful_value("count").unwrap(),
        before_expiration
    );
    let bytes = first.checkpoint_stateful_value("count").unwrap();
    let (_, restored) = pipeline.snapshot().unwrap();
    restored.restore_stateful_value("count", &bytes).unwrap();
    assert_eq!(apply(&restored, 1), 3);
    assert_eq!(apply(&restored, 2), 2);
    assert_eq!(apply(&restored, 3), 1);
}

#[test]
fn malformed_value_state_snapshots_fail_before_mutation() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(
        registry
            .restore_stateful_value("count", b"not JSON")
            .is_err()
    );
    assert!(
        registry
            .restore_stateful_value("count", br#"{"version":2,"entries":[]}"#)
            .is_err()
    );
    let key = 1_i32.encode().unwrap();
    let value = 2_i32.encode().unwrap();
    let duplicate = serde_json::json!({
        "version": 1,
        "entries": [[null, key, value], [null, 1_i32.encode().unwrap(), 2_i32.encode().unwrap()]],
    });
    assert!(
        registry
            .restore_stateful_value("count", &serde_json::to_vec(&duplicate).unwrap())
            .is_err()
    );
    let invalid_key =
        serde_json::json!({"version": 1, "entries": [[null, [1], 2_i32.encode().unwrap()]]});
    assert!(
        registry
            .restore_stateful_value("count", &serde_json::to_vec(&invalid_key).unwrap())
            .is_err()
    );
    let invalid_state =
        serde_json::json!({"version": 1, "entries": [[null, 1_i32.encode().unwrap(), [1]]]});
    assert!(
        registry
            .restore_stateful_value("count", &serde_json::to_vec(&invalid_state).unwrap())
            .is_err()
    );
    let invalid_window = serde_json::json!({"version": 1, "entries": [[{"start_ns": 0, "end_ns": 10}, 1_i32.encode().unwrap(), 2_i32.encode().unwrap()]]});
    assert!(
        registry
            .restore_stateful_value("count", &serde_json::to_vec(&invalid_window).unwrap())
            .is_err()
    );
    assert_eq!(apply(&registry, 1), 1);
    assert!(registry.checkpoint_stateful_value("missing").is_err());
    assert!(registry.restore_stateful_value("missing", b"{}").is_err());
    assert!(registry.expire_stateful_value("missing", Some(10)).is_err());
    let wrong_type = registry
        .apply_many(
            "count",
            Envelope {
                value: Box::new(1_i32),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None,
                pane: PaneInfo::NO_FIRING,
            },
        )
        .err()
        .unwrap();
    assert_eq!(wrong_type.kind, ErrorKind::Incompatible);
}

#[test]
fn clearing_a_value_cell_restarts_its_count() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "count",
            "tests/clearing-value-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((if count == 2 { None } else { Some(count) }, count))
            },
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(apply(&registry, 1), 1);
    assert_eq!(apply(&registry, 1), 2);
    assert_eq!(apply(&registry, 1), 1);
}

#[test]
fn runner_owned_value_cell_callback_checks_types_and_coder_bytes() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    let input = |value: Box<dyn std::any::Any + Send>| Envelope {
        value,
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
    };
    assert!(
        registry
            .apply_stateful_value("missing", input(Box::new((1_i32, 1_i32))), None)
            .is_err()
    );
    assert!(
        registry
            .apply_stateful_value("count", input(Box::new(1_i32)), None)
            .is_err()
    );
    assert!(
        registry
            .apply_stateful_value("count", input(Box::new((1_i32, 1_i32))), Some(&[1]))
            .is_err()
    );
    let (next, output) = registry
        .apply_stateful_value("count", input(Box::new((1_i32, 1_i32))), None)
        .unwrap();
    assert_eq!(i32::decode(&next.unwrap()).unwrap(), 1);
    assert_eq!(*output.value.downcast::<(i32, i32)>().unwrap(), (1, 1));
}

#[test]
fn windowed_cells_expire_at_the_watermark_and_restore_independently() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .fixed_windows("window", 10)
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "count",
            "tests/windowed-checkpoint-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), count))
            },
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(
        registry
            .apply_many(
                "count",
                Envelope {
                    value: Box::new((1_i32, 1_i32)),
                    timestamp_ns: 0,
                    window: Window::Fixed { size_ns: 10 },
                    assigned_window: None,
                    pane: PaneInfo::NO_FIRING,
                }
            )
            .is_err()
    );
    let null_window = serde_json::json!({"version": 1, "entries": [[null, 1_i32.encode().unwrap(), 1_i32.encode().unwrap()]]});
    assert!(
        registry
            .restore_stateful_value("count", &serde_json::to_vec(&null_window).unwrap())
            .is_err()
    );
    let empty_window = serde_json::json!({"version": 1, "entries": [[{"start_ns": 10, "end_ns": 10}, 1_i32.encode().unwrap(), 1_i32.encode().unwrap()]]});
    assert!(
        registry
            .restore_stateful_value("count", &serde_json::to_vec(&empty_window).unwrap())
            .is_err()
    );
    let apply_window = |registry: &ironbeam_core::Registry, start_ns| {
        let output = registry
            .apply_many(
                "count",
                Envelope {
                    value: Box::new((1_i32, 1_i32)),
                    timestamp_ns: start_ns,
                    window: Window::Fixed { size_ns: 10 },
                    assigned_window: Some(WindowInterval {
                        start_ns,
                        end_ns: start_ns + 10,
                    }),
                    pane: PaneInfo::NO_FIRING,
                },
            )
            .unwrap();
        let [row] = output.try_into().ok().unwrap();
        row.value.downcast::<(i32, i32)>().unwrap().1
    };
    assert_eq!(apply_window(&registry, 0), 1);
    assert_eq!(apply_window(&registry, 20), 1);
    registry.expire_stateful_value("count", None).unwrap();
    registry.expire_stateful_value("count", Some(10)).unwrap();
    let snapshot = registry.checkpoint_stateful_value("count").unwrap();
    let parsed: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
    assert_eq!(parsed["entries"].as_array().unwrap().len(), 1);
    let (_, restored) = pipeline.snapshot().unwrap();
    restored.restore_stateful_value("count", &snapshot).unwrap();
    assert_eq!(apply_window(&restored, 20), 2);
    assert_eq!(apply_window(&restored, 0), 1);
}
