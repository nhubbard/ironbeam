use ironbeam_core::{Element, Envelope, PaneInfo, Pipeline, Version, Window};

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
    let invalid_window = serde_json::json!({"version": 1, "entries": [[{"start_ns": 0, "end_ns": 10}, 1_i32.encode().unwrap(), 2_i32.encode().unwrap()]]});
    assert!(
        registry
            .restore_stateful_value("count", &serde_json::to_vec(&invalid_window).unwrap())
            .is_err()
    );
    assert_eq!(apply(&registry, 1), 1);
}
