use ironbeam_core::{CombineFn, Element, Envelope, ErrorKind, Pipeline, Window, WindowInterval};

#[test]
fn session_group_checkpoint_merges_bridge_after_restore() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .group_by_session_named("session-group", 5)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let window = Window::Session { gap_ns: 5 };
    let mut fold = registry.open_fold("session-group").unwrap();
    for (key, value, time) in [(1, 10, 0), (1, 20, 8), (2, 50, 4)] {
        fold.add(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((key, value)),
            timestamp_ns: time,
            window: window.clone(),
            assigned_window: window.assign(time).unwrap(),
        })
        .unwrap();
    }
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut resumed = registry.open_fold("session-group").unwrap();
    resumed.restore_checkpoint_state(&saved).unwrap();
    for (value, time) in [(30, 4), (40, 13)] {
        resumed
            .add(Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: Box::new((1_i32, value)),
                timestamp_ns: time,
                window: window.clone(),
                assigned_window: window.assign(time).unwrap(),
            })
            .unwrap();
    }
    let mut rows = resumed
        .finish()
        .unwrap()
        .into_iter()
        .map(|event| {
            let (key, mut values) = *event.value.downcast::<(i32, Vec<i32>)>().unwrap();
            values.sort_unstable();
            (
                key,
                values,
                event.timestamp_ns,
                event.assigned_window.unwrap(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        rows,
        [
            (
                1,
                vec![10, 20, 30, 40],
                0,
                WindowInterval {
                    start_ns: 0,
                    end_ns: 18
                }
            ),
            (
                2,
                vec![50],
                4,
                WindowInterval {
                    start_ns: 4,
                    end_ns: 9
                }
            ),
        ]
    );
}

#[test]
fn grouped_fold_restores_ordered_values_and_window_metadata() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    let window = Window::Fixed { size_ns: 10 };
    let first = WindowInterval {
        start_ns: 0,
        end_ns: 10,
    };
    let second = WindowInterval {
        start_ns: 10,
        end_ns: 20,
    };
    for (key, value, timestamp_ns, interval) in
        [(1, 10, 2, first), (2, 20, 3, first), (1, 30, 12, second)]
    {
        fold.add(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((key, value)),
            timestamp_ns,
            window: window.clone(),
            assigned_window: Some(interval),
        })
        .unwrap();
    }
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut resumed = registry.open_fold("group").unwrap();
    resumed.restore_checkpoint_state(&saved).unwrap();
    assert_eq!(resumed.checkpoint_state().unwrap().unwrap(), saved);
    resumed
        .add(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((1_i32, 40_i32)),
            timestamp_ns: 4,
            window: window.clone(),
            assigned_window: Some(first),
        })
        .unwrap();
    let events = resumed.finish().unwrap();
    let rows: Vec<_> = events
        .into_iter()
        .map(|event| {
            (
                *event.value.downcast::<(i32, Vec<i32>)>().unwrap(),
                event.timestamp_ns,
                event.window,
                event.assigned_window,
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            ((1, vec![10, 40]), 2, window.clone(), Some(first)),
            ((2, vec![20]), 3, window.clone(), Some(first)),
            ((1, vec![30]), 12, window, Some(second)),
        ]
    );
}

#[test]
fn grouped_fold_rejects_corrupt_duplicate_and_incompatible_state() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    fold.add_vec_batch(Box::new(vec![(1_i32, 10_i32)])).unwrap();
    let saved = fold.checkpoint_state().unwrap().unwrap();
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(b"broken")
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
    let mut version: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    // Version 1 used first-arrival group time; it must not be restored under
    // the earliest-input timestamp contract introduced in version 2.
    version["version"]["major"] = 1.into();
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&version).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
    let mut coder: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    coder["value_coder"] = "wrong-coder".into();
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&coder).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
    let mut duplicate: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    let row = duplicate["groups"][0].clone();
    duplicate["groups"].as_array_mut().unwrap().push(row);
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&duplicate).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
    let mut empty: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    empty["groups"][0]["values"] =
        serde_json::to_value(Vec::<i32>::new().encode().unwrap()).unwrap();
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&empty).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
    assert_eq!(
        fold.restore_checkpoint_state(&saved).unwrap_err().kind,
        ErrorKind::Recovery
    );
    let mut bad_key: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    bad_key["groups"][0]["key"] = serde_json::json!([1]);
    assert!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&bad_key).unwrap())
            .is_err()
    );
    let mut bad_values: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    bad_values["groups"][0]["values"] = serde_json::json!([1]);
    assert!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&bad_values).unwrap())
            .is_err()
    );
}

#[test]
fn folds_without_a_state_coder_decline_in_progress_recovery() {
    #[derive(Clone)]
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "test/opaque-group-value";
        const CODER_ID: &'static str = "test/opaque-group-value";
    }
    struct Sum;
    impl CombineFn<i32, i32, i32> for Sum {
        fn create(&self) -> i32 {
            0
        }
        fn add_input(&self, accumulator: &mut i32, value: i32) {
            *accumulator += value;
        }
        fn merge(&self, accumulator: &mut i32, other: i32) {
            *accumulator += other;
        }
        fn finish(&self, accumulator: i32) -> i32 {
            accumulator
        }
    }
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, Opaque)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut group = registry.open_fold("group").unwrap();
    group
        .add_vec_batch(Box::new(vec![(1_i32, Opaque)]))
        .unwrap();
    assert!(group.checkpoint_state().unwrap().is_none());

    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .combine_globally_lifted_named("fold", Sum)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("fold").unwrap();
    assert!(fold.checkpoint_state().unwrap().is_none());
    assert!(matches!(
        fold.restore_checkpoint_state(b"state").unwrap_err().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
}
