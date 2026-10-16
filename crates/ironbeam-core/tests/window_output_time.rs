use ironbeam_core::{Envelope, PaneInfo, Pipeline, Window, WindowInterval, WindowOutputTime};

#[test]
fn end_of_window_output_time_is_validated_and_changes_graph_identity() {
    let baseline = Pipeline::default();
    baseline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap();
    let baseline_id = baseline.snapshot().unwrap().0.fingerprint().unwrap();

    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap();
    assert!(
        values
            .end_of_window_output_time_named("global-policy")
            .is_err()
    );
    let windowed = values.fixed_windows("windows", 10_000_000).unwrap();
    let grouped = windowed
        .end_of_window_output_time_named("output-policy")
        .unwrap()
        .group_by_key_named("groups")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node(grouped.key()).unwrap().time.output_time,
        WindowOutputTime::EndOfWindow
    );
    assert_ne!(graph.fingerprint().unwrap(), baseline_id);

    let json = serde_json::to_string(&ironbeam_core::Time::default()).unwrap();
    assert!(!json.contains("output_time"));
}

#[test]
fn end_of_window_policy_requires_millisecond_aligned_windows() {
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("values", vec![1_i32]).unwrap();
    let windowed = values.fixed_windows("window", 1_000_001).unwrap();
    assert!(
        windowed
            .end_of_window_output_time_named("invalid-policy")
            .is_err()
    );
}

#[test]
fn grouped_output_time_survives_fold_checkpoint_restore() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![(1_i32, 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .end_of_window_output_time_named("output-policy")
        .unwrap()
        .group_by_key_named("groups")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("groups").unwrap();
    fold.add(Envelope {
        pane: PaneInfo::NO_FIRING,
        value: Box::new((1_i32, 2_i32)),
        timestamp_ns: 2_000_000,
        window: Window::Fixed {
            size_ns: 10_000_000,
        },
        assigned_window: Some(WindowInterval {
            start_ns: 0,
            end_ns: 10_000_000,
        }),
    })
    .unwrap();
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut restored = registry.open_fold("groups").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    let (output, released) = restored.drain_closed(10_000_000).unwrap();
    assert_eq!(released, 1);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].timestamp_ns, 9_000_000);
}

#[test]
fn latest_in_pane_policy_tracks_maximum_event_time_across_checkpoint_restore() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![(1_i32, 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .latest_in_pane_output_time_named("output-policy")
        .unwrap()
        .group_by_key_named("groups")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("groups").unwrap();
    for timestamp_ns in [7_000_000, 2_000_000] {
        fold.add(Envelope {
            pane: PaneInfo::NO_FIRING,
            value: Box::new((1_i32, 2_i32)),
            timestamp_ns,
            window: Window::Fixed {
                size_ns: 10_000_000,
            },
            assigned_window: Some(WindowInterval {
                start_ns: 0,
                end_ns: 10_000_000,
            }),
        })
        .unwrap();
    }
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut corrupt: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    corrupt["groups"][0]
        .as_object_mut()
        .unwrap()
        .remove("latest_timestamp_ns");
    let mut rejected = registry.open_fold("groups").unwrap();
    assert!(
        rejected
            .restore_checkpoint_state(&serde_json::to_vec(&corrupt).unwrap())
            .is_err()
    );
    let mut restored = registry.open_fold("groups").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    let (output, released) = restored.drain_closed(10_000_000).unwrap();
    assert_eq!(released, 2);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].timestamp_ns, 7_000_000);
}

#[test]
fn merged_session_keeps_latest_output_time_across_checkpoint_restore() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![(1_i32, 2_i32)])
        .unwrap()
        .session_windows_named("sessions", 5_000_000)
        .unwrap()
        .latest_in_pane_output_time_named("output-policy")
        .unwrap()
        .group_by_key_named("groups")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("groups").unwrap();
    let add = |fold: &mut Box<dyn ironbeam_core::FoldSession>, time| {
        fold.add(Envelope {
            pane: PaneInfo::NO_FIRING,
            value: Box::new((1_i32, 2_i32)),
            timestamp_ns: time,
            window: Window::Session { gap_ns: 5_000_000 },
            assigned_window: Some(WindowInterval {
                start_ns: time,
                end_ns: time + 5_000_000,
            }),
        })
        .unwrap();
    };
    add(&mut fold, 0);
    add(&mut fold, 1_000_000);
    add(&mut fold, 8_000_000);
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut restored = registry.open_fold("groups").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    add(&mut restored, 4_000_000);
    let (output, released) = restored.drain_closed(13_000_000).unwrap();
    assert_eq!(released, 4);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].timestamp_ns, 8_000_000);
    assert_eq!(
        output[0].assigned_window,
        Some(WindowInterval {
            start_ns: 0,
            end_ns: 13_000_000
        })
    );
}

#[test]
fn allowed_lateness_changes_time_identity_and_requires_finite_millisecond_windows() {
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("values", vec![(1_i32, 2_i32)]).unwrap();
    assert!(
        values
            .allowed_lateness_named("global-lateness", 5_000_000)
            .is_err()
    );
    let windowed = values.fixed_windows("windows", 10_000_000).unwrap();
    assert!(
        windowed
            .allowed_lateness_named("negative", -1_000_000)
            .is_err()
    );
    assert!(
        windowed
            .allowed_lateness_named("submillisecond", 1)
            .is_err()
    );
    assert!(
        values
            .fixed_windows("misaligned-window", 1_000_001)
            .unwrap()
            .allowed_lateness_named("misaligned-lateness", 5_000_000)
            .is_err()
    );
    let baseline_id = pipeline.snapshot().unwrap().0.fingerprint().unwrap();
    let late = windowed
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap();
    assert!(late.latest_per_key_named("unsupported-latest").is_err());
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node(late.key()).unwrap().time.allowed_lateness_ns,
        5_000_000
    );
    assert_ne!(graph.fingerprint().unwrap(), baseline_id);
    assert!(
        !serde_json::to_string(&ironbeam_core::Time::default())
            .unwrap()
            .contains("allowed_lateness_ns")
    );
}
