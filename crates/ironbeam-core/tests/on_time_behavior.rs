use ironbeam_core::{
    Envelope, PaneInfo, PaneTiming, Pipeline, Window, WindowInterval, WindowOnTimeBehavior,
};

fn grouped_pipeline(behavior: WindowOnTimeBehavior, latest_in_pane: bool) -> Pipeline {
    let pipeline = Pipeline::default();
    let triggered = pipeline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .early_count_trigger_named("early", 1)
        .unwrap();
    let triggered = if latest_in_pane {
        triggered
            .latest_in_pane_output_time_named("output-time")
            .unwrap()
    } else {
        triggered
    };
    triggered
        .on_time_behavior_named("on-time", behavior)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    pipeline
}

#[test]
fn explicit_on_time_behavior_changes_graph_identity() {
    let nonempty = grouped_pipeline(WindowOnTimeBehavior::FireIfNonempty, false);
    let always = grouped_pipeline(WindowOnTimeBehavior::FireAlways, false);
    let (nonempty_graph, _) = nonempty.snapshot().unwrap();
    let (always_graph, _) = always.snapshot().unwrap();
    assert_ne!(
        nonempty_graph.fingerprint().unwrap(),
        always_graph.fingerprint().unwrap()
    );
    assert_eq!(
        always_graph.node("group").unwrap().time.on_time_behavior,
        WindowOnTimeBehavior::FireAlways
    );
    let encoded = serde_json::to_string(&always_graph.node("group").unwrap().time).unwrap();
    assert!(encoded.contains("FireAlways"));
}

#[test]
fn fire_always_restores_empty_on_time_pane_after_early_firing() {
    for (behavior, latest_in_pane) in [
        (WindowOnTimeBehavior::FireIfNonempty, false),
        (WindowOnTimeBehavior::FireAlways, false),
        (WindowOnTimeBehavior::FireAlways, true),
    ] {
        let pipeline = grouped_pipeline(behavior, latest_in_pane);
        let (_, registry) = pipeline.snapshot().unwrap();
        let mut fold = registry.open_fold("group").unwrap();
        fold.add(Envelope {
            pane: PaneInfo::NO_FIRING,
            value: Box::new((1_i32, 7_i32)),
            timestamp_ns: 1_000_000,
            window: Window::Fixed {
                size_ns: 10_000_000,
            },
            assigned_window: Some(WindowInterval {
                start_ns: 0,
                end_ns: 10_000_000,
            }),
        })
        .unwrap();
        let (early, released) = fold.drain_ready_count(1).unwrap();
        assert_eq!(released, 1);
        assert_eq!(early.len(), 1);
        assert_eq!(early[0].pane.timing, PaneTiming::Early);
        assert_eq!(early[0].pane.index, 0);
        let saved = fold.checkpoint_state().unwrap().unwrap();
        let mut restored = registry.open_fold("group").unwrap();
        restored.restore_checkpoint_state(&saved).unwrap();
        let (on_time, released) = restored.drain_closed(10_000_000).unwrap();
        assert_eq!(released, 0);
        assert_eq!(
            on_time.len(),
            usize::from(behavior == WindowOnTimeBehavior::FireAlways)
        );
        if let Some(pane) = on_time.into_iter().next() {
            assert_eq!(pane.timestamp_ns, 9_000_000);
            assert_eq!(pane.pane.timing, PaneTiming::OnTime);
            assert_eq!(pane.pane.index, 1);
            assert!(!pane.pane.is_first);
            assert!(pane.pane.is_last);
            assert_eq!(
                *pane.value.downcast::<(i32, Vec<i32>)>().unwrap(),
                (1, vec![])
            );
        }
    }
}

#[test]
fn processing_time_early_firing_preserves_empty_on_time_pane() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .early_processing_time_trigger_named("early", 1_000_000)
        .unwrap()
        .on_time_behavior_named("on-time", WindowOnTimeBehavior::FireAlways)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    fold.add(Envelope {
        pane: PaneInfo::NO_FIRING,
        value: Box::new((1_i32, 7_i32)),
        timestamp_ns: 1_000_000,
        window: Window::Fixed {
            size_ns: 10_000_000,
        },
        assigned_window: Some(WindowInterval {
            start_ns: 0,
            end_ns: 10_000_000,
        }),
    })
    .unwrap();
    fold.arm_processing_time(0, 1_000_000).unwrap();
    let (early, released) = fold.drain_ready_processing_time(1_000_000).unwrap();
    assert_eq!(released, 1);
    assert_eq!(early.len(), 1);
    assert_eq!(early[0].pane.timing, PaneTiming::Early);
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut restored = registry.open_fold("group").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    let (on_time, released) = restored.drain_closed(10_000_000).unwrap();
    assert_eq!(released, 0);
    assert_eq!(on_time.len(), 1);
    assert_eq!(on_time[0].pane.timing, PaneTiming::OnTime);
    assert_eq!(on_time[0].pane.index, 1);
    assert_eq!(on_time[0].timestamp_ns, 9_000_000);
    assert_eq!(
        *on_time
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![])
    );
}

#[test]
fn accumulating_on_time_policy_uses_new_values_since_early_firing() {
    for behavior in [
        WindowOnTimeBehavior::FireIfNonempty,
        WindowOnTimeBehavior::FireAlways,
    ] {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![(1_i32, 7_i32)])
            .unwrap()
            .fixed_windows("window", 10_000_000)
            .unwrap()
            .accumulating_count_trigger_named("early", 1)
            .unwrap()
            .on_time_behavior_named("on-time", behavior)
            .unwrap()
            .group_by_key_named("group")
            .unwrap();
        let (_, registry) = pipeline.snapshot().unwrap();
        let mut fold = registry.open_fold("group").unwrap();
        fold.add(Envelope {
            pane: PaneInfo::NO_FIRING,
            value: Box::new((1_i32, 7_i32)),
            timestamp_ns: 1_000_000,
            window: Window::Fixed {
                size_ns: 10_000_000,
            },
            assigned_window: Some(WindowInterval {
                start_ns: 0,
                end_ns: 10_000_000,
            }),
        })
        .unwrap();
        assert_eq!(fold.drain_ready_count(1).unwrap().0.len(), 1);
        let (panes, released) = fold.drain_closed(10_000_000).unwrap();
        assert_eq!(released, 1);
        assert_eq!(
            panes.len(),
            usize::from(behavior == WindowOnTimeBehavior::FireAlways)
        );
        if let Some(pane) = panes.into_iter().next() {
            assert_eq!(pane.pane.timing, PaneTiming::OnTime);
            assert_eq!(pane.timestamp_ns, 9_000_000);
            assert_eq!(
                *pane.value.downcast::<(i32, Vec<i32>)>().unwrap(),
                (1, vec![7])
            );
        }
    }
}

#[test]
fn skipped_accumulating_on_time_pane_keeps_state_for_late_values() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .accumulating_early_and_late_count_trigger_named("trigger", 1, 1)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    let input = |value| Envelope {
        pane: PaneInfo::NO_FIRING,
        value: Box::new((1_i32, value)),
        timestamp_ns: 1_000_000,
        window: Window::Fixed {
            size_ns: 10_000_000,
        },
        assigned_window: Some(WindowInterval {
            start_ns: 0,
            end_ns: 10_000_000,
        }),
    };
    fold.add(input(7)).unwrap();
    assert_eq!(fold.drain_ready_count(1).unwrap().0.len(), 1);
    assert!(fold.drain_closed(10_000_000).unwrap().0.is_empty());
    fold.add(input(8)).unwrap();
    let (late, _) = fold.drain_late_at(Some(10_000_000), false).unwrap();
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].pane.timing, PaneTiming::Late);
    assert_eq!(
        *late
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![7, 8])
    );
}

#[test]
fn skipped_discarding_on_time_pane_keeps_late_trigger_after_checkpoint() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .early_and_late_count_trigger_named("trigger", 1, 1)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    let input = |value| Envelope {
        pane: PaneInfo::NO_FIRING,
        value: Box::new((1_i32, value)),
        timestamp_ns: 1_000_000,
        window: Window::Fixed {
            size_ns: 10_000_000,
        },
        assigned_window: Some(WindowInterval {
            start_ns: 0,
            end_ns: 10_000_000,
        }),
    };
    fold.add(input(7)).unwrap();
    assert_eq!(fold.drain_ready_count(1).unwrap().0.len(), 1);
    let state = fold.checkpoint_state().unwrap().unwrap();
    let mut restored = registry.open_fold("group").unwrap();
    restored.restore_checkpoint_state(&state).unwrap();
    assert!(restored.drain_closed(10_000_000).unwrap().0.is_empty());
    restored.add(input(8)).unwrap();
    let (late, released) = restored.drain_late_at(Some(10_000_000), false).unwrap();
    assert_eq!(released, 1);
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].pane.timing, PaneTiming::Late);
    assert_eq!(late[0].pane.index, 1);
    assert_eq!(
        *late
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![8])
    );
}
