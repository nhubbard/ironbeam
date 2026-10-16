use ironbeam_core::{
    CombineFn, Element, Envelope, ErrorKind, PaneTiming, Pipeline, Window, WindowInterval,
};

#[test]
fn global_count_fold_restores_pane_index_and_accumulated_values() {
    for accumulating in [false, true] {
        let pipeline = Pipeline::default();
        let values = pipeline
            .test_stream_source(
                "source",
                vec![ironbeam_core::TestStreamEvent::Elements(vec![(
                    (1_i32, 2_i32),
                    2,
                )])],
            )
            .unwrap();
        let triggered = if accumulating {
            values
                .accumulating_count_trigger_named("trigger", 2)
                .unwrap()
        } else {
            values.early_count_trigger_named("trigger", 2).unwrap()
        };
        triggered.group_by_key_named("group").unwrap();
        let (_, registry) = pipeline.snapshot().unwrap();
        let add = |fold: &mut Box<dyn ironbeam_core::FoldSession>, value| {
            fold.add(Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: Box::new((1_i32, value)),
                timestamp_ns: i64::from(value),
                window: Window::Global,
                assigned_window: None,
            })
            .unwrap();
        };
        let mut fold = registry.open_fold("group").unwrap();
        add(&mut fold, 2_i32);
        add(&mut fold, 3_i32);
        let (first, released) = fold.drain_ready_count(2).unwrap();
        assert_eq!(released, if accumulating { 0 } else { 2 });
        assert_eq!(first[0].pane.index, 0);
        assert_eq!(first[0].assigned_window, None);
        let saved = fold.checkpoint_state().unwrap().unwrap();
        let mut restored = registry.open_fold("group").unwrap();
        restored.restore_checkpoint_state(&saved).unwrap();
        assert_eq!(restored.checkpoint_state().unwrap().unwrap(), saved);
        add(&mut restored, 4_i32);
        let final_panes = restored.finish().unwrap();
        assert_eq!(final_panes.len(), 1);
        assert_eq!(final_panes[0].pane.index, 1);
        assert_eq!(final_panes[0].pane.timing, PaneTiming::OnTime);
        assert_eq!(final_panes[0].assigned_window, None);
        assert_eq!(
            *final_panes
                .into_iter()
                .next()
                .unwrap()
                .value
                .downcast::<(i32, Vec<i32>)>()
                .unwrap(),
            (1, if accumulating { vec![2, 3, 4] } else { vec![4] })
        );
    }
}

#[test]
fn count_trigger_fold_restores_next_pane_and_rejects_tampered_index() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .fixed_windows("window", 10)
        .unwrap()
        .early_count_trigger_named("trigger", 1)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let interval = WindowInterval {
        start_ns: 0,
        end_ns: 10,
    };
    let add = |fold: &mut Box<dyn ironbeam_core::FoldSession>, value, timestamp_ns| {
        fold.add(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((1_i32, value)),
            timestamp_ns,
            window: Window::Fixed { size_ns: 10 },
            assigned_window: Some(interval),
        })
        .unwrap();
    };
    let mut fold = registry.open_fold("group").unwrap();
    add(&mut fold, 10_i32, 1);
    let (first, released) = fold.drain_ready_count(1).unwrap();
    assert_eq!(released, 1);
    assert_eq!(first[0].pane.timing, PaneTiming::Early);
    assert_eq!(first[0].pane.index, 0);
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut restored = registry.open_fold("group").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    assert_eq!(restored.checkpoint_state().unwrap().unwrap(), saved);
    add(&mut restored, 20_i32, 2);
    let (second, released) = restored.drain_ready_count(1).unwrap();
    assert_eq!(released, 1);
    assert_eq!(second[0].pane.timing, PaneTiming::Early);
    assert_eq!(second[0].pane.index, 1);
    assert!(!second[0].pane.is_first);

    let mut invalid: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    invalid["fired_panes"][0]["next_index"] = 0.into();
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&invalid).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn accumulating_count_fold_restores_values_and_threshold_progress() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .fixed_windows("window", 10)
        .unwrap()
        .accumulating_count_trigger_named("trigger", 2)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let add = |fold: &mut Box<dyn ironbeam_core::FoldSession>, value| {
        fold.add(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((1_i32, value)),
            timestamp_ns: 1,
            window: Window::Fixed { size_ns: 10 },
            assigned_window: Some(WindowInterval {
                start_ns: 0,
                end_ns: 10,
            }),
        })
        .unwrap();
    };
    let mut fold = registry.open_fold("group").unwrap();
    add(&mut fold, 10_i32);
    add(&mut fold, 20_i32);
    let (first, released) = fold.drain_ready_count(2).unwrap();
    assert_eq!(released, 0);
    assert_eq!(first[0].pane.index, 0);
    assert_eq!(
        *first
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![10, 20])
    );
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut invalid: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    invalid["fired_panes"][0]["fired_value_count"] = 3.into();
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&invalid).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
    invalid["fired_panes"][0]
        .as_object_mut()
        .unwrap()
        .remove("fired_value_count");
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&invalid).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
    let mut restored = registry.open_fold("group").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    assert!(restored.drain_ready_count(2).unwrap().0.is_empty());
    add(&mut restored, 30_i32);
    assert!(restored.drain_ready_count(2).unwrap().0.is_empty());
    add(&mut restored, 40_i32);
    let (second, released) = restored.drain_ready_count(2).unwrap();
    assert_eq!(released, 0);
    assert_eq!(second[0].pane.index, 1);
    assert_eq!(
        *second
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![10, 20, 30, 40])
    );
    let (closed, released) = restored.drain_closed(10).unwrap();
    assert_eq!(released, 4);
    assert_eq!(closed[0].pane.timing, PaneTiming::OnTime);
    assert_eq!(closed[0].pane.index, 2);
    assert_eq!(
        *closed
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![10, 20, 30, 40])
    );
}

#[test]
fn older_late_fold_state_restores_non_speculative_pane_number() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let interval = WindowInterval {
        start_ns: 0,
        end_ns: 10_000_000,
    };
    let add = |fold: &mut Box<dyn ironbeam_core::FoldSession>, value, timestamp_ns| {
        fold.add(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((1_i32, value)),
            timestamp_ns,
            window: Window::Fixed {
                size_ns: 10_000_000,
            },
            assigned_window: Some(interval),
        })
        .unwrap();
    };
    let mut fold = registry.open_fold("group").unwrap();
    add(&mut fold, 2_i32, 2_000_000);
    assert_eq!(fold.drain_closed(10_000_000).unwrap().0[0].pane.index, 0);
    let mut saved: serde_json::Value =
        serde_json::from_slice(&fold.checkpoint_state().unwrap().unwrap()).unwrap();
    saved["version"]["minor"] = 0.into();
    saved["fired_panes"][0]
        .as_object_mut()
        .unwrap()
        .remove("nonspeculative_base");
    let mut restored = registry.open_fold("group").unwrap();
    restored
        .restore_checkpoint_state(&serde_json::to_vec(&saved).unwrap())
        .unwrap();
    add(&mut restored, 3_i32, 3_000_000);
    let (late, released) = restored.drain_late(false).unwrap();
    assert_eq!(released, 1);
    assert_eq!(late[0].pane.timing, PaneTiming::Late);
    assert_eq!(late[0].pane.index, 1);
    assert_eq!(late[0].pane.nonspeculative_index, 1);

    saved["fired_panes"][0]["nonspeculative_base"] = 9.into();
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&saved).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn count_two_trigger_preserves_partial_group_across_checkpoint() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .fixed_windows("window", 10)
        .unwrap()
        .early_count_trigger_named("trigger", 2)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let add = |fold: &mut Box<dyn ironbeam_core::FoldSession>, value| {
        fold.add(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((1_i32, value)),
            timestamp_ns: i64::from(value),
            window: Window::Fixed { size_ns: 10 },
            assigned_window: Some(WindowInterval {
                start_ns: 0,
                end_ns: 10,
            }),
        })
        .unwrap();
    };
    let mut fold = registry.open_fold("group").unwrap();
    add(&mut fold, 1_i32);
    assert!(fold.drain_ready_count(2).unwrap().0.is_empty());
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut restored = registry.open_fold("group").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    add(&mut restored, 2_i32);
    let (panes, released) = restored.drain_ready_count(2).unwrap();
    assert_eq!(released, 2);
    assert_eq!(panes.len(), 1);
    assert_eq!(panes[0].pane.timing, PaneTiming::Early);
    assert_eq!(panes[0].pane.index, 0);
    assert_eq!(
        *panes
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![1, 2])
    );
}

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
    for (value, time) in [(30, 4), (40, 12)] {
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
                    end_ns: 17
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
fn closed_session_lineage_survives_checkpoint_for_late_bridge() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 10_000_000)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    // The graph guard remains until runner semantics are complete. Exercise
    // the fold's session state through a configured positive-lateness fold.
    let window = Window::Session { gap_ns: 5_000_000 };
    let event = |time, value| Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new((1_i32, value)),
        timestamp_ns: time,
        window: window.clone(),
        assigned_window: window.assign(time).unwrap(),
    };
    let mut fold = registry.open_fold("group").unwrap();
    fold.add(event(0, 2)).unwrap();
    assert_eq!(fold.drain_closed(5_000_000).unwrap().0.len(), 1);
    assert!(fold.drain_closed(10_000_000).unwrap().0.is_empty());
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut corrupt: serde_json::Value = serde_json::from_slice(&saved).unwrap();
    let duplicate = corrupt["session_lineage"][0].clone();
    corrupt["session_lineage"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    assert_eq!(
        registry
            .open_fold("group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&corrupt).unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
    let mut resumed = registry.open_fold("group").unwrap();
    resumed.restore_checkpoint_state(&saved).unwrap();
    let bridge = event(4_000_000, 3);
    assert_eq!(
        resumed.resolved_interval(&bridge).unwrap(),
        Some(WindowInterval {
            start_ns: 0,
            end_ns: 9_000_000
        })
    );
    resumed.add(bridge).unwrap();
    let (late, released) = resumed.drain_late(false).unwrap();
    assert_eq!(released, 1);
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].assigned_window.unwrap().start_ns, 0);
    assert_eq!(late[0].pane.timing, PaneTiming::Late);
    assert_eq!(late[0].pane.index, 1);
    assert!(resumed.drain_closed(19_000_000).unwrap().0.is_empty());
    assert_eq!(
        resumed.resolved_interval(&event(4_000_000, 4)).unwrap(),
        Some(WindowInterval {
            start_ns: 4_000_000,
            end_ns: 9_000_000
        })
    );
    let expired: serde_json::Value =
        serde_json::from_slice(&resumed.checkpoint_state().unwrap().unwrap()).unwrap();
    assert!(expired["session_lineage"].is_null());
    assert!(expired["fired_panes"].is_null());
}

#[test]
fn accumulating_late_group_releases_state_at_expiry() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .accumulating_late_count_trigger_named("trigger", 1)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    fold.add(Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
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
    assert_eq!(fold.drain_closed(10_000_000).unwrap().0.len(), 1);
    let (closed, released) = fold.drain_closed(15_000_000).unwrap();
    assert!(closed.is_empty());
    assert_eq!(released, 1);
    let saved: serde_json::Value =
        serde_json::from_slice(&fold.checkpoint_state().unwrap().unwrap()).unwrap();
    assert!(saved["groups"].as_array().unwrap().is_empty());
}

#[test]
fn touching_sessions_stay_separate_until_an_overlapping_bridge_arrives() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 0_i32)])
        .unwrap()
        .group_by_session_named("session-group", 5)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let window = Window::Session { gap_ns: 5 };
    let add = |fold: &mut Box<dyn ironbeam_core::FoldSession>, value, time| {
        fold.add(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((1_i32, value)),
            timestamp_ns: time,
            window: window.clone(),
            assigned_window: window.assign(time).unwrap(),
        })
        .unwrap();
    };
    let mut fold = registry.open_fold("session-group").unwrap();
    add(&mut fold, 10_i32, 0);
    add(&mut fold, 20_i32, 5);
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let separate = fold.finish().unwrap();
    assert_eq!(separate.len(), 2);
    assert_eq!(
        separate[0].assigned_window,
        Some(WindowInterval {
            start_ns: 0,
            end_ns: 5
        })
    );
    assert_eq!(
        separate[1].assigned_window,
        Some(WindowInterval {
            start_ns: 5,
            end_ns: 10
        })
    );

    let mut restored = registry.open_fold("session-group").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    add(&mut restored, 30_i32, 4);
    let merged = restored.finish().unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0].assigned_window,
        Some(WindowInterval {
            start_ns: 0,
            end_ns: 10
        })
    );
    let (_, mut values) = *merged
        .into_iter()
        .next()
        .unwrap()
        .value
        .downcast::<(i32, Vec<i32>)>()
        .unwrap();
    values.sort_unstable();
    assert_eq!(values, [10, 20, 30]);
}

#[test]
fn restored_session_resolves_a_late_candidate_against_its_live_merge() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 0_i32)])
        .unwrap()
        .group_by_session_named("session-group", 5)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let window = Window::Session { gap_ns: 5 };
    let event = |value, time| Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new((1_i32, value)),
        timestamp_ns: time,
        window: window.clone(),
        assigned_window: window.assign(time).unwrap(),
    };
    let mut fold = registry.open_fold("session-group").unwrap();
    fold.add(event(1_i32, 0)).unwrap();
    fold.add(event(2_i32, 4)).unwrap();
    let saved = fold.checkpoint_state().unwrap().unwrap();
    let mut restored = registry.open_fold("session-group").unwrap();
    restored.restore_checkpoint_state(&saved).unwrap();
    let candidate = event(3_i32, 0);
    assert_eq!(
        restored.resolved_interval(&candidate).unwrap(),
        Some(WindowInterval {
            start_ns: 0,
            end_ns: 9
        })
    );
    assert_eq!(
        restored.resolved_interval(&event(4_i32, 20)).unwrap(),
        Some(WindowInterval {
            start_ns: 20,
            end_ns: 25
        })
    );
    let mut missing = event(4_i32, 20);
    missing.assigned_window = None;
    assert_eq!(restored.resolved_interval(&missing).unwrap(), None);
    let mut fixed = event(4_i32, 20);
    fixed.window = Window::Fixed { size_ns: 5 };
    assert_eq!(
        restored.resolved_interval(&fixed).unwrap(),
        fixed.assigned_window
    );
    let mut wrong_type = event(4_i32, 20);
    wrong_type.value = Box::new(4_i32);
    assert_eq!(
        restored.resolved_interval(&wrong_type).unwrap_err().kind,
        ErrorKind::Execution
    );
    restored.add(candidate).unwrap();
    let rows = restored.finish().unwrap();
    assert_eq!(rows.len(), 1);
    let (_, mut values) = *rows
        .into_iter()
        .next()
        .unwrap()
        .value
        .downcast::<(i32, Vec<i32>)>()
        .unwrap();
    values.sort_unstable();
    assert_eq!(values, [1, 2, 3]);
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
