use ironbeam_core::{
    Envelope, PaneInfo, PaneTiming, Pipeline, Window, WindowClosingBehavior, WindowInterval,
};

fn pipeline(early: bool) -> Pipeline {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap();
    let values = if early {
        values.early_count_trigger_named("early", 1).unwrap()
    } else {
        values
    };
    values
        .closing_behavior_named("closing", WindowClosingBehavior::EmitAlways)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    pipeline
}

fn add(fold: &mut Box<dyn ironbeam_core::FoldSession>, value: i32) {
    fold.add(Envelope {
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
    })
    .unwrap();
}

#[test]
fn closing_policy_requires_lateness_and_changes_identity() {
    let invalid = Pipeline::default();
    let values = invalid.from_vec("source", vec![(1_i32, 7_i32)]).unwrap();
    assert!(
        values
            .closing_behavior_named("closing", WindowClosingBehavior::EmitAlways)
            .is_err()
    );

    let always = pipeline(false);
    let (graph, _) = always.snapshot().unwrap();
    assert_eq!(
        graph.node("group").unwrap().time.closing_behavior,
        WindowClosingBehavior::EmitAlways
    );
    let baseline = Pipeline::default();
    baseline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    assert_ne!(
        graph.fingerprint().unwrap(),
        baseline.snapshot().unwrap().0.fingerprint().unwrap()
    );
}

#[test]
fn empty_final_pane_survives_checkpoint_after_skipped_on_time_pane() {
    let pipeline = pipeline(true);
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    add(&mut fold, 7);
    assert_eq!(fold.drain_ready_count(1).unwrap().0.len(), 1);
    assert!(fold.drain_closed(10_000_000).unwrap().0.is_empty());
    let state = fold.checkpoint_state().unwrap().unwrap();
    let mut resumed = registry.open_fold("group").unwrap();
    resumed.restore_checkpoint_state(&state).unwrap();
    let (final_panes, released) = resumed.drain_late_at(Some(15_000_000), true).unwrap();
    assert_eq!(released, 0);
    assert_eq!(final_panes.len(), 1);
    let pane = &final_panes[0];
    assert_eq!(pane.pane.timing, PaneTiming::Late);
    assert_eq!(pane.pane.index, 1);
    assert_eq!(pane.pane.nonspeculative_index, 0);
    assert!(pane.pane.is_last);
    assert_eq!(pane.timestamp_ns, 9_000_000);
    assert_eq!(
        *final_panes
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
fn empty_final_pane_follows_on_time_and_late_values() {
    let pipeline = pipeline(false);
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    add(&mut fold, 7);
    let (on_time, released) = fold.drain_closed(10_000_000).unwrap();
    assert_eq!(released, 1);
    assert_eq!(on_time.len(), 1);
    assert_eq!(on_time[0].pane.timing, PaneTiming::OnTime);
    add(&mut fold, 8);
    let (late, released) = fold.drain_late_at(Some(10_000_000), false).unwrap();
    assert_eq!(released, 1);
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].pane.index, 1);
    let state = fold.checkpoint_state().unwrap().unwrap();
    let mut resumed = registry.open_fold("group").unwrap();
    resumed.restore_checkpoint_state(&state).unwrap();
    let (final_panes, released) = resumed.drain_late_at(Some(15_000_000), true).unwrap();
    assert_eq!(released, 0);
    assert_eq!(final_panes.len(), 1);
    assert_eq!(final_panes[0].pane.index, 2);
    assert_eq!(final_panes[0].pane.nonspeculative_index, 2);
    assert!(final_panes[0].pane.is_last);
    assert_eq!(final_panes[0].timestamp_ns, 9_000_000);
    assert_eq!(
        *final_panes
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
fn watermark_jump_past_expiry_keeps_the_final_pane() {
    for early in [false, true] {
        let pipeline = pipeline(early);
        let (_, registry) = pipeline.snapshot().unwrap();
        let mut fold = registry.open_fold("group").unwrap();
        add(&mut fold, 7);
        if early {
            assert_eq!(fold.drain_ready_count(1).unwrap().0.len(), 1);
        }
        let (panes, released) = fold.drain_closed(20_000_000).unwrap();
        assert_eq!(released, usize::from(!early));
        let timings = panes
            .iter()
            .map(|pane| pane.pane.timing)
            .collect::<Vec<_>>();
        assert_eq!(
            timings,
            if early {
                vec![PaneTiming::Late]
            } else {
                vec![PaneTiming::OnTime, PaneTiming::Late]
            }
        );
        assert!(panes.last().unwrap().pane.is_last);
        assert_eq!(panes.last().unwrap().timestamp_ns, 9_000_000);
    }
}

#[test]
fn accumulating_final_pane_retains_values_and_uses_window_max_timestamp() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .closing_behavior_named("closing", WindowClosingBehavior::EmitAlways)
        .unwrap()
        .accumulating_early_and_late_count_trigger_named("trigger", 1, 1)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut fold = registry.open_fold("group").unwrap();
    add(&mut fold, 7);
    assert_eq!(fold.drain_ready_count(1).unwrap().0.len(), 1);
    assert!(fold.drain_closed(10_000_000).unwrap().0.is_empty());
    let (final_panes, released) = fold.drain_closed(15_000_000).unwrap();
    assert_eq!(released, 1);
    assert_eq!(final_panes.len(), 1);
    assert_eq!(final_panes[0].pane.timing, PaneTiming::Late);
    assert_eq!(final_panes[0].timestamp_ns, 9_000_000);
    assert_eq!(
        *final_panes
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![7])
    );
}
