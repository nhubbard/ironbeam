use super::drain_grouped_session;
use ironbeam_core::{Envelope, PaneInfo, PaneTiming, Pipeline, Window};

#[test]
fn session_late_pane_does_not_consume_an_open_window() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .session_windows_named("window", 5_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 10_000_000)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let window = Window::Session { gap_ns: 5_000_000 };
    let event = |time, value| Envelope {
        pane: PaneInfo::NO_FIRING,
        value: Box::new((1_i32, value)),
        timestamp_ns: time,
        window: window.clone(),
        assigned_window: window.assign(time).unwrap(),
    };
    let mut fold = registry.open_fold("group").unwrap();
    fold.add(event(0, 10)).unwrap();
    fold.add(event(20_000_000, 20)).unwrap();
    let (on_time, released) = drain_grouped_session(fold.as_mut(), 5_000_000, false).unwrap();
    assert_eq!(on_time.len(), 1);
    assert_eq!(released, 1);
    fold.add(event(0, 30)).unwrap();
    let (late, released) = drain_grouped_session(fold.as_mut(), 5_000_000, false).unwrap();
    assert_eq!(released, 1);
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].pane.timing, PaneTiming::Late);
    let (open, released) = drain_grouped_session(fold.as_mut(), 25_000_000, false).unwrap();
    assert_eq!(released, 1);
    assert_eq!(open.len(), 1);
    assert_eq!(
        *open[0].value.downcast_ref::<(i32, Vec<i32>)>().unwrap(),
        (1, vec![20])
    );
}

#[test]
fn expiring_session_flushes_partial_late_count_without_closing_other_session() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 10_i32)])
        .unwrap()
        .session_windows_named("window", 5_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 10_000_000)
        .unwrap()
        .late_count_trigger_named("trigger", 2)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let window = Window::Session { gap_ns: 5_000_000 };
    let event = |time, value| Envelope {
        pane: PaneInfo::NO_FIRING,
        value: Box::new((1_i32, value)),
        timestamp_ns: time,
        window: window.clone(),
        assigned_window: window.assign(time).unwrap(),
    };
    let mut fold = registry.open_fold("group").unwrap();
    fold.add(event(0, 10)).unwrap();
    fold.add(event(20_000_000, 20)).unwrap();
    assert_eq!(
        drain_grouped_session(fold.as_mut(), 5_000_000, false)
            .unwrap()
            .0
            .len(),
        1
    );
    fold.add(event(0, 30)).unwrap();
    assert!(
        drain_grouped_session(fold.as_mut(), 5_000_000, false)
            .unwrap()
            .0
            .is_empty()
    );
    let (late, released) = drain_grouped_session(fold.as_mut(), 15_000_000, false).unwrap();
    assert_eq!(released, 1);
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].pane.timing, PaneTiming::Late);
    assert!(late[0].pane.is_last);
    let (open, released) = drain_grouped_session(fold.as_mut(), 25_000_000, false).unwrap();
    assert_eq!(released, 1);
    assert_eq!(open.len(), 1);
    assert_eq!(
        *open[0].value.downcast_ref::<(i32, Vec<i32>)>().unwrap(),
        (1, vec![20])
    );
}
