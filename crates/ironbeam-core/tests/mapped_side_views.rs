use ironbeam_core::{Pipeline, TestStreamEvent};

#[test]
fn mapped_side_views_reject_unbounded_foreign_or_unsupported_windows() {
    let pipeline = Pipeline::default();
    let main = pipeline
        .from_vec("main", vec![1_i32])
        .unwrap()
        .fixed_windows("main-windows", 4_000_000)
        .unwrap();
    let same = pipeline
        .from_vec("same", vec![2_i32])
        .unwrap()
        .fixed_windows("same-windows", 4_000_000)
        .unwrap();
    assert!(
        main.map_with_mapped_window_side_named("same-view", &same, |value, _| value)
            .is_err()
    );

    let submillisecond = pipeline
        .from_vec("sub-ms", vec![2_i32])
        .unwrap()
        .fixed_windows("sub-ms-windows", 10_500_000)
        .unwrap();
    assert!(
        main.map_with_mapped_window_side_named("sub-ms-view", &submillisecond, |value, _| value)
            .is_err()
    );

    let sessions = pipeline
        .from_vec("sessions", vec![2_i32])
        .unwrap()
        .session_windows_named("session-windows", 10_000_000)
        .unwrap();
    assert!(
        main.map_with_mapped_window_side_named("session-view", &sessions, |value, _| value)
            .is_err()
    );

    let unbounded = pipeline
        .test_stream_source::<i32>(
            "unbounded",
            vec![TestStreamEvent::AdvanceWatermark(10_000_000)],
        )
        .unwrap()
        .fixed_windows("unbounded-windows", 10_000_000)
        .unwrap();
    assert!(
        main.map_with_mapped_window_side_named("unbounded-view", &unbounded, |value, _| value)
            .is_err()
    );

    let foreign = Pipeline::default()
        .from_vec("foreign", vec![2_i32])
        .unwrap()
        .fixed_windows("foreign-windows", 10_000_000)
        .unwrap();
    assert!(
        main.map_with_mapped_window_side_named("foreign-view", &foreign, |value, _| value)
            .is_err()
    );

    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(graph.node("same-view").is_none());
    assert!(graph.node("sub-ms-view").is_none());
    assert!(graph.node("session-view").is_none());
    assert!(graph.node("unbounded-view").is_none());
    assert!(graph.node("foreign-view").is_none());
}
