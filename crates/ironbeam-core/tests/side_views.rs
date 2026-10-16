use ironbeam_core::{Envelope, Operation, PaneInfo, Pipeline, TestStreamEvent, Window};
use std::collections::HashMap;

#[test]
fn multiple_windowed_signals_have_distinct_dependencies() {
    let pipeline = Pipeline::default();
    let main = pipeline
        .from_vec("main", vec![1_i32])
        .unwrap()
        .fixed_windows("main-window", 10)
        .unwrap();
    let first = pipeline
        .from_vec("first", vec![2_i32])
        .unwrap()
        .fixed_windows("first-window", 10)
        .unwrap();
    let second = pipeline
        .from_vec("second", vec![3_i32])
        .unwrap()
        .fixed_windows("second-window", 10)
        .unwrap();
    assert!(
        main.wait_on_all_windowed_named::<i32>("empty", &[])
            .is_err()
    );
    let other = Pipeline::default().from_vec("other", vec![4_i32]).unwrap();
    assert!(
        main.wait_on_all_windowed_named("foreign", &[first.clone(), other])
            .is_err()
    );
    let mismatched = pipeline
        .from_vec("mismatched", vec![5_i32])
        .unwrap()
        .fixed_windows("mismatched-window", 20)
        .unwrap();
    assert!(
        main.wait_on_all_windowed_named("mismatch", &[first.clone(), mismatched])
            .is_err()
    );
    assert_eq!(
        main.wait_on_all_windowed_named("single-barrier", std::slice::from_ref(&first))
            .unwrap()
            .key(),
        "single-barrier"
    );

    let waited = main
        .wait_on_all_windowed_named("barrier", &[first, second])
        .unwrap();
    assert_eq!(waited.key(), "barrier");
    let (graph, _) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    assert!(graph.node("barrier/group/flatten").is_some());
    assert!(graph.node("barrier/group/group").is_some());
}

#[test]
fn independent_unbounded_signals_use_a_window_barrier() {
    let pipeline = Pipeline::default();
    let main = pipeline
        .test_stream_source::<i32>("main", vec![TestStreamEvent::AdvanceWatermark(10)])
        .unwrap()
        .fixed_windows("main-window", 10)
        .unwrap();
    let first = pipeline
        .test_stream_source::<i32>("first", vec![TestStreamEvent::AdvanceWatermark(10)])
        .unwrap()
        .fixed_windows("first-window", 10)
        .unwrap();
    let second = pipeline
        .test_stream_source::<i32>("second", vec![TestStreamEvent::AdvanceWatermark(10)])
        .unwrap()
        .fixed_windows("second-window", 10)
        .unwrap();
    main.wait_on_all_windowed_named("independent", &[first.clone(), second])
        .unwrap();
    main.wait_on_all_windowed_named("shared", &[first.clone(), first])
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(matches!(
        graph.node("independent").unwrap().operation,
        Operation::WaitOnWindowed
    ));
    assert!(matches!(
        graph.node("independent/signals").unwrap().operation,
        Operation::Flatten
    ));
    assert!(matches!(
        graph.node("shared/group/group").unwrap().operation,
        Operation::GroupByKey
    ));
}

#[test]
fn heterogeneous_windowed_signals_build_one_group_boundary() {
    let pipeline = Pipeline::default();
    let main = pipeline
        .from_vec("main", vec![1_i32])
        .unwrap()
        .fixed_windows("main-window", 10)
        .unwrap();
    let text = pipeline
        .from_vec("text", vec!["ready".to_owned()])
        .unwrap()
        .fixed_windows("text-window", 10)
        .unwrap()
        .windowed_wait_signal_named("text-marker")
        .unwrap();
    let flag = pipeline
        .from_vec("flag", vec![true])
        .unwrap()
        .fixed_windows("flag-window", 10)
        .unwrap()
        .windowed_wait_signal_named("flag-marker")
        .unwrap();
    assert!(main.wait_on_windowed_signals_named("empty", &[]).is_err());
    let foreign = Pipeline::default()
        .from_vec("foreign", vec![false])
        .unwrap()
        .fixed_windows("foreign-window", 10)
        .unwrap()
        .windowed_wait_signal_named("foreign-marker")
        .unwrap();
    assert!(
        main.wait_on_windowed_signals_named("foreign-barrier", &[foreign])
            .is_err()
    );
    let mismatched = pipeline
        .from_vec("mismatched", vec![false])
        .unwrap()
        .fixed_windows("mismatched-window", 20)
        .unwrap()
        .windowed_wait_signal_named("mismatched-marker")
        .unwrap();
    assert!(
        main.wait_on_windowed_signals_named("mismatched-barrier", &[mismatched])
            .is_err()
    );
    let waited = main
        .wait_on_windowed_signals_named("barrier", &[text, flag])
        .unwrap();
    assert_eq!(waited.key(), "barrier");
    let (graph, _) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    assert!(graph.node("barrier/group/flatten").is_some());
    assert!(graph.node("barrier/group/group").is_some());
}

#[test]
fn singleton_side_view_requires_exactly_one_value() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let side = pipeline.from_vec("side", vec![10_i32]).unwrap();
    main.map_with_singleton_side_named("single", &side, |value, side| value + side)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    for count in [0, 2] {
        let values = (0..count).map(|value| Box::new(value) as _).collect();
        assert!(
            registry
                .materialize_side_input("single", values)
                .unwrap_err()
                .to_string()
                .contains("exactly one")
        );
    }
    let view = registry
        .materialize_side_input("single", vec![Box::new(10_i32)])
        .unwrap();
    assert_eq!(view.downcast_ref::<i32>(), Some(&10));
}

#[test]
fn map_side_view_rejects_duplicate_keys() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec!["a".to_owned()]).unwrap();
    let side = pipeline
        .from_vec("side", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    main.map_with_map_side_named("map", &side, |key, view| {
        view.get(&key).copied().unwrap_or_default()
    })
    .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let values = vec![
        Box::new(("a".to_owned(), 1_i32)) as _,
        Box::new(("a".to_owned(), 2_i32)) as _,
    ];
    assert!(
        registry
            .materialize_side_input("map", values)
            .unwrap_err()
            .to_string()
            .contains("duplicate key")
    );
}

#[test]
fn multimap_side_view_retains_all_values_for_a_key() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec!["a".to_owned()]).unwrap();
    let side = pipeline
        .from_vec("side", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    main.map_with_multimap_side_named("multimap", &side, |key, view| {
        i32::try_from(view.get(&key).map_or(0, Vec::len)).unwrap()
    })
    .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let values = vec![
        Box::new(("a".to_owned(), 2_i32)) as _,
        Box::new(("a".to_owned(), 1_i32)) as _,
    ];
    let view = registry.materialize_side_input("multimap", values).unwrap();
    let view = view.downcast_ref::<HashMap<String, Vec<i32>>>().unwrap();
    let mut values = view["a"].clone();
    values.sort_unstable();
    assert_eq!(values, [1, 2]);
}

#[test]
fn windowed_side_helpers_build_distinct_valid_views() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let values = pipeline.from_vec("values", vec![10_i32]).unwrap();
    let pairs = pipeline
        .from_vec("pairs", vec![("a".to_owned(), 10_i32)])
        .unwrap();

    let mapped = main
        .map_with_windowed_side_named("window-list", &values, |value, side| {
            value + side.iter().sum::<i32>()
        })
        .unwrap();
    let singleton = main
        .map_with_windowed_singleton_side_named("window-single", &values, |value, side| {
            value + side
        })
        .unwrap();
    let map = main
        .map_with_windowed_map_side_named("window-map", &pairs, |value, side| value + side["a"])
        .unwrap();
    let multimap = main
        .map_with_windowed_multimap_side_named("window-multi", &pairs, |value, side| {
            value + side["a"].iter().sum::<i32>()
        })
        .unwrap();
    let waited = main.wait_on_windowed_named("window-wait", &values).unwrap();
    for (key, collection) in [
        ("window-list", mapped),
        ("window-single", singleton),
        ("window-map", map),
        ("window-multi", multimap),
        ("window-wait", waited),
    ] {
        assert_eq!(collection.key(), key);
    }
    let (graph, _) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    for key in [
        "window-list",
        "window-single",
        "window-map",
        "window-multi",
        "window-wait",
    ] {
        assert!(graph.node(key).is_some());
    }
}

#[test]
fn windowed_and_global_side_views_reject_foreign_or_incompatible_inputs() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let foreign = Pipeline::default()
        .from_vec("foreign", vec![2_i32])
        .unwrap();
    assert!(
        main.map_with_windowed_side_named("foreign-window", &foreign, |value, _| value)
            .is_err()
    );
    assert!(
        main.map_with_side_named("foreign-global", &foreign, |value, _| value)
            .is_err()
    );

    let mismatched = pipeline
        .from_vec("side", vec![2_i32])
        .unwrap()
        .fixed_windows("side-window", 5)
        .unwrap();
    assert!(
        main.map_with_windowed_side_named("mismatch-window", &mismatched, |value, _| value)
            .is_err()
    );
    assert!(
        main.map_with_side_named("mismatch-global", &mismatched, |value, _| value)
            .is_err()
    );
}

#[test]
fn global_side_view_preserves_main_event_metadata_and_applies_value() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![2_i32]).unwrap();
    let side = pipeline.from_vec("side", vec![10_i32]).unwrap();
    main.map_with_singleton_side_named("sum", &side, |value, side| value + side)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let event = |value| Envelope {
        pane: PaneInfo::FIRST_LAST_ON_TIME,
        value: Box::new(value),
        timestamp_ns: 7,
        window: Window::Global,
        assigned_window: None,
    };
    let output = registry
        .apply_aggregate("sum", vec![vec![event(2)], vec![event(10)]])
        .unwrap();
    assert_eq!(output.len(), 1);
    let result = &output[0];
    assert_eq!(result.value.downcast_ref::<i32>(), Some(&12));
    assert_eq!(result.timestamp_ns, 7);
    assert_eq!(result.pane, PaneInfo::FIRST_LAST_ON_TIME);
}
