use ironbeam_core::{
    Envelope, Error, ErrorKind, PaneInfo, Pipeline, Version, Window, WindowInterval,
};

fn event(value: impl Send + 'static) -> Envelope {
    Envelope {
        pane: PaneInfo::NO_FIRING,
        value: Box::new(value),
        timestamp_ns: 1_000_000,
        window: Window::Global,
        assigned_window: None,
    }
}

#[test]
fn collection_metadata_transforms_reject_wrong_input_types() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    source.reify_timestamps_named("time").unwrap();
    source.reify_panes_named("pane").unwrap();
    source
        .assign_timestamps_named("assign", |value| i64::from(*value))
        .unwrap();
    source
        .sliding_windows("sliding", 2_000_000, 1_000_000, 0)
        .unwrap();
    assert_eq!(
        source
            .with_window_named("global-window")
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    let (graph, registry) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    for key in ["time", "pane", "assign", "sliding"] {
        assert_eq!(
            registry
                .apply_many(key, event("wrong type"))
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
    }
}

#[test]
fn fallible_semantic_flat_map_preserves_its_identity_and_error() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let version = Version { major: 2, minor: 1 };
    source
        .flat_map_fallible_with_semantic_named(
            "expand",
            "tests/fallible-expand",
            version,
            |value| {
                if value < 0 {
                    Err(Error::execution("expand", "direct", "negative value"))
                } else {
                    Ok(vec![value, value + 1])
                }
            },
        )
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node = graph.node("expand").unwrap();
    assert_eq!(node.semantic_id, "tests/fallible-expand");
    assert_eq!(node.semantic_version, version);
    let output = registry.apply_many("expand", event(1_i32)).unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].value.downcast_ref::<i32>(), Some(&1));
    assert_eq!(output[1].value.downcast_ref::<i32>(), Some(&2));
    assert_eq!(
        registry
            .apply_many("expand", event(-1_i32))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn explicit_batch_size_emits_last_partial_batch() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2, 3]).unwrap();
    source.batch_elements_named("batches", 2).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let rows = registry
        .apply_many("batches", event(vec![1_i32, 2, 3]))
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].value.downcast_ref::<Vec<i32>>(), Some(&vec![1, 2]));
    assert_eq!(rows[1].value.downcast_ref::<Vec<i32>>(), Some(&vec![3]));
}

#[test]
fn window_metadata_and_restored_timestamp_require_matching_encoded_elements() {
    let pipeline = Pipeline::default();
    let windowed = pipeline
        .from_vec("values", vec![1_i32])
        .unwrap()
        .fixed_windows("fixed", 2_000_000)
        .unwrap();
    windowed.with_window_named("with-window").unwrap();
    let pairs = pipeline
        .from_vec("pairs", vec![(1_000_000_i64, 1_i32)])
        .unwrap()
        .fixed_windows("pairs-window", 2_000_000)
        .unwrap();
    pairs
        .timestamps_from_pairs_in_window_named("restore")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(
        registry
            .apply_many("with-window", event(1_i32))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
    let mut wrong_type = event("wrong type");
    wrong_type.window = Window::Fixed { size_ns: 2_000_000 };
    wrong_type.assigned_window = Some(WindowInterval {
        start_ns: 0,
        end_ns: 2_000_000,
    });
    assert_eq!(
        registry
            .apply_many("with-window", wrong_type)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
    let mut wrong_pair = event("wrong pair");
    wrong_pair.window = Window::Fixed { size_ns: 2_000_000 };
    wrong_pair.assigned_window = Some(WindowInterval {
        start_ns: 0,
        end_ns: 2_000_000,
    });
    assert_eq!(
        registry
            .apply_many("restore/assign", wrong_pair)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn wait_and_early_count_reject_incompatible_inputs() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let foreign = Pipeline::default()
        .from_vec("foreign", vec![1_i32])
        .unwrap();
    assert_eq!(
        main.wait_on_named("foreign-wait", &foreign)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    let session = main.session_windows_named("session", 2_000_000).unwrap();
    assert_eq!(
        session
            .early_count_trigger_named("early", 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
}
