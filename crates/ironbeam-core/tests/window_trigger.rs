use ironbeam_core::{Pipeline, WindowTrigger};

fn triggered_fingerprint(count: u64) -> String {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .early_count_trigger_named("count-trigger", count)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("groups").unwrap().time.trigger,
        WindowTrigger::EarlyElementCount { count }
    );
    graph.fingerprint().unwrap()
}

#[test]
fn early_count_is_validated_and_changes_graph_identity() {
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("source", vec![1_i32]).unwrap();
    assert!(values.early_count_trigger_named("global", 1).is_err());
    let windowed = values.fixed_windows("window", 10_000_000).unwrap();
    assert!(windowed.early_count_trigger_named("zero", 0).is_err());
    assert!(
        windowed
            .early_count_trigger_named("overflow", i32::MAX as u64 + 1)
            .is_err()
    );
    assert_ne!(triggered_fingerprint(1), triggered_fingerprint(2));
}

#[test]
fn default_trigger_keeps_legacy_time_json_shape() {
    let time = ironbeam_core::Time::default();
    let json = serde_json::to_string(&time).unwrap();
    assert!(!json.contains("trigger"));
    assert_eq!(
        serde_json::from_str::<ironbeam_core::Time>(&json).unwrap(),
        time
    );
}
