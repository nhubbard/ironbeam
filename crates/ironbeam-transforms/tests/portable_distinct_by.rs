use ironbeam_core::{Element, ErrorKind, Pipeline, TestStreamEvent};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::ElementwiseExt;

fn representatives(rows: Vec<(&str, i64)>) -> Vec<(i64, String)> {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "input",
            rows.into_iter()
                .map(|(value, timestamp)| (value.to_owned(), timestamp))
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .assign_timestamps_named("timestamps", |row| row.1)
        .unwrap()
        .map_named("values", |row| row.0)
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .distinct_by_per_window_named("representatives", |value| value[..1].to_owned())
        .unwrap()
        .with_window_named("window")
        .unwrap()
        .output("result")
        .unwrap();
    let (mut result, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    result.sort_by(|left, right| {
        left.0
            .start_ns
            .cmp(&right.0.start_ns)
            .then(left.1.cmp(&right.1))
    });
    result
        .into_iter()
        .map(|(window, value)| (window.start_ns, value))
        .collect()
}

#[test]
fn representative_distinct_is_windowed_and_independent_of_input_order() {
    let rows = vec![
        ("a2", 1_000_000),
        ("a1", 2_000_000),
        ("b2", 3_000_000),
        ("b1", 4_000_000),
        ("a3", 11_000_000),
    ];
    let expected = vec![
        (0, "a1".to_owned()),
        (0, "b1".to_owned()),
        (10_000_000, "a3".to_owned()),
    ];
    assert_eq!(representatives(rows.clone()), expected);
    assert_eq!(representatives(rows.into_iter().rev().collect()), expected);
}

#[derive(Clone)]
struct UncodedValue;

impl Element for UncodedValue {
    const TYPE_ID: &'static str = "test/uncoded-value";
    const CODER_ID: &'static str = "test/uncoded-value";
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct UncodedKey;

impl Element for UncodedKey {
    const TYPE_ID: &'static str = "test/uncoded-key";
    const CODER_ID: &'static str = "test/uncoded-key";
}

#[test]
fn representative_distinct_rejects_uncoded_values_and_keys_without_mutating_graph() {
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("values", vec![UncodedValue]).unwrap();
    let coded = pipeline.from_vec("coded", vec![1_i32]).unwrap();
    let before = pipeline.snapshot().unwrap().0.nodes.len();
    assert_eq!(
        values
            .distinct_by_per_window_named("bad-value", |_| 1_i32)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        coded
            .distinct_by_per_window_named("bad-key", |_| UncodedKey)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(pipeline.snapshot().unwrap().0.nodes.len(), before);
}

#[test]
fn distinct_per_window_emits_each_value_once_in_each_window() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "input",
            vec![
                ("a".to_owned(), 1_i64),
                ("a".to_owned(), 2),
                ("a".to_owned(), 12),
            ],
        )
        .unwrap()
        .assign_timestamps_named("timestamps", |row| row.1)
        .unwrap()
        .map_named("values", |row| row.0)
        .unwrap()
        .fixed_windows("windows", 10)
        .unwrap()
        .distinct_per_window_named("distinct")
        .unwrap()
        .with_window_named("window")
        .unwrap()
        .output("result")
        .unwrap();
    let (mut rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    rows.sort_by_key(|row| row.0.start_ns);
    assert_eq!(
        rows.into_iter()
            .map(|(window, value)| (window.start_ns, value))
            .collect::<Vec<_>>(),
        [(0, "a".to_owned()), (10, "a".to_owned())]
    );
}

#[test]
fn ordered_distinct_keeps_first_encounter_in_direct_runner() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "input",
            vec!["b".to_owned(), "a".to_owned(), "b".to_owned()],
        )
        .unwrap()
        .distinct_named("ordered-distinct")
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        ["b", "a"]
    );
}

#[test]
fn streaming_count_per_element_builds_a_portable_keyed_group() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "events",
            vec![
                TestStreamEvent::Elements(vec![("a".to_owned(), 1_i64)]),
                TestStreamEvent::AdvanceWatermark(10),
            ],
        )
        .unwrap()
        .fixed_windows("windows", 10)
        .unwrap()
        .count_per_element_named("counts")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    assert!(graph.node("counts/group").is_some());
}
