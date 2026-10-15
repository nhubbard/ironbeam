use ironbeam_core::Pipeline;
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
