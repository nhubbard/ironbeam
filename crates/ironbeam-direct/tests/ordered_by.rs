use ironbeam_core::{ErrorKind, OrderedFoldFn, Pipeline, WindowInterval};
use ironbeam_direct::{DirectRunner, JobConfig};

struct Concatenate;

impl OrderedFoldFn<(i32, String), String, String> for Concatenate {
    fn create(&self) -> String {
        String::new()
    }

    fn add_input(&self, accumulator: &mut String, value: (i32, String)) {
        accumulator.push_str(&value.1);
    }

    fn finish(&self, accumulator: String) -> String {
        accumulator
    }
}

#[test]
fn keyed_ordered_fold_sorts_each_key_window_and_uses_first_ordered_timestamp() {
    let pipeline = Pipeline::default();
    let folded = pipeline
        .from_vec(
            "source",
            vec![
                (1_i64, (1_i32, (3_i32, "c".to_owned()))),
                (7, (1, (1, "a".to_owned()))),
                (4, (1, (2, "b".to_owned()))),
                (12, (1, (2, "y".to_owned()))),
                (11, (1, (1, "x".to_owned()))),
                (5, (2, (1, "z".to_owned()))),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("time")
        .unwrap()
        .fixed_windows("window", 10)
        .unwrap()
        .fold_values_ordered_by_named("fold", |value| value.0, Concatenate)
        .unwrap()
        .reify_timestamps_named("result-time")
        .unwrap()
        .with_window_named("result-window")
        .unwrap()
        .output("result")
        .unwrap();

    for parallelism in [1, 4] {
        let mut rows = DirectRunner
            .run_collect(
                &folded,
                &JobConfig {
                    parallelism,
                    ..JobConfig::default()
                },
            )
            .unwrap()
            .0;
        rows.sort_by_key(|(window, (_, (key, _)))| (window.start_ns, *key));
        assert_eq!(
            rows,
            vec![
                (
                    WindowInterval {
                        start_ns: 0,
                        end_ns: 10
                    },
                    (7, (1, "abc".to_owned()))
                ),
                (
                    WindowInterval {
                        start_ns: 0,
                        end_ns: 10
                    },
                    (5, (2, "z".to_owned()))
                ),
                (
                    WindowInterval {
                        start_ns: 10,
                        end_ns: 20
                    },
                    (11, (1, "xy".to_owned()))
                ),
            ]
        );
    }
}

#[test]
fn keyed_ordered_fold_rejects_duplicate_order_keys_per_key_window() {
    let pipeline = Pipeline::default();
    let folded = pipeline
        .from_vec(
            "source",
            vec![
                (1_i32, (1_i32, "a".to_owned())),
                (1_i32, (1_i32, "b".to_owned())),
            ],
        )
        .unwrap()
        .fold_values_ordered_by_named("fold", |value| value.0, Concatenate)
        .unwrap()
        .output("result")
        .unwrap();
    let error = DirectRunner
        .run_collect(&folded, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("fold"));
    assert!(error.message.contains("duplicate ordered fold key"));
}

#[test]
fn global_ordered_fold_preserves_ordered_timestamp_and_empty_identity() {
    let pipeline = Pipeline::default();
    let result = pipeline
        .from_vec(
            "source",
            vec![
                (1_i64, (3_i32, "c".to_owned())),
                (7, (1, "a".to_owned())),
                (4, (2, "b".to_owned())),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("time")
        .unwrap()
        .fold_globally_ordered_by_named("fold", |value| value.0, Concatenate)
        .unwrap()
        .reify_timestamps_named("result-time")
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&result, &JobConfig::default())
            .unwrap()
            .0,
        vec![(7, "abc".to_owned())]
    );

    let empty_pipeline = Pipeline::default();
    let empty = empty_pipeline
        .from_vec("source", Vec::<(i32, String)>::new())
        .unwrap()
        .fold_globally_ordered_by_named("fold", |value| value.0, Concatenate)
        .unwrap()
        .reify_timestamps_named("result-time")
        .unwrap()
        .output("result")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&empty, &JobConfig::default())
            .unwrap()
            .0,
        vec![(0, String::new())]
    );
}

#[test]
fn global_ordered_fold_rejects_duplicate_order_keys() {
    let pipeline = Pipeline::default();
    let result = pipeline
        .from_vec("source", vec![(1_i32, "a".to_owned()), (1, "b".to_owned())])
        .unwrap()
        .fold_globally_ordered_by_named("fold", |value| value.0, Concatenate)
        .unwrap()
        .output("result")
        .unwrap();
    let error = DirectRunner
        .run_collect(&result, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("fold"));
    assert!(error.message.contains("duplicate ordered fold key"));
}
