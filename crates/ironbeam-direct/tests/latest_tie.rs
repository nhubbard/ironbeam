use ironbeam_core::{ErrorKind, Pipeline, WindowInterval};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn keyed_latest_tie_key_selects_same_value_and_metadata_with_one_or_many_workers() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "source",
            vec![
                (9_i64, (1_i32, (1_i32, "a".to_owned()))),
                (9, (1, (3, "c".to_owned()))),
                (4, (1, (8, "old".to_owned()))),
                (12, (1, (2, "next".to_owned()))),
                (9, (2, (2, "other".to_owned()))),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("time")
        .unwrap()
        .fixed_windows("windows", 10)
        .unwrap()
        .latest_per_key_by_named("latest", |value| value.0)
        .unwrap()
        .reify_timestamps_named("result-time")
        .unwrap()
        .with_window_named("result-window")
        .unwrap()
        .output("result")
        .unwrap();
    for parallelism in [1, 4] {
        let mut actual = DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism,
                    ..JobConfig::default()
                },
            )
            .unwrap()
            .0;
        actual.sort_by_key(|(window, (_, (key, _)))| (window.start_ns, *key));
        assert_eq!(
            actual,
            vec![
                (
                    WindowInterval {
                        start_ns: 0,
                        end_ns: 10
                    },
                    (9, (1, (3, "c".to_owned())))
                ),
                (
                    WindowInterval {
                        start_ns: 0,
                        end_ns: 10
                    },
                    (9, (2, (2, "other".to_owned())))
                ),
                (
                    WindowInterval {
                        start_ns: 10,
                        end_ns: 20
                    },
                    (12, (1, (2, "next".to_owned())))
                ),
            ]
        );
    }
}

#[test]
fn global_latest_tie_key_selects_greatest_key_and_preserves_window() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "source",
            vec![
                (9_i64, (1_i32, "a".to_owned())),
                (9, (3, "c".to_owned())),
                (4, (8, "old".to_owned())),
                (12, (2, "next".to_owned())),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("time")
        .unwrap()
        .fixed_windows("windows", 10)
        .unwrap()
        .latest_globally_by_named("latest", |value| value.0)
        .unwrap()
        .reify_timestamps_named("result-time")
        .unwrap()
        .with_window_named("result-window")
        .unwrap()
        .output("result")
        .unwrap();
    let mut actual = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    actual.sort_by_key(|(window, _)| window.start_ns);
    assert_eq!(
        actual,
        vec![
            (
                WindowInterval {
                    start_ns: 0,
                    end_ns: 10
                },
                (9, (3, "c".to_owned()))
            ),
            (
                WindowInterval {
                    start_ns: 10,
                    end_ns: 20
                },
                (12, (2, "next".to_owned()))
            ),
        ]
    );
}

#[test]
fn repeated_latest_tie_key_fails_in_keyed_and_global_variants() {
    let keyed_pipeline = Pipeline::default();
    let keyed = keyed_pipeline
        .from_vec(
            "source",
            vec![
                (9_i64, (1_i32, (2_i32, "a".to_owned()))),
                (9, (1, (2, "b".to_owned()))),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("time")
        .unwrap()
        .latest_per_key_by_named("latest", |value| value.0)
        .unwrap()
        .output("result")
        .unwrap();
    let global_pipeline = Pipeline::default();
    let global = global_pipeline
        .from_vec(
            "source",
            vec![(9_i64, (2_i32, "a".to_owned())), (9, (2, "b".to_owned()))],
        )
        .unwrap()
        .timestamps_from_pairs_named("time")
        .unwrap()
        .latest_globally_by_named("latest", |value| value.0)
        .unwrap()
        .output("result")
        .unwrap();
    for result in [
        DirectRunner
            .run_collect(&keyed, &JobConfig::default())
            .err()
            .unwrap(),
        DirectRunner
            .run_collect(&global, &JobConfig::default())
            .err()
            .unwrap(),
    ] {
        assert_eq!(result.kind, ErrorKind::Execution);
        assert_eq!(result.node.as_deref(), Some("latest"));
        assert!(result.message.contains("duplicate latest tie key"));
    }
}
