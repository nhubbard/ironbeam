use ironbeam_core::{
    Capabilities, ErrorKind, Pipeline, Replay, SourceProgress, cogroup_by_key_3_named,
    cogroup_by_key_10_named,
};
use ironbeam_direct::{CheckpointConfig, DirectCollectExt, DirectRunner, JobConfig};

#[test]
fn keyed_group_streams_source_batches_and_preserves_resource_limit() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec(
            "group-source",
            (0..100).map(|value| (value % 4, value)).collect(),
        )
        .unwrap();
    let grouped = input
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_fold("group"));
    let (len, batch_type, _) = registry.erased_vec_source_batches("group-source").unwrap();
    assert_eq!(len, 100);
    assert!(
        registry
            .open_fold("group")
            .unwrap()
            .accepts_vec_batch(batch_type)
    );

    let config = JobConfig {
        buffer_budget_elements: 3,
        ..JobConfig::default()
    };
    let mut groups = DirectRunner.run_collect(&grouped, &config).unwrap().0;
    groups.sort_by_key(|(key, _)| *key);
    assert_eq!(groups.len(), 4);
    for (key, values) in groups {
        assert_eq!(
            values,
            (0..100)
                .filter(|value| value % 4 == key)
                .collect::<Vec<_>>()
        );
    }

    let limited = JobConfig {
        memory_budget_bytes: 1,
        ..config
    };
    assert_eq!(
        DirectRunner
            .run_collect(&grouped, &limited)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
}

#[test]
fn keyed_group_uses_envelopes_for_non_vector_sources() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "group-source-v1".into(),
            Some(4),
            || {
                Ok(Box::new(
                    [(1_i32, 10_i32), (2, 20), (1, 30), (2, 40)]
                        .into_iter()
                        .map(Ok),
                ))
            },
        )
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.erased_vec_source_batches("source").is_none());
    let mut groups = output.collect_seq().unwrap();
    groups.sort_by_key(|(key, _)| *key);
    assert_eq!(groups, [(1, vec![10, 30]), (2, vec![20, 40])]);
    let limited = JobConfig {
        memory_budget_bytes: 1,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &limited)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
}

#[test]
fn external_source_flat_map_can_drop_rows_before_grouping() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "flat-map-group-source-v1".into(),
            Some(3),
            || {
                Ok(Box::new(
                    [(1_i32, 10_i32), (0, 99), (1, 20)].into_iter().map(Ok),
                ))
            },
        )
        .unwrap()
        .flat_map_named(
            "drop-zero",
            |row| {
                if row.0 == 0 { Vec::new() } else { vec![row] }
            },
        )
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, job) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(rows, [(1, vec![10, 20])]);
    assert_eq!(job.metrics().source_elements, 3);
    assert_eq!(job.metrics().filtered_elements, 1);
}

#[test]
fn keyed_vector_batch_attributes_clone_and_fold_panics() {
    #[derive(Clone, Eq, PartialEq)]
    struct PanickingHash;
    impl Hash for PanickingHash {
        fn hash<H: Hasher>(&self, _: &mut H) {
            panic!("group hash failed")
        }
    }
    impl Element for PanickingHash {
        const TYPE_ID: &'static str = "test/group-panic-hash";
        const CODER_ID: &'static str = "test/group-panic-hash";
    }
    use ironbeam_core::Element;
    use std::hash::{Hash, Hasher};

    struct PanickingClone;
    impl Clone for PanickingClone {
        fn clone(&self) -> Self {
            panic!("source clone failed")
        }
    }
    impl Element for PanickingClone {
        const TYPE_ID: &'static str = "test/group-panic-clone";
        const CODER_ID: &'static str = "test/group-panic-clone";
    }
    let clone_output = Pipeline::default()
        .from_vec("source", vec![(1_i32, PanickingClone)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let clone_error = DirectRunner
        .run_collect(&clone_output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(clone_error.kind, ErrorKind::Execution);
    assert_eq!(clone_error.node.as_deref(), Some("source"));

    let fold_output = Pipeline::default()
        .from_vec("source", vec![(PanickingHash, 1_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let fold_error = DirectRunner
        .run_collect(&fold_output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(fold_error.kind, ErrorKind::Execution);
    assert_eq!(fold_error.node.as_deref(), Some("group"));
}

#[test]
fn keyed_group_materializes_non_linear_input_without_changing_groups() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![(1_i32, 10_i32), (2, 20)])
        .unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 30_i32)]).unwrap();
    let grouped = left
        .flatten_named("merged", &[right])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let mut groups = DirectRunner
        .run_collect(&grouped, &JobConfig::default())
        .unwrap()
        .0;
    groups.sort_by_key(|(key, _)| *key);
    assert_eq!(groups, [(1, vec![10, 30]), (2, vec![20])]);
}

#[test]
fn grouping_join_and_combine_execute_from_typed_graph() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec(
            "left",
            vec![(1_i32, "a".to_string()), (1, "b".into()), (2, "c".into())],
        )
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![(1_i32, 10_i32), (1, 20), (3, 30)])
        .unwrap();
    let grouped = left
        .group_by_key_named("group")
        .unwrap()
        .output("groups")
        .unwrap();
    let joined = left
        .join_inner_named("join", &right)
        .unwrap()
        .output("pairs")
        .unwrap();
    let summed = right
        .map_named("values", |(_, v)| v)
        .unwrap()
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("total")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph
            .validate_for("other", &Capabilities::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "group-by-key".into()
        }
    );
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            buffer_budget_elements: 2,
            ..JobConfig::default()
        };
        let (groups, grouped_job) = DirectRunner.run_collect(&grouped, &config).unwrap();
        assert_eq!(
            groups,
            vec![
                (1, vec!["a".to_string(), "b".into()]),
                (2, vec!["c".into()])
            ]
        );
        assert_eq!(grouped_job.metrics().output_elements, 2);
        assert_eq!(grouped_job.processed(), 2);
        let (pairs, joined_job) = DirectRunner.run_collect(&joined, &config).unwrap();
        assert_eq!(
            pairs,
            vec![
                (1, ("a".to_string(), 10)),
                (1, ("b".to_string(), 10)),
                (1, ("a".to_string(), 20)),
                (1, ("b".to_string(), 20))
            ]
        );
        assert_eq!(joined_job.metrics().output_elements, 4);
        assert_eq!(joined_job.processed(), 4);
        assert_eq!(
            DirectRunner.run_collect(&summed, &config).unwrap().0,
            vec![60]
        );
    }
}

#[test]
fn outer_joins_preserve_duplicates_and_missing_sides() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec(
            "left-outer",
            vec![(1_i32, "a".to_string()), (1, "b".into()), (2, "c".into())],
        )
        .unwrap();
    let right = pipeline
        .from_vec("right-outer", vec![(1_i32, 10_i32), (1, 20), (3, 30)])
        .unwrap();
    let left_out = left
        .join_left_named("left-join", &right)
        .unwrap()
        .output("left-result")
        .unwrap();
    let right_out = left
        .join_right_named("right-join", &right)
        .unwrap()
        .output("right-result")
        .unwrap();
    let full_out = left
        .join_full_named("full-join", &right)
        .unwrap()
        .output("full-result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph
            .validate_for("preview", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "join-outer".into()
        }
    );
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&left_out, &config).unwrap().0,
            vec![
                (1, ("a".into(), Some(10))),
                (1, ("b".into(), Some(10))),
                (1, ("a".into(), Some(20))),
                (1, ("b".into(), Some(20))),
                (2, ("c".into(), None))
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&right_out, &config).unwrap().0,
            vec![
                (1, (Some("a".into()), 10)),
                (1, (Some("b".into()), 10)),
                (1, (Some("a".into()), 20)),
                (1, (Some("b".into()), 20)),
                (3, (None, 30))
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&full_out, &config).unwrap().0,
            vec![
                (1, (Some("a".into()), Some(10))),
                (1, (Some("b".into()), Some(10))),
                (1, (Some("a".into()), Some(20))),
                (1, (Some("b".into()), Some(20))),
                (3, (None, Some(30))),
                (2, (Some("c".into()), None))
            ]
        );
    }
}

#[test]
fn bounded_fixed_windows_scope_group_combine_and_join() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("timed-left", vec![(1_i32, -1_i32), (1, 0), (1, 12)])
        .unwrap()
        .assign_timestamps_named("left-time", |(_, value)| i64::from(*value))
        .unwrap()
        .fixed_windows("left-window", 10)
        .unwrap();
    let right = pipeline
        .from_vec("timed-right", vec![(1_i32, -2_i32), (1, 3), (1, 20)])
        .unwrap()
        .assign_timestamps_named("right-time", |(_, value)| i64::from(*value))
        .unwrap()
        .fixed_windows("right-window", 10)
        .unwrap();
    let grouped = left
        .group_by_key_named("timed-group")
        .unwrap()
        .output("timed-group-out")
        .unwrap();
    let combined = left
        .map_named("timed-value", |(_, value)| value)
        .unwrap()
        .combine_globally_named("timed-sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("timed-sum-out")
        .unwrap();
    let joined = left
        .join_full_named("timed-join", &right)
        .unwrap()
        .output("timed-join-out")
        .unwrap();
    let config = JobConfig::default();
    assert_eq!(
        DirectRunner.run_collect(&grouped, &config).unwrap().0,
        vec![(1, vec![-1]), (1, vec![0]), (1, vec![12])]
    );
    assert_eq!(
        DirectRunner.run_collect(&combined, &config).unwrap().0,
        vec![-1, 0, 12]
    );
    assert_eq!(
        DirectRunner.run_collect(&joined, &config).unwrap().0,
        vec![
            (1, (Some(-1), Some(-2))),
            (1, (Some(0), Some(3))),
            (1, (None, Some(20))),
            (1, (Some(12), None))
        ]
    );
    assert!(left.assign_timestamps_named("too-late", |_| 0).is_err());
}

#[test]
fn multiway_cogroup_preserves_keys_values_and_empty_sides() {
    let pipeline = Pipeline::default();
    let users = pipeline
        .from_vec(
            "users",
            vec![(1_i32, "Alice".to_string()), (2, "Bob".into())],
        )
        .unwrap();
    let orders = pipeline
        .from_vec("orders", vec![(1_i32, 10_i32), (1, 20)])
        .unwrap();
    let scores = pipeline.from_vec("scores", vec![(3_i32, true)]).unwrap();
    let grouped = cogroup_by_key_3_named("co-group", &users, &orders, &scores)
        .unwrap()
        .output("co-group-out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph
            .validate_for("preview", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "co-group-by-key".into()
        }
    );
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&grouped, &config).unwrap().0,
            vec![
                (1, (vec!["Alice".into()], vec![10, 20], vec![])),
                (2, (vec!["Bob".into()], vec![], vec![])),
                (3, (vec![], vec![], vec![true]))
            ]
        );
    }
    let ten = cogroup_by_key_10_named(
        "ten", &users, &users, &users, &users, &users, &users, &users, &users, &users, &users,
    )
    .unwrap()
    .output("ten-out")
    .unwrap();
    let rows = DirectRunner
        .run_collect(&ten, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].1.9, vec!["Alice".to_string()]);
    assert_eq!(rows[1].1.8, vec!["Bob".to_string()]);
    let two = ironbeam_core::cogroup_by_key_named!("two", users, orders)
        .unwrap()
        .output("two-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&two, &JobConfig::default())
            .unwrap()
            .0,
        vec![
            (1, (vec!["Alice".into()], vec![10, 20])),
            (2, (vec!["Bob".into()], vec![]))
        ]
    );
}

#[test]
#[allow(clippy::many_single_char_names)]
fn every_public_cogroup_arity_executes_all_inputs() {
    let pipeline = Pipeline::default();
    let a = pipeline.from_vec("arity-a", vec![(1_i32, 1_i32)]).unwrap();
    let b = pipeline.from_vec("arity-b", vec![(1_i32, 2_i32)]).unwrap();
    let c = pipeline.from_vec("arity-c", vec![(1_i32, 3_i32)]).unwrap();
    let d = pipeline.from_vec("arity-d", vec![(1_i32, 4_i32)]).unwrap();
    let e = pipeline.from_vec("arity-e", vec![(1_i32, 5_i32)]).unwrap();
    let f = pipeline.from_vec("arity-f", vec![(1_i32, 6_i32)]).unwrap();
    let g = pipeline.from_vec("arity-g", vec![(1_i32, 7_i32)]).unwrap();
    let h = pipeline.from_vec("arity-h", vec![(1_i32, 8_i32)]).unwrap();
    let i = pipeline.from_vec("arity-i", vec![(1_i32, 9_i32)]).unwrap();

    macro_rules! check {
        ($key:literal, ($($input:expr),+), ($($values:expr),+)) => {{
            let output = ironbeam_core::cogroup_by_key_named!($key, $($input),+)
                .unwrap()
                .output(concat!($key, "-out"))
                .unwrap();
            for parallelism in [1, 3] {
                let rows = DirectRunner
                    .run_collect(
                        &output,
                        &JobConfig { parallelism, ..JobConfig::default() },
                    )
                    .unwrap()
                    .0;
                assert_eq!(rows, vec![(1, ($(vec![$values]),+))]);
            }
        }};
    }
    check!("arity-four", (a, b, c, d), (1, 2, 3, 4));
    check!("arity-five", (a, b, c, d, e), (1, 2, 3, 4, 5));
    check!("arity-six", (a, b, c, d, e, f), (1, 2, 3, 4, 5, 6));
    check!("arity-seven", (a, b, c, d, e, f, g), (1, 2, 3, 4, 5, 6, 7));
    check!(
        "arity-eight",
        (a, b, c, d, e, f, g, h),
        (1, 2, 3, 4, 5, 6, 7, 8)
    );
    check!(
        "arity-nine",
        (a, b, c, d, e, f, g, h, i),
        (1, 2, 3, 4, 5, 6, 7, 8, 9)
    );
}

#[test]
fn cogroup_scopes_values_by_fixed_window() {
    let pipeline = Pipeline::default();
    let first = pipeline
        .from_vec("co-first", vec![(1_i32, 1_i32), (1, 12)])
        .unwrap()
        .assign_timestamps_named("co-first-time", |(_, value)| i64::from(*value))
        .unwrap()
        .fixed_windows("co-first-window", 10)
        .unwrap();
    let second = pipeline
        .from_vec("co-second", vec![(1_i32, 2_i32), (1, 25)])
        .unwrap()
        .assign_timestamps_named("co-second-time", |(_, value)| i64::from(*value))
        .unwrap()
        .fixed_windows("co-second-window", 10)
        .unwrap();
    let output = ironbeam_core::cogroup_by_key_named!("co-window", first, second)
        .unwrap()
        .output("co-window-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![
            (1, (vec![1], vec![2])),
            (1, (vec![12], vec![])),
            (1, (vec![], vec![25]))
        ]
    );
}

#[test]
fn latest_uses_event_time_globally_per_key_and_per_window() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec(
            "latest-values",
            vec![
                (3_i32, "third".to_string()),
                (1, "first".into()),
                (3, "tie".into()),
                (12, "next".into()),
            ],
        )
        .unwrap()
        .assign_timestamps_named("latest-time", |(timestamp, _)| i64::from(*timestamp))
        .unwrap();
    let global = values
        .latest_globally_named("latest-global")
        .unwrap()
        .output("latest-global-out")
        .unwrap();
    let keyed = values
        .map_named("latest-key", |(timestamp, value)| {
            (
                i32::try_from(value.len()).expect("sample length fits i32"),
                (timestamp, value),
            )
        })
        .unwrap()
        .latest_per_key_named("latest-keyed")
        .unwrap()
        .output("latest-keyed-out")
        .unwrap();
    let windowed = values
        .fixed_windows("latest-window", 10)
        .unwrap()
        .latest_globally_named("latest-window-global")
        .unwrap()
        .output("latest-window-out")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&global, &config).unwrap().0,
            vec![(12, "next".into())]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed, &config).unwrap().0,
            vec![
                (5, (3, "third".into())),
                (3, (3, "tie".into())),
                (4, (12, "next".into()))
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&windowed, &config).unwrap().0,
            vec![(3, "third".into()), (12, "next".into())]
        );
    }
    let empty = pipeline
        .from_vec::<i32>("latest-empty", vec![])
        .unwrap()
        .latest_globally_named("latest-empty-op")
        .unwrap()
        .output("latest-empty-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&empty, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn latest_per_key_restores_selected_time_within_fixed_window() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "source",
            vec![
                (1_000_000_i64, (1_i32, 7_i32)),
                (3_000_000, (1, 9)),
                (3_000_000, (1, 10)),
                (2_000_000, (2, 8)),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("time")
        .unwrap()
        .fixed_windows("window", 4_000_000)
        .unwrap()
        .latest_per_key_named("latest")
        .unwrap()
        .reify_timestamps_named("reify")
        .unwrap()
        .with_window_named("interval")
        .unwrap()
        .output("out")
        .unwrap();
    let rows = output.collect_seq().unwrap();
    let interval = ironbeam_core::WindowInterval {
        start_ns: 0,
        end_ns: 4_000_000,
    };
    assert_eq!(
        rows,
        vec![
            (interval, (3_000_000, (1, 9))),
            (interval, (2_000_000, (2, 8))),
        ]
    );
}

#[test]
fn latest_per_key_uses_typed_timestamp_batches_and_attributes_failures() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "source",
            vec![(1_i32, 2_i32), (2, 1), (1, 5), (1, 5), (2, 4)],
        )
        .unwrap()
        .assign_timestamps_named("time", |(_, value)| i64::from(*value) * 1_000_000)
        .unwrap()
        .latest_per_key_named("latest")
        .unwrap()
        .reify_timestamps_named("result-time")
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, job) = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                buffer_budget_elements: 2,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(rows, vec![(5_000_000, (1, 5)), (4_000_000, (2, 4))]);
    assert_eq!(job.metrics().source_elements, 5);
    assert_eq!(job.metrics().batches, 3);
    let resource_error = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                memory_budget_bytes: 1,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(resource_error.kind, ErrorKind::Resource);
    assert_eq!(resource_error.node.as_deref(), Some("source"));

    let failing = Pipeline::default();
    let output = failing
        .from_vec("source", vec![(1_i32, 2_i32), (1, 5)])
        .unwrap()
        .assign_timestamps_named("time", |(_, value)| {
            assert_ne!(*value, 5, "intentional timestamp failure");
            i64::from(*value)
        })
        .unwrap()
        .latest_per_key_named("latest")
        .unwrap()
        .output("result")
        .unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("time"));
    assert_eq!(error.message, "user transform panicked");

    let cancellation = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_during_time = std::sync::Arc::clone(&cancellation);
    let cancelled_pipeline = Pipeline::default();
    let output = cancelled_pipeline
        .from_vec("source", vec![(1_i32, 2_i32), (1, 5)])
        .unwrap()
        .assign_timestamps_named("time", move |(_, value)| {
            if *value == 5 {
                cancel_during_time.store(true, std::sync::atomic::Ordering::Release);
            }
            i64::from(*value)
        })
        .unwrap()
        .latest_per_key_named("latest")
        .unwrap()
        .output("result")
        .unwrap();
    let error = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                cancellation,
                buffer_budget_elements: 2,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
}

#[test]
fn latest_per_key_recovers_a_saved_group_and_finishes_downstream_stages() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    let fail_once = Arc::new(AtomicBool::new(true));
    let timestamp_calls = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let fail = Arc::clone(&fail_once);
    let calls = Arc::clone(&timestamp_calls);
    let output = pipeline
        .from_vec(
            "source",
            vec![(1_i32, 1_i32), (2, 2), (1, 3), (2, 4), (1, 5)],
        )
        .unwrap()
        .assign_timestamps_named("time", move |(_, value)| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                *value != 3 || !fail.swap(false, Ordering::SeqCst),
                "fail after the first saved group batch"
            );
            i64::from(*value) * 1_000_000
        })
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .latest_per_key_named("latest")
        .unwrap()
        .reify_timestamps_named("reify")
        .unwrap()
        .with_window_named("interval")
        .unwrap()
        .output("result")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 2,
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            binary_build_id: "latest-recovery-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };

    let error = DirectRunner.run_collect(&output, &config).err().unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("time"));
    assert_eq!(timestamp_calls.load(Ordering::SeqCst), 3);

    let (rows, resumed) = DirectRunner.run_collect(&output, &config).unwrap();
    let interval = ironbeam_core::WindowInterval {
        start_ns: 0,
        end_ns: 10_000_000,
    };
    assert_eq!(
        rows,
        [
            (interval, (5_000_000, (1, 5))),
            (interval, (4_000_000, (2, 4)))
        ]
    );
    assert_eq!(resumed.metrics().source_elements, 3);
    assert_eq!(timestamp_calls.load(Ordering::SeqCst), 6);

    let (rows, completed) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(
        rows,
        [
            (interval, (5_000_000, (1, 5))),
            (interval, (4_000_000, (2, 4)))
        ]
    );
    assert_eq!(completed.metrics().source_elements, 0);
    assert_eq!(timestamp_calls.load(Ordering::SeqCst), 6);
}

#[test]
fn grouped_checkpoint_retries_downstream_flat_map_without_reopening_source() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let fail_once = Arc::new(AtomicBool::new(true));
    let fail = Arc::clone(&fail_once);
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, 1_i32), (1, 2), (2, 3)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .flat_map_named("expand", move |(key, values)| {
            assert!(
                !fail.swap(false, Ordering::SeqCst),
                "first downstream attempt fails"
            );
            values.into_iter().map(|value| (key, value)).collect()
        })
        .unwrap()
        .filter_named("odd", |(_, value)| value % 2 == 1)
        .unwrap()
        .output("result")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 2,
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            binary_build_id: "group-downstream-retry-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let error = DirectRunner.run_collect(&output, &config).err().unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("expand"));

    let (rows, resumed) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(rows, [(1, 1), (2, 3)]);
    assert_eq!(resumed.metrics().source_elements, 0);
    assert_eq!(resumed.metrics().filtered_elements, 1);
}

#[test]
fn windowed_combine_helpers_emit_explicit_offset_intervals() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("window-helper-source", vec![-1_i32, 1, 4, 13])
        .unwrap()
        .assign_timestamps_named("window-helper-time", |value| i64::from(*value))
        .unwrap();
    let global = input
        .combine_per_window_named("window-helper-global", 10, 3, |values| {
            values.into_iter().sum::<i32>()
        })
        .unwrap()
        .output("window-helper-global-out")
        .unwrap();
    let keyed = input
        .map_named("window-helper-key", |value| (value.rem_euclid(2), value))
        .unwrap()
        .combine_per_key_and_window_named("window-helper-keyed", 10, 3, |values| {
            values.into_iter().sum::<i32>()
        })
        .unwrap()
        .output("window-helper-keyed-out")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        let global_rows = DirectRunner.run_collect(&global, &config).unwrap().0;
        assert_eq!(
            global_rows,
            vec![
                (
                    ironbeam_core::WindowInterval {
                        start_ns: -7,
                        end_ns: 3
                    },
                    0
                ),
                (
                    ironbeam_core::WindowInterval {
                        start_ns: 3,
                        end_ns: 13
                    },
                    4
                ),
                (
                    ironbeam_core::WindowInterval {
                        start_ns: 13,
                        end_ns: 23
                    },
                    13
                )
            ]
        );
        let keyed_rows = DirectRunner.run_collect(&keyed, &config).unwrap().0;
        assert_eq!(
            keyed_rows,
            vec![
                (
                    (
                        1,
                        ironbeam_core::WindowInterval {
                            start_ns: -7,
                            end_ns: 3
                        }
                    ),
                    0
                ),
                (
                    (
                        0,
                        ironbeam_core::WindowInterval {
                            start_ns: 3,
                            end_ns: 13
                        }
                    ),
                    4
                ),
                (
                    (
                        1,
                        ironbeam_core::WindowInterval {
                            start_ns: 13,
                            end_ns: 23
                        }
                    ),
                    13
                )
            ]
        );
    }
}

#[test]
fn wait_on_drains_signal_before_data_and_reshuffle_breaks_fusion() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let pipeline = Pipeline::default();
    let ready = Arc::new(AtomicBool::new(false));
    let signal_ready = Arc::clone(&ready);
    let signal = pipeline
        .from_vec("wait-signal-source", vec![7_i32])
        .unwrap()
        .map_named("wait-signal-effect", move |value| {
            signal_ready.store(true, Ordering::SeqCst);
            value
        })
        .unwrap();
    let data_ready = Arc::clone(&ready);
    let seen = Arc::new(AtomicUsize::new(0));
    let seen_before = Arc::clone(&seen);
    let data = pipeline
        .from_vec("wait-data-source", (0_i32..20).collect())
        .unwrap()
        .map_named("wait-data-effect", move |value| {
            assert!(data_ready.load(Ordering::SeqCst));
            seen_before.fetch_add(1, Ordering::SeqCst);
            value
        })
        .unwrap();
    let seen_after = Arc::clone(&seen);
    let output = data
        .wait_on_named("wait", &signal)
        .unwrap()
        .reshuffle_named("reshuffle")
        .unwrap()
        .map_named("after-reshuffle", move |value| {
            assert_eq!(seen_after.load(Ordering::SeqCst), 20);
            value
        })
        .unwrap()
        .output("wait-out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph
            .validate_for("preview", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "wait-on".into()
        }
    );
    for parallelism in [1, 3] {
        seen.store(0, Ordering::SeqCst);
        ready.store(false, Ordering::SeqCst);
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&output, &config).unwrap().0,
            (0_i32..20).collect::<Vec<_>>()
        );
    }
}

#[test]
fn bounded_batch_helpers_keep_chunks_and_key_groups() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("batch-values", (0_i32..7).collect())
        .unwrap();
    let batches = values
        .batch_elements_named("batch-elements", 3)
        .unwrap()
        .output("batch-elements-out")
        .unwrap();
    let mapped = values
        .map_batches_named("batch-map", 3, |batch| {
            batch.iter().map(|value| value * 2).collect::<Vec<_>>()
        })
        .unwrap()
        .output("batch-map-out")
        .unwrap();
    let zero = values
        .batch_elements_named("batch-zero", 0)
        .unwrap()
        .output("batch-zero-out")
        .unwrap();
    let words = pipeline
        .from_vec(
            "batch-words",
            vec!["aa".to_string(), "bb".into(), "cccc".into(), "d".into()],
        )
        .unwrap();
    let bytes = words
        .batch_by_size_named("batch-bytes", 4, String::len)
        .unwrap()
        .output("batch-bytes-out")
        .unwrap();
    let keyed = pipeline
        .from_vec(
            "batch-keyed",
            vec![(1_i32, 1_i32), (2, 9), (1, 2), (1, 3), (2, 8)],
        )
        .unwrap();
    let keyed_batches = keyed
        .group_into_batches_named("batch-key-groups", 2)
        .unwrap()
        .output("batch-key-groups-out")
        .unwrap();
    let keyed_mapped = keyed
        .map_values_batches_named("batch-key-map", 2, |values| {
            values.iter().map(|value| value * 2).collect::<Vec<_>>()
        })
        .unwrap()
        .output("batch-key-map-out")
        .unwrap();
    let wrong_length = keyed
        .map_values_batches_named("batch-wrong-length", 2, |_| Vec::<i32>::new())
        .unwrap()
        .output("batch-wrong-length-out")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&batches, &config).unwrap().0,
            vec![vec![0, 1, 2], vec![3, 4, 5], vec![6]]
        );
        assert_eq!(
            DirectRunner.run_collect(&mapped, &config).unwrap().0,
            vec![0, 2, 4, 6, 8, 10, 12]
        );
        assert_eq!(
            DirectRunner.run_collect(&zero, &config).unwrap().0,
            (0_i32..7).map(|value| vec![value]).collect::<Vec<_>>()
        );
        assert_eq!(
            DirectRunner.run_collect(&bytes, &config).unwrap().0,
            vec![
                vec!["aa".to_string(), "bb".into()],
                vec!["cccc".into()],
                vec!["d".into()]
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_batches, &config).unwrap().0,
            vec![(1, vec![1, 2]), (1, vec![3]), (2, vec![9, 8])]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed_mapped, &config).unwrap().0,
            vec![(1, 2), (2, 18), (1, 4), (1, 6), (2, 16)]
        );
        assert_eq!(
            DirectRunner
                .run_collect(&wrong_length, &config)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
    }
}

#[test]
fn keyed_batch_map_preserves_each_event_timestamp() {
    let pipeline = Pipeline::default();
    let keyed = pipeline
        .from_vec("timed-batch-values", vec![(1_i32, 1_i32), (1, 12)])
        .unwrap()
        .assign_timestamps_named("timed-batch-stamps", |(_, value)| i64::from(*value))
        .unwrap();
    let output = keyed
        .map_values_batches_named("timed-batch-map", 2, |values| {
            values.iter().map(|value| value * 2).collect::<Vec<_>>()
        })
        .unwrap()
        .latest_per_key_named("timed-batch-latest")
        .unwrap()
        .output("timed-batch-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![(1, 24)]
    );
}

#[test]
fn lifted_combines_stream_linear_sources_with_small_memory_budget() {
    use ironbeam_core::CombineFn;
    struct Sum;
    impl CombineFn<i32, i64, i64> for Sum {
        fn create(&self) -> i64 {
            0
        }
        fn add_input(&self, accumulator: &mut i64, value: i32) {
            *accumulator += i64::from(value);
        }
        fn merge(&self, accumulator: &mut i64, other: i64) {
            *accumulator += other;
        }
        fn finish(&self, accumulator: i64) -> i64 {
            accumulator
        }
    }
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("fold-source", (0_i32..1000).collect())
        .unwrap()
        .filter_named("fold-even", |value| value % 2 == 0)
        .unwrap();
    let total = values
        .combine_globally_lifted_named("fold-total", Sum)
        .unwrap()
        .output("fold-total-out")
        .unwrap();
    let keyed = values
        .map_named("fold-key", |value| (value % 4, value))
        .unwrap()
        .combine_values_lifted_named("fold-keyed", Sum)
        .unwrap()
        .output("fold-keyed-out")
        .unwrap();
    let windowed = values
        .assign_timestamps_named("fold-time", |value| i64::from(*value))
        .unwrap()
        .fixed_windows("fold-window", 200)
        .unwrap()
        .combine_globally_lifted_named("fold-windowed", Sum)
        .unwrap()
        .output("fold-windowed-out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph
            .validate_for("preview", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "incremental-combine".into()
        }
    );
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            memory_budget_bytes: 512,
            buffer_budget_elements: 17,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&total, &config).unwrap().0,
            vec![249_500]
        );
        assert_eq!(
            DirectRunner.run_collect(&keyed, &config).unwrap().0,
            vec![(0, 124_500), (2, 125_000)]
        );
        assert_eq!(
            DirectRunner.run_collect(&windowed, &config).unwrap().0,
            vec![9_900, 29_900, 49_900, 69_900, 89_900]
        );
    }
    let empty = pipeline
        .from_vec::<i32>("fold-empty", vec![])
        .unwrap()
        .combine_globally_lifted_named("fold-empty-sum", Sum)
        .unwrap()
        .output("fold-empty-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&empty, &JobConfig::default())
            .unwrap()
            .0,
        vec![0]
    );
}

#[test]
fn lifted_combine_reports_user_panics_in_accumulation_and_finish() {
    use ironbeam_core::CombineFn;
    struct PanickingCombine {
        in_finish: bool,
    }
    impl CombineFn<i32, i32, i32> for PanickingCombine {
        fn create(&self) -> i32 {
            0
        }
        fn add_input(&self, accumulator: &mut i32, value: i32) {
            assert!(self.in_finish, "add panic");
            *accumulator += value;
        }
        fn merge(&self, accumulator: &mut i32, other: i32) {
            *accumulator += other;
        }
        fn finish(&self, accumulator: i32) -> i32 {
            assert!(!self.in_finish, "finish panic");
            accumulator
        }
    }
    for in_finish in [false, true] {
        let pipeline = Pipeline::default();
        let output = pipeline
            .from_vec("source", vec![1_i32, 2])
            .unwrap()
            .combine_globally_lifted_named("fold", PanickingCombine { in_finish })
            .unwrap()
            .output("out")
            .unwrap();
        let error = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
        assert_eq!(error.node.as_deref(), Some("fold"));
    }
}

#[test]
fn lifted_combine_accepts_a_materialized_multi_source_input() {
    use ironbeam_core::CombineFn;
    struct Sum;
    impl CombineFn<i32, i32, i32> for Sum {
        fn create(&self) -> i32 {
            0
        }
        fn add_input(&self, accumulator: &mut i32, value: i32) {
            *accumulator += value;
        }
        fn merge(&self, accumulator: &mut i32, other: i32) {
            *accumulator += other;
        }
        fn finish(&self, accumulator: i32) -> i32 {
            accumulator
        }
    }
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![1_i32, 2]).unwrap();
    let right = pipeline.from_vec("right", vec![3_i32, 4]).unwrap();
    let output = left
        .flatten_named("merged", &[right])
        .unwrap()
        .combine_globally_lifted_named("sum", Sum)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [10]
    );
}

#[test]
fn lifted_combine_stops_when_cancelled_during_accumulation() {
    use ironbeam_core::CombineFn;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct CancelOnFirst(Arc<AtomicBool>);
    impl CombineFn<i32, i32, i32> for CancelOnFirst {
        fn create(&self) -> i32 {
            0
        }
        fn add_input(&self, accumulator: &mut i32, value: i32) {
            *accumulator += value;
            self.0.store(true, Ordering::Release);
        }
        fn merge(&self, accumulator: &mut i32, other: i32) {
            *accumulator += other;
        }
        fn finish(&self, accumulator: i32) -> i32 {
            accumulator
        }
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .combine_globally_lifted_named("fold", CancelOnFirst(Arc::clone(&cancelled)))
        .unwrap()
        .output("out")
        .unwrap();
    let error = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                cancellation: cancelled,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
}

#[test]
fn completed_aggregate_checkpoint_recovers_without_reopening_source() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    let output = input
        .combine_globally_named("sum", |v| v.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let (values, first) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(values, [3]);
    assert_eq!(first.metrics().checkpoints, 1);
    assert_eq!(first.metrics().source_elements, 2);
    let (values, recovered) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(values, [3]);
    assert_eq!(recovered.metrics().source_elements, 0);
    assert_eq!(recovered.metrics().recovered_elements, 1);
    assert_eq!(recovered.processed(), 1);
    let changed = Pipeline::default();
    let changed_output = changed
        .from_vec("source", vec![1_i32, 3])
        .unwrap()
        .combine_globally_named("sum", |v| v.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&changed_output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    let marker = input
        .global_combine_marker("marker")
        .unwrap()
        .output("marker-out")
        .unwrap();
    assert!(matches!(
        DirectRunner
            .run_collect(&marker, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
}

#[test]
fn aggregate_checkpoint_rejects_non_vector_source_before_opening() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let opens = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&opens);
    let pipeline = Pipeline::default();
    let output = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "aggregate-source-v1".into(),
            Some(2),
            move || {
                counted.fetch_add(1, Ordering::Relaxed);
                Ok(Box::new([1_i32, 2].into_iter().map(Ok)))
            },
        )
        .unwrap()
        .combine_globally_named("sum", |v| v.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let error = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                checkpoint: Some(CheckpointConfig {
                    directory: dir.path().to_path_buf(),
                    binary_build_id: "non-vector".into(),
                    ..CheckpointConfig::default()
                }),
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(opens.load(Ordering::Relaxed), 0);
}

#[test]
fn completed_group_checkpoint_recovers_spilled_output() {
    use ironbeam_direct::SpillConfig;
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "source",
            (0..100_u32).map(|value| (value % 10, value)).collect(),
        )
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        memory_budget_bytes: 16 * 1024,
        spill: Some(SpillConfig {
            directory: dir.path().join("spill"),
            threshold_bytes: 64,
        }),
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().join("checkpoint"),
            binary_build_id: "group-spill".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let (mut first, job) = DirectRunner.run_collect(&output, &config).unwrap();
    first.sort_by_key(|(key, _)| *key);
    assert_eq!(job.metrics().checkpoints, 2);
    assert!(job.metrics().spill_files > 0);
    let (mut second, recovered) = DirectRunner.run_collect(&output, &config).unwrap();
    second.sort_by_key(|(key, _)| *key);
    assert_eq!(second, first);
    assert_eq!(recovered.metrics().source_elements, 0);
    assert_eq!(recovered.metrics().recovered_elements, 10);
}

#[test]
fn grouped_checkpoint_respects_batch_interval() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, 2_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            interval_batches: 2,
            binary_build_id: "group-interval-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let (groups, first) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(groups, vec![(1, vec![2])]);
    assert_eq!(first.metrics().checkpoints, 1);
    let (again, recovered) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(again, groups);
    assert_eq!(recovered.metrics().source_elements, 0);
}

#[test]
#[allow(clippy::too_many_lines)] // Recovery fixture checks both runs and the durable cursor.
fn grouped_fold_resumes_from_last_committed_batch_after_cancellation() {
    use ironbeam_core::Element;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct CloneControl {
        count: AtomicUsize,
        cancellation: Arc<AtomicBool>,
    }
    struct CancelValue {
        value: i32,
        control: Arc<CloneControl>,
    }
    impl Clone for CancelValue {
        fn clone(&self) -> Self {
            if self.control.count.fetch_add(1, Ordering::Relaxed) == 1 {
                self.control.cancellation.store(true, Ordering::Release);
            }
            Self {
                value: self.value,
                control: Arc::clone(&self.control),
            }
        }
    }
    impl Element for CancelValue {
        const TYPE_ID: &'static str = "test/group-resume-value";
        const CODER_ID: &'static str = "test/group-resume-value-i32-le";
        fn supports_coder() -> bool {
            true
        }
        fn encode(&self) -> Result<Vec<u8>, ironbeam_core::Error> {
            Ok(self.value.to_le_bytes().to_vec())
        }
        fn decode(bytes: &[u8]) -> Result<Self, ironbeam_core::Error> {
            let value = i32::decode(bytes)?;
            Ok(Self {
                value,
                control: Arc::new(CloneControl {
                    count: AtomicUsize::new(0),
                    cancellation: Arc::new(AtomicBool::new(false)),
                }),
            })
        }
    }
    let token = Arc::new(AtomicBool::new(false));
    let control = Arc::new(CloneControl {
        count: AtomicUsize::new(0),
        cancellation: Arc::clone(&token),
    });
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "source",
            (1..=3)
                .map(|value| {
                    (
                        1_i32,
                        CancelValue {
                            value,
                            control: Arc::clone(&control),
                        },
                    )
                })
                .collect(),
        )
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 1,
        cancellation: Arc::clone(&token),
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            interval_batches: 1,
            binary_build_id: "group-resume-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("latest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["kind"], "fold_state");
    assert_eq!(manifest["source_offset"], 1);
    token.store(false, Ordering::Release);
    let (groups, job) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].0, 1);
    assert_eq!(
        groups[0]
            .1
            .iter()
            .map(|value| value.value)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(job.metrics().source_elements, 2);
    assert_eq!(job.metrics().recovered_elements, 0);
    let (again, completed) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(again[0].1.len(), 3);
    assert_eq!(completed.metrics().source_elements, 0);
    assert_eq!(completed.metrics().recovered_elements, 1);
}

#[test]
fn grouped_fold_resumes_through_map_and_filter_without_replaying_committed_rows() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    let cancellation = Arc::new(AtomicBool::new(false));
    let visits = Arc::new(AtomicUsize::new(0));
    let map_cancellation = Arc::clone(&cancellation);
    let map_visits = Arc::clone(&visits);
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", (1..=4).map(|value| (1_i32, value)).collect())
        .unwrap()
        .map_named("double", move |(key, value)| {
            if map_visits.fetch_add(1, Ordering::Relaxed) == 1 {
                map_cancellation.store(true, Ordering::Release);
            }
            (key, value * 2)
        })
        .unwrap()
        .filter_named("keep", |pair| pair.1 != 6)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 1,
        cancellation: Arc::clone(&cancellation),
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            interval_batches: 1,
            binary_build_id: "staged-group-resume-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("latest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["kind"], "fold_state");
    assert_eq!(manifest["source_offset"], 1);
    cancellation.store(false, Ordering::Release);
    let (groups, resumed) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(groups, vec![(1, vec![2, 4, 8])]);
    assert_eq!(resumed.metrics().source_elements, 3);
    assert_eq!(visits.load(Ordering::Relaxed), 5);
    let (again, completed) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(again, groups);
    assert_eq!(completed.metrics().source_elements, 0);
    assert_eq!(visits.load(Ordering::Relaxed), 5);
}

#[test]
fn grouped_fold_restores_fixed_window_state_after_restart() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    let cancellation = Arc::new(AtomicBool::new(false));
    let visits = Arc::new(AtomicUsize::new(0));
    let map_cancellation = Arc::clone(&cancellation);
    let map_visits = Arc::clone(&visits);
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, 1_i32), (1, 2), (1, 11), (1, 12)])
        .unwrap()
        .map_named("mark", move |pair| {
            if map_visits.fetch_add(1, Ordering::Relaxed) == 2 {
                map_cancellation.store(true, Ordering::Release);
            }
            pair
        })
        .unwrap()
        .assign_timestamps_named("time", |(_, value)| i64::from(*value))
        .unwrap()
        .fixed_windows("window", 10)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 1,
        cancellation: Arc::clone(&cancellation),
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            interval_batches: 1,
            binary_build_id: "windowed-group-resume-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("latest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["source_offset"], 2);
    cancellation.store(false, Ordering::Release);
    let (groups, resumed) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(groups, vec![(1, vec![1, 2]), (1, vec![11, 12])]);
    assert_eq!(resumed.metrics().source_elements, 2);
    assert_eq!(visits.load(Ordering::Relaxed), 5);
}

#[test]
fn grouped_fold_restores_flat_map_outputs_after_restart() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    let cancellation = Arc::new(AtomicBool::new(false));
    let visits = Arc::new(AtomicUsize::new(0));
    let map_cancellation = Arc::clone(&cancellation);
    let map_visits = Arc::clone(&visits);
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![(1_i32, 1_i32), (1, 2)])
        .unwrap()
        .flat_map_named("duplicate", move |(key, value)| {
            if map_visits.fetch_add(1, Ordering::Relaxed) == 1 {
                map_cancellation.store(true, Ordering::Release);
            }
            vec![(key, value), (key, value * 10)]
        })
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 1,
        cancellation: Arc::clone(&cancellation),
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            interval_batches: 1,
            binary_build_id: "flat-map-group-resume-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("latest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["source_offset"], 1);
    cancellation.store(false, Ordering::Release);
    let (groups, resumed) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(groups, vec![(1, vec![1, 10, 2, 20])]);
    assert_eq!(resumed.metrics().source_elements, 1);
    assert_eq!(visits.load(Ordering::Relaxed), 3);
}

#[test]
fn bounded_side_input_is_explicit_and_executes() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32, 2, 3]).unwrap();
    let side = pipeline.from_vec("side", vec![10_i32, 20]).unwrap();
    let output = main
        .map_with_side_named("add-side", &side, |x, values| {
            x + values.iter().sum::<i32>()
        })
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("add-side").unwrap().additional_inputs[0].key,
        "side"
    );
    assert_eq!(
        graph
            .validate_for("other", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "side-input-map".into()
        }
    );
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![31, 32, 33]
    );
    assert_eq!(output.collect_seq().unwrap(), vec![31, 32, 33]);
    assert_eq!(output.collect_par(2).unwrap(), vec![31, 32, 33]);
}

#[test]
fn deferred_sink_commits_only_after_success_and_aborts_on_write_failure() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    use std::sync::{Arc, Mutex};
    struct RecordingSink {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }
    impl TypedSink<i32> for RecordingSink {
        fn write(&mut self, value: i32) -> Result<(), Error> {
            self.events.lock().unwrap().push("write");
            if self.fail && value == 2 {
                return Err(Error::io("sink", "test", "injected failure"));
            }
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            self.events.lock().unwrap().push("commit");
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.events.lock().unwrap().push("abort");
            Ok(())
        }
    }
    for fail in [false, true] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
        let cloned = events.clone();
        let sink = input
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::FinalResult,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                move || {
                    Ok(Box::new(RecordingSink {
                        events: cloned.clone(),
                        fail,
                    }))
                },
            )
            .unwrap();
        let result = DirectRunner.run_sink(&sink, &JobConfig::default());
        if fail {
            assert_eq!(result.err().unwrap().kind, ErrorKind::Io);
            assert_eq!(*events.lock().unwrap(), ["write", "write", "abort"]);
        } else {
            assert!(result.is_ok());
            assert_eq!(*events.lock().unwrap(), ["write", "write", "commit"]);
        }
    }
}

#[test]
fn aggregate_sink_runs_and_rejects_checkpoint_before_source() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    use std::sync::{Arc, Mutex};
    struct SumSink(Arc<Mutex<Vec<i32>>>);
    impl TypedSink<i32> for SumSink {
        fn write(&mut self, value: i32) -> Result<(), Error> {
            self.0.lock().unwrap().push(value);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.0.lock().unwrap().clear();
            Ok(())
        }
    }
    let pipeline = Pipeline::default();
    let count = pipeline
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap();
    let values = Arc::new(Mutex::new(Vec::new()));
    let cloned = values.clone();
    let sink = count
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            move || Ok(Box::new(SumSink(cloned.clone()))),
        )
        .unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(*values.lock().unwrap(), [6]);
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_sink(&sink, &config).err().unwrap().kind,
        ErrorKind::UnsupportedCapability {
            capability: "sink-checkpoint".into()
        }
    );
}

#[test]
fn materialized_path_handles_filter_flatten_failure_and_budget() {
    use ironbeam_core::Error;
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![1_i32, 2, 3]).unwrap();
    let right = pipeline.from_vec("right", vec![10_i32]).unwrap();
    let combined = left
        .filter_named("odd", |value| value % 2 == 1)
        .unwrap()
        .flatten_named("flat", &[right])
        .unwrap();
    let result = combined
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&result, &JobConfig::default())
            .unwrap()
            .0,
        [14]
    );
    let budget = JobConfig {
        memory_budget_bytes: 1,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&result, &budget)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );

    let pipeline = Pipeline::default();
    let input = pipeline
        .bounded_source(
            "source",
            ironbeam_core::SourceProgress {
                stable_split_id: true,
                replay: ironbeam_core::Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            None,
            || {
                Ok(Box::new(
                    vec![Err::<i32, _>(Error::io("source", "test", "read"))].into_iter(),
                ))
            },
        )
        .unwrap();
    let output = input
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );

    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .map_named("panic", |_| -> i32 { panic!("transform") })
        .unwrap()
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );

    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .combine_globally_named("panic", |_| -> i32 { panic!("aggregate") })
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn sink_commit_failures_abort_and_cancelled_job_does_not_open_sink() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct FailingCommit(Arc<AtomicUsize>);
    impl TypedSink<i32> for FailingCommit {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Err(Error::io("sink", "test", "commit failed"))
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let copy = count.clone();
    let sink = input
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            move || {
                copy.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(FailingCommit(copy.clone())))
            },
        )
        .unwrap();
    let cancelled = JobConfig {
        cancellation: Arc::new(AtomicBool::new(true)),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_sink(&sink, &cancelled).err().unwrap().kind,
        ErrorKind::Cancelled
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[test]
fn cancellation_while_writing_sink_aborts_before_commit() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct CancelOnWrite {
        cancelled: Arc<AtomicBool>,
        writes: Arc<AtomicUsize>,
        aborts: Arc<AtomicUsize>,
        commits: Arc<AtomicUsize>,
    }
    impl TypedSink<i32> for CancelOnWrite {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.cancelled.store(true, Ordering::Release);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    for rows in [1, 2] {
        let cancelled = Arc::new(AtomicBool::new(false));
        let writes = Arc::new(AtomicUsize::new(0));
        let aborts = Arc::new(AtomicUsize::new(0));
        let commits = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::default();
        let input = pipeline
            .from_vec("source", (0..rows).collect::<Vec<i32>>())
            .unwrap();
        let sink = input
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::FinalResult,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                {
                    let cancelled = Arc::clone(&cancelled);
                    let writes = Arc::clone(&writes);
                    let aborts = Arc::clone(&aborts);
                    let commits = Arc::clone(&commits);
                    move || {
                        Ok(Box::new(CancelOnWrite {
                            cancelled: Arc::clone(&cancelled),
                            writes: Arc::clone(&writes),
                            aborts: Arc::clone(&aborts),
                            commits: Arc::clone(&commits),
                        }))
                    }
                },
            )
            .unwrap();
        let error = DirectRunner
            .run_sink(
                &sink,
                &JobConfig {
                    cancellation: cancelled,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Cancelled);
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        assert_eq!(commits.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn sink_panics_and_missing_commit_contract_are_typed_errors() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    struct PanickingSink(bool);
    impl TypedSink<i32> for PanickingSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            assert!(!self.0, "write panic");
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            panic!("commit panic")
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    for write_panic in [true, false] {
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
        let sink = input
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::FinalResult,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                move || Ok(Box::new(PanickingSink(write_panic))),
            )
            .unwrap();
        assert_eq!(
            DirectRunner
                .run_sink(&sink, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
    }
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let sink = input
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            || -> Result<Box<dyn TypedSink<i32>>, Error> { panic!("factory panic") },
        )
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let sink = input
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: false,
            },
            || Ok(Box::new(PanickingSink(false))),
        )
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "sink-commit".into()
        }
    );
}

#[test]
fn aggregate_empty_input_and_mid_job_cancellation_are_defined() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", Vec::<i32>::new())
        .unwrap()
        .combine_globally_named("count", |values| i32::try_from(values.len()).unwrap())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [0]
    );
    let cancelled = Arc::new(AtomicBool::new(false));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let flag = cancelled.clone();
    let output = input
        .combine_globally_named("combine", move |values| {
            flag.store(true, Ordering::Release);
            values.into_iter().sum::<i32>()
        })
        .unwrap()
        .output("out")
        .unwrap();
    let config = JobConfig {
        cancellation: cancelled,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}

#[test]
fn durable_sink_requires_an_element_coder_before_running() {
    use ironbeam_core::{Element, Error, Replay, SinkMode, SinkSpec, TypedSink};
    #[derive(Clone)]
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "tests/opaque";
        const CODER_ID: &'static str = "tests/opaque";
    }
    struct Noop;
    impl TypedSink<Opaque> for Noop {
        fn write(&mut self, _: Opaque) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![Opaque]).unwrap();
    let sink = input
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            || Ok(Box::new(Noop)),
        )
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        spill: Some(ironbeam_direct::SpillConfig {
            directory: dir.path().to_path_buf(),
            threshold_bytes: 1,
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_sink(&sink, &config).err().unwrap().kind,
        ErrorKind::UnsupportedCapability {
            capability: "element-coder-for-durable-output".into()
        }
    );
}

#[test]
fn unrelated_aggregate_marker_does_not_block_selected_output() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    source.global_combine_marker("unrelated-marker").unwrap();
    let output = source
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [3]
    );
}

#[test]
fn source_factory_and_iterator_panics_are_typed_execution_errors() {
    use ironbeam_core::{Error, Replay, SourceProgress, TypedSourceIterator};
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: false,
    };
    for materialized in [false, true] {
        let pipeline = Pipeline::default();
        let input = pipeline
            .bounded_source::<i32>(
                "source",
                progress.clone(),
                "source-v1".into(),
                None,
                || -> Result<TypedSourceIterator<i32>, Error> { panic!("factory") },
            )
            .unwrap();
        let output = if materialized {
            input
                .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
                .unwrap()
                .output("out")
                .unwrap()
        } else {
            input.output("out").unwrap()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
        let pipeline = Pipeline::default();
        let input = pipeline
            .bounded_source::<i32>("source", progress.clone(), "source-v1".into(), None, || {
                Ok(Box::new(std::iter::from_fn(
                    || -> Option<Result<i32, Error>> { panic!("iterator") },
                )))
            })
            .unwrap();
        let output = if materialized {
            input
                .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
                .unwrap()
                .output("out")
                .unwrap()
        } else {
            input.output("out").unwrap()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
    }
}
