use ironbeam_core::{Element, ErrorKind, Pipeline, Replay, SourceProgress, Version};
use ironbeam_direct::{
    CheckpointConfig, DirectCollectExt, DirectRunner, JobConfig, JobHandle, JobStatus, SpillConfig,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn small_graph_runs_with_job_owned_settings() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32, 2, 3, 4]).unwrap();
    let doubled = input.map_named("double/v1", |x| x * 2).unwrap();
    let even = doubled.filter_named("even/v1", |x| x % 2 == 0).unwrap();
    let output = even.output("result").unwrap();
    let runner = DirectRunner;
    let (values, job) = runner
        .run_collect(
            &output,
            &JobConfig {
                parallelism: 1,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(values, [2, 4, 6, 8]);
    assert_eq!(job.status(), JobStatus::Succeeded);
    assert_eq!(job.processed(), 4);
    let (values, second) = runner
        .run_collect(
            &output,
            &JobConfig {
                parallelism: 3,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(values, [2, 4, 6, 8]);
    assert_ne!(job.id(), second.id());
}

#[test]
fn versioned_semantic_map_executes_with_direct() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .map_with_semantic_named(
            "custom-map",
            "example/add-five",
            Version { major: 1, minor: 0 },
            |value| value + 5,
        )
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(values, [6, 7, 8]);
}

#[test]
fn versioned_filter_and_flat_map_execute_with_direct() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("source", vec![1_i32, 2, 3, 4])
        .unwrap()
        .filter_with_semantic_named(
            "odd",
            "example/odd",
            Version { major: 1, minor: 0 },
            |value| value % 2 != 0,
        )
        .unwrap()
        .flat_map_with_semantic_named(
            "duplicate",
            "example/duplicate",
            Version { major: 1, minor: 0 },
            |value| vec![value, value],
        )
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(values, [1, 1, 3, 3]);
}

#[test]
fn typed_map_filter_route_preserves_values_and_metrics() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .from_vec("source", (0..17).collect::<Vec<i32>>())
        .unwrap();
    let mapped = source.map_named("double", |value| value * 2).unwrap();
    let filtered = mapped
        .filter_named("multiple-of-four", |value| value % 4 == 0)
        .unwrap();
    let output = filtered.output("result").unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.typed_value_transform::<i32>("double").is_some());
    assert!(
        registry
            .typed_value_transform::<i32>("multiple-of-four")
            .is_some()
    );
    assert!(registry.typed_value_transform::<String>("double").is_none());
    for parallelism in [1, 3] {
        let (values, job) = DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism,
                    buffer_budget_elements: 5,
                    ..JobConfig::default()
                },
            )
            .unwrap();
        assert_eq!(values, vec![0, 4, 8, 12, 16, 20, 24, 28, 32]);
        assert_eq!(job.metrics().source_elements, 17);
        assert_eq!(job.metrics().filtered_elements, 8);
        assert_eq!(job.metrics().output_elements, 9);
        assert_eq!(job.metrics().batches, 4);
    }
    let labels = filtered
        .map_named("label", |value| value.to_string())
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.typed_value_transform::<i32>("label").is_none());
    assert!(registry.typed_value_transform::<String>("label").is_none());
    assert_eq!(
        labels.collect_seq().unwrap(),
        vec!["0", "4", "8", "12", "16", "20", "24", "28", "32"]
    );
    let after_filter = filtered
        .map_named("after-filter", |value| value + 1)
        .unwrap();
    assert_eq!(
        after_filter.collect_seq().unwrap(),
        vec![1, 5, 9, 13, 17, 21, 25, 29, 33]
    );
}

#[test]
fn typed_filter_map_route_skips_downstream_side_effects_for_filtered_rows() {
    let mapped = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&mapped);
    let output = Pipeline::default()
        .from_vec("source", vec![1_i32, 2, 3, 4])
        .unwrap()
        .filter_named("even", |value| value % 2 == 0)
        .unwrap()
        .map_named("times-ten", move |value| {
            observed.fetch_add(1, Ordering::SeqCst);
            value * 10
        })
        .unwrap()
        .output("result")
        .unwrap();
    let (values, job) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(values, [20, 40]);
    assert_eq!(mapped.load(Ordering::SeqCst), 2);
    assert_eq!(job.metrics().filtered_elements, 2);
}

#[test]
fn typed_source_only_route_preserves_results_and_metrics() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .bounded_source::<i32>(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "test-source-v1".into(),
            Some(5),
            || Ok(Box::new((0..5).map(Ok))),
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(
        registry
            .open_typed_source::<String>("source")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        registry
            .open_typed_source::<i32>("source")
            .unwrap()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        vec![0, 1, 2, 3, 4]
    );
    let (values, job) = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                buffer_budget_elements: 2,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(values, vec![0, 1, 2, 3, 4]);
    assert_eq!(job.metrics().source_elements, 5);
    assert_eq!(job.metrics().output_elements, 5);
    assert_eq!(job.metrics().batches, 3);
    assert_eq!(job.metrics().peak_batch_elements, 2);
    assert_eq!(job.processed(), 5);
}

#[test]
fn typed_source_batch_append_respects_exact_memory_and_spill_limits() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32, 2, 3, 4, 5])
        .unwrap()
        .output("out")
        .unwrap();
    let (_, registry) = output.snapshot().unwrap();
    let (len, read_batch) = registry.vec_source_batches::<i32>("source").unwrap();
    assert_eq!(len, 5);
    assert_eq!(read_batch(1, 2).unwrap(), [2, 3]);
    assert_eq!(read_batch(5, 1).err().unwrap().kind, ErrorKind::Graph);
    assert_eq!(
        read_batch(usize::MAX, 2).err().unwrap().kind,
        ErrorKind::Graph
    );
    assert!(registry.vec_source_batches::<String>("source").is_none());
    let exact = JobConfig {
        memory_budget_bytes: 20,
        buffer_budget_elements: 2,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_collect(&output, &exact).unwrap().0,
        [1, 2, 3, 4, 5]
    );
    let too_small = JobConfig {
        memory_budget_bytes: 19,
        ..exact.clone()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &too_small)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );

    let directory = tempfile::tempdir().unwrap();
    let spilled = JobConfig {
        memory_budget_bytes: 8,
        spill: Some(SpillConfig {
            directory: directory.path().to_path_buf(),
            threshold_bytes: 8,
        }),
        ..exact
    };
    let (values, job) = DirectRunner.run_collect(&output, &spilled).unwrap();
    assert_eq!(values, [1, 2, 3, 4, 5]);
    assert!(job.metrics().spill_files > 0);

    let tuples = Pipeline::default()
        .from_vec("tuples", vec![(1_u32, true), (2_u32, false)])
        .unwrap()
        .output("tuple-out")
        .unwrap();
    let exact_tuple_budget = JobConfig {
        memory_budget_bytes: 42,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&tuples, &exact_tuple_budget)
            .unwrap()
            .0,
        [(1, true), (2, false)]
    );
    let undersized = JobConfig {
        memory_budget_bytes: 41,
        ..exact_tuple_budget
    };
    assert_eq!(
        DirectRunner
            .run_collect(&tuples, &undersized)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
}

#[test]
fn typed_transform_batch_append_preserves_budget_spill_and_filter_metrics() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", (0_i32..10).collect())
        .unwrap()
        .map_named("double", |value| value * 2)
        .unwrap()
        .filter_named("positive-multiples-of-four", |value| {
            *value > 0 && value % 4 == 0
        })
        .unwrap()
        .output("out")
        .unwrap();
    let exact = JobConfig {
        buffer_budget_elements: 3,
        memory_budget_bytes: 16,
        ..JobConfig::default()
    };
    let (values, job) = DirectRunner.run_collect(&output, &exact).unwrap();
    assert_eq!(values, [4, 8, 12, 16]);
    assert_eq!(job.metrics().filtered_elements, 6);
    assert_eq!(job.metrics().output_elements, 4);
    assert_eq!(
        DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    memory_budget_bytes: 15,
                    ..exact.clone()
                }
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );

    let directory = tempfile::tempdir().unwrap();
    let (spilled, job) = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                memory_budget_bytes: 8,
                spill: Some(SpillConfig {
                    directory: directory.path().to_path_buf(),
                    threshold_bytes: 8,
                }),
                ..exact
            },
        )
        .unwrap();
    assert_eq!(spilled, values);
    assert_eq!(job.metrics().filtered_elements, 6);
    assert!(job.metrics().spill_files > 0);
}

#[test]
fn vector_source_clone_panic_reports_the_source_node() {
    struct PanickingClone;
    impl Clone for PanickingClone {
        fn clone(&self) -> Self {
            panic!("expected source clone panic")
        }
    }
    impl Element for PanickingClone {
        const TYPE_ID: &'static str = "test/panicking-source-clone";
        const CODER_ID: &'static str = "test/panicking-source-clone";
    }
    let output = Pipeline::default()
        .from_vec("source", vec![PanickingClone])
        .unwrap()
        .output("out")
        .unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("source"));
}

#[test]
fn transformed_vector_source_clone_panic_reports_the_source_node() {
    struct PanickingClone;
    impl Clone for PanickingClone {
        fn clone(&self) -> Self {
            panic!("expected source clone panic")
        }
    }
    impl Element for PanickingClone {
        const TYPE_ID: &'static str = "test/panicking-transformed-source-clone";
        const CODER_ID: &'static str = "test/panicking-transformed-source-clone";
    }
    let output = Pipeline::default()
        .from_vec("source", vec![PanickingClone])
        .unwrap()
        .filter_named("filter", |_| true)
        .unwrap()
        .output("out")
        .unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("source"));
}

#[test]
fn vector_source_honors_cancellation_between_batches() {
    struct CancelOnClone(Arc<AtomicBool>);
    impl Clone for CancelOnClone {
        fn clone(&self) -> Self {
            self.0.store(true, Ordering::Release);
            Self(Arc::clone(&self.0))
        }
    }
    impl Element for CancelOnClone {
        const TYPE_ID: &'static str = "test/cancel-on-clone";
        const CODER_ID: &'static str = "test/cancel-on-clone";
    }
    let config = JobConfig {
        buffer_budget_elements: 1,
        ..JobConfig::default()
    };
    let output = Pipeline::default()
        .from_vec(
            "source",
            vec![
                CancelOnClone(Arc::clone(&config.cancellation)),
                CancelOnClone(Arc::clone(&config.cancellation)),
            ],
        )
        .unwrap()
        .output("out")
        .unwrap();
    let error = DirectRunner.run_collect(&output, &config).err().unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
}

#[test]
fn empty_vector_source_completes_without_a_batch() {
    let output = Pipeline::default()
        .from_vec("source", Vec::<i32>::new())
        .unwrap()
        .output("out")
        .unwrap();
    let (values, job) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(values, Vec::<i32>::new());
    assert_eq!(job.status(), JobStatus::Succeeded);
    assert_eq!(job.metrics().source_elements, 0);
    assert_eq!(job.metrics().batches, 0);
}

#[test]
fn typed_batch_append_attributes_coder_panics_to_output() {
    #[derive(Clone)]
    struct PanickingSize;
    impl Element for PanickingSize {
        const TYPE_ID: &'static str = "test/panicking-size";
        const CODER_ID: &'static str = "test/panicking-size-v1";
        fn supports_coder() -> bool {
            true
        }
        fn encode(&self) -> Result<Vec<u8>, ironbeam_core::Error> {
            Ok(vec![1])
        }
        fn encoded_len(&self) -> Result<usize, ironbeam_core::Error> {
            panic!("size panic")
        }
    }
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![PanickingSize])
        .unwrap()
        .output("out")
        .unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("out"));
    assert_eq!(error.message, "output collection panicked");
}

#[test]
fn typed_source_only_route_reports_factory_and_iterator_panics() {
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: false,
    };
    let pipeline = Pipeline::default();
    let output = pipeline
        .bounded_source::<i32>(
            "source",
            progress.clone(),
            "factory-panic-v1".into(),
            None,
            || panic!("factory panic"),
        )
        .unwrap()
        .output("result")
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
        .bounded_source::<i32>("source", progress, "iterator-panic-v1".into(), None, || {
            Ok(Box::new(std::iter::once_with(|| panic!("iterator panic"))))
        })
        .unwrap()
        .output("result")
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
fn collection_collect_repeats_without_mutating_the_graph() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("input", vec![1_i32, 2, 3, 4]).unwrap();
    let filtered = source.filter_named("even", |value| value % 2 == 0).unwrap();
    let original = pipeline.snapshot().unwrap().0.fingerprint().unwrap();
    assert_eq!(filtered.collect_seq().unwrap(), vec![2, 4]);
    assert_eq!(filtered.collect_par(2).unwrap(), vec![2, 4]);
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        buffer_budget_elements: 2,
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            binary_build_id: "detached-collect-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(filtered.collect_with(&config).unwrap().0, vec![2, 4]);
    let (recovered, job) = filtered.collect_with(&config).unwrap();
    assert_eq!(recovered, vec![2, 4]);
    assert_eq!(job.metrics().recovered_elements, 2);
    assert_eq!(
        pipeline.snapshot().unwrap().0.fingerprint().unwrap(),
        original
    );
    assert_eq!(
        filtered.output("explicit").unwrap().collect_seq().unwrap(),
        vec![2, 4]
    );
}

#[test]
fn single_output_route_preserves_filter_metrics_and_multi_output_fallback() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![1_i32, 2, 3, 4]).unwrap();
    let mapped = input.map_named("double", |value| value * 2).unwrap();
    let filtered = mapped
        .filter_named("keep-four", |value| value % 4 == 0)
        .unwrap();
    let output = filtered.output("single-result").unwrap();
    let (values, job) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(values, vec![4, 8]);
    assert_eq!(job.metrics().source_elements, 4);
    assert_eq!(job.metrics().filtered_elements, 2);
    assert_eq!(job.metrics().output_elements, 2);

    let expanded = mapped
        .flat_map_named("expand", |value| vec![value, value + 1])
        .unwrap()
        .filter_named("keep-even", |value| value % 2 == 0)
        .unwrap();
    let output = expanded.output("multi-result").unwrap();
    let (values, job) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(values, vec![2, 4, 6, 8]);
    assert_eq!(job.metrics().source_elements, 4);
    assert_eq!(job.metrics().filtered_elements, 4);
    assert_eq!(job.metrics().output_elements, 4);
}

#[test]
fn distinct_job_pools_are_visible_to_user_code() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32]).unwrap();
    let output = input
        .map_named("threads", |_| {
            u32::try_from(rayon::current_num_threads()).unwrap()
        })
        .unwrap()
        .output("result")
        .unwrap();
    let runner = DirectRunner;
    assert_eq!(
        runner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism: 1,
                    ..JobConfig::default()
                }
            )
            .unwrap()
            .0,
        [1]
    );
    assert_eq!(
        runner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism: 3,
                    ..JobConfig::default()
                }
            )
            .unwrap()
            .0,
        [3]
    );
}

#[test]
fn validation_precedes_source_side_effects() {
    #[derive(Clone)]
    struct Probe(Arc<AtomicUsize>);
    impl Element for Probe {
        const TYPE_ID: &'static str = "test/probe";
        const CODER_ID: &'static str = "test/probe-v1";
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![Probe(drops.clone())]).unwrap();
    let output = input
        .custom_named("unsupported", "vendor/custom", ironbeam_core::GRAPH_VERSION)
        .unwrap()
        .output("result")
        .unwrap();
    let err = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(
        err.kind,
        ErrorKind::UnsupportedCapability {
            capability: "vendor/custom".into()
        }
    );
    assert_eq!(drops.load(Ordering::Relaxed), 0);
}

#[test]
fn unbounded_fixture_rejected_and_long_lived_handle_cancels() {
    let p = Pipeline::default();
    let events = p
        .unbounded_source::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            -1,
        )
        .unwrap();
    events
        .map_named("map", |x| x)
        .unwrap()
        .fixed_windows("window", 100)
        .unwrap()
        .append_sink("sink", Replay::Idempotent)
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    let err = DirectRunner.validate(&graph).unwrap_err();
    assert_eq!(err.node.as_deref(), Some("events"));
    assert_eq!(
        err.kind,
        ErrorKind::UnsupportedCapability {
            capability: "unbounded-source".into()
        }
    );
    let handle = JobHandle::running_fixture();
    assert_eq!(handle.status(), JobStatus::Running);
    handle.cancel();
    handle.cancel();
    assert_eq!(handle.status(), JobStatus::Cancelled);
}

#[test]
fn registered_unbounded_reader_is_rejected_before_opening() {
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "stream-config-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |_| {
                    opens.fetch_add(1, Ordering::Relaxed);
                    Err(ironbeam_core::Error::incompatible("reader must not open"))
                }
            },
        )
        .unwrap();
    let output = source.output("out").unwrap();
    let error = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.node.as_deref(), Some("events"));
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "unbounded-source".into(),
        }
    );
    assert_eq!(opens.load(Ordering::Relaxed), 0);
}

#[test]
fn cancelled_job_never_opens_source() {
    let p = Pipeline::default();
    let output = p
        .from_vec("input", vec![1_i32])
        .unwrap()
        .output("result")
        .unwrap();
    let token = Arc::new(AtomicBool::new(true));
    let err = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                cancellation: token,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(err.kind, ErrorKind::Cancelled);
}

#[test]
fn cancellation_during_source_read_stops_typed_and_materialized_routes() {
    for materialized in [false, true] {
        let cancelled = Arc::new(AtomicBool::new(false));
        let pipeline = Pipeline::default();
        let source = pipeline
            .bounded_source::<i32>(
                "source",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: false,
                },
                "cancel-on-read-v1".into(),
                Some(2),
                {
                    let cancelled = Arc::clone(&cancelled);
                    move || {
                        let cancelled = Arc::clone(&cancelled);
                        Ok(Box::new((0..2).map(move |value| {
                            cancelled.store(true, Ordering::Release);
                            Ok(value)
                        })))
                    }
                },
            )
            .unwrap();
        let config = JobConfig {
            cancellation: cancelled,
            buffer_budget_elements: 1,
            ..JobConfig::default()
        };
        let error = if materialized {
            let output = source
                .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
                .unwrap()
                .output("out")
                .unwrap();
            DirectRunner.run_collect(&output, &config).err().unwrap()
        } else {
            let output = source
                .map_named("map", |value| value + 1)
                .unwrap()
                .output("out")
                .unwrap();
            DirectRunner.run_collect(&output, &config).err().unwrap()
        };
        assert_eq!(error.kind, ErrorKind::Cancelled);
    }
}

#[test]
fn empty_and_filtered_outputs_are_valid() {
    let p = Pipeline::default();
    let output = p
        .from_vec("input", Vec::<i32>::new())
        .unwrap()
        .map_named("map", |x| x + 1)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<i32>::new()
    );

    let p = Pipeline::default();
    let output = p
        .from_vec("input", vec![1_i32, 2])
        .unwrap()
        .filter_named("none", |_| false)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<i32>::new()
    );
}

#[test]
fn invalid_resources_are_rejected_and_bounded_windows_execute() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32]).unwrap();
    let output = input.output("out").unwrap();
    let bad = JobConfig {
        parallelism: 0,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_collect(&output, &bad).err().unwrap().kind,
        ErrorKind::Graph
    );
    for bad in [
        JobConfig {
            memory_budget_bytes: 0,
            ..JobConfig::default()
        },
        JobConfig {
            buffer_budget_elements: 0,
            ..JobConfig::default()
        },
    ] {
        assert_eq!(
            DirectRunner.run_collect(&output, &bad).err().unwrap().kind,
            ErrorKind::Graph
        );
    }
    let windowed = input
        .fixed_windows("window", 100)
        .unwrap()
        .output("window-out")
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    DirectRunner.validate(&graph).unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&windowed, &JobConfig::default())
            .unwrap()
            .0,
        vec![1]
    );
}
