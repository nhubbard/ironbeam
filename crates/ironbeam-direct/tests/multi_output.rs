use ironbeam_core::{
    Element, ErrorKind, Pipeline, Replay, SourceProgress, TypedSourceIterator, WindowInterval,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, SpillConfig};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn pair_outputs_open_shared_source_once_and_preserve_branch_results() {
    let opens = Arc::new(AtomicUsize::new(0));
    let shared_calls = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source_opens = Arc::clone(&opens);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/multi-output-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            Some(4),
            move || {
                source_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new([1, 2, 3, 4].into_iter().map(Ok)) as TypedSourceIterator<i32>)
            },
        )
        .unwrap();
    let calls = Arc::clone(&shared_calls);
    let shared = source
        .map_named("shared-add-one", move |value| {
            calls.fetch_add(1, Ordering::SeqCst);
            value + 1
        })
        .unwrap();
    let left = shared
        .map_named("double", |value| value * 2)
        .unwrap()
        .output("numbers")
        .unwrap();
    let right = shared
        .map_named("label", |value| format!("n={value}"))
        .unwrap()
        .output("labels")
        .unwrap();

    for (run_number, parallelism) in [(1, 1), (2, 3)] {
        let config = JobConfig {
            parallelism,
            buffer_budget_elements: 2,
            ..JobConfig::default()
        };
        let result = DirectRunner
            .run_collect_pair(&left, &right, &config)
            .unwrap();
        assert_eq!(result.left, [4, 6, 8, 10]);
        assert_eq!(result.right, ["n=2", "n=3", "n=4", "n=5"]);
        assert_eq!(result.job.metrics().source_elements, 4);
        assert_eq!(result.job.metrics().output_elements, 8);
        assert_eq!(opens.load(Ordering::SeqCst), run_number);
        assert_eq!(shared_calls.load(Ordering::SeqCst), run_number * 4);
    }
}

#[test]
fn pair_checkpoint_recovers_both_branches_without_rerunning_accepted_rows() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let fail_once = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let pipeline = Pipeline::default();
    let source_opens = Arc::clone(&opens);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/checkpointed-pair-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "checkpointed-pair-source-v1".into(),
            Some(4),
            move || {
                source_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new([1, 2, 3, 4].into_iter().map(Ok)) as TypedSourceIterator<i32>)
            },
        )
        .unwrap();
    let left = source
        .map_named("double", |value| value * 2)
        .unwrap()
        .output("numbers")
        .unwrap();
    let trigger = Arc::clone(&fail_once);
    let right = source
        .map_named("label", move |value| {
            if value == 3 && trigger.swap(false, Ordering::AcqRel) {
                panic!("fail after first pair checkpoint");
            }
            format!("n={value}")
        })
        .unwrap()
        .output("labels")
        .unwrap();
    let config = JobConfig {
        buffer_budget_elements: 2,
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            interval_batches: 1,
            binary_build_id: "pair-checkpoint-build-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };

    assert_eq!(
        DirectRunner
            .run_collect_pair(&left, &right, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    assert!(directory.path().join("latest.json").exists());

    let mut changed_build = config.clone();
    changed_build.checkpoint.as_mut().unwrap().binary_build_id = "different-build".into();
    assert_eq!(
        DirectRunner
            .run_collect_pair(&left, &right, &changed_build)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    assert_eq!(opens.load(Ordering::SeqCst), 1);

    let mut resumed_config = config.clone();
    resumed_config.checkpoint.as_mut().unwrap().interval_batches = 3;
    let resumed = DirectRunner
        .run_collect_pair(&left, &right, &resumed_config)
        .unwrap();
    assert_eq!(resumed.left, [2, 4, 6, 8]);
    assert_eq!(resumed.right, ["n=1", "n=2", "n=3", "n=4"]);
    assert_eq!(resumed.job.metrics().recovered_elements, 4);
    assert_eq!(resumed.job.metrics().source_elements, 2);
    assert_eq!(resumed.job.metrics().checkpoints, 1);
    assert_eq!(opens.load(Ordering::SeqCst), 2);

    let completed = DirectRunner
        .run_collect_pair(&left, &right, &resumed_config)
        .unwrap();
    assert_eq!(completed.left, [2, 4, 6, 8]);
    assert_eq!(completed.right, ["n=1", "n=2", "n=3", "n=4"]);
    assert_eq!(completed.job.metrics().recovered_elements, 8);
    assert_eq!(completed.job.metrics().source_elements, 0);
    assert_eq!(opens.load(Ordering::SeqCst), 2);
}

#[test]
fn pair_checkpoint_without_manifest_still_opens_empty_source() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source_opens = Arc::clone(&opens);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/empty-pair-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "empty-pair-source-v1".into(),
            Some(0),
            move || {
                source_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::empty()) as TypedSourceIterator<i32>)
            },
        )
        .unwrap();
    let left = source.output("left").unwrap();
    let right = source
        .map_named("format", |value| value.to_string())
        .unwrap()
        .output("right")
        .unwrap();
    let result = DirectRunner
        .run_collect_pair(
            &left,
            &right,
            &JobConfig {
                checkpoint: Some(CheckpointConfig {
                    directory: directory.path().to_path_buf(),
                    binary_build_id: "empty-pair-build-v1".into(),
                    ..CheckpointConfig::default()
                }),
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert!(result.left.is_empty());
    assert!(result.right.is_empty());
    assert_eq!(opens.load(Ordering::SeqCst), 1);
}

#[test]
fn pair_checkpoint_restores_asymmetric_spilled_branches() {
    let directory = tempfile::tempdir().unwrap();
    let spill = tempfile::tempdir().unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source_opens = Arc::clone(&opens);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/spilled-pair-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "spilled-pair-source-v1".into(),
            Some(3),
            move || {
                source_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new([1, 2, 3].into_iter().map(Ok)) as TypedSourceIterator<i32>)
            },
        )
        .unwrap();
    let left = source
        .filter_named("only-odd", |value| value % 2 == 1)
        .unwrap()
        .output("numbers")
        .unwrap();
    let right = source
        .map_named("long-label", |value| format!("value-{value:020}"))
        .unwrap()
        .output("labels")
        .unwrap();
    let config = JobConfig {
        memory_budget_bytes: 48,
        spill: Some(SpillConfig {
            directory: spill.path().to_path_buf(),
            threshold_bytes: 16,
        }),
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            binary_build_id: "spilled-pair-build-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };

    let first = DirectRunner
        .run_collect_pair(&left, &right, &config)
        .unwrap();
    assert_eq!(first.left, [1, 3]);
    assert_eq!(first.right.len(), 3);
    assert!(first.job.metrics().spill_files > 0);
    assert_eq!(opens.load(Ordering::SeqCst), 1);

    let recovered = DirectRunner
        .run_collect_pair(&left, &right, &config)
        .unwrap();
    assert_eq!(recovered.left, first.left);
    assert_eq!(recovered.right, first.right);
    assert_eq!(recovered.job.metrics().recovered_elements, 5);
    assert_eq!(recovered.job.metrics().source_elements, 0);
    assert_eq!(opens.load(Ordering::SeqCst), 1);
}

#[test]
fn pair_outputs_preserve_time_and_window_metadata_on_both_branches() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2, 3]).unwrap();
    let shared = source
        .assign_timestamps_named("stamp", |value| i64::from(*value) * 1_000_000)
        .unwrap()
        .fixed_windows_with_offset("windows", 2_000_000, 0)
        .unwrap();
    let left = shared
        .reify_timestamps_named("reify")
        .unwrap()
        .output("timestamps")
        .unwrap();
    let right = shared
        .with_window_named("window-context")
        .unwrap()
        .output("windowed")
        .unwrap();
    let result = DirectRunner
        .run_collect_pair(&left, &right, &JobConfig::default())
        .unwrap();
    assert_eq!(
        result.left,
        [(1_000_000, 1), (2_000_000, 2), (3_000_000, 3)]
    );
    assert_eq!(
        result.right,
        [
            (
                WindowInterval {
                    start_ns: 0,
                    end_ns: 2_000_000
                },
                1
            ),
            (
                WindowInterval {
                    start_ns: 2_000_000,
                    end_ns: 4_000_000
                },
                2
            ),
            (
                WindowInterval {
                    start_ns: 2_000_000,
                    end_ns: 4_000_000
                },
                3
            ),
        ]
    );
}

#[test]
fn pair_routes_filter_flat_map_and_reshuffle_independently() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2, 3, 4]).unwrap();
    let shared = source
        .filter_named("shared-even", |value| value % 2 == 0)
        .unwrap();
    let left = shared
        .flat_map_named("expand", |value| vec![value, value * 10])
        .unwrap()
        .reshuffle_named("left-shuffle")
        .unwrap()
        .output("left-out")
        .unwrap();
    let right = shared
        .filter_named("over-two", |value| *value > 2)
        .unwrap()
        .output("right-out")
        .unwrap();
    let result = DirectRunner
        .run_collect_pair(&left, &right, &JobConfig::default())
        .unwrap();
    assert_eq!(result.left, [2, 20, 4, 40]);
    assert_eq!(result.right, [4]);
    assert_eq!(result.job.metrics().filtered_elements, 3);
}

#[test]
fn pair_rejects_shared_and_branch_expansion_over_buffer_budget() {
    let config = JobConfig {
        buffer_budget_elements: 2,
        ..JobConfig::default()
    };
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let shared = source
        .flat_map_named("shared-expand", |value| vec![value; 3])
        .unwrap();
    let left = shared.output("left").unwrap();
    let right = shared
        .map_named("right-map", |value| value)
        .unwrap()
        .output("right")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect_pair(&left, &right, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let left = source
        .flat_map_named("left-expand", |value| vec![value; 3])
        .unwrap()
        .output("left")
        .unwrap();
    let right = source.output("right").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect_pair(&left, &right, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
}

#[test]
fn pair_rejects_accumulated_branch_expansion_after_shared_fanout() {
    let config = JobConfig {
        buffer_budget_elements: 2,
        parallelism: 1,
        ..JobConfig::default()
    };
    for overflowing_left in [true, false] {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
        let shared = source
            .flat_map_named("shared-fanout", |value| vec![value, value + 1])
            .unwrap();
        let expanded = shared
            .flat_map_named("branch-fanout", |value| vec![value, value * 10])
            .unwrap();
        let (left, right) = if overflowing_left {
            (
                expanded.output("left").unwrap(),
                shared.output("right").unwrap(),
            )
        } else {
            (
                shared.output("left").unwrap(),
                expanded.output("right").unwrap(),
            )
        };
        let error = DirectRunner
            .run_collect_pair(&left, &right, &config)
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Resource);
    }
}

struct Uncoded(i32);
impl Element for Uncoded {
    const TYPE_ID: &'static str = "tests/uncoded-pair";
    const CODER_ID: &'static str = "tests/uncoded-pair";
}

#[test]
fn checkpointed_pair_requires_coders_for_both_output_types() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let coded = source.output("coded").unwrap();
    let uncoded = source
        .map_named("uncoded", Uncoded)
        .unwrap()
        .output("uncoded-out")
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            binary_build_id: "uncoded-pair-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let left_error = DirectRunner
        .run_collect_pair(&uncoded, &coded, &config)
        .err()
        .unwrap();
    let right_error = DirectRunner
        .run_collect_pair(&coded, &uncoded, &config)
        .err()
        .unwrap();
    for error in [left_error, right_error] {
        assert_eq!(
            error.kind,
            ErrorKind::UnsupportedCapability {
                capability: "element-coder-for-durable-output".into()
            }
        );
    }
    assert!(!directory.path().join("latest.json").exists());
}

struct PanickingCoder(i32);
impl Element for PanickingCoder {
    const TYPE_ID: &'static str = "tests/panicking-pair-coder";
    const CODER_ID: &'static str = "tests/panicking-pair-coder";

    fn supports_coder() -> bool {
        true
    }

    fn encode(&self) -> Result<Vec<u8>, ironbeam_core::Error> {
        panic!("expected coder panic")
    }

    fn decode(_bytes: &[u8]) -> Result<Self, ironbeam_core::Error> {
        Ok(Self(0))
    }
}

struct PanickingDecoder(i32);
impl Element for PanickingDecoder {
    const TYPE_ID: &'static str = "tests/panicking-pair-decoder";
    const CODER_ID: &'static str = "tests/panicking-pair-decoder";

    fn supports_coder() -> bool {
        true
    }

    fn encode(&self) -> Result<Vec<u8>, ironbeam_core::Error> {
        self.0.encode()
    }

    fn decode(_bytes: &[u8]) -> Result<Self, ironbeam_core::Error> {
        panic!("expected decoder panic")
    }
}

#[test]
fn pair_catches_split_and_output_coder_panics() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .bounded_source_with_semantic::<PanickingCoder>(
            "source",
            "tests/panicking-pair-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "panicking-source-v1".into(),
            Some(1),
            || Ok(Box::new(std::iter::once(Ok(PanickingCoder(5))))),
        )
        .unwrap();
    let left = source
        .map_named("left", |value| value.0)
        .unwrap()
        .output("left-out")
        .unwrap();
    let right = source
        .map_named("right", |value| value.0)
        .unwrap()
        .output("right-out")
        .unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &right, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let left = source
        .map_named("broken-output", PanickingCoder)
        .unwrap()
        .output("broken-out")
        .unwrap();
    let right = source.output("right-out").unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &right, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    let left = source
        .map_named("decode-later", PanickingDecoder)
        .unwrap()
        .output("decode-out")
        .unwrap();
    let right = source.output("right-out").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = JobConfig {
        memory_budget_bytes: 8,
        spill: Some(SpillConfig {
            directory: directory.path().join("spill"),
            threshold_bytes: 1,
        }),
        ..JobConfig::default()
    };
    let error = DirectRunner
        .run_collect_pair(&left, &right, &config)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);

    let left = source.output("plain-left-out").unwrap();
    let right = source
        .map_named("decode-right-later", PanickingDecoder)
        .unwrap()
        .output("decode-right-out")
        .unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &right, &config)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
}

#[test]
fn pair_observes_cancellation_between_shared_and_branch_stages() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let config = JobConfig::default();
    let cancellation = Arc::clone(&config.cancellation);
    let shared = source
        .map_named("cancel-after-shared", move |value| {
            cancellation.store(true, Ordering::SeqCst);
            value
        })
        .unwrap();
    let left = shared
        .map_named("left", |value| value)
        .unwrap()
        .output("left-out")
        .unwrap();
    let right = shared.output("right-out").unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &right, &config)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
}

#[test]
fn pair_observes_cancellation_before_the_next_event_and_batch() {
    for batch_size in [1, 2] {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
        let config = JobConfig {
            parallelism: 1,
            buffer_budget_elements: batch_size,
            ..JobConfig::default()
        };
        let cancellation = Arc::clone(&config.cancellation);
        let left = source.output("left-out").unwrap();
        let right = source
            .map_named("cancel-after-first-event", move |value| {
                cancellation.store(true, Ordering::SeqCst);
                value
            })
            .unwrap()
            .output("right-out")
            .unwrap();
        let error = DirectRunner
            .run_collect_pair(&left, &right, &config)
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Cancelled);
    }
}

#[test]
fn pair_validation_rejects_uncoded_split_and_different_builders_before_source_access() {
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source_opens = Arc::clone(&opens);
    let source = pipeline
        .bounded_source_with_semantic::<Uncoded>(
            "source",
            "tests/uncoded-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "uncoded-source-v1".into(),
            Some(1),
            move || {
                source_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(Ok(Uncoded(7)))))
            },
        )
        .unwrap();
    let left = source
        .map_named("left", |value| value.0)
        .unwrap()
        .output("left-out")
        .unwrap();
    let right = source
        .map_named("right", |value| value.0 + 1)
        .unwrap()
        .output("right-out")
        .unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &right, &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(opens.load(Ordering::SeqCst), 0);

    let checkpointed = JobConfig {
        checkpoint: Some(CheckpointConfig {
            binary_build_id: "pair-test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let error = DirectRunner
        .run_collect_pair(&left, &right, &checkpointed)
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(opens.load(Ordering::SeqCst), 0);

    let other = Pipeline::default()
        .from_vec("other", vec![1_i32])
        .unwrap()
        .output("other-out")
        .unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &other, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Graph);
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[test]
fn pair_rejects_fan_in_and_panicking_transforms_with_typed_errors() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    let left = source.output("left-out").unwrap();
    let fan_in = source
        .flatten_named("flatten", std::slice::from_ref(&source))
        .unwrap()
        .output("fan-in-out")
        .unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &fan_in, &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));

    let panics = source
        .map_named("panic", |_value| -> i32 { panic!("expected test panic") })
        .unwrap()
        .output("panic-out")
        .unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &panics, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
}

#[test]
fn pair_rejects_different_sources_and_aggregate_routes() {
    let pipeline = Pipeline::default();
    let first = pipeline.from_vec("first", vec![1_i32, 2]).unwrap();
    let second = pipeline.from_vec("second", vec![3_i32, 4]).unwrap();
    let left = first.output("left-out").unwrap();
    let other = second.output("other-out").unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &other, &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));

    let aggregate = first
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("sum-out")
        .unwrap();
    let error = DirectRunner
        .run_collect_pair(&left, &aggregate, &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
}

#[test]
fn pair_cancellation_prevents_source_access() {
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source_opens = Arc::clone(&opens);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/cancelled-pair",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "cancelled-pair-v1".into(),
            Some(1),
            move || {
                source_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(Ok(1))))
            },
        )
        .unwrap();
    let left = source.output("left-out").unwrap();
    let right = source
        .map_named("right", |value| value + 1)
        .unwrap()
        .output("right-out")
        .unwrap();
    let config = JobConfig::default();
    config.cancellation.store(true, Ordering::SeqCst);
    let error = DirectRunner
        .run_collect_pair(&left, &right, &config)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[test]
fn pair_outputs_enforce_combined_memory_budget_and_support_spill() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", (0_u64..8).collect()).unwrap();
    let left = source
        .map_named("left", |value| value)
        .unwrap()
        .output("left-out")
        .unwrap();
    let right = source
        .map_named("right", |value| value)
        .unwrap()
        .output("right-out")
        .unwrap();
    let budget = JobConfig {
        memory_budget_bytes: 80,
        buffer_budget_elements: 2,
        ..JobConfig::default()
    };
    let error = DirectRunner
        .run_collect_pair(&left, &right, &budget)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Resource);

    let directory = tempfile::tempdir().unwrap();
    let spilling = JobConfig {
        spill: Some(SpillConfig {
            directory: directory.path().join("spill"),
            threshold_bytes: 32,
        }),
        ..budget
    };
    let result = DirectRunner
        .run_collect_pair(&left, &right, &spilling)
        .unwrap();
    assert_eq!(result.left, (0_u64..8).collect::<Vec<_>>());
    assert_eq!(result.right, result.left);
    assert!(result.job.metrics().spill_files > 0);

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![7_u64]).unwrap();
    let left = source.output("left-out").unwrap();
    let right = source
        .flat_map_named("expand-right", |value| vec![value, value + 1])
        .unwrap()
        .output("right-out")
        .unwrap();
    let asymmetric = JobConfig {
        memory_budget_bytes: 20,
        spill: Some(SpillConfig {
            directory: directory.path().join("asymmetric-spill"),
            threshold_bytes: 20,
        }),
        ..JobConfig::default()
    };
    let result = DirectRunner
        .run_collect_pair(&left, &right, &asymmetric)
        .unwrap();
    assert_eq!(result.left, [7]);
    assert_eq!(result.right, [7, 8]);
    assert!(result.job.metrics().spill_files > 0);

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![9_u64]).unwrap();
    let left = source
        .flat_map_named("expand-left", |value| vec![value, value + 1])
        .unwrap()
        .output("left-out")
        .unwrap();
    let right = source.output("right-out").unwrap();
    let result = DirectRunner
        .run_collect_pair(&left, &right, &asymmetric)
        .unwrap();
    assert_eq!(result.left, [9, 10]);
    assert_eq!(result.right, [9]);
    assert!(result.job.metrics().spill_files > 0);
}
