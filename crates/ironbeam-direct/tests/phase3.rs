use ironbeam_core::{Element, ErrorKind, Pipeline, Replay, SourceProgress};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, SpillConfig};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[test]
fn physical_plan_fuses_operations_and_batches_preserve_order() {
    let pipeline = Pipeline::default();
    let values: Vec<i32> = (0..101).collect();
    let output = pipeline
        .from_vec("source", values.clone())
        .unwrap()
        .map_named("double/v1", |x| x * 2)
        .unwrap()
        .filter_named("divisible-by-three/v1", |x| x % 3 == 0)
        .unwrap()
        .map_named("label/v1", |x| format!("{x}"))
        .unwrap()
        .output("out")
        .unwrap();
    let runner = DirectRunner;
    let config = JobConfig {
        parallelism: 4,
        buffer_budget_elements: 7,
        ..JobConfig::default()
    };
    let plan = runner.explain(&output, &config).unwrap();
    assert_eq!(plan.source_key, "source");
    assert_eq!(plan.output_key, "out");
    assert_eq!(plan.operation_count, 3);
    assert_eq!(plan.fused_stages, 1);
    assert_eq!(plan.source_len_hint, Some(101));
    assert_eq!(plan.estimated_partitions, Some(15));
    assert!(!plan.materialized);
    assert!(plan.explain().contains("Direct plan: source -> out"));
    assert!(plan.explain().contains("Estimated partitions: 15"));
    assert_eq!(
        plan.graph_fingerprint,
        pipeline.snapshot().unwrap().0.fingerprint().unwrap()
    );
    let (actual, job) = runner.run_collect(&output, &config).unwrap();
    let expected: Vec<String> = values
        .into_iter()
        .map(|x| x * 2)
        .filter(|x| x % 3 == 0)
        .map(|x| x.to_string())
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(job.metrics().source_elements, 101);
    assert_eq!(job.metrics().output_elements, expected.len() as u64);
    assert_eq!(job.metrics().filtered_elements, 101 - expected.len() as u64);
    assert_eq!(job.metrics().batches, 15);
    assert_eq!(job.metrics().peak_batch_elements, 7);
    assert_eq!(job.processed(), expected.len() as u64);
}

#[test]
fn batched_sequential_transform_panic_reports_the_failing_node() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", (0..8).collect::<Vec<i32>>())
        .unwrap()
        .map_named("first", |value| value + 1)
        .unwrap()
        .filter_named("second", |value| {
            assert!(*value != 6, "intentional transform failure");
            *value % 2 == 0
        })
        .unwrap()
        .output("result")
        .unwrap();
    for parallelism in [1, 2] {
        let error = DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism,
                    buffer_budget_elements: 3,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
        assert_eq!(error.node.as_deref(), Some("second"));
        assert_eq!(error.message, "user transform panicked");
    }
}

#[test]
fn explain_does_not_open_a_source() {
    let opened = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let output = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/counting-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "counting-source-v1".into(),
            Some(2),
            {
                let opened = Arc::clone(&opened);
                move || {
                    opened.fetch_add(1, Ordering::Relaxed);
                    Ok(Box::new([Ok(1), Ok(2)].into_iter()))
                }
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let summary = DirectRunner.explain(&output, &JobConfig::default()).unwrap();
    assert!(!summary.materialized);
    assert_eq!(summary.source_len_hint, Some(2));
    assert_eq!(opened.load(Ordering::Relaxed), 0);
    assert_eq!(DirectRunner.run_collect(&output, &JobConfig::default()).unwrap().0, [1, 2]);
    assert_eq!(opened.load(Ordering::Relaxed), 1);
}

#[test]
fn flat_map_fuses_with_elementwise_stages_and_preserves_order() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .flat_map_named("expand", |x| vec![x, -x])
        .unwrap()
        .filter_named("positive", |x| *x > 0)
        .unwrap()
        .map_named("label", |x| x.to_string())
        .unwrap()
        .output("out")
        .unwrap();
    let config = JobConfig {
        parallelism: 3,
        buffer_budget_elements: 2,
        ..JobConfig::default()
    };
    let plan = DirectRunner.explain(&output, &config).unwrap();
    assert_eq!(plan.operation_count, 3);
    assert_eq!(plan.fused_stages, 1);
    let (values, job) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(values, ["1", "2", "3"]);
    assert_eq!(job.metrics().output_elements, 3);
    assert_eq!(job.metrics().filtered_elements, 3);
}

#[test]
fn flatten_runs_branches_in_declared_order_and_rejects_checkpoint() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![1_i32, 2])
        .unwrap()
        .map_named("left-double", |x| x * 2)
        .unwrap();
    let right = pipeline.from_vec("right", vec![3_i32, 4]).unwrap();
    let output = left
        .flatten_named("merged", &[right])
        .unwrap()
        .map_named("plus-one", |x| x + 1)
        .unwrap()
        .output("out")
        .unwrap();
    let config = JobConfig {
        parallelism: 3,
        buffer_budget_elements: 1,
        ..JobConfig::default()
    };
    let summary = DirectRunner.explain(&output, &config).unwrap();
    assert_eq!(summary.routes, 2);
    assert_eq!(summary.source_len_hint, Some(4));
    assert_eq!(
        DirectRunner.run_collect(&output, &config).unwrap().0,
        vec![3, 5, 4, 5]
    );
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "build".into(),
            ..CheckpointConfig::default()
        }),
        ..config
    };
    assert!(matches!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
}

#[test]
fn spill_restores_order_and_cleans_job_files() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", (0..30_i32).collect())
        .unwrap()
        .output("out")
        .unwrap();
    let config = JobConfig {
        memory_budget_bytes: 16,
        buffer_budget_elements: 3,
        spill: Some(SpillConfig {
            directory: dir.path().to_path_buf(),
            threshold_bytes: 8,
        }),
        ..JobConfig::default()
    };
    let (values, job) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(values, (0..30).collect::<Vec<_>>());
    assert!(job.metrics().spill_files > 0);
    assert!(job.metrics().spilled_bytes > 0);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn budget_without_spill_is_typed_resource_error() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .output("out")
        .unwrap();
    let error = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                memory_budget_bytes: 4,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Resource);
    assert!(error.message.contains("memory budget"));
}

#[test]
fn checkpoint_recovers_only_committed_batches() {
    let dir = tempfile::tempdir().unwrap();
    let token = Arc::new(AtomicBool::new(false));
    let cancel_once = Arc::new(AtomicBool::new(true));
    let mapped = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", (0..6_i32).collect())
        .unwrap()
        .map_named("map/v1", {
            let token = token.clone();
            let mapped = mapped.clone();
            move |x| {
                mapped.fetch_add(1, Ordering::Relaxed);
                if x == 2 && cancel_once.swap(false, Ordering::Relaxed) {
                    token.store(true, Ordering::Release);
                }
                x * 2
            }
        })
        .unwrap()
        .output("out")
        .unwrap();
    let config = JobConfig {
        parallelism: 1,
        buffer_budget_elements: 2,
        cancellation: token.clone(),
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            interval_batches: 1,
            recover: true,
            binary_build_id: "test-build-1".into(),
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
    assert!(dir.path().join("latest.json").exists());
    token.store(false, Ordering::Release);
    let (values, job) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(values, vec![0, 2, 4, 6, 8, 10]);
    assert_eq!(job.metrics().recovered_elements, 2);
    assert_eq!(job.metrics().output_elements, 6);
    assert_eq!(job.metrics().source_elements, 4);
    assert_eq!(job.processed(), 6);
    assert_eq!(mapped.load(Ordering::Relaxed), 7);
}

#[test]
fn checkpoint_rejects_changed_source_and_corrupt_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "test-build-1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first = Pipeline::default();
    let output = first
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .output("out")
        .unwrap();
    DirectRunner.run_collect(&output, &config).unwrap();
    let mut changed_build = config.clone();
    changed_build.checkpoint.as_mut().unwrap().binary_build_id = "test-build-2".into();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &changed_build)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    let changed = Pipeline::default();
    let output = changed
        .from_vec("source", vec![1_i32, 3])
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.path().join("latest.json")).unwrap()).unwrap();
    let data = dir.path().join(manifest["data_file"].as_str().unwrap());
    fs::write(data, b"corrupt").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    let original = Pipeline::default();
    let output = original
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn checkpoint_manifest_validation_and_disabled_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .output("out")
        .unwrap();
    let mut config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "build-a".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    DirectRunner.run_collect(&output, &config).unwrap();
    let path = dir.path().join("latest.json");
    let original: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let mut changed = original.clone();
    changed["version"]["major"] = serde_json::json!(99);
    fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
    let mut changed = original.clone();
    changed["output_count"] = serde_json::json!(1);
    fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    let mut changed = original.clone();
    changed["source_offset"] = serde_json::json!(1);
    fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    let mut changed = original.clone();
    changed["source_offset"] = serde_json::json!(3);
    fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    let mut changed = original;
    changed["data_file"] = serde_json::json!("../other");
    fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    fs::write(&path, b"not json").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    config.checkpoint.as_mut().unwrap().recover = false;
    assert_eq!(
        DirectRunner.run_collect(&output, &config).unwrap().0,
        vec![1, 2]
    );
}

#[test]
fn checkpoint_saves_final_partial_interval() {
    let dir = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", (0..5_i32).collect())
        .unwrap()
        .output("out")
        .unwrap();
    let config = JobConfig {
        buffer_budget_elements: 2,
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            interval_batches: 2,
            binary_build_id: "build-a".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let (values, job) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(values, vec![0, 1, 2, 3, 4]);
    assert_eq!(job.metrics().checkpoints, 2);
    let (values, job) = DirectRunner.run_collect(&output, &config).unwrap();
    assert_eq!(values, vec![0, 1, 2, 3, 4]);
    assert_eq!(job.metrics().recovered_elements, 5);
}

#[test]
fn invalid_durable_config_and_io_failures_are_typed() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .output("out")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("occupied");
    fs::write(&file, b"file").unwrap();
    let mut config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
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
        ErrorKind::Graph
    );
    config.checkpoint.as_mut().unwrap().binary_build_id = "build-a".into();
    config.checkpoint.as_mut().unwrap().interval_batches = 0;
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    config.checkpoint.as_mut().unwrap().interval_batches = 1;
    config.checkpoint.as_mut().unwrap().directory = file.clone();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    config.checkpoint = None;
    config.spill = Some(SpillConfig {
        directory: file,
        threshold_bytes: 1,
    });
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    config.spill.as_mut().unwrap().directory = dir.path().to_path_buf();
    config.spill.as_mut().unwrap().threshold_bytes = 0;
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
}

#[test]
fn durable_output_requires_coder_and_panics_are_typed() {
    #[derive(Clone)]
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "test/opaque";
        const CODER_ID: &'static str = "test/opaque";
    }
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![Opaque])
        .unwrap()
        .output("out")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        spill: Some(SpillConfig {
            directory: dir.path().to_path_buf(),
            threshold_bytes: 1,
        }),
        ..JobConfig::default()
    };
    assert!(matches!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "test-build-1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert!(matches!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let _ = output;
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .map_named("panic/v1", |_| -> i32 { panic!("boom") })
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
