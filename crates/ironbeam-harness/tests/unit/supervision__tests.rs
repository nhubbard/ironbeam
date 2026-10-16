use super::*;
use ironbeam_core::{Error, SinkMode, SinkSpec, TypedSink};
use ironbeam_io_protobuf::protobuf_fragment_sink;

struct Ordinary;

impl TypedSink<i32> for Ordinary {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn supervisor_rejects_a_sink_that_can_duplicate_on_replay() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .sink_named_with_config(
            "unsafe-append",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            "supervision-test".into(),
            || Ok(Box::new(Ordinary)),
        )
        .unwrap();
    let error = validate_pipeline(&pipeline).unwrap_err().to_string();
    assert!(
        error.contains("unsafe-append is not replay-safe"),
        "{error}"
    );
}

#[test]
fn supervisor_accepts_an_idempotent_sink() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .sink_named_with_config(
            "safe-append",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "supervision-test".into(),
            || Ok(Box::new(Ordinary)),
        )
        .unwrap();
    validate_pipeline(&pipeline).unwrap();
}

#[test]
fn supervisor_accepts_a_finalizing_sink_with_an_object_journal() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    protobuf_fragment_sink(
        &source,
        "final",
        directory.path().join("fragments"),
        "job",
        "graph",
    )
    .unwrap();

    assert!(validate_pipeline_with_object_journal(&pipeline, false)
        .unwrap_err()
        .to_string()
        .contains("needs a durable finalization journal"));
    validate_pipeline_with_object_journal(&pipeline, true).unwrap();
}

#[tokio::test]
async fn supervised_split_reaches_a_registered_bundle() {
    use beam_proto::fn_execution_v1::process_bundle_split_request::DesiredSplit;

    let runner =
        SupervisedV5Runner::new(PbdCache::new(), "test-worker".into(), &Pipeline::default())
            .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let signal = directory.path().join("split-requested");
    let (outcome, _) = watch::channel(SplitOutcome::Pending);
    runner.active.lock().unwrap().insert(
        "bundle-1".into(),
        Arc::new(ActiveBundle {
            source_id: "input".into(),
            signal: signal.clone(),
            request: directory.path().join("split-request.pb"),
            fractional_eligible: true,
            split_mode: Mutex::new(SplitMode::Unset),
            outcome,
            acknowledgment: Mutex::new(None),
        }),
    );
    runner
        .split_bundle(&ProcessBundleSplitRequest {
            instruction_id: "bundle-1".into(),
            desired_splits: HashMap::from([("input".into(), Default::default())]),
        })
        .await
        .unwrap();
    assert_eq!(std::fs::read(signal).unwrap(), b"split");
    let response = runner
        .split_bundle(&ProcessBundleSplitRequest {
            instruction_id: "bundle-1".into(),
            desired_splits: HashMap::from([(
                "input".into(),
                DesiredSplit {
                    fraction_of_remainder: 0.5,
                    ..Default::default()
                },
            )]),
        })
        .await
        .unwrap();
    assert!(response.residual_roots.is_empty());
    assert!(!directory.path().join("split-request.pb").exists());
}

#[tokio::test]
async fn supervised_fractional_split_waits_for_attempt_commit() {
    use beam_proto::fn_execution_v1::{
        process_bundle_split_request::DesiredSplit, BundleApplication,
    };

    let runner = Arc::new(
        SupervisedV5Runner::new(PbdCache::new(), "test-worker".into(), &Pipeline::default())
            .unwrap(),
    );
    let directory = tempfile::tempdir().unwrap();
    let request_path = directory.path().join("split-request.pb");
    let (outcome, _) = watch::channel(SplitOutcome::Pending);
    runner.active.lock().unwrap().insert(
        "bundle-1".into(),
        Arc::new(ActiveBundle {
            source_id: "input".into(),
            signal: directory.path().join("split"),
            request: request_path.clone(),
            fractional_eligible: true,
            split_mode: Mutex::new(SplitMode::Unset),
            outcome: outcome.clone(),
            acknowledgment: Mutex::new(None),
        }),
    );
    let request = ProcessBundleSplitRequest {
        instruction_id: "bundle-1".into(),
        desired_splits: HashMap::from([(
            "input".into(),
            DesiredSplit {
                fraction_of_remainder: 0.5,
                ..Default::default()
            },
        )]),
    };
    let task = tokio::spawn({
        let runner = Arc::clone(&runner);
        async move { runner.split_bundle(&request).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !request_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !task.is_finished(),
        "split roots escaped before bundle commit"
    );
    runner
        .split_bundle(&ProcessBundleSplitRequest {
            instruction_id: "bundle-1".into(),
            desired_splits: HashMap::from([("input".into(), Default::default())]),
        })
        .await
        .unwrap();
    assert!(!directory.path().join("split").exists());
    let committed = ProcessBundleSplitResponse {
        primary_roots: vec![BundleApplication::default()],
        ..Default::default()
    };
    let acknowledgment = runner
        .active
        .lock()
        .unwrap()
        .get("bundle-1")
        .unwrap()
        .acknowledgment
        .lock()
        .unwrap()
        .take()
        .unwrap();
    outcome.send_replace(SplitOutcome::Complete(Some(committed.clone())));
    acknowledgment.await.unwrap();
    assert_eq!(task.await.unwrap().unwrap(), committed);
}

#[tokio::test]
async fn failed_supervised_attempt_releases_no_split_roots() {
    use beam_proto::fn_execution_v1::process_bundle_split_request::DesiredSplit;

    let runner = Arc::new(
        SupervisedV5Runner::new(PbdCache::new(), "test-worker".into(), &Pipeline::default())
            .unwrap(),
    );
    let directory = tempfile::tempdir().unwrap();
    let request_path = directory.path().join("split-request.pb");
    let (outcome, _) = watch::channel(SplitOutcome::Pending);
    runner.active.lock().unwrap().insert(
        "bundle-1".into(),
        Arc::new(ActiveBundle {
            source_id: "input".into(),
            signal: directory.path().join("split"),
            request: request_path.clone(),
            fractional_eligible: true,
            split_mode: Mutex::new(SplitMode::Unset),
            outcome: outcome.clone(),
            acknowledgment: Mutex::new(None),
        }),
    );
    let request = ProcessBundleSplitRequest {
        instruction_id: "bundle-1".into(),
        desired_splits: HashMap::from([(
            "input".into(),
            DesiredSplit {
                fraction_of_remainder: 0.5,
                ..Default::default()
            },
        )]),
    };
    let task = tokio::spawn({
        let runner = Arc::clone(&runner);
        async move { runner.split_bundle(&request).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !request_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    outcome.send_replace(SplitOutcome::Complete(None));
    let response = task.await.unwrap().unwrap();
    assert!(response.primary_roots.is_empty());
    assert!(response.residual_roots.is_empty());
}

#[test]
fn cancelled_split_listener_moves_residual_to_bundle_response() {
    use beam_proto::fn_execution_v1::DelayedBundleApplication;

    let split = ProcessBundleSplitResponse {
        residual_roots: vec![DelayedBundleApplication::default()],
        ..Default::default()
    };
    let mut response = ProcessBundleResponse::default();
    assert!(route_split_roots(&mut response, Some(split.clone()), false).is_none());
    assert_eq!(response.residual_roots, split.residual_roots);

    let mut response = ProcessBundleResponse::default();
    assert_eq!(
        route_split_roots(&mut response, Some(split.clone()), true),
        Some(split)
    );
    assert!(response.residual_roots.is_empty());
}

#[tokio::test]
async fn cancelled_supervised_split_request_drops_its_acknowledgment() {
    use beam_proto::fn_execution_v1::process_bundle_split_request::DesiredSplit;

    let runner = Arc::new(
        SupervisedV5Runner::new(PbdCache::new(), "test-worker".into(), &Pipeline::default())
            .unwrap(),
    );
    let directory = tempfile::tempdir().unwrap();
    let request_path = directory.path().join("split-request.pb");
    let (outcome, _) = watch::channel(SplitOutcome::Pending);
    let active = Arc::new(ActiveBundle {
        source_id: "input".into(),
        signal: directory.path().join("split"),
        request: request_path.clone(),
        fractional_eligible: true,
        split_mode: Mutex::new(SplitMode::Unset),
        outcome,
        acknowledgment: Mutex::new(None),
    });
    runner
        .active
        .lock()
        .unwrap()
        .insert("bundle-1".into(), Arc::clone(&active));
    let request = ProcessBundleSplitRequest {
        instruction_id: "bundle-1".into(),
        desired_splits: HashMap::from([(
            "input".into(),
            DesiredSplit {
                fraction_of_remainder: 0.5,
                ..Default::default()
            },
        )]),
    };
    let task = tokio::spawn({
        let runner = Arc::clone(&runner);
        async move { runner.split_bundle(&request).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !request_path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.is_err());
    let acknowledgment = active.acknowledgment.lock().unwrap().take().unwrap();
    assert!(acknowledgment.await.is_err());
}
