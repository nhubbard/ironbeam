use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
    UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use std::collections::HashSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

struct OneDelivery(bool);

impl UnboundedSource<Timestamped<i32>> for OneDelivery {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        if std::mem::replace(&mut self.0, true) {
            return Ok(UnboundedPoll::Pending);
        }
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: vec![Timestamped {
                value: 7,
                timestamp_ns: 1_000_000,
            }],
            progress: ProgressSignal {
                source_key: "source".into(),
                split_id: "subscription".into(),
                checkpoint_position: Some(vec![7]),
                event_time_ns: Some(1_000_000),
                completed: true,
            },
        }))
    }
}

struct CommitLog {
    name: &'static str,
    events: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    fail_once: Arc<AtomicBool>,
    bundle: Option<String>,
}

impl TypedSink<i32> for CommitLog {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        self.bundle = Some(id.into());
        Ok(())
    }

    fn write(&mut self, _: i32) -> Result<(), Error> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if self.fail_once.swap(false, Ordering::SeqCst) {
            self.events
                .lock()
                .unwrap()
                .push(format!("fail-{}", self.name));
            return Err(Error::execution(
                self.name,
                "test",
                "injected commit failure",
            ));
        }
        let id = self.bundle.take().expect("bundle began");
        if self.accepted.lock().unwrap().insert(id) {
            self.events
                .lock()
                .unwrap()
                .push(format!("commit-{}", self.name));
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

fn sink(
    values: &ironbeam_core::PCollection<i32>,
    key: &'static str,
    events: &Arc<Mutex<Vec<String>>>,
    fail_once: bool,
) -> ironbeam_core::Sink<i32> {
    let events = Arc::clone(events);
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let failure = Arc::new(AtomicBool::new(fail_once));
    values
        .sink_named_with_config(
            key,
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("source-finalization-{key}-v1"),
            move || {
                Ok(Box::new(CommitLog {
                    name: key,
                    events: Arc::clone(&events),
                    accepted: Arc::clone(&accepted),
                    fail_once: Arc::clone(&failure),
                    bundle: None,
                }))
            },
        )
        .unwrap()
}

#[test]
fn source_finalization_follows_single_sink_commit() {
    let directory = tempfile::tempdir().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let callback_events = Arc::clone(&events);
    let values = pipeline
        .unbounded_source_with_finalizer(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "one-delivery-v1".into(),
            0,
            |_| Ok(Box::new(OneDelivery(false)) as TypedUnboundedSourceReader<i32>),
            move |progress| {
                callback_events.lock().unwrap().push(format!(
                    "ack-{}",
                    progress.checkpoint_position.as_ref().unwrap()[0]
                ));
                Ok(())
            },
        )
        .unwrap();
    let sink = sink(&values, "sink", &events, false);
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "source-finalization-single".into(),
        }),
        ..JobConfig::default()
    };
    DirectRunner
        .start_stream_sink(&sink, &config)
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(events.lock().unwrap().as_slice(), ["commit-sink", "ack-7"]);
}

#[test]
fn source_finalization_requires_durable_checkpoint() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let values = pipeline
        .unbounded_source_with_finalizer(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "one-delivery-v1".into(),
            0,
            |_| Ok(Box::new(OneDelivery(false)) as TypedUnboundedSourceReader<i32>),
            |_| Ok(()),
        )
        .unwrap();
    let sink = sink(&values, "sink", &events, false);
    let error = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .err()
        .expect("finalized source needs a checkpoint");
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "source-finalization-checkpoint".into(),
        }
    );
    assert!(events.lock().unwrap().is_empty());
}

#[test]
fn failed_finalizer_is_retried_from_completed_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let fail_once = Arc::new(AtomicBool::new(true));
    let callback_fail_once = Arc::clone(&fail_once);
    let callback_events = Arc::clone(&events);
    let pipeline = Pipeline::default();
    let values = pipeline
        .unbounded_source_with_finalizer(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "one-delivery-v1".into(),
            0,
            |_| Ok(Box::new(OneDelivery(false)) as TypedUnboundedSourceReader<i32>),
            move |_| {
                if callback_fail_once.swap(false, Ordering::SeqCst) {
                    callback_events.lock().unwrap().push("ack-failed".into());
                    return Err(Error::execution("source", "test", "injected ack failure"));
                }
                callback_events.lock().unwrap().push("ack-succeeded".into());
                Ok(())
            },
        )
        .unwrap();
    let sink = sink(&values, "sink", &events, false);
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "source-finalization-retry".into(),
        }),
        ..JobConfig::default()
    };
    assert!(
        DirectRunner
            .start_stream_sink(&sink, &config)
            .unwrap()
            .wait()
            .is_err()
    );
    DirectRunner
        .start_stream_sink(&sink, &config)
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["commit-sink", "ack-failed", "ack-succeeded"]
    );
}

#[test]
fn source_finalization_waits_for_both_fanout_sinks_and_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let callback_events = Arc::clone(&events);
    let values = pipeline
        .unbounded_source_with_finalizer(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "one-delivery-v1".into(),
            0,
            |_| Ok(Box::new(OneDelivery(false)) as TypedUnboundedSourceReader<i32>),
            move |progress| {
                callback_events.lock().unwrap().push(format!(
                    "ack-{}",
                    progress.checkpoint_position.as_ref().unwrap()[0]
                ));
                Ok(())
            },
        )
        .unwrap();
    let left = sink(
        &values.map_named("left", |value| value + 1).unwrap(),
        "left-sink",
        &events,
        false,
    );
    let right = sink(
        &values.map_named("right", |value| value + 2).unwrap(),
        "right-sink",
        &events,
        true,
    );
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "source-finalization-build".into(),
        }),
        ..JobConfig::default()
    };
    let failure = DirectRunner
        .start_stream_sink_pair(&left, &right, &config)
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(failure.kind, ErrorKind::Execution);
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["commit-left-sink", "fail-right-sink"]
    );
    DirectRunner
        .start_stream_sink_pair(&left, &right, &config)
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "commit-left-sink",
            "fail-right-sink",
            "commit-right-sink",
            "ack-7",
        ]
    );
}
