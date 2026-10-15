use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ironbeam_core::{
    Error, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress, TestStreamEvent,
    TimedStateResult, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll, UnboundedSource, UserTimerDomain, UserTimerRequest, Version,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};

struct RecoverySource {
    cursor: Option<Vec<u8>>,
    emitted: bool,
}

struct AdvancingRecoverySource {
    cursor: Option<Vec<u8>>,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for AdvancingRecoverySource {
    fn poll(&mut self, limit: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.cursor.as_deref() == Some(&[2][..]) {
            return Ok(UnboundedPoll::Pending);
        }
        let mut source = RecoverySource {
            cursor: self.cursor.clone(),
            emitted: false,
        };
        let polled = source.poll(limit)?;
        if let UnboundedPoll::Ready(batch) = &polled {
            self.cursor = batch.progress.checkpoint_position.clone();
        }
        Ok(polled)
    }
}

struct WallPendingSource {
    emitted: bool,
    polls: Arc<AtomicUsize>,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for WallPendingSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if self.emitted {
            return Ok(UnboundedPoll::Pending);
        }
        self.emitted = true;
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: vec![Timestamped {
                value: (5, 8),
                timestamp_ns: 0,
            }],
            progress: ProgressSignal {
                source_key: "source".into(),
                split_id: "default".into(),
                checkpoint_position: Some(vec![1]),
                event_time_ns: Some(0),
                completed: false,
            },
        }))
    }
}

impl UnboundedSource<Timestamped<(i32, i32)>> for RecoverySource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.emitted {
            return Ok(UnboundedPoll::Pending);
        }
        self.emitted = true;
        let (elements, position, watermark, completed) = match self.cursor.as_deref() {
            None => (vec![(7, 3)], 1, 0, false),
            Some([1]) => (Vec::new(), 2, 5_000_000, true),
            _ => return Err(Error::incompatible("unexpected timer source cursor")),
        };
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: elements
                .into_iter()
                .map(|value| Timestamped {
                    value,
                    timestamp_ns: 0,
                })
                .collect(),
            progress: ProgressSignal {
                source_key: "source".into(),
                split_id: "default".into(),
                checkpoint_position: Some(vec![position]),
                event_time_ns: Some(watermark),
                completed,
            },
        }))
    }
}

struct Capture {
    committed: Arc<Mutex<Vec<String>>>,
    pending: Vec<String>,
}

struct FailOnceCapture {
    committed: Arc<Mutex<Vec<String>>>,
    attempts: Arc<AtomicUsize>,
    pending: Vec<String>,
}

impl TypedSink<String> for FailOnceCapture {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(Error::execution(
                "sink",
                "test",
                "first timer commit failed",
            ));
        }
        self.committed
            .lock()
            .unwrap()
            .extend(self.pending.drain(..));
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

impl TypedSink<String> for Capture {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.committed
            .lock()
            .unwrap()
            .extend(self.pending.drain(..));
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

#[test]
fn event_timer_callback_emits_through_downstream_map() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((7_i32, 3_i32), 0)]),
                TestStreamEvent::AdvanceWatermark(5_000_000),
            ],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/event-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                assert_eq!(event.dynamic_tag, "flush");
                assert_eq!(event.fire_timestamp_ns, 5_000_000);
                assert_eq!(event.output_timestamp_ns, 5_000_000);
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![state.unwrap()],
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/timed-state-capture-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*committed.lock().unwrap(), ["7:3"]);
}

#[test]
fn processing_timer_uses_the_source_clock_and_input_hold_timestamp() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((9_i32, 4_i32), 2_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
            ],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/processing-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "process".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                assert_eq!(event.domain, UserTimerDomain::ProcessingTime);
                assert_eq!(event.output_timestamp_ns, 2_000_000);
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![state.unwrap()],
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/processing-timer-capture-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*committed.lock().unwrap(), ["9:4"]);
}

#[test]
fn armed_event_timer_and_state_resume_from_one_source_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "timed-state-recovery-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(RecoverySource {
                        cursor,
                        emitted: false,
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/recovered-event-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![state.unwrap()],
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/recovered-timer-capture-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 2,
            recover: true,
            binary_build_id: "timed-state-recovery-build".into(),
        }),
        ..JobConfig::default()
    };
    let first = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while first
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert_ne!(first.handle().status(), JobStatus::Failed);
        assert!(Instant::now() < deadline, "timer was not checkpointed");
        std::thread::sleep(Duration::from_millis(1));
    }
    first.cancel();
    assert!(first.wait().is_err());
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
    assert_eq!(*committed.lock().unwrap(), ["7:3"]);
}

#[test]
fn failed_timer_sink_commit_retries_from_armed_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "failed-timer-commit-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(AdvancingRecoverySource { cursor })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/failed-timer-commit",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![state.unwrap()],
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/failed-timer-commit-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let attempts = Arc::clone(&attempts);
                move || {
                    Ok(Box::new(FailOnceCapture {
                        committed: Arc::clone(&committed),
                        attempts: Arc::clone(&attempts),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 2,
            recover: true,
            binary_build_id: "failed-timer-commit-build".into(),
        }),
        ..JobConfig::default()
    };
    assert!(
        DirectRunner
            .start_stream_sink(&sink, &config())
            .unwrap()
            .wait()
            .is_err()
    );
    assert!(committed.lock().unwrap().is_empty());
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(*committed.lock().unwrap(), ["7:3"]);
}

#[test]
fn idle_source_fires_wall_clock_processing_timer_without_another_batch() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let polls = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "wall-pending-timer-source-v1".into(),
            0,
            {
                let polls = Arc::clone(&polls);
                move |_| {
                    Ok(Box::new(WallPendingSource {
                        emitted: false,
                        polls: Arc::clone(&polls),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/idle-processing-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, value, _, _| {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let fire_timestamp_ns = i64::try_from(now.as_nanos()).unwrap() + 50_000_000;
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "process".into(),
                        dynamic_tag: "idle".into(),
                        fire_timestamp_ns: fire_timestamp_ns - fire_timestamp_ns % 1_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                assert_eq!(event.dynamic_tag, "idle");
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![state.unwrap()],
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/idle-timer-capture-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while committed.lock().unwrap().is_empty() {
        assert_ne!(job.handle().status(), JobStatus::Failed);
        assert!(Instant::now() < deadline, "idle timer did not fire");
        std::thread::sleep(Duration::from_millis(1));
    }
    job.cancel();
    assert!(job.wait().is_err());
    assert_eq!(*committed.lock().unwrap(), ["5:8"]);
    assert!(polls.load(Ordering::SeqCst) > 1);
}

#[test]
fn clearing_one_tag_preserves_another_and_timer_callback_can_emit_many() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((3_i32, 10_i32), 0)]),
                TestStreamEvent::AdvanceWatermark(5_000_000),
            ],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/tagged-event-timers",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![
                        UserTimerRequest::Set {
                            family_id: "event".into(),
                            dynamic_tag: "drop".into(),
                            fire_timestamp_ns: 3_000_000,
                            output_timestamp_ns: None,
                        },
                        UserTimerRequest::Set {
                            family_id: "event".into(),
                            dynamic_tag: "keep".into(),
                            fire_timestamp_ns: 5_000_000,
                            output_timestamp_ns: None,
                        },
                        UserTimerRequest::Clear {
                            family_id: "event".into(),
                            dynamic_tag: "drop".into(),
                        },
                    ],
                })
            },
            |_, state, event| {
                assert_eq!(event.dynamic_tag, "keep");
                let value = state.unwrap();
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![value, value + 1],
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/tagged-timer-capture-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*committed.lock().unwrap(), ["3:10", "3:11"]);
}
