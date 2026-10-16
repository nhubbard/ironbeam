use std::collections::{BTreeMap, HashSet};
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

struct SlidingTimerRecoverySource {
    cursor: Option<Vec<u8>>,
    emitted: bool,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for SlidingTimerRecoverySource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.emitted {
            return Ok(UnboundedPoll::Pending);
        }
        self.emitted = true;
        let (elements, position, watermark_ns, completed) = match self.cursor.as_deref() {
            None => (
                vec![
                    Timestamped {
                        value: (1, 3),
                        timestamp_ns: 7_000_000,
                    },
                    Timestamped {
                        value: (1, 4),
                        timestamp_ns: 8_000_000,
                    },
                ],
                1,
                0,
                false,
            ),
            Some([1]) => (Vec::new(), 2, 20_000_000, true),
            _ => {
                return Err(Error::incompatible(
                    "unexpected sliding timer source cursor",
                ));
            }
        };
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements,
            progress: ProgressSignal {
                source_key: "source".into(),
                split_id: "default".into(),
                checkpoint_position: Some(vec![position]),
                event_time_ns: Some(watermark_ns),
                completed,
            },
        }))
    }
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
            self.cursor.clone_from(&batch.progress.checkpoint_position);
        }
        Ok(polled)
    }
}

struct WallPendingSource {
    emitted: bool,
    polls: Arc<AtomicUsize>,
}

struct PendingRecoverySource {
    cursor: Option<Vec<u8>>,
    emitted: bool,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for PendingRecoverySource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.cursor.is_some() || self.emitted {
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

struct BundleIdempotentCapture {
    committed: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    bundle: Option<String>,
    pending: Vec<String>,
}

impl TypedSink<String> for BundleIdempotentCapture {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.bundle = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: String) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let bundle = self.bundle.take().unwrap();
        if self.accepted.lock().unwrap().insert(bundle) {
            self.committed
                .lock()
                .unwrap()
                .extend(self.pending.drain(..));
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

struct TimerDeliverySink {
    mode: u8,
}

impl TypedSink<String> for TimerDeliverySink {
    fn write(&mut self, _: String) -> Result<(), Error> {
        assert_ne!(self.mode, 0, "injected timer writer panic");
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        assert_ne!(self.mode, 1, "injected timer commit panic");
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
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
fn fixed_windows_keep_same_key_and_timer_tag_isolated() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((7_i32, 3_i32), 1_000_000)]),
                TestStreamEvent::Elements(vec![((7_i32, 4_i32), 11_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/windowed-event-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, metadata| {
                let window = metadata.assigned_window.unwrap();
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: window.end_ns,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                let window = event.window.unwrap();
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![
                        state.unwrap() + i32::try_from(window.start_ns / 1_000_000).unwrap(),
                    ],
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
            "tests/windowed-timer-capture-v1".into(),
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
    assert_eq!(*committed.lock().unwrap(), ["7:3", "7:14"]);
}

#[test]
fn shared_event_timer_fires_once_into_both_stream_sinks() {
    let first_rows = Arc::new(Mutex::new(Vec::new()));
    let second_rows = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let shared = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((7_i32, 3_i32), 0)]),
                TestStreamEvent::Elements(vec![((7_i32, 4_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(5_000_000),
            ],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "shared-timer",
            "tests/shared-event-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, state, _| {
                Ok(TimedStateResult {
                    state: Some(state.unwrap_or(0) + value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: Some(1_000_000),
                    }],
                })
            },
            |_, state, event| {
                assert_eq!(event.output_timestamp_ns, 1_000_000);
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap();
    let first = shared
        .sink_named_with_config(
            "first",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-timer-first-v1".into(),
            {
                let committed = Arc::clone(&first_rows);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let second = shared
        .map_named("copy-label", |label| format!("copy:{label}"))
        .unwrap()
        .sink_named_with_config(
            "second",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-timer-second-v1".into(),
            {
                let committed = Arc::clone(&second_rows);
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
        .start_stream_sink_pair(&first, &second, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*first_rows.lock().unwrap(), ["7:7"]);
    assert_eq!(*second_rows.lock().unwrap(), ["copy:7:7"]);
}

#[test]
fn window_expiration_callback_sees_each_key_and_window_before_state_cleanup() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((7_i32, 3_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(9_000_000),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((7_i32, 4_i32), 11_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .stateful_timer_map_with_window_expiration_named::<i32, i32>(
            "timer",
            "tests/window-expiration-callback",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("expiry".into(), UserTimerDomain::EventTime)]),
            "expiry",
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
            |_, state, event| {
                let window = event.window.unwrap();
                assert_eq!(event.family_id, "expiry");
                assert_eq!(event.fire_timestamp_ns, window.end_ns - 1_000_000);
                assert_eq!(event.output_timestamp_ns, event.fire_timestamp_ns);
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![
                        state.unwrap() + i32::try_from(window.start_ns / 1_000_000).unwrap(),
                    ],
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
            "tests/window-expiration-capture-v1".into(),
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
    assert_eq!(*committed.lock().unwrap(), ["7:3", "7:14"]);
}

#[test]
fn global_window_expiration_flushes_when_finite_stream_completes() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((7_i32, 3_i32), 1_000_000)])],
        )
        .unwrap()
        .stateful_timer_map_with_window_expiration_named::<i32, i32>(
            "timer",
            "tests/global-window-expiration-callback",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("expiry".into(), UserTimerDomain::EventTime)]),
            "expiry",
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
            |_, state, event| {
                assert!(event.window.is_none());
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
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
            "tests/global-window-expiration-sink-v1".into(),
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
fn timer_callback_panic_fails_the_job_with_transform_context() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
            ],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/panicking-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "panic".into(),
                        fire_timestamp_ns: 1_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, _, _| panic!("timer callback deliberately panicked"),
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
            "tests/panicking-timer-capture-v1".into(),
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
    let Err(error) = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
    else {
        panic!("timer callback panic succeeded");
    };
    assert!(error.to_string().contains("timer callback panicked"));
    assert!(committed.lock().unwrap().is_empty());
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
#[allow(clippy::too_many_lines)] // The saved source cursor and timer state must be checked together.
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
#[allow(clippy::too_many_lines)] // Both overlapping cells and timer scopes must survive one checkpoint.
fn overlapping_sliding_timers_resume_from_one_source_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
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
            "sliding-timer-recovery-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(SlidingTimerRecoverySource {
                        cursor,
                        emitted: false,
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .sliding_windows("windows", 10_000_000, 5_000_000, 0)
        .unwrap()
        .stateful_timer_map_named::<i32, String>(
            "timer",
            "tests/sliding-timer-recovery",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, state, metadata| {
                let window = metadata.assigned_window.unwrap();
                Ok(TimedStateResult {
                    state: Some(state.unwrap_or(0) + value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: window.end_ns - 1_000_000,
                        output_timestamp_ns: Some(7_000_000),
                    }],
                })
            },
            |_, state, event| {
                let window = event.window.unwrap();
                assert_eq!(event.dynamic_tag, "flush");
                assert_eq!(event.fire_timestamp_ns, window.end_ns - 1_000_000);
                assert_eq!(event.output_timestamp_ns, 7_000_000);
                let total = state.unwrap();
                let adjusted = total + i32::try_from(window.start_ns / 5_000_000).unwrap();
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![format!(
                        "{}:{adjusted}:{}:{}",
                        event.output_timestamp_ns, window.start_ns, window.end_ns
                    )],
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("row", |(_, row)| row)
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/sliding-timer-recovery-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleIdempotentCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
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
            binary_build_id: "sliding-timer-recovery-build".into(),
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
        assert!(
            Instant::now() < deadline,
            "sliding timers were not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(committed.lock().unwrap().is_empty());
    first.cancel();
    assert!(first.wait().is_err());
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
    let mut rows = committed.lock().unwrap().clone();
    rows.sort();
    assert_eq!(rows, ["7000000:7:0:10000000", "7000000:8:5000000:15000000"]);
}

#[test]
#[allow(clippy::too_many_lines)] // Both sink branches must recover the same armed timer and cursor.
fn shared_event_timer_and_both_sinks_resume_from_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let first_rows = Arc::new(Mutex::new(Vec::new()));
    let second_rows = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let shared = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "shared-timed-recovery-source-v1".into(),
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
            "tests/shared-recovered-event-timer",
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
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap();
    let first = shared
        .sink_named_with_config(
            "first",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-recovered-first-v1".into(),
            {
                let committed = Arc::clone(&first_rows);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let second = shared
        .map_named("copy-label", |label| format!("copy:{label}"))
        .unwrap()
        .sink_named_with_config(
            "second",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-recovered-second-v1".into(),
            {
                let committed = Arc::clone(&second_rows);
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
            binary_build_id: "shared-timed-recovery-build".into(),
        }),
        ..JobConfig::default()
    };
    let first_run = DirectRunner
        .start_stream_sink_pair(&first, &second, &config())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while first_run
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert_ne!(first_run.handle().status(), JobStatus::Failed);
        assert!(
            Instant::now() < deadline,
            "shared timer was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    first_run.cancel();
    assert!(first_run.wait().is_err());
    DirectRunner
        .start_stream_sink_pair(&first, &second, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
    assert_eq!(*first_rows.lock().unwrap(), ["7:3"]);
    assert_eq!(*second_rows.lock().unwrap(), ["copy:7:3"]);
}

#[test]
#[allow(clippy::too_many_lines)] // Recovery must observe each branch's own armed timer and sink.
fn independent_branch_timers_resume_from_one_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let left_rows = Arc::new(Mutex::new(Vec::new()));
    let right_rows = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "independent-timers-source-v1".into(),
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
        .unwrap();
    let families = BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]);
    let left_capture = Arc::clone(&left_rows);
    let left = source
        .stateful_timer_map_named::<i32, i32>(
            "left-timer",
            "tests/independent-left-timer-v1",
            Version { major: 1, minor: 0 },
            families.clone(),
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
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("left-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named_with_config(
            "left",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "independent-left-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&left_capture),
                    pending: Vec::new(),
                }))
            },
        )
        .unwrap();
    let right_capture = Arc::clone(&right_rows);
    let right = source
        .stateful_timer_map_named::<i32, i32>(
            "right-timer",
            "tests/independent-right-timer-v1",
            Version { major: 1, minor: 0 },
            families,
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value * 10),
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
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("right-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named_with_config(
            "right",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "independent-right-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&right_capture),
                    pending: Vec::new(),
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 2,
            recover: true,
            binary_build_id: "independent-timers-build".into(),
        }),
        ..JobConfig::default()
    };
    let first = DirectRunner
        .start_stream_sink_pair(&left, &right, &config())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while first
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert_ne!(first.handle().status(), JobStatus::Failed);
        assert!(
            Instant::now() < deadline,
            "both timers were not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    first.cancel();
    assert!(first.wait().is_err());
    DirectRunner
        .start_stream_sink_pair(&left, &right, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
    assert_eq!(*left_rows.lock().unwrap(), ["7:3"]);
    assert_eq!(*right_rows.lock().unwrap(), ["7:30"]);
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
#[allow(clippy::too_many_lines)] // Two commit attempts and one saved timer are asserted together.
fn failed_second_timer_sink_retries_same_bundle_once() {
    let directory = tempfile::tempdir().unwrap();
    let first_rows = Arc::new(Mutex::new(Vec::new()));
    let first_bundles = Arc::new(Mutex::new(HashSet::new()));
    let second_rows = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let shared = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "failed-shared-timer-source-v1".into(),
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
            "tests/failed-shared-timer-commit",
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
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap();
    let first = shared
        .sink_named_with_config(
            "first",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "failed-shared-first-v1".into(),
            {
                let committed = Arc::clone(&first_rows);
                let accepted = Arc::clone(&first_bundles);
                move || {
                    Ok(Box::new(BundleIdempotentCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let second = shared
        .map_named("copy-label", |label| format!("copy:{label}"))
        .unwrap()
        .sink_named_with_config(
            "second",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "failed-shared-second-v1".into(),
            {
                let committed = Arc::clone(&second_rows);
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
            binary_build_id: "failed-shared-timer-build".into(),
        }),
        ..JobConfig::default()
    };
    assert!(
        DirectRunner
            .start_stream_sink_pair(&first, &second, &config())
            .unwrap()
            .wait()
            .is_err()
    );
    assert_eq!(*first_rows.lock().unwrap(), ["7:3"]);
    assert!(second_rows.lock().unwrap().is_empty());
    DirectRunner
        .start_stream_sink_pair(&first, &second, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(*first_rows.lock().unwrap(), ["7:3"]);
    assert_eq!(first_bundles.lock().unwrap().len(), 1);
    assert_eq!(*second_rows.lock().unwrap(), ["copy:7:3"]);
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
#[allow(clippy::too_many_lines)] // The idle timer's two sink commits and checkpoint are one scenario.
fn idle_shared_processing_timer_reaches_both_stream_sinks() {
    let directory = tempfile::tempdir().unwrap();
    let first_rows = Arc::new(Mutex::new(Vec::new()));
    let second_rows = Arc::new(Mutex::new(Vec::new()));
    let polls = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let shared = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "wall-pending-pair-timer-source-v1".into(),
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
            "tests/idle-shared-processing-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, value, _, _| {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let deadline = i64::try_from(now.as_nanos()).unwrap() + 50_000_000;
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "process".into(),
                        dynamic_tag: "idle".into(),
                        fire_timestamp_ns: deadline - deadline % 1_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                assert_eq!(event.dynamic_tag, "idle");
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap();
    let first = shared
        .sink_named_with_config(
            "first",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "idle-pair-first-v1".into(),
            {
                let committed = Arc::clone(&first_rows);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let second = shared
        .map_named("copy-label", |label| format!("copy:{label}"))
        .unwrap()
        .sink_named_with_config(
            "second",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "idle-pair-second-v1".into(),
            {
                let committed = Arc::clone(&second_rows);
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
        .start_stream_sink_pair(
            &first,
            &second,
            &JobConfig {
                checkpoint: Some(CheckpointConfig {
                    directory: directory.path().into(),
                    interval_batches: 1,
                    max_checkpoints: 2,
                    recover: true,
                    binary_build_id: "idle-shared-timer-build".into(),
                }),
                ..JobConfig::default()
            },
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while second_rows.lock().unwrap().is_empty() {
        assert_ne!(job.handle().status(), JobStatus::Failed);
        assert!(Instant::now() < deadline, "idle pair timer did not fire");
        std::thread::sleep(Duration::from_millis(1));
    }
    job.cancel();
    assert!(job.wait().is_err());
    assert_eq!(*first_rows.lock().unwrap(), ["5:8"]);
    assert_eq!(*second_rows.lock().unwrap(), ["copy:5:8"]);
    assert!(polls.load(Ordering::SeqCst) > 1);
}

#[test]
#[allow(clippy::too_many_lines)] // The idle timer, failed second commit, and replay share one checkpoint.
fn failed_idle_pair_timer_retries_from_armed_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let first_rows = Arc::new(Mutex::new(Vec::new()));
    let first_bundles = Arc::new(Mutex::new(HashSet::new()));
    let second_rows = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let shared = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "failed-idle-pair-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(PendingRecoverySource {
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
            "tests/failed-idle-pair-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, value, _, _| {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let deadline = i64::try_from(now.as_nanos()).unwrap() + 50_000_000;
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "process".into(),
                        dynamic_tag: "idle".into(),
                        fire_timestamp_ns: deadline - deadline % 1_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("label", |(key, value)| format!("{key}:{value}"))
        .unwrap();
    let first = shared
        .sink_named_with_config(
            "first",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "failed-idle-pair-first-v1".into(),
            {
                let committed = Arc::clone(&first_rows);
                let accepted = Arc::clone(&first_bundles);
                move || {
                    Ok(Box::new(BundleIdempotentCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let second = shared
        .map_named("copy-label", |label| format!("copy:{label}"))
        .unwrap()
        .sink_named_with_config(
            "second",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "failed-idle-pair-second-v1".into(),
            {
                let committed = Arc::clone(&second_rows);
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
            binary_build_id: "failed-idle-pair-build".into(),
        }),
        ..JobConfig::default()
    };
    let first_run = DirectRunner
        .start_stream_sink_pair(&first, &second, &config())
        .unwrap();
    assert!(first_run.wait().is_err());
    assert_eq!(*first_rows.lock().unwrap(), ["5:8"]);
    assert!(second_rows.lock().unwrap().is_empty());
    let resumed = DirectRunner
        .start_stream_sink_pair(&first, &second, &config())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while second_rows.lock().unwrap().is_empty() {
        assert_ne!(resumed.handle().status(), JobStatus::Failed);
        assert!(
            Instant::now() < deadline,
            "recovered idle timer did not fire"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    resumed.cancel();
    assert!(resumed.wait().is_err());
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(*first_rows.lock().unwrap(), ["5:8"]);
    assert_eq!(first_bundles.lock().unwrap().len(), 1);
    assert_eq!(*second_rows.lock().unwrap(), ["copy:5:8"]);
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

#[test]
fn timer_outputs_obey_downstream_failure_and_buffer_limits() {
    for panic_in_map in [false, true] {
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
                "tests/timer-output-failure",
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
                    let value = state.unwrap();
                    Ok(TimedStateResult {
                        state: None,
                        outputs: vec![value],
                        timers: Vec::new(),
                    })
                },
            )
            .unwrap()
            .flat_map_named("label", move |(key, value)| {
                assert!(!panic_in_map, "injected timer output failure");
                vec![format!("{key}:{value}"), format!("{key}:{}", value + 1)]
            })
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "tests/timer-output-failure-capture-v1".into(),
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
        let config = JobConfig {
            buffer_budget_elements: if panic_in_map { 16 } else { 1 },
            ..JobConfig::default()
        };
        let Err(error) = DirectRunner
            .start_stream_sink(&sink, &config)
            .unwrap()
            .wait()
        else {
            panic!("timer output failure was accepted");
        };
        let expected = if panic_in_map {
            "timer output transform panicked"
        } else {
            "timer output exceeds buffer budget"
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert!(committed.lock().unwrap().is_empty());
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Each case crosses the timer callback and sink delivery boundary.
fn timer_sink_panics_and_filtered_results_preserve_job_outcomes() {
    for mode in 0..3_u8 {
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
                "tests/timer-sink-fault",
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
            .filter_named("filter", move |_| mode != 2)
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
                "tests/timer-sink-fault-v1".into(),
                move || Ok(Box::new(TimerDeliverySink { mode })),
            )
            .unwrap();
        let result = DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait();
        match mode {
            0 => assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("timer sink writer panicked")
            ),
            1 => {
                let error = result.err().unwrap();
                assert!(
                    error.to_string().contains("sink commit panicked"),
                    "{error}"
                );
            }
            2 => assert!(result.is_ok()),
            _ => unreachable!(),
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)] // A pending source must let its timer run through a separate sink commit.
fn idle_processing_timer_reports_sink_commit_panic() {
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "idle-timer-commit-panic-source-v1".into(),
            0,
            move |_| {
                Ok(Box::new(WallPendingSource {
                    emitted: false,
                    polls: Arc::new(AtomicUsize::new(0)),
                }) as TypedUnboundedSourceReader<(i32, i32)>)
            },
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/idle-timer-commit-panic",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, value, _, _| {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                let fire_timestamp_ns = i64::try_from(now.as_nanos()).unwrap() + 20_000_000;
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
            "tests/idle-timer-commit-panic-v1".into(),
            move || Ok(Box::new(TimerDeliverySink { mode: 1 })),
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job.handle().status() != JobStatus::Failed {
        assert!(
            Instant::now() < deadline,
            "idle timer did not reach sink commit"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let Err(error) = job.wait() else {
        panic!("panicking timer sink commit was accepted");
    };
    assert!(error.to_string().contains("timer sink commit panicked"));
}

#[test]
#[allow(clippy::too_many_lines)] // The two callbacks distinguish empty delivery from excess timer state.
fn idle_processing_timer_handles_no_output_and_state_budget() {
    for exceed_budget in [false, true] {
        let fired = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::default();
        let sink = pipeline
            .unbounded_source_with_factory(
                "source",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "idle-timer-state-budget-source-v1".into(),
                0,
                move |_| {
                    Ok(Box::new(WallPendingSource {
                        emitted: false,
                        polls: Arc::new(AtomicUsize::new(0)),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                },
            )
            .unwrap()
            .stateful_timer_map_named::<i32, i32>(
                "timer",
                "tests/idle-timer-state-budget",
                Version { major: 1, minor: 0 },
                BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
                |_, value, _, _| {
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    let fire_timestamp_ns = i64::try_from(now.as_nanos()).unwrap() + 20_000_000;
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
                {
                    let fired = Arc::clone(&fired);
                    move |_, state, event| {
                        fired.fetch_add(1, Ordering::SeqCst);
                        let timers = if exceed_budget {
                            (0..100)
                                .map(|index| UserTimerRequest::Set {
                                    family_id: "process".into(),
                                    dynamic_tag: format!("future-{index}"),
                                    fire_timestamp_ns: event.fire_timestamp_ns + 10_000_000_000,
                                    output_timestamp_ns: None,
                                })
                                .collect()
                        } else {
                            Vec::new()
                        };
                        Ok(TimedStateResult {
                            state,
                            outputs: Vec::new(),
                            timers,
                        })
                    }
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
                "tests/idle-timer-state-budget-sink-v1".into(),
                move || Ok(Box::new(TimerDeliverySink { mode: 2 })),
            )
            .unwrap();
        let config = JobConfig {
            memory_budget_bytes: 5000,
            ..JobConfig::default()
        };
        let job = DirectRunner.start_stream_sink(&sink, &config).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while fired.load(Ordering::SeqCst) == 0
            || (exceed_budget && job.handle().status() != JobStatus::Failed)
        {
            assert!(
                fired.load(Ordering::SeqCst) != 0 || job.handle().status() != JobStatus::Failed,
                "idle timer failed before callback"
            );
            assert!(
                Instant::now() < deadline,
                "idle timer did not finish callback"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        if exceed_budget {
            let Err(error) = job.wait() else {
                panic!("oversized timer state was accepted");
            };
            assert!(
                error
                    .to_string()
                    .contains("stream timer state exceeds memory budget")
            );
        } else {
            assert_ne!(job.handle().status(), JobStatus::Failed);
            job.cancel();
            assert!(job.wait().is_err());
        }
    }
}
