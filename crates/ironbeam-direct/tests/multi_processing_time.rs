mod processing_time_support;

use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    TestStreamEvent, Timestamped, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
    UnboundedSource, WindowOnTimeBehavior,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use processing_time_support::{Capture, TimedScript, Value, WallPending};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

fn progress(key: &str, cursor: u8, watermark_ns: i64, completed: bool) -> ProgressSignal {
    ProgressSignal {
        source_key: key.into(),
        split_id: "split-0".into(),
        checkpoint_position: Some(vec![cursor]),
        event_time_ns: Some(watermark_ns),
        completed,
    }
}

struct CountingPending(Arc<AtomicUsize>);

impl UnboundedSource<Timestamped<Value>> for CountingPending {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<Value>>, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(UnboundedPoll::Pending)
    }
}

#[test]
fn two_independent_sources_fire_one_processing_time_pane() {
    for accumulating in [false, true] {
        let pipeline = Pipeline::default();
        let make_source = |key: &str, value| {
            let source = pipeline
                .test_stream_source(
                    key,
                    vec![
                        TestStreamEvent::Elements(vec![((1_i32, value), 1_000_000)]),
                        TestStreamEvent::AdvanceProcessingTime(5_000_000),
                        TestStreamEvent::AdvanceWatermark(10_000_000),
                    ],
                )
                .unwrap()
                .fixed_windows(&format!("{key}-window"), 10_000_000)
                .unwrap();
            let triggered = if accumulating {
                source.accumulating_processing_time_trigger_named(
                    &format!("{key}-trigger"),
                    5_000_000,
                )
            } else {
                source.early_processing_time_trigger_named(&format!("{key}-trigger"), 5_000_000)
            }
            .unwrap();
            if accumulating {
                triggered
                    .on_time_behavior_named(
                        &format!("{key}-on-time"),
                        WindowOnTimeBehavior::FireAlways,
                    )
                    .unwrap()
            } else {
                triggered
            }
        };
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let sink = make_source("first", 2)
            .co_group_homogeneous_named("group", &[make_source("second", 4)])
            .unwrap()
            .reify_panes_named("pane")
            .unwrap()
            .map_named("format", |(pane, (key, values))| {
                format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
            })
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("multi-processing-{accumulating}-v1"),
                {
                    let committed = Arc::clone(&committed);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(Capture {
                            committed: Arc::clone(&committed),
                            accepted: Arc::clone(&accepted),
                            fail_once: None,
                            panic_once: None,
                            fail_write_once: None,
                            bundle_ids: None,
                            staged: Vec::new(),
                            bundle: None,
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
        let rows = committed.lock().unwrap().clone();
        if accumulating {
            assert_eq!(rows, ["1:[[2], [4]]:Early:0", "1:[[2], [4]]:OnTime:1",]);
        } else {
            assert_eq!(rows, ["1:[[2], [4]]:Early:0"]);
        }
    }
}

#[test]
fn two_sources_accumulate_count_or_processing_time_panes() {
    run_two_sources_accumulating_count_and_timer(CountTimerMode::Any);
}

#[test]
fn two_sources_accumulate_count_then_processing_time_panes() {
    run_two_sources_accumulating_count_and_timer(CountTimerMode::Each);
}

#[test]
fn two_sources_accumulate_count_and_processing_time_panes() {
    run_two_sources_accumulating_count_and_timer(CountTimerMode::All);
}

#[test]
fn two_source_count_and_time_fires_within_batch_after_timer_is_ready() {
    let pipeline = Pipeline::default();
    let first = pipeline
        .test_stream_source(
            "first",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
                TestStreamEvent::Elements(vec![
                    ((1, 3), 2_000_000),
                    ((1, 4), 3_000_000),
                    ((1, 5), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("first-window", 10_000_000)
        .unwrap()
        .count_and_processing_time_trigger_named("first-trigger", 2, 5_000_000)
        .unwrap();
    let second = pipeline
        .test_stream_source(
            "second",
            vec![TestStreamEvent::AdvanceWatermark(10_000_000)],
        )
        .unwrap()
        .fixed_windows("second-window", 10_000_000)
        .unwrap()
        .count_and_processing_time_trigger_named("second-trigger", 2, 5_000_000)
        .unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let sink = first
        .co_group_homogeneous_named("group", &[second])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, values))| {
            format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "multi-count-and-time-element-boundary-v1".to_owned(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        fail_once: None,
                        panic_once: None,
                        fail_write_once: None,
                        bundle_ids: None,
                        staged: Vec::new(),
                        bundle: None,
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
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["1:[[2, 3], []]:Early:0", "1:[[4, 5], []]:OnTime:1"]
    );
}

#[derive(Clone, Copy)]
enum CountTimerMode {
    Any,
    All,
    Each,
}

fn run_two_sources_accumulating_count_and_timer(mode: CountTimerMode) {
    let pipeline = Pipeline::default();
    let make_source = |key: &str, value, first: bool| {
        let mut events = vec![TestStreamEvent::Elements(vec![((1_i32, value), 1_000_000)])];
        if first {
            events.push(TestStreamEvent::Elements(vec![((1_i32, 3_i32), 2_000_000)]));
        } else {
            events.push(TestStreamEvent::AdvanceWatermark(0));
        }
        events.push(TestStreamEvent::AdvanceProcessingTime(5_000_000));
        events.push(TestStreamEvent::AdvanceWatermark(10_000_000));
        let source = pipeline
            .test_stream_source(key, events)
            .unwrap()
            .fixed_windows(&format!("{key}-window"), 10_000_000)
            .unwrap();
        match mode {
            CountTimerMode::Each => source.accumulating_count_then_processing_time_trigger_named(
                &format!("{key}-trigger"),
                2,
                5_000_000,
            ),
            CountTimerMode::Any => source.accumulating_count_or_processing_time_trigger_named(
                &format!("{key}-trigger"),
                2,
                5_000_000,
            ),
            CountTimerMode::All => source.accumulating_count_and_processing_time_trigger_named(
                &format!("{key}-trigger"),
                2,
                5_000_000,
            ),
        }
        .unwrap()
        .on_time_behavior_named(&format!("{key}-on-time"), WindowOnTimeBehavior::FireAlways)
        .unwrap()
    };
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let sink = make_source("first", 2, true)
        .co_group_homogeneous_named("group", &[make_source("second", 4, false)])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, values))| {
            format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("multi-count-timer-{}-v1", mode.name()),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        fail_once: None,
                        panic_once: None,
                        fail_write_once: None,
                        bundle_ids: None,
                        staged: Vec::new(),
                        bundle: None,
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
    let expected: &[&str] = match mode {
        CountTimerMode::All => &["1:[[2, 3], [4]]:Early:0", "1:[[2, 3], [4]]:OnTime:1"],
        CountTimerMode::Any | CountTimerMode::Each => &[
            "1:[[2], [4]]:Early:0",
            "1:[[2, 3], [4]]:Early:1",
            "1:[[2, 3], [4]]:OnTime:2",
        ],
    };
    assert_eq!(committed.lock().unwrap().as_slice(), expected);
}

impl CountTimerMode {
    const fn name(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::All => "all",
            Self::Each => "each",
        }
    }
}

#[test]
fn bounded_input_arms_timer_while_unbounded_input_is_idle() {
    let pipeline = Pipeline::default();
    let bounded = pipeline
        .from_vec("bounded", vec![(1_000_000_i64, (1_i32, 2_i32))])
        .unwrap()
        .timestamps_from_pairs_named("bounded-time")
        .unwrap()
        .fixed_windows("bounded-window", 10_000_000)
        .unwrap()
        .accumulating_processing_time_trigger_named("bounded-trigger", 5_000_000)
        .unwrap()
        .on_time_behavior_named("bounded-on-time", WindowOnTimeBehavior::FireAlways)
        .unwrap();
    let unbounded = pipeline
        .test_stream_source::<(i32, i32)>(
            "unbounded",
            vec![
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("unbounded-window", 10_000_000)
        .unwrap()
        .accumulating_processing_time_trigger_named("unbounded-trigger", 5_000_000)
        .unwrap()
        .on_time_behavior_named("unbounded-on-time", WindowOnTimeBehavior::FireAlways)
        .unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let sink = bounded
        .co_group_homogeneous_named("group", &[unbounded])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, values))| {
            format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "bounded-multi-processing-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        fail_once: None,
                        panic_once: None,
                        fail_write_once: None,
                        bundle_ids: None,
                        staged: Vec::new(),
                        bundle: None,
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
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["1:[[2], []]:Early:0", "1:[[2], []]:OnTime:1"]
    );
}

#[test]
#[allow(clippy::too_many_lines)] // Every malformed progress step shares one two-source graph.
fn multi_source_timer_rejects_changed_progress_identity() {
    for fault in [
        "source",
        "split",
        "changed-split",
        "repeated-cursor",
        "backward-watermark",
    ] {
        let pipeline = Pipeline::default();
        let first = pipeline
            .unbounded_source_with_factory(
                "first",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("malformed-timer-source-{fault}"),
                0,
                move |_| {
                    let mut initial = progress(
                        "first",
                        1,
                        if fault == "backward-watermark" { 5 } else { 0 },
                        false,
                    );
                    if fault == "source" {
                        initial.source_key = "other".into();
                    } else if fault == "split" {
                        initial.split_id.clear();
                    }
                    let mut polls = VecDeque::from([(
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: initial,
                        }),
                        0,
                    )]);
                    if fault == "changed-split" {
                        let mut changed = progress("first", 2, 0, false);
                        changed.split_id = "other-split".into();
                        polls.push_back((
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: Vec::new(),
                                progress: changed,
                            }),
                            0,
                        ));
                    } else if fault == "repeated-cursor" {
                        polls.push_back((
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: vec![Timestamped {
                                    value: (1, 2),
                                    timestamp_ns: 1_000_000,
                                }],
                                progress: progress("first", 1, 0, false),
                            }),
                            0,
                        ));
                    } else if fault == "backward-watermark" {
                        polls.push_back((
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: Vec::new(),
                                progress: progress("first", 2, 0, false),
                            }),
                            0,
                        ));
                    }
                    Ok(Box::new(TimedScript { polls, clock_ns: 0 })
                        as TypedUnboundedSourceReader<Value>)
                },
            )
            .unwrap()
            .fixed_windows("first-window", 10_000_000)
            .unwrap()
            .early_processing_time_trigger_named("first-timer", 5_000_000)
            .unwrap();
        let second = pipeline
            .test_stream_source::<Value>(
                "second",
                vec![TestStreamEvent::AdvanceWatermark(10_000_000)],
            )
            .unwrap()
            .fixed_windows("second-window", 10_000_000)
            .unwrap()
            .early_processing_time_trigger_named("second-timer", 5_000_000)
            .unwrap();
        let sink = first
            .co_group_homogeneous_named("group", &[second])
            .unwrap()
            .map_named("format", |(key, values)| format!("{key}:{values:?}"))
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("malformed-timer-sink-{fault}"),
                || {
                    Ok(Box::new(Capture {
                        committed: Arc::new(Mutex::new(Vec::new())),
                        accepted: Arc::new(Mutex::new(HashSet::new())),
                        fail_once: None,
                        panic_once: None,
                        fail_write_once: None,
                        bundle_ids: None,
                        staged: Vec::new(),
                        bundle: None,
                    }))
                },
            )
            .unwrap();
        assert_eq!(
            DirectRunner
                .start_stream_sink(&sink, &JobConfig::default())
                .unwrap()
                .wait()
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution,
            "{fault}"
        );
    }
}

#[test]
fn multi_source_timer_rejects_clock_overflow_and_mixed_domains() {
    for mixed in [false, true] {
        let pipeline = Pipeline::default();
        let events = if mixed {
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)])]
        } else {
            vec![
                TestStreamEvent::AdvanceProcessingTime(i64::MAX - 1),
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)]),
            ]
        };
        let first = pipeline
            .test_stream_source("first", events)
            .unwrap()
            .fixed_windows("first-window", 10_000_000)
            .unwrap()
            .early_processing_time_trigger_named("first-timer", 5_000_000)
            .unwrap();
        let second = if mixed {
            pipeline
                .unbounded_source_with_factory(
                    "second",
                    SourceProgress {
                        stable_split_id: true,
                        replay: Replay::AtLeastOnce,
                        reports_event_time: true,
                    },
                    "wall-pending-second-v1".into(),
                    0,
                    |_| {
                        Ok(Box::new(WallPending { first: None })
                            as TypedUnboundedSourceReader<Value>)
                    },
                )
                .unwrap()
        } else {
            pipeline
                .test_stream_source::<Value>(
                    "second",
                    vec![TestStreamEvent::AdvanceWatermark(10_000_000)],
                )
                .unwrap()
        }
        .fixed_windows("second-window", 10_000_000)
        .unwrap()
        .early_processing_time_trigger_named("second-timer", 5_000_000)
        .unwrap();
        let sink = first
            .co_group_homogeneous_named("group", &[second])
            .unwrap()
            .map_named("format", |(key, values)| format!("{key}:{values:?}"))
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("invalid-multi-clock-{mixed}-v1"),
                || {
                    Ok(Box::new(Capture {
                        committed: Arc::new(Mutex::new(Vec::new())),
                        accepted: Arc::new(Mutex::new(HashSet::new())),
                        fail_once: None,
                        panic_once: None,
                        fail_write_once: None,
                        bundle_ids: None,
                        staged: Vec::new(),
                        bundle: None,
                    }))
                },
            )
            .unwrap();
        let error = DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .err()
            .unwrap();
        assert_eq!(
            error.kind,
            if mixed {
                ErrorKind::UnsupportedCapability {
                    capability: "mixed-scripted-and-wall-clocks".into(),
                }
            } else {
                ErrorKind::Resource
            }
        );
    }
}

#[test]
fn idle_multi_source_timer_stops_on_cancellation() {
    let pipeline = Pipeline::default();
    let polls = Arc::new(AtomicUsize::new(0));
    let make = |key: &str| {
        pipeline
            .unbounded_source_with_factory(
                key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("{key}-pending-timer-v1"),
                0,
                {
                    let polls = Arc::clone(&polls);
                    move |_| {
                        Ok(Box::new(CountingPending(Arc::clone(&polls)))
                            as TypedUnboundedSourceReader<Value>)
                    }
                },
            )
            .unwrap()
            .fixed_windows(&format!("{key}-window"), 10_000_000)
            .unwrap()
            .early_processing_time_trigger_named(&format!("{key}-timer"), 5_000_000)
            .unwrap()
    };
    let sink = make("first")
        .co_group_homogeneous_named("group", &[make("second")])
        .unwrap()
        .map_named("format", |(key, values)| format!("{key}:{values:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "idle-multi-timer-sink-v1".into(),
            || {
                Ok(Box::new(Capture {
                    committed: Arc::new(Mutex::new(Vec::new())),
                    accepted: Arc::new(Mutex::new(HashSet::new())),
                    fail_once: None,
                    panic_once: None,
                    fail_write_once: None,
                    bundle_ids: None,
                    staged: Vec::new(),
                    bundle: None,
                }))
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while polls.load(Ordering::SeqCst) < 2 {
        assert!(Instant::now() < deadline, "sources were not polled");
        std::thread::sleep(Duration::from_millis(1));
    }
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
}

#[test]
fn two_source_timer_recovers_deadline_and_retries_committed_bundle() {
    for mode in [
        TimerRecoveryMode::Processing,
        TimerRecoveryMode::CountOrTime,
    ] {
        run_two_source_timer_recovery(mode);
    }
}

#[test]
fn two_source_conjunctive_early_and_late_timer_recovers_and_retries() {
    run_two_source_timer_recovery(TimerRecoveryMode::CountAndTimeLate);
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TimerRecoveryMode {
    Processing,
    CountOrTime,
    CountAndTimeLate,
}

impl TimerRecoveryMode {
    const fn name(self) -> &'static str {
        match self {
            Self::Processing => "processing",
            Self::CountOrTime => "count-or-time",
            Self::CountAndTimeLate => "count-and-time-late",
        }
    }
}

#[allow(clippy::too_many_lines)] // The saved deadline, failed commit, and replay use one fixture.
fn run_two_source_timer_recovery(mode: TimerRecoveryMode) {
    let directory = tempfile::tempdir().unwrap();
    let hold = Arc::new(AtomicBool::new(true));
    let fail_once = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let bundle_ids = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let make_source = |key: &str, value| {
        let source_key = key.to_owned();
        let source = pipeline
            .unbounded_source_with_factory(
                key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("{key}-timer-source-{}-v1", mode.name()),
                0,
                {
                    let hold = Arc::clone(&hold);
                    let opens = Arc::clone(&opens);
                    move |cursor| {
                        opens
                            .lock()
                            .unwrap()
                            .push((source_key.clone(), cursor.clone()));
                        let polls = match cursor.as_deref() {
                            None => VecDeque::from([(
                                UnboundedPoll::Ready(UnboundedBatch {
                                    elements: vec![Timestamped {
                                        value: (1, value),
                                        timestamp_ns: 1_000_000,
                                    }],
                                    progress: progress(
                                        &source_key,
                                        1,
                                        if source_key == "first" { 10_000_000 } else { 0 },
                                        source_key == "first",
                                    ),
                                }),
                                0,
                            )]),
                            Some([1]) if hold.load(Ordering::SeqCst) => VecDeque::new(),
                            Some([1]) => {
                                let mut polls = VecDeque::from([
                                    (
                                        UnboundedPoll::Ready(UnboundedBatch {
                                            elements: Vec::new(),
                                            progress: progress(&source_key, 2, 0, false),
                                        }),
                                        5_000_000,
                                    ),
                                    (
                                        UnboundedPoll::Ready(UnboundedBatch {
                                            elements: Vec::new(),
                                            progress: progress(
                                                &source_key,
                                                3,
                                                10_000_000,
                                                mode != TimerRecoveryMode::CountAndTimeLate,
                                            ),
                                        }),
                                        5_000_000,
                                    ),
                                ]);
                                if mode == TimerRecoveryMode::CountAndTimeLate {
                                    polls.push_back((
                                        UnboundedPoll::Ready(UnboundedBatch {
                                            elements: vec![
                                                Timestamped {
                                                    value: (1, 5),
                                                    timestamp_ns: 5_000_000,
                                                },
                                                Timestamped {
                                                    value: (1, 7),
                                                    timestamp_ns: 7_000_000,
                                                },
                                            ],
                                            progress: progress(&source_key, 4, 10_000_000, false),
                                        }),
                                        5_000_000,
                                    ));
                                    polls.push_back((
                                        UnboundedPoll::Ready(UnboundedBatch {
                                            elements: Vec::new(),
                                            progress: progress(&source_key, 5, 15_000_000, true),
                                        }),
                                        5_000_000,
                                    ));
                                }
                                polls
                            }
                            _ => return Err(Error::incompatible("unexpected timer source cursor")),
                        };
                        Ok(Box::new(TimedScript { polls, clock_ns: 0 })
                            as TypedUnboundedSourceReader<Value>)
                    }
                },
            )
            .unwrap()
            .fixed_windows(&format!("{key}-window"), 10_000_000)
            .unwrap();
        match mode {
            TimerRecoveryMode::Processing => source
                .accumulating_processing_time_trigger_named(&format!("{key}-timer"), 5_000_000),
            TimerRecoveryMode::CountOrTime => source
                .accumulating_count_or_processing_time_trigger_named(
                    &format!("{key}-timer"),
                    3,
                    5_000_000,
                ),
            TimerRecoveryMode::CountAndTimeLate => source
                .allowed_lateness_named(&format!("{key}-lateness"), 5_000_000)
                .unwrap()
                .accumulating_count_and_processing_time_and_late_count_trigger_named(
                    &format!("{key}-timer"),
                    2,
                    5_000_000,
                    2,
                ),
        }
        .unwrap()
        .on_time_behavior_named(&format!("{key}-on-time"), WindowOnTimeBehavior::FireAlways)
        .unwrap()
    };
    let sink = make_source("first", 2)
        .co_group_homogeneous_named("group", &[make_source("second", 4)])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, values))| {
            format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("multi-timer-recovery-sink-{}-v1", mode.name()),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                let fail_once = Arc::clone(&fail_once);
                let bundle_ids = Arc::clone(&bundle_ids);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        fail_once: Some(Arc::clone(&fail_once)),
                        panic_once: None,
                        fail_write_once: None,
                        bundle_ids: Some(Arc::clone(&bundle_ids)),
                        staged: Vec::new(),
                        bundle: None,
                    }))
                }
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "multi-timer-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while first
        .progress()
        .and_then(|signal| signal.checkpoint_position)
        != Some(2_u64.to_le_bytes().to_vec())
    {
        assert!(
            Instant::now() < deadline,
            "open timer state was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(committed.lock().unwrap().is_empty());
    first.cancel();
    assert_eq!(first.wait().err().unwrap().kind, ErrorKind::Cancelled);

    hold.store(false, Ordering::SeqCst);
    fail_once.store(true, Ordering::SeqCst);
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &config())
            .unwrap()
            .wait()
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
    assert!(committed.lock().unwrap().is_empty());
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    let expected: &[&str] = if mode == TimerRecoveryMode::CountAndTimeLate {
        &[
            "1:[[2], [4]]:Early:0",
            "1:[[2], [4]]:OnTime:1",
            "1:[[2], [4, 5, 7]]:Late:2",
        ]
    } else {
        &["1:[[2], [4]]:Early:0", "1:[[2], [4]]:OnTime:1"]
    };
    assert_eq!(committed.lock().unwrap().as_slice(), expected);
    let ids = bundle_ids.lock().unwrap();
    assert_eq!(ids[0], ids[1], "failed timer must retry one bundle ID");
    let opened = opens.lock().unwrap();
    assert_eq!(opened.len(), 4);
    assert!(opened[..2].iter().all(|(_, cursor)| cursor.is_none()));
    assert!(
        opened[2..]
            .iter()
            .all(|(key, cursor)| key == "second" && cursor.as_deref() == Some(&[1][..]))
    );
    drop(opened);
}
