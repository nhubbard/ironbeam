#![allow(clippy::too_many_lines)] // Timer replay and sink fault boundaries are exercised as one fixture.

mod processing_time_support;
use ironbeam_core::{
    Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress, Timestamped,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, WindowOnTimeBehavior,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use processing_time_support::{CancelCapture, Capture, TimedScript, Value, WallPending, progress};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

#[test]
fn wall_clock_timer_fires_while_source_is_pending_and_stays_fired_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let fail_once = Arc::new(AtomicBool::new(true));
    let panic_once = Arc::new(AtomicBool::new(false));
    let fail_write_once = Arc::new(AtomicBool::new(false));
    let bundle_ids = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "wall-pending-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    let first = match cursor.as_deref() {
                        None => Some(UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value: (1, 2),
                                timestamp_ns: 1_000_000,
                            }],
                            progress: progress(1, 0, false),
                        })),
                        Some([1]) => None,
                        _ => return Err(Error::incompatible("unexpected wall source cursor")),
                    };
                    Ok(Box::new(WallPending { first }) as TypedUnboundedSourceReader<Value>)
                }
            },
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .accumulating_processing_time_trigger_named("timer", 5_000_000)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink_fail_once = Arc::clone(&fail_once);
    let sink_panic_once = Arc::clone(&panic_once);
    let sink_fail_write_once = Arc::clone(&fail_write_once);
    let sink_bundle_ids = Arc::clone(&bundle_ids);
    let sink = source
        .group_by_key_named("group")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
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
            "wall-pending-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    fail_once: Some(Arc::clone(&sink_fail_once)),
                    panic_once: Some(Arc::clone(&sink_panic_once)),
                    fail_write_once: Some(Arc::clone(&sink_fail_write_once)),
                    bundle_ids: Some(Arc::clone(&sink_bundle_ids)),
                    staged: Vec::new(),
                    bundle: None,
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "wall-pending-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while bundle_ids.lock().unwrap().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(bundle_ids.lock().unwrap().len(), 1);
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Execution);
    assert!(committed.lock().unwrap().is_empty());
    let resumed = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while committed.lock().unwrap().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(committed.lock().unwrap().as_slice(), ["1:[2]:Early:0"]);
    let ids = bundle_ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1]);
    std::thread::sleep(Duration::from_millis(30));
    resumed.cancel();
    assert_eq!(resumed.wait().err().unwrap().kind, ErrorKind::Cancelled);
    let completed_timer = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    std::thread::sleep(Duration::from_millis(40));
    assert_eq!(committed.lock().unwrap().as_slice(), ["1:[2]:Early:0"]);
    assert_eq!(bundle_ids.lock().unwrap().len(), 2);
    completed_timer.cancel();
    assert_eq!(
        completed_timer.wait().err().unwrap().kind,
        ErrorKind::Cancelled
    );
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1]), Some(vec![1])]);

    let replay = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while bundle_ids.lock().unwrap().len() < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(bundle_ids.lock().unwrap().len(), 3);
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["1:[2]:Early:0", "1:[2]:Early:0"]
    );
    replay.cancel();
    assert_eq!(replay.wait().err().unwrap().kind, ErrorKind::Cancelled);

    panic_once.store(true, Ordering::SeqCst);
    let panic_job = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while bundle_ids.lock().unwrap().len() < 4 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(bundle_ids.lock().unwrap().len(), 4);
    assert_eq!(panic_job.wait().err().unwrap().kind, ErrorKind::Execution);

    fail_write_once.store(true, Ordering::SeqCst);
    let failed_write = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while bundle_ids.lock().unwrap().len() < 5 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(bundle_ids.lock().unwrap().len(), 5);
    assert_eq!(
        failed_write.wait().err().unwrap().kind,
        ErrorKind::Execution
    );
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["1:[2]:Early:0", "1:[2]:Early:0"]
    );

    let filtered = source
        .group_by_key_named("filtered-group")
        .unwrap()
        .filter_named("drop-timer-pane", |_| false)
        .unwrap()
        .map_named("filtered-row", |(key, values)| format!("{key}:{values:?}"))
        .unwrap()
        .sink_named_with_config(
            "filtered-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "filtered-timer-sink-v1".into(),
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
    let filtered_job = DirectRunner
        .start_stream_sink(&filtered, &JobConfig::default())
        .unwrap();
    std::thread::sleep(Duration::from_millis(60));
    filtered_job.cancel();
    assert_eq!(
        filtered_job.wait().err().unwrap().kind,
        ErrorKind::Cancelled
    );

    let failed_stage = source
        .group_by_key_named("failed-stage-group")
        .unwrap()
        .flat_map_fallible_named("fail-timer-pane", |_| -> Result<Vec<String>, Error> {
            Err(Error::execution("timer", "test", "injected stage failure"))
        })
        .unwrap()
        .sink_named_with_config(
            "failed-stage-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "failed-stage-timer-sink-v1".into(),
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
            .start_stream_sink(&failed_stage, &JobConfig::default())
            .unwrap()
            .wait()
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );

    let cancellation = Arc::new(AtomicBool::new(false));
    let sink_cancellation = Arc::clone(&cancellation);
    let cancelled_sink = source
        .group_by_key_named("cancelled-group")
        .unwrap()
        .map_named("cancelled-row", |(key, values)| format!("{key}:{values:?}"))
        .unwrap()
        .sink_named_with_config(
            "cancelled-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "cancelled-timer-sink-v1".into(),
            move || {
                Ok(Box::new(CancelCapture {
                    cancellation: Arc::clone(&sink_cancellation),
                }))
            },
        )
        .unwrap();
    let cancelled_job = DirectRunner
        .start_stream_sink(
            &cancelled_sink,
            &JobConfig {
                cancellation: Arc::clone(&cancellation),
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(
        cancelled_job.wait().err().unwrap().kind,
        ErrorKind::Cancelled
    );
    assert!(cancellation.load(Ordering::SeqCst));
}

#[test]
fn staggered_timers_retry_the_earliest_deadline_before_later_due_work() {
    let directory = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let fail_once = Arc::new(AtomicBool::new(true));
    let bundle_ids = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "staggered-timer-source-v1".into(),
            0,
            move |cursor| {
                let (polls, clock_ns) = match cursor.as_deref() {
                    None => (
                        VecDeque::from([
                            (
                                UnboundedPoll::Ready(UnboundedBatch {
                                    elements: vec![Timestamped {
                                        value: (1, 2),
                                        timestamp_ns: 1_000_000,
                                    }],
                                    progress: progress(1, 0, false),
                                }),
                                0,
                            ),
                            (
                                UnboundedPoll::Ready(UnboundedBatch {
                                    elements: vec![Timestamped {
                                        value: (3, 4),
                                        timestamp_ns: 2_000_000,
                                    }],
                                    progress: progress(2, 0, false),
                                }),
                                2_000_000,
                            ),
                            (UnboundedPoll::Pending, 5_000_000),
                        ]),
                        0,
                    ),
                    Some([2]) => (VecDeque::new(), 20_000_000),
                    _ => return Err(Error::incompatible("unexpected staggered source cursor")),
                };
                Ok(Box::new(TimedScript { polls, clock_ns }) as TypedUnboundedSourceReader<Value>)
            },
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .early_processing_time_trigger_named("timer", 5_000_000)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink_fail_once = Arc::clone(&fail_once);
    let sink_bundle_ids = Arc::clone(&bundle_ids);
    let sink = source
        .group_by_key_named("group")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
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
            "staggered-timer-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    fail_once: Some(Arc::clone(&sink_fail_once)),
                    panic_once: None,
                    fail_write_once: None,
                    bundle_ids: Some(Arc::clone(&sink_bundle_ids)),
                    staged: Vec::new(),
                    bundle: None,
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "staggered-timer-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    assert_eq!(first.wait().err().unwrap().kind, ErrorKind::Execution);
    assert!(committed.lock().unwrap().is_empty());
    let resumed = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while committed.lock().unwrap().len() < 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["1:[2]:Early:0", "3:[4]:Early:0"]
    );
    let ids = bundle_ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 3);
    assert_eq!(ids[0], ids[1]);
    assert_ne!(ids[1], ids[2]);
    resumed.cancel();
    assert_eq!(resumed.wait().err().unwrap().kind, ErrorKind::Cancelled);
}

#[test]
fn empty_progress_at_one_cursor_keeps_early_and_on_time_bundles_distinct() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let bundle_ids = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "same-cursor-heartbeat-source-v1".into(),
            0,
            |_| {
                Ok(Box::new(TimedScript {
                    polls: VecDeque::from([
                        (
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: vec![Timestamped {
                                    value: (1, 2),
                                    timestamp_ns: 1_000_000,
                                }],
                                progress: progress(1, 0, false),
                            }),
                            0,
                        ),
                        (
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: Vec::new(),
                                progress: progress(1, 0, false),
                            }),
                            5_000_000,
                        ),
                        (
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: Vec::new(),
                                progress: progress(1, 10_000_000, true),
                            }),
                            5_000_000,
                        ),
                    ]),
                    clock_ns: 0,
                }) as TypedUnboundedSourceReader<Value>)
            },
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .accumulating_processing_time_trigger_named("timer", 5_000_000)
        .unwrap()
        .on_time_behavior_named("on-time", WindowOnTimeBehavior::FireAlways)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink_bundle_ids = Arc::clone(&bundle_ids);
    let sink = source
        .group_by_key_named("group")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
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
            "same-cursor-heartbeat-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    fail_once: None,
                    panic_once: None,
                    fail_write_once: None,
                    bundle_ids: Some(Arc::clone(&sink_bundle_ids)),
                    staged: Vec::new(),
                    bundle: None,
                }))
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
        ["1:[2]:Early:0", "1:[2]:OnTime:1"]
    );
    let ids = bundle_ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
}
