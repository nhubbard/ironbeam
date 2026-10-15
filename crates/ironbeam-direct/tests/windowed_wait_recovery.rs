#![allow(clippy::too_many_lines)] // Keep the checkpoint, restart, and cursor assertions together.

use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
    UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

struct Script {
    polls: VecDeque<UnboundedPoll<Timestamped<i32>>>,
}

impl UnboundedSource<Timestamped<i32>> for Script {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

struct Capture {
    committed: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    bundle: Option<String>,
    staged: Vec<String>,
}

#[test]
fn one_windowed_signal_releases_main_before_main_watermark() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let main = pipeline
        .unbounded_source_with_factory(
            "main",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "main-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![UnboundedPoll::Ready(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: 7,
                            timestamp_ns: 1_000_000,
                        }],
                        progress: ProgressSignal {
                            source_key: "main".into(),
                            split_id: "main-split".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(1_000_000),
                            completed: false,
                        },
                    })]
                    .into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .fixed_windows("main-window", 10_000_000)
        .unwrap();
    let signal = pipeline
        .unbounded_source_with_factory(
            "signal",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "signal-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![UnboundedPoll::Ready(UnboundedBatch {
                        elements: Vec::new(),
                        progress: ProgressSignal {
                            source_key: "signal".into(),
                            split_id: "signal-split".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(10_000_000),
                            completed: false,
                        },
                    })]
                    .into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .fixed_windows("signal-window", 10_000_000)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = main
        .wait_on_windowed_named("barrier", &signal)
        .unwrap()
        .map_named("format", |value| value.to_string())
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "early-windowed-wait-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while committed.lock().unwrap().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(*committed.lock().unwrap(), ["7".to_owned()]);
    assert_eq!(job.progress().unwrap().event_time_ns, Some(1_000_000));
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
}

#[test]
fn early_windowed_wait_releases_after_checkpoint_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let signal_ready = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let main = pipeline
        .unbounded_source_with_factory(
            "main",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "checkpoint-main-v1".into(),
            0,
            |cursor| {
                let polls = if cursor.is_none() {
                    vec![UnboundedPoll::Ready(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: 7_i32,
                            timestamp_ns: 1_000_000,
                        }],
                        progress: ProgressSignal {
                            source_key: "main".into(),
                            split_id: "main-split".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(1_000_000),
                            completed: false,
                        },
                    })]
                } else {
                    assert_eq!(cursor, Some(vec![1]));
                    Vec::new()
                };
                Ok(Box::new(Script {
                    polls: polls.into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .fixed_windows("main-window", 10_000_000)
        .unwrap();
    let ready = Arc::clone(&signal_ready);
    let signal = pipeline
        .unbounded_source_with_factory(
            "signal",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "checkpoint-signal-v1".into(),
            0,
            move |cursor| {
                assert!(cursor.is_none());
                let polls = if ready.load(Ordering::SeqCst) {
                    vec![UnboundedPoll::Ready(UnboundedBatch {
                        elements: Vec::new(),
                        progress: ProgressSignal {
                            source_key: "signal".into(),
                            split_id: "signal-split".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(10_000_000),
                            completed: true,
                        },
                    })]
                } else {
                    Vec::new()
                };
                Ok(Box::new(Script {
                    polls: polls.into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .fixed_windows("signal-window", 10_000_000)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = main
        .wait_on_windowed_named("barrier", &signal)
        .unwrap()
        .map_named("format", |value| value.to_string())
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "checkpoint-early-wait-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "checkpoint-early-wait-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while first.progress().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(first.progress().is_some());
    assert!(committed.lock().unwrap().is_empty());
    first.cancel();
    assert_eq!(first.wait().err().unwrap().kind, ErrorKind::Cancelled);
    signal_ready.store(true, Ordering::SeqCst);
    let resumed = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while committed.lock().unwrap().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(*committed.lock().unwrap(), ["7".to_owned()]);
    resumed.cancel();
    assert_eq!(resumed.wait().err().unwrap().kind, ErrorKind::Cancelled);
}

impl TypedSink<String> for Capture {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        self.bundle = Some(id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: String) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if self
            .accepted
            .lock()
            .unwrap()
            .insert(self.bundle.take().unwrap())
        {
            self.committed.lock().unwrap().append(&mut self.staged);
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

#[test]
fn heterogeneous_windowed_wait_restores_open_window_and_three_cursors() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let hold = Arc::new(AtomicBool::new(true));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let make_source = |index: i32| {
        let key = format!("source-{index}");
        let source_key = key.clone();
        let hold = Arc::clone(&hold);
        let opens = Arc::clone(&opens);
        pipeline
            .unbounded_source_with_factory(
                &key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("windowed-wait-source-{index}-v1"),
                0,
                move |cursor| {
                    opens
                        .lock()
                        .unwrap()
                        .push((source_key.clone(), cursor.clone()));
                    let batch = match cursor.as_deref() {
                        None => UnboundedBatch {
                            elements: vec![Timestamped {
                                value: index,
                                timestamp_ns: 1_000_000,
                            }],
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![1]),
                                event_time_ns: Some(1_000_000),
                                completed: false,
                            },
                        },
                        Some([1]) if !hold.load(Ordering::SeqCst) => UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![2]),
                                event_time_ns: Some(10_000_000),
                                completed: true,
                            },
                        },
                        Some([1]) => {
                            return Ok(Box::new(Script {
                                polls: VecDeque::new(),
                            })
                                as TypedUnboundedSourceReader<i32>);
                        }
                        _ => return Err(Error::incompatible("unexpected wait source cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: vec![UnboundedPoll::Ready(batch)].into(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap()
            .fixed_windows(&format!("window-{index}"), 10_000_000)
            .unwrap()
    };
    let main = make_source(0);
    let text = make_source(1)
        .map_named("text", |_| "ready".to_owned())
        .unwrap()
        .windowed_wait_signal_named("text-marker")
        .unwrap();
    let flag = make_source(2)
        .map_named("flag", |_| true)
        .unwrap()
        .windowed_wait_signal_named("flag-marker")
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = main
        .wait_on_windowed_signals_named("barrier", &[text, flag])
        .unwrap()
        .with_window_named("with-window")
        .unwrap()
        .reify_timestamps_named("with-time")
        .unwrap()
        .map_named("format", |(time, (window, value))| {
            format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "windowed-wait-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "windowed-wait-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first_job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let expected_progress = 3_u64.to_le_bytes().to_vec();
    let deadline = Instant::now() + Duration::from_secs(5);
    while first_job
        .progress()
        .and_then(|signal| signal.checkpoint_position)
        != Some(expected_progress.clone())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        first_job
            .progress()
            .and_then(|signal| signal.checkpoint_position),
        Some(expected_progress)
    );
    assert!(committed.lock().unwrap().is_empty());
    first_job.cancel();
    assert_eq!(first_job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    let resumed = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    assert_eq!(resumed.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(
        *committed.lock().unwrap(),
        ["0..10000000:1000000:0".to_owned()]
    );
    let opens = opens.lock().unwrap();
    assert_eq!(opens.len(), 6);
    assert_eq!(
        opens.iter().filter(|(_, cursor)| cursor.is_none()).count(),
        3
    );
    assert_eq!(
        opens
            .iter()
            .filter(|(_, cursor)| cursor.as_deref() == Some([1].as_slice()))
            .count(),
        3
    );
}
