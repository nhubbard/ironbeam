#![allow(clippy::too_many_lines)] // End-to-end checkpoint fixtures keep setup and assertions together.

use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

struct Script {
    polls: VecDeque<UnboundedPoll<Timestamped<(i32, i32)>>>,
}

#[test]
fn one_unbounded_input_uses_one_group_bucket() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let source = pipeline
        .test_stream_source(
            "single",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 5_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = source
        .co_group_homogeneous_named("group", &[])
        .unwrap()
        .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "single-group-sink-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*committed.lock().unwrap(), ["1:[[5]]".to_owned()]);
}

#[test]
fn shared_unbounded_source_runs_common_transform_once_before_three_branches() {
    let pipeline = Pipeline::default();
    let visits = Arc::new(AtomicUsize::new(0));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let common_visits = Arc::clone(&visits);
    let source = pipeline
        .test_stream_source(
            "shared",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, (0_i32, 10_i32)), 1_000_000),
                    ((1, (1, 20)), 2_000_000),
                    ((1, (2, 30)), 3_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .map_named("common", move |value| {
            common_visits.fetch_add(1, Ordering::SeqCst);
            value
        })
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let inputs = (0..3)
        .map(|index| {
            source
                .filter_named(&format!("branch-{index}"), move |(_, (branch, _))| {
                    *branch == index
                })
                .unwrap()
                .map_named(&format!("value-{index}"), |(key, (_, value))| (key, value))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = inputs[0]
        .co_group_homogeneous_named("group", &inputs[1..])
        .unwrap()
        .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-group-sink-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        *committed.lock().unwrap(),
        ["1:[[10], [20], [30]]".to_owned()]
    );
    assert_eq!(visits.load(Ordering::SeqCst), 3);
}

#[test]
fn shared_unbounded_source_restores_one_cursor_and_open_group() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let hold = Arc::new(AtomicBool::new(true));
    let visits = Arc::new(AtomicUsize::new(0));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let factory_hold = Arc::clone(&hold);
    let factory_opens = Arc::clone(&opens);
    let common_visits = Arc::clone(&visits);
    let source = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "shared-recovery-v1".into(),
            0,
            move |cursor| {
                factory_opens.lock().unwrap().push(cursor.clone());
                let batch = match cursor.as_deref() {
                    None => UnboundedBatch {
                        elements: vec![
                            Timestamped {
                                value: (1_i32, 10_i32),
                                timestamp_ns: 1_000_000,
                            },
                            Timestamped {
                                value: (1, 20),
                                timestamp_ns: 2_000_000,
                            },
                        ],
                        progress: ProgressSignal {
                            source_key: "source".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(2_000_000),
                            completed: false,
                        },
                    },
                    Some([1]) if !factory_hold.load(Ordering::SeqCst) => UnboundedBatch {
                        elements: Vec::new(),
                        progress: ProgressSignal {
                            source_key: "source".into(),
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
                            as TypedUnboundedSourceReader<(i32, i32)>);
                    }
                    _ => return Err(Error::incompatible("unexpected shared cursor")),
                };
                Ok(Box::new(Script {
                    polls: vec![UnboundedPoll::Ready(batch)].into(),
                }) as TypedUnboundedSourceReader<(i32, i32)>)
            },
        )
        .unwrap()
        .map_named("common", move |row| {
            common_visits.fetch_add(1, Ordering::SeqCst);
            row
        })
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let first = source
        .filter_named("first", |(_, value)| *value == 10)
        .unwrap();
    let second = source
        .filter_named("second", |(_, value)| *value == 20)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = first
        .co_group_homogeneous_named("group", &[second])
        .unwrap()
        .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-recovery-sink-v1".into(),
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
            binary_build_id: "shared-recovery-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let expected_progress = 1_u64.to_le_bytes().to_vec();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.progress().and_then(|signal| signal.checkpoint_position)
        != Some(expected_progress.clone())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        job.progress().and_then(|signal| signal.checkpoint_position),
        Some(expected_progress)
    );
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*committed.lock().unwrap(), ["1:[[10], [20]]".to_owned()]);
    assert_eq!(visits.load(Ordering::SeqCst), 2);
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
}

#[test]
fn grouped_stream_rejects_wrong_source_identity_and_regressing_watermark() {
    for wrong_identity in [true, false] {
        let pipeline = Pipeline::default();
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let source = pipeline
            .unbounded_source_with_factory(
                "source",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "invalid-progress-v1".into(),
                0,
                move |_| {
                    let progress = |cursor, watermark, key: &str| ProgressSignal {
                        source_key: key.into(),
                        split_id: "split-0".into(),
                        checkpoint_position: Some(vec![cursor]),
                        event_time_ns: Some(watermark),
                        completed: false,
                    };
                    let first = UnboundedPoll::Ready(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: (1_i32, 5_i32),
                            timestamp_ns: 1_000_000,
                        }],
                        progress: progress(
                            1,
                            5_000_000,
                            if wrong_identity { "wrong" } else { "source" },
                        ),
                    });
                    let second = UnboundedPoll::Ready(UnboundedBatch {
                        elements: Vec::new(),
                        progress: progress(2, 4_000_000, "source"),
                    });
                    Ok(Box::new(Script {
                        polls: vec![first, second].into(),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                },
            )
            .unwrap()
            .fixed_windows("window", 10_000_000)
            .unwrap();
        let sink_committed = Arc::clone(&committed);
        let sink_accepted = Arc::clone(&accepted);
        let sink = source
            .co_group_homogeneous_named("group", &[])
            .unwrap()
            .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "invalid-progress-sink-v1".into(),
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
        let error = DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
        assert!(committed.lock().unwrap().is_empty());
    }
}

impl UnboundedSource<Timestamped<(i32, i32)>> for Script {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

struct Capture {
    committed: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    bundle: Option<String>,
    staged: Vec<String>,
}

impl TypedSink<String> for Capture {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.bundle = Some(instruction_id.to_owned());
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
fn three_unbounded_sources_restore_open_group_and_independent_cursors() {
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
                format!("source-{index}-v1"),
                0,
                move |cursor| {
                    opens
                        .lock()
                        .unwrap()
                        .push((source_key.clone(), cursor.clone()));
                    let batch = match cursor.as_deref() {
                        None => UnboundedBatch {
                            elements: vec![Timestamped {
                                value: (1, index),
                                timestamp_ns: i64::from(index) * 1_000_000,
                            }],
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![1]),
                                event_time_ns: Some(i64::from(index) * 1_000_000),
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
                                as TypedUnboundedSourceReader<(i32, i32)>);
                        }
                        _ => return Err(Error::incompatible("unexpected source cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: vec![UnboundedPoll::Ready(batch)].into(),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                },
            )
            .unwrap()
            .fixed_windows(&format!("window-{index}"), 10_000_000)
            .unwrap()
    };
    let first = make_source(0);
    let second = make_source(1);
    let third = make_source(2);
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = first
        .co_group_homogeneous_named("group", &[second, third])
        .unwrap()
        .with_window_named("with-window")
        .unwrap()
        .reify_timestamps_named("with-time")
        .unwrap()
        .map_named("format", |(time, (window, (key, mut buckets)))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!(
                "{}..{}:{time}:{key}:{buckets:?}",
                window.start_ns, window.end_ns
            )
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "group-sink-v1".into(),
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
            binary_build_id: "multi-group-test-build".into(),
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
        ["0..10000000:0:1:[[0], [1], [2]]".to_owned()]
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
