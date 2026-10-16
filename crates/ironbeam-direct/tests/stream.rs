#![allow(clippy::too_many_lines)] // End-to-end streaming scenarios retain their setup and assertions together.

use ironbeam_core::cogroup_by_key_2_named;
use ironbeam_core::{
    DynOp, Error, ErrorKind, Partition, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec,
    SourceProgress, TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader,
    UnboundedBatch, UnboundedPoll, UnboundedSource, Version,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus, SpillConfig};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
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
    committed: Arc<Mutex<Vec<i32>>>,
    staged: Vec<i32>,
    fail_commit: Arc<AtomicBool>,
}

struct BundleCapture {
    committed: Arc<Mutex<Vec<i32>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    staged: Vec<i32>,
    bundle: Option<String>,
    fail_commit: Arc<AtomicBool>,
    cancel_on_write: Option<Arc<AtomicBool>>,
}

impl TypedSink<i32> for BundleCapture {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.bundle = Some(instruction_id.to_owned());
        Ok(())
    }
    fn write(&mut self, value: i32) -> Result<(), Error> {
        self.staged.push(value);
        if let Some(token) = &self.cancel_on_write {
            token.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(Error::execution("sink", "direct", "commit failed"));
        }
        let id = self.bundle.take().unwrap();
        if self.accepted.lock().unwrap().insert(id) {
            self.committed.lock().unwrap().extend(self.staged.drain(..));
        }
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

fn pair_fixture(
    left_values: Arc<Mutex<Vec<i32>>>,
    right_values: Arc<Mutex<Vec<i32>>>,
    opens: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    common_visits: Arc<AtomicUsize>,
    hold_after_first: Arc<AtomicBool>,
    right_fails: Arc<AtomicBool>,
    cancel_on_left_write: Option<Arc<AtomicBool>>,
) -> (ironbeam_core::Sink<i32>, ironbeam_core::Sink<i32>) {
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "pair-source-v1".into(),
            0,
            move |position| {
                opens.lock().unwrap().push(position.clone());
                let polls = match position.as_deref() {
                    None if hold_after_first.load(Ordering::SeqCst) => {
                        vec![batch(1, 1, false)].into()
                    }
                    None => vec![batch(1, 1, false), batch(2, 2, true)].into(),
                    Some([1]) if hold_after_first.load(Ordering::SeqCst) => VecDeque::new(),
                    Some([1]) => vec![batch(2, 2, true)].into(),
                    _ => return Err(Error::incompatible("unexpected pair source cursor")),
                };
                Ok(Box::new(Script { polls }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let common = source
        .map_named("plus-ten", move |value| {
            common_visits.fetch_add(1, Ordering::SeqCst);
            value + 10
        })
        .unwrap();
    let left_accepted = Arc::new(Mutex::new(HashSet::new()));
    let right_accepted = Arc::new(Mutex::new(HashSet::new()));
    let left = common
        .map_named("double", |value| value * 2)
        .unwrap()
        .sink_named_with_config(
            "left-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "pair-left-v1".into(),
            move || {
                Ok(Box::new(BundleCapture {
                    committed: Arc::clone(&left_values),
                    accepted: Arc::clone(&left_accepted),
                    staged: Vec::new(),
                    bundle: None,
                    fail_commit: Arc::new(AtomicBool::new(false)),
                    cancel_on_write: cancel_on_left_write.clone(),
                }))
            },
        )
        .unwrap();
    let right = common
        .map_named("plus-hundred", |value| value + 100)
        .unwrap()
        .sink_named_with_config(
            "right-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "pair-right-v1".into(),
            move || {
                Ok(Box::new(BundleCapture {
                    committed: Arc::clone(&right_values),
                    accepted: Arc::clone(&right_accepted),
                    staged: Vec::new(),
                    bundle: None,
                    fail_commit: Arc::clone(&right_fails),
                    cancel_on_write: None,
                }))
            },
        )
        .unwrap();
    (left, right)
}

impl TypedSink<i32> for Capture {
    fn write(&mut self, value: i32) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(Error::execution("sink", "direct", "commit failed"));
        }
        self.committed.lock().unwrap().extend(self.staged.drain(..));
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

fn signal(cursor: u8, completed: bool) -> ProgressSignal {
    ProgressSignal {
        source_key: "events".into(),
        split_id: "split-0".into(),
        checkpoint_position: Some(vec![cursor]),
        event_time_ns: Some(i64::from(cursor) * 100),
        completed,
    }
}

fn batch(value: i32, cursor: u8, completed: bool) -> UnboundedPoll<Timestamped<i32>> {
    UnboundedPoll::Ready(UnboundedBatch {
        elements: vec![Timestamped {
            value,
            timestamp_ns: i64::from(cursor) * 100,
        }],
        progress: signal(cursor, completed),
    })
}

#[test]
fn grouped_stream_closes_fixed_windows_on_watermarks() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "grouped-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |_| {
                    opens.fetch_add(1, Ordering::SeqCst);
                    let ready = |cursor, watermark, completed, values: Vec<(i32, i64)>| {
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: values
                                .into_iter()
                                .map(|(value, timestamp_ns)| Timestamped {
                                    value,
                                    timestamp_ns,
                                })
                                .collect(),
                            progress: ProgressSignal {
                                source_key: "events".into(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![cursor]),
                                event_time_ns: Some(watermark),
                                completed,
                            },
                        })
                    };
                    Ok(Box::new(Script {
                        polls: vec![
                            ready(1, 10, false, vec![(1, 10), (2, 20)]),
                            ready(2, 30, false, vec![(3, 30)]),
                            ready(3, 100, false, vec![]),
                            ready(4, 110, false, vec![(4, 110)]),
                            ready(5, 200, true, vec![]),
                        ]
                        .into(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap();
    let sink = source
        .fixed_windows("windows", 100)
        .unwrap()
        .map_named("key", |value| (value % 2, value))
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_timestamps_named("group-time")
        .unwrap()
        .map_named("sum", |(timestamp, (key, values))| {
            i32::try_from(timestamp).unwrap() * 1_000 + key * 100 + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "grouped-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let job_config = config(temporary.path());
    let job = DirectRunner.start_stream_sink(&sink, &job_config).unwrap();
    let handle = job.wait().unwrap();
    assert_eq!(handle.status(), JobStatus::Succeeded);
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    DirectRunner
        .start_stream_sink(&sink, &job_config)
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    let mut rows = committed.lock().unwrap().clone();
    rows.sort_unstable();
    assert_eq!(rows, [10_104, 20_002, 110_004]);
}

#[test]
fn unbounded_sessions_merge_bridge_before_watermark_and_drop_closed_late_row() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "events",
            vec![
                TestStreamEvent::Elements(vec![
                    ((3_i32, 11_i32), -10_000_000),
                    ((1_i32, 1_i32), 0),
                    ((1, 2), 8_000_000),
                    ((2, 7), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
                TestStreamEvent::Elements(vec![((1, 99), 0)]),
            ],
        )
        .unwrap()
        .group_by_session_named("sessions", 5_000_000)
        .unwrap()
        .with_window_named("bounds")
        .unwrap()
        .map_named("score", |(window, (key, values))| {
            i32::try_from(window.start_ns / 1_000_000).unwrap() * 1_000
                + i32::try_from(window.end_ns / 1_000_000).unwrap() * 10
                + key * 100
                + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "session-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
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
    let mut actual = committed.lock().unwrap().clone();
    actual.sort_unstable();
    assert_eq!(actual, [-9739, 236, 4297]);
}

#[test]
fn live_merged_session_accepts_a_bridge_past_its_original_expiry() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "events",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 1_i32), 0), ((1, 2), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(6_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 0)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 99), 0)]),
            ],
        )
        .unwrap()
        .group_by_session_named("sessions", 5_000_000)
        .unwrap()
        .with_window_named("bounds")
        .unwrap()
        .map_named("score", |(window, (_key, values))| {
            i32::try_from(window.end_ns / 1_000_000).unwrap() * 100 + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "live-session-merge-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
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
    assert_eq!(*committed.lock().unwrap(), [906]);
}

#[test]
fn unbounded_session_bridge_merges_after_checkpoint_restart() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pause = Arc::new(AtomicBool::new(true));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "session-restart-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                let pause = Arc::clone(&pause);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let ready = |cursor, watermark, completed, values: Vec<(i32, i64)>| {
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: values
                                .into_iter()
                                .map(|(value, timestamp_ns)| Timestamped {
                                    value,
                                    timestamp_ns,
                                })
                                .collect(),
                            progress: ProgressSignal {
                                source_key: "events".into(),
                                split_id: "session-split".into(),
                                checkpoint_position: Some(vec![cursor]),
                                event_time_ns: Some(watermark),
                                completed,
                            },
                        })
                    };
                    let first = ready(
                        1,
                        1_000_000,
                        false,
                        vec![(11, -10_000_000), (1, 0), (2, 8_000_000)],
                    );
                    let second = ready(2, 20_000_000, true, vec![(3, 4_000_000)]);
                    let polls = match position.as_deref() {
                        None if pause.load(Ordering::SeqCst) => vec![first],
                        None => vec![first, second],
                        Some([1]) => vec![second],
                        _ => return Err(Error::incompatible("unexpected session cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap();
    let sink = source
        .map_named("key", |value| (1_i32, value))
        .unwrap()
        .group_by_session_named("sessions", 5_000_000)
        .unwrap()
        .with_window_named("bounds")
        .unwrap()
        .map_named("score", |(window, (_key, values))| {
            i32::try_from(window.end_ns / 1_000_000).unwrap() * 10 + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "session-restart-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job.progress().and_then(|signal| signal.checkpoint_position) != Some(vec![1]) {
        assert!(
            Instant::now() < deadline,
            "session checkpoint was not saved"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(committed.lock().unwrap().as_slice(), &[-39]);
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    pause.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_eq!(committed.lock().unwrap().as_slice(), &[-39, 136]);
}

#[test]
fn restored_live_session_accepts_candidate_past_its_original_expiry() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pause = Arc::new(AtomicBool::new(true));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "resolved-session-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                let pause = Arc::clone(&pause);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let ready = |cursor, watermark, completed, values: Vec<(i32, i64)>| {
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: values
                                .into_iter()
                                .map(|(value, timestamp_ns)| Timestamped {
                                    value,
                                    timestamp_ns,
                                })
                                .collect(),
                            progress: ProgressSignal {
                                source_key: "events".into(),
                                split_id: "session-split".into(),
                                checkpoint_position: Some(vec![cursor]),
                                event_time_ns: Some(watermark),
                                completed,
                            },
                        })
                    };
                    let first = ready(1, 6_000_000, false, vec![(1, 0), (2, 4_000_000)]);
                    let second = ready(2, 10_000_000, true, vec![(3, 0)]);
                    let polls = match position.as_deref() {
                        None if pause.load(Ordering::SeqCst) => vec![first],
                        None => vec![first, second],
                        Some([1]) => vec![second],
                        _ => return Err(Error::incompatible("unexpected session cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap();
    let sink = source
        .map_named("key", |value| (1_i32, value))
        .unwrap()
        .group_by_session_named("sessions", 5_000_000)
        .unwrap()
        .with_window_named("bounds")
        .unwrap()
        .map_named("score", |(window, (_key, values))| {
            i32::try_from(window.end_ns / 1_000_000).unwrap() * 10 + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "resolved-session-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job.progress().and_then(|signal| signal.checkpoint_position) != Some(vec![1]) {
        assert!(
            Instant::now() < deadline,
            "session checkpoint was not saved"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    pause.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_eq!(committed.lock().unwrap().as_slice(), &[96]);
}

#[test]
fn unbounded_main_uses_bounded_side_by_fixed_window() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pause_after_first = Arc::new(AtomicBool::new(true));
    let pipeline = Pipeline::default();
    let main = pipeline
        .unbounded_source_with_factory(
            "main",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "windowed-side-main-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                let pause_after_first = Arc::clone(&pause_after_first);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let ready = |cursor, watermark, completed, values: Vec<(i32, i64)>| {
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: values
                                .into_iter()
                                .map(|(value, timestamp_ns)| Timestamped {
                                    value,
                                    timestamp_ns,
                                })
                                .collect(),
                            progress: ProgressSignal {
                                source_key: "main".into(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![cursor]),
                                event_time_ns: Some(watermark),
                                completed,
                            },
                        })
                    };
                    let first = ready(1, 0, false, vec![(1, 1_000_000)]);
                    let later = vec![
                        ready(2, 2_000_000, false, vec![]),
                        ready(3, 2_000_000, false, vec![(2, 3_000_000)]),
                        ready(4, 4_000_000, false, vec![]),
                        ready(5, 4_000_000, false, vec![(3, 5_000_000)]),
                        ready(6, 6_000_000, true, vec![]),
                    ];
                    let polls = match position.as_deref() {
                        None if pause_after_first.load(Ordering::SeqCst) => vec![first],
                        None => std::iter::once(first).chain(later).collect(),
                        Some([1]) => later,
                        _ => return Err(Error::incompatible("unexpected side-view resume cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap()
        .fixed_windows("main-window", 2_000_000)
        .unwrap();
    let side = pipeline
        .from_vec("side", vec![(0_i64, 10_i32), (2_000_000, 20)])
        .unwrap()
        .timestamps_from_pairs_named("side-time")
        .unwrap()
        .fixed_windows("side-window", 2_000_000)
        .unwrap();
    let sink = main
        .map_with_windowed_side_named("side-add", &side, |value, values| {
            value + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "windowed-side-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let tight = JobConfig {
        buffer_budget_elements: 1,
        ..JobConfig::default()
    };
    let error = DirectRunner
        .start_stream_sink(&sink, &tight)
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Resource);
    assert!(opens.lock().unwrap().is_empty());
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job.progress().and_then(|signal| signal.checkpoint_position) != Some(vec![1]) {
        assert!(
            Instant::now() < deadline,
            "open side-view window was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    pause_after_first.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_eq!(*committed.lock().unwrap(), [11, 22, 3]);
    DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().len(), 2);
}

#[test]
fn unbounded_full_join_emits_bounded_side_only_and_main_only_rows() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pause_after_first = Arc::new(AtomicBool::new(true));
    let pipeline = Pipeline::default();
    let main = pipeline
        .unbounded_source_with_factory(
            "main",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "join-main-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                let pause_after_first = Arc::clone(&pause_after_first);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let first = UnboundedPoll::Ready(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: 10,
                            timestamp_ns: 1,
                        }],
                        progress: ProgressSignal {
                            source_key: "main".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(10),
                            completed: false,
                        },
                    });
                    let last = UnboundedPoll::Ready(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: 40,
                            timestamp_ns: 11,
                        }],
                        progress: ProgressSignal {
                            source_key: "main".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: Some(vec![2]),
                            event_time_ns: Some(20),
                            completed: true,
                        },
                    });
                    let polls = match position.as_deref() {
                        None if pause_after_first.load(Ordering::SeqCst) => vec![first],
                        None => vec![first, last],
                        Some([1]) => vec![last],
                        _ => return Err(Error::incompatible("unexpected join resume cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap()
        .fixed_windows("main-window", 10)
        .unwrap()
        .map_named("main-key", |value| (if value == 10 { 1 } else { 3 }, value))
        .unwrap();
    let side = pipeline
        .from_vec("side", vec![(0_i64, (1_i32, 20_i32)), (0, (2, 30))])
        .unwrap()
        .timestamps_from_pairs_named("side-time")
        .unwrap()
        .fixed_windows("side-window", 10)
        .unwrap();
    let sink = main
        .join_full_named("join", &side)
        .unwrap()
        .map_named("sum", |(key, (left, right))| {
            key * 100 + left.unwrap_or(0) + right.unwrap_or(0)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "join-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job.progress().and_then(|signal| signal.checkpoint_position) != Some(vec![1]) {
        assert!(
            Instant::now() < deadline,
            "join window was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(committed.lock().unwrap().len(), 2);
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    pause_after_first.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .unwrap();
    let mut rows = committed.lock().unwrap().clone();
    rows.sort_unstable();
    assert_eq!(rows, [130, 230, 340]);
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
}

#[test]
fn two_unbounded_join_waits_for_both_watermarks_and_recovers_open_window() {
    dual_join_recovers_open_window(false, false);
}

#[test]
fn two_unbounded_session_join_resolves_expired_candidate_after_restore() {
    dual_join_recovers_open_window(true, false);
}

#[test]
fn two_unbounded_session_cogroup_resolves_expired_candidate_after_restore() {
    dual_join_recovers_open_window(true, true);
}

fn dual_join_recovers_open_window(session: bool, cogroup: bool) {
    let temporary = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pause_side = Arc::new(AtomicBool::new(true));
    let make_source = |key: &str, value: i32| {
        let source_key = key.to_owned();
        let opens = Arc::clone(&opens);
        let pause_side = Arc::clone(&pause_side);
        let collection = pipeline
            .unbounded_source_with_factory(
                key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("{key}-v1"),
                0,
                move |cursor| {
                    opens
                        .lock()
                        .unwrap()
                        .push((source_key.clone(), cursor.clone()));
                    let ready = |position, watermark, completed, values: Vec<i32>| {
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: values
                                .into_iter()
                                .map(|value| Timestamped {
                                    value,
                                    timestamp_ns: if session {
                                        match value {
                                            11 => 4,
                                            20 => 0,
                                            _ => 0,
                                        }
                                    } else {
                                        1
                                    },
                                })
                                .collect(),
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![position]),
                                event_time_ns: Some(watermark),
                                completed,
                            },
                        })
                    };
                    let first_values = if session {
                        if source_key == "main" {
                            vec![value, 11]
                        } else {
                            vec![]
                        }
                    } else {
                        vec![value]
                    };
                    let last_values = if session && source_key == "side" {
                        vec![value]
                    } else {
                        vec![]
                    };
                    // In the session case the [0, 5) candidate arrives from
                    // the side after this watermark and after checkpoint
                    // restore. The main side's merged [0, 9) session is open.
                    let first = ready(1, if session { 6 } else { 0 }, false, first_values);
                    let last = ready(2, 20, true, last_values);
                    let polls = match (source_key.as_str(), cursor.as_deref()) {
                        ("main", None) => vec![first, last],
                        ("side", None) if pause_side.load(Ordering::SeqCst) => vec![first],
                        ("side", Some([1])) => vec![last],
                        _ => return Err(Error::incompatible("unexpected dual stream cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap();
        let windowed = if session {
            collection.session_windows_named(&format!("{key}-window"), 5)
        } else {
            collection.fixed_windows(&format!("{key}-window"), 10)
        }
        .unwrap();
        windowed
            .map_named(&format!("{key}-key"), |value| (1_i32, value))
            .unwrap()
    };
    let main = make_source("main", 10);
    let side = make_source("side", 20);
    let scored = if cogroup {
        cogroup_by_key_2_named("co-group", &main, &side)
            .unwrap()
            .map_named("sum-groups", |(key, (left, right))| {
                key * 100 + left.iter().sum::<i32>() + right.iter().sum::<i32>()
            })
            .unwrap()
    } else {
        main.join_inner_named("join", &side)
            .unwrap()
            .map_named("sum", |(key, (left, right))| key * 100 + left + right)
            .unwrap()
    };
    let sink = scored
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-join-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.progress().and_then(|signal| signal.checkpoint_position)
        != Some(3_u64.to_le_bytes().to_vec())
    {
        assert!(
            Instant::now() < deadline,
            "both open windows were not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    pause_side.store(false, Ordering::SeqCst);
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &config(temporary.path()))
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    assert_eq!(
        *committed.lock().unwrap(),
        if cogroup {
            vec![141]
        } else if session {
            vec![130, 131]
        } else {
            vec![130]
        }
    );
    assert_eq!(
        *opens.lock().unwrap(),
        vec![
            ("main".into(), None),
            ("side".into(), None),
            ("side".into(), Some(vec![1])),
        ]
    );
}

#[test]
fn two_unbounded_full_join_emits_both_unmatched_sides() {
    let temporary = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let fail_commit = Arc::new(AtomicBool::new(true));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let make_source = |key: &str, value: i32, join_key: i32| {
        let source_key = key.to_owned();
        let opens = Arc::clone(&opens);
        pipeline
            .unbounded_source_with_factory(
                key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("{key}-v1"),
                0,
                move |cursor| {
                    opens
                        .lock()
                        .unwrap()
                        .push((source_key.clone(), cursor.clone()));
                    if cursor.is_some() {
                        return Err(Error::incompatible("unexpected full-join resume cursor"));
                    }
                    Ok(Box::new(Script {
                        polls: vec![UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value,
                                timestamp_ns: 1,
                            }],
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![1]),
                                event_time_ns: Some(10),
                                completed: true,
                            },
                        })]
                        .into(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap()
            .fixed_windows(&format!("{key}-window"), 10)
            .unwrap()
            .map_named(&format!("{key}-key"), move |value| (join_key, value))
            .unwrap()
    };
    let main = make_source("main", 10, 1);
    let side = make_source("side", 20, 2);
    let sink = main
        .join_full_named("join", &side)
        .unwrap()
        .map_named("sum", |(key, (left, right))| {
            key * 100 + left.unwrap_or(0) + right.unwrap_or(0)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-full-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                let fail_commit = Arc::clone(&fail_commit);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::clone(&fail_commit),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let error = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(committed.lock().unwrap().is_empty());
    fail_commit.store(false, Ordering::SeqCst);
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &config(temporary.path()))
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    let mut rows = committed.lock().unwrap().clone();
    rows.sort_unstable();
    assert_eq!(rows, vec![110, 220]);
    assert_eq!(
        *opens.lock().unwrap(),
        vec![
            ("main".into(), None),
            ("side".into(), None),
            ("side".into(), None),
        ]
    );
}

#[test]
fn two_unbounded_cogroup_preserves_duplicate_and_side_only_keys() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let source = |key: &str, rows: Vec<(i32, i32)>| {
        pipeline
            .test_stream_source(
                key,
                vec![
                    TestStreamEvent::Elements(rows.into_iter().map(|row| (row, 1_i64)).collect()),
                    TestStreamEvent::AdvanceWatermark(10),
                ],
            )
            .unwrap()
            .fixed_windows(&format!("{key}-window"), 10)
            .unwrap()
    };
    let main = source("group-main", vec![(1, 10), (1, 11)]);
    let side = source("group-side", vec![(1, 20), (2, 30)]);
    let sink = ironbeam_core::cogroup_by_key_2_named("group", &main, &side)
        .unwrap()
        .map_named("group-counts", |(key, (main, side))| {
            key * 100 + i32::try_from(main.len()).unwrap() * 10 + i32::try_from(side.len()).unwrap()
        })
        .unwrap()
        .sink_named_with_config(
            "group-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "group-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    let mut rows = committed.lock().unwrap().clone();
    rows.sort_unstable();
    assert_eq!(rows, vec![121, 201]);
}

#[test]
fn two_unbounded_join_drops_late_rows_after_both_watermarks_close_window() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let main = pipeline
        .test_stream_source(
            "late-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 10_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 11), 1_000_000)]),
            ],
        )
        .unwrap()
        .fixed_windows("late-main-window", 10_000_000)
        .unwrap();
    let side = pipeline
        .test_stream_source(
            "late-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 20_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("late-side-window", 10_000_000)
        .unwrap();
    let sink = main
        .join_inner_named("late-join", &side)
        .unwrap()
        .map_named("late-sum", |(key, (left, right))| key * 100 + left + right)
        .unwrap()
        .sink_named_with_config(
            "late-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "late-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
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
    assert_eq!(*committed.lock().unwrap(), [130]);
}

#[test]
fn two_unbounded_session_join_accepts_live_bridge_then_drops_closed_row() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let main = pipeline
        .test_stream_source(
            "session-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 1_i32), 0), ((2, 99), 0)]),
                TestStreamEvent::Elements(vec![((1, 2), 8)]),
                TestStreamEvent::AdvanceWatermark(6),
                TestStreamEvent::Elements(vec![((1, 3), 0)]),
                TestStreamEvent::AdvanceWatermark(20),
                TestStreamEvent::Elements(vec![((1, 4), 0)]),
            ],
        )
        .unwrap()
        .session_windows_named("session-main-window", 5)
        .unwrap();
    let side = pipeline
        .test_stream_source(
            "session-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 10_i32), 4)]),
                TestStreamEvent::AdvanceWatermark(6),
                TestStreamEvent::AdvanceWatermark(20),
            ],
        )
        .unwrap()
        .session_windows_named("session-side-window", 5)
        .unwrap();
    let sink = main
        .join_inner_named("session-join", &side)
        .unwrap()
        .with_window_named("session-window")
        .unwrap()
        .map_named("session-value", |(window, (_, (left, right)))| {
            i32::try_from(window.end_ns).unwrap() * 100 + left + right
        })
        .unwrap()
        .sink_named_with_config(
            "session-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "session-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
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
    let mut rows = committed.lock().unwrap().clone();
    rows.sort_unstable();
    assert_eq!(rows, [1311, 1312, 1313]);
}

#[test]
fn two_unbounded_session_cogroup_accepts_live_bridge_then_drops_closed_row() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let main = pipeline
        .test_stream_source(
            "co-session-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 1_i32), 0), ((1, 2), 8)]),
                TestStreamEvent::AdvanceWatermark(6),
                TestStreamEvent::Elements(vec![((1, 3), 0)]),
                TestStreamEvent::AdvanceWatermark(20),
                TestStreamEvent::Elements(vec![((1, 99), 0)]),
            ],
        )
        .unwrap()
        .session_windows_named("co-session-main-window", 5)
        .unwrap();
    let side = pipeline
        .test_stream_source(
            "co-session-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 10_i32), 4)]),
                TestStreamEvent::AdvanceWatermark(6),
                TestStreamEvent::AdvanceWatermark(20),
            ],
        )
        .unwrap()
        .session_windows_named("co-session-side-window", 5)
        .unwrap();
    let sink = cogroup_by_key_2_named("co-session-group", &main, &side)
        .unwrap()
        .with_window_named("co-session-window")
        .unwrap()
        .map_named(
            "co-session-value",
            |(window, (_, (mut left, mut right)))| {
                left.sort_unstable();
                right.sort_unstable();
                i32::try_from(window.end_ns).unwrap() * 100
                    + left.iter().sum::<i32>()
                    + right.iter().sum::<i32>()
            },
        )
        .unwrap()
        .sink_named_with_config(
            "co-session-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "co-session-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
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
    assert_eq!(*committed.lock().unwrap(), [1316]);
}

#[test]
fn stateful_two_source_fanout_recovers_open_window_and_completed_checkpoint() {
    stateful_two_source_fanout_recovers(false);
}

#[test]
fn stateful_two_source_session_fanout_recovers_bridge_and_completed_checkpoint() {
    stateful_two_source_fanout_recovers(true);
}

fn stateful_two_source_fanout_recovers(session: bool) {
    let temporary = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let source_opens = Arc::new(AtomicUsize::new(0));
    let sink_opens = Arc::new(AtomicUsize::new(0));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let fail_commit = Arc::new(AtomicBool::new(false));
    let finish_side = Arc::new(AtomicBool::new(false));
    let source = |key: &str| {
        let opens = Arc::clone(&source_opens);
        let source_key = key.to_owned();
        let finish_side = Arc::clone(&finish_side);
        let collection = pipeline
            .unbounded_source_with_factory(
                key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("{key}-fanout-v1"),
                0,
                move |cursor| -> Result<TypedUnboundedSourceReader<i32>, Error> {
                    assert!(cursor.is_none());
                    opens.fetch_add(1, Ordering::SeqCst);
                    let polls =
                        if source_key == "fanout-side" && !finish_side.load(Ordering::SeqCst) {
                            Vec::new()
                        } else {
                            vec![UnboundedPoll::Ready(UnboundedBatch {
                                elements: if source_key == "fanout-main" {
                                    let mut rows = vec![Timestamped {
                                        value: 2,
                                        timestamp_ns: 1,
                                    }];
                                    if session {
                                        rows.push(Timestamped {
                                            value: 4,
                                            timestamp_ns: 9,
                                        });
                                    }
                                    rows
                                } else {
                                    vec![Timestamped {
                                        value: 3,
                                        timestamp_ns: if session { 5 } else { 1 },
                                    }]
                                },
                                progress: ProgressSignal {
                                    source_key: source_key.clone(),
                                    split_id: "split-0".into(),
                                    checkpoint_position: Some(vec![1]),
                                    event_time_ns: Some(10),
                                    completed: true,
                                },
                            })]
                        };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }))
                },
            )
            .unwrap();
        let windowed = if session {
            collection.session_windows_named(&format!("{key}-window"), 5)
        } else {
            collection.fixed_windows(&format!("{key}-window"), 10)
        }
        .unwrap();
        windowed
            .map_named(&format!("{key}-key"), |value| (1_i32, value))
            .unwrap()
    };
    let main = source("fanout-main");
    let side = source("fanout-side");
    let inner = main
        .join_inner_named("fanout-inner", &side)
        .unwrap()
        .map_named("inner-value", |(key, (left, right))| {
            10 + key + left + right
        })
        .unwrap();
    let left = main
        .join_left_named("fanout-left", &side)
        .unwrap()
        .map_named("left-value", |(key, (left, right))| {
            20 + key + left + right.unwrap_or_default()
        })
        .unwrap();
    let right = main
        .join_right_named("fanout-right", &side)
        .unwrap()
        .map_named("right-value", |(key, (left, right))| {
            30 + key + left.unwrap_or_default() + right
        })
        .unwrap();
    let full = main
        .join_full_named("fanout-full", &side)
        .unwrap()
        .map_named("full-value", |(key, (left, right))| {
            40 + key + left.unwrap_or_default() + right.unwrap_or_default()
        })
        .unwrap();
    let observed_sink_opens = Arc::clone(&sink_opens);
    let sink = inner
        .flatten_named("fanout-merge", &[left, right, full])
        .unwrap()
        .map_named("after-fanout", |value| value + 1)
        .unwrap()
        .sink_named_with_config(
            "fanout-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "fanout-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                let fail_commit = Arc::clone(&fail_commit);
                move || {
                    observed_sink_opens.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::clone(&fail_commit),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.progress().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(job.progress().is_some());
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    finish_side.store(true, Ordering::SeqCst);
    fail_commit.store(true, Ordering::SeqCst);
    assert!(
        DirectRunner
            .start_stream_sink(&sink, &config(temporary.path()))
            .unwrap()
            .wait()
            .is_err()
    );
    assert!(committed.lock().unwrap().is_empty());
    fail_commit.store(false, Ordering::SeqCst);
    for _ in 0..2 {
        assert_eq!(
            DirectRunner
                .start_stream_sink(&sink, &config(temporary.path()))
                .unwrap()
                .wait()
                .unwrap()
                .status(),
            JobStatus::Succeeded
        );
    }
    let mut values = committed.lock().unwrap().clone();
    values.sort_unstable();
    if session {
        assert_eq!(values, [17, 19, 27, 29, 37, 39, 47, 49]);
    } else {
        assert_eq!(values, [17, 27, 37, 47]);
    }
    assert_eq!(source_opens.load(Ordering::SeqCst), 4);
    assert_eq!(sink_opens.load(Ordering::SeqCst), 2);
}

#[test]
fn dual_stream_reopens_cursorless_watermark_after_checkpoint() {
    let temporary = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let opens = Arc::new(AtomicUsize::new(0));
    let finish_side = Arc::new(AtomicBool::new(false));
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
                            value: 10,
                            timestamp_ns: 1,
                        }],
                        progress: ProgressSignal {
                            source_key: "main".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(10),
                            completed: true,
                        },
                    })]
                    .into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .fixed_windows("main-window", 10)
        .unwrap()
        .map_named("main-key", |value| (1_i32, value))
        .unwrap();
    let side = pipeline
        .unbounded_source_with_factory(
            "side",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "side-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                let finish_side = Arc::clone(&finish_side);
                move |cursor| {
                    assert!(cursor.is_none());
                    opens.fetch_add(1, Ordering::SeqCst);
                    let watermark = UnboundedPoll::Ready(UnboundedBatch {
                        elements: Vec::new(),
                        progress: ProgressSignal {
                            source_key: "side".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: None,
                            event_time_ns: Some(10),
                            completed: false,
                        },
                    });
                    let mut polls = vec![watermark];
                    if finish_side.load(Ordering::SeqCst) {
                        polls.push(UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                source_key: "side".into(),
                                split_id: "split-0".into(),
                                checkpoint_position: None,
                                event_time_ns: Some(10),
                                completed: true,
                            },
                        }));
                    }
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap()
        .fixed_windows("side-window", 10)
        .unwrap()
        .map_named("side-key", |value| (1_i32, value))
        .unwrap();
    let sink = main
        .join_left_named("join", &side)
        .unwrap()
        .map_named("sum", |(key, (left, right))| {
            key * 100 + left + right.unwrap_or(0)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "cursorless-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.handle().processed() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(*committed.lock().unwrap(), vec![110]);
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    finish_side.store(true, Ordering::SeqCst);
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &config(temporary.path()))
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    assert_eq!(*committed.lock().unwrap(), vec![110]);
    assert_eq!(opens.load(Ordering::SeqCst), 2);
}

fn bounded_side_stream_run(
    polls: Vec<UnboundedPoll<Timestamped<i32>>>,
    side_values: Vec<(i64, i32)>,
    budget: usize,
    panic_in_map: bool,
    fail_commit: bool,
) -> (Result<(), Error>, Vec<i32>) {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let main = pipeline
        .unbounded_source_with_factory(
            "main",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "bounded-side-failure-main-v1".into(),
            0,
            move |_| {
                Ok(Box::new(Script {
                    polls: polls.clone().into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .fixed_windows("main-window", 10)
        .unwrap();
    let side = pipeline
        .from_vec("side", side_values)
        .unwrap()
        .timestamps_from_pairs_named("side-time")
        .unwrap()
        .fixed_windows("side-window", 10)
        .unwrap();
    let sink = main
        .map_with_windowed_side_named("side-add", &side, move |value, values| {
            assert!(!panic_in_map, "side-view callback failed");
            value + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "bounded-side-failure-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                let fail = Arc::new(AtomicBool::new(fail_commit));
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::clone(&fail),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let config = JobConfig {
        buffer_budget_elements: budget,
        ..JobConfig::default()
    };
    let result = DirectRunner
        .start_stream_sink(&sink, &config)
        .unwrap()
        .wait()
        .map(|_| ());
    let rows = committed.lock().unwrap().clone();
    (result, rows)
}

#[test]
fn bounded_side_stream_commits_two_windows_from_one_source_cursor() {
    let ready = |watermark, completed, values: Vec<(i32, i64)>| {
        UnboundedPoll::Ready(UnboundedBatch {
            elements: values
                .into_iter()
                .map(|(value, timestamp_ns)| Timestamped {
                    value,
                    timestamp_ns,
                })
                .collect(),
            progress: ProgressSignal {
                source_key: "main".into(),
                split_id: "split-0".into(),
                checkpoint_position: Some(vec![1]),
                event_time_ns: Some(watermark),
                completed,
            },
        })
    };
    for final_watermark in [10, 20] {
        let (result, committed) = bounded_side_stream_run(
            vec![
                ready(0, false, vec![(1, 1), (2, 11)]),
                ready(10, false, vec![]),
                ready(final_watermark, true, vec![]),
            ],
            vec![(0, 10), (10, 20)],
            4,
            false,
            false,
        );
        result.unwrap();
        assert_eq!(committed, [11, 22]);
    }
}

#[test]
fn bounded_side_stream_drops_late_main_and_rejects_excess_open_state() {
    let ready = |cursor, watermark, completed, values: Vec<(i32, i64)>| {
        UnboundedPoll::Ready(UnboundedBatch {
            elements: values
                .into_iter()
                .map(|(value, timestamp_ns)| Timestamped {
                    value,
                    timestamp_ns,
                })
                .collect(),
            progress: ProgressSignal {
                source_key: "main".into(),
                split_id: "split-0".into(),
                checkpoint_position: Some(vec![cursor]),
                event_time_ns: Some(watermark),
                completed,
            },
        })
    };
    let (late, committed) = bounded_side_stream_run(
        vec![
            ready(1, 10, false, vec![]),
            ready(2, 10, true, vec![(7, 1)]),
        ],
        vec![],
        4,
        false,
        false,
    );
    late.unwrap();
    assert_eq!(committed.len(), 0);
    let (excess, committed) = bounded_side_stream_run(
        vec![
            ready(1, 0, false, vec![(1, 1)]),
            ready(2, 0, true, vec![(2, 2)]),
        ],
        vec![],
        1,
        false,
        false,
    );
    assert_eq!(excess.unwrap_err().kind, ErrorKind::Resource);
    assert_eq!(committed.len(), 0);
}

#[test]
fn bounded_side_stream_aborts_on_callback_panic_or_commit_failure() {
    let poll = UnboundedPoll::Ready(UnboundedBatch {
        elements: vec![Timestamped {
            value: 1,
            timestamp_ns: 1,
        }],
        progress: ProgressSignal {
            source_key: "main".into(),
            split_id: "split-0".into(),
            checkpoint_position: Some(vec![1]),
            event_time_ns: Some(10),
            completed: true,
        },
    });
    for (panic_in_map, fail_commit) in [(true, false), (false, true)] {
        let (error, committed) = bounded_side_stream_run(
            vec![UnboundedPoll::Pending, poll.clone()],
            vec![(0, 10)],
            4,
            panic_in_map,
            fail_commit,
        );
        assert_eq!(error.unwrap_err().kind, ErrorKind::Execution);
        assert_eq!(committed.len(), 0);
    }
}

#[test]
fn grouped_stream_recovers_open_window_with_source_cursor() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pause_after_first = Arc::new(AtomicBool::new(true));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "grouped-recovery-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                let pause_after_first = Arc::clone(&pause_after_first);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let ready = |cursor, watermark, completed, values: Vec<(i32, i64)>| {
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: values
                                .into_iter()
                                .map(|(value, timestamp_ns)| Timestamped {
                                    value,
                                    timestamp_ns,
                                })
                                .collect(),
                            progress: ProgressSignal {
                                source_key: "events".into(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![cursor]),
                                event_time_ns: Some(watermark),
                                completed,
                            },
                        })
                    };
                    let first = ready(1, 20, false, vec![(1, 10), (2, 20)]);
                    let second = ready(2, 100, false, vec![(3, 30)]);
                    let final_poll = ready(3, 200, true, vec![(4, 110)]);
                    let polls = match position.as_deref() {
                        None if pause_after_first.load(Ordering::SeqCst) => vec![first],
                        None => vec![first, second, final_poll],
                        Some([1]) => vec![second, final_poll],
                        Some([2]) => vec![final_poll],
                        _ => return Err(Error::incompatible("unexpected grouped resume cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap();
    let sink = source
        .fixed_windows("windows", 100)
        .unwrap()
        .map_named("key", |value| (value % 2, value))
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .map_named("sum", |(key, values)| {
            key * 100 + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "grouped-recovery-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let job_config = config(temporary.path());
    let job = DirectRunner.start_stream_sink(&sink, &job_config).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job.progress().and_then(|signal| signal.checkpoint_position) != Some(vec![1]) {
        assert!(
            Instant::now() < deadline,
            "first grouped checkpoint was not saved"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    pause_after_first.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    let mut rows = committed.lock().unwrap().clone();
    rows.sort_unstable();
    assert_eq!(rows, [2, 4, 104]);
}

fn grouped_stream_run(
    polls: Vec<UnboundedPoll<Timestamped<i32>>>,
    budget: usize,
) -> (Result<(), Error>, Vec<i32>) {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "grouped-failure-source-v1".into(),
            0,
            move |_| {
                Ok(Box::new(Script {
                    polls: polls.clone().into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .fixed_windows("windows", 100)
        .unwrap()
        .map_named("key", |value| (value % 2, value))
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .map_named("sum", |(key, values)| {
            key * 100 + values.iter().sum::<i32>()
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "grouped-failure-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let result = DirectRunner
        .start_stream_sink(
            &sink,
            &JobConfig {
                buffer_budget_elements: budget,
                ..JobConfig::default()
            },
        )
        .unwrap()
        .wait()
        .map(|_| ());
    let values = committed.lock().unwrap().clone();
    (result, values)
}

#[test]
fn grouped_stream_drops_late_input_and_rejects_excess_state() {
    let ready = |cursor, watermark, completed, value: i32, timestamp_ns| {
        UnboundedPoll::Ready(UnboundedBatch {
            elements: vec![Timestamped {
                value,
                timestamp_ns,
            }],
            progress: ProgressSignal {
                source_key: "events".into(),
                split_id: "split-0".into(),
                checkpoint_position: Some(vec![cursor]),
                event_time_ns: Some(watermark),
                completed,
            },
        })
    };
    let (result, committed) = grouped_stream_run(
        vec![ready(1, 100, false, 1, 10), ready(2, 100, true, 3, 20)],
        8,
    );
    result.unwrap();
    assert_eq!(committed, [101]);
    let (result, committed) = grouped_stream_run(
        vec![
            ready(1, 0, false, 1, 10),
            ready(2, 0, false, 2, 20),
            ready(3, 0, true, 3, 30),
        ],
        2,
    );
    let error = result.unwrap_err();
    assert_eq!(error.kind, ErrorKind::Resource);
    assert_eq!(error.node.as_deref(), Some("groups"));
    assert_eq!(committed.len(), 0);
}

fn fixture(
    committed: Arc<Mutex<Vec<i32>>>,
    opens: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    fail_commit: Arc<AtomicBool>,
    hold_after_first: Arc<AtomicBool>,
) -> ironbeam_core::Sink<i32> {
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source-config-v1".into(),
            0,
            move |position| {
                opens.lock().unwrap().push(position.clone());
                let polls = match position.as_deref() {
                    None => vec![UnboundedPoll::Pending, batch(1, 1, false)].into(),
                    Some([1]) => {
                        if hold_after_first.load(Ordering::SeqCst) {
                            vec![].into()
                        } else {
                            vec![batch(2, 2, true)].into()
                        }
                    }
                    _ => panic!("unexpected cursor"),
                };
                Ok(Box::new(Script { polls }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let mapped = source.map_named("plus-ten", |value| value + 10).unwrap();
    mapped
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sink-config-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&committed),
                    staged: Vec::new(),
                    fail_commit: Arc::clone(&fail_commit),
                }))
            },
        )
        .unwrap()
}

fn config(directory: &std::path::Path) -> JobConfig {
    JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "stream-test-build".into(),
        }),
        ..JobConfig::default()
    }
}

#[test]
fn stream_pair_shares_source_and_recovers_both_branches() {
    let temporary = tempfile::tempdir().unwrap();
    let left_values = Arc::new(Mutex::new(Vec::new()));
    let right_values = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let visits = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(AtomicBool::new(true));
    let (left, right) = pair_fixture(
        Arc::clone(&left_values),
        Arc::clone(&right_values),
        Arc::clone(&opens),
        Arc::clone(&visits),
        Arc::clone(&hold),
        Arc::new(AtomicBool::new(false)),
        None,
    );
    let job = DirectRunner
        .start_stream_sink_pair(&left, &right, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while job
        .progress()
        .as_ref()
        .and_then(|progress| progress.checkpoint_position.as_deref())
        != Some(&[1][..])
    {
        assert!(Instant::now() < deadline, "first pair batch did not commit");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(*left_values.lock().unwrap(), [22]);
    assert_eq!(*right_values.lock().unwrap(), [111]);
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);

    let resumed = DirectRunner
        .start_stream_sink_pair(&left, &right, &config(temporary.path()))
        .unwrap();
    assert_eq!(resumed.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(*left_values.lock().unwrap(), [22, 24]);
    assert_eq!(*right_values.lock().unwrap(), [111, 112]);
    assert_eq!(visits.load(Ordering::SeqCst), 2);
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    let completed = DirectRunner
        .start_stream_sink_pair(&left, &right, &config(temporary.path()))
        .unwrap();
    assert!(completed.progress().unwrap().completed);
    assert_eq!(completed.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(opens.lock().unwrap().len(), 2);
}

#[test]
fn stream_pair_replays_from_verified_history_without_duplicate_sink_commits() {
    let temporary = tempfile::tempdir().unwrap();
    let left_values = Arc::new(Mutex::new(Vec::new()));
    let right_values = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let visits = Arc::new(AtomicUsize::new(0));
    let (left, right) = pair_fixture(
        Arc::clone(&left_values),
        Arc::clone(&right_values),
        Arc::clone(&opens),
        Arc::clone(&visits),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        None,
    );
    let mut config = config(temporary.path());
    config.checkpoint.as_mut().unwrap().max_checkpoints = 2;
    assert_eq!(
        DirectRunner
            .start_stream_sink_pair(&left, &right, &config)
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    assert_eq!(*left_values.lock().unwrap(), [22, 24]);
    assert_eq!(*right_values.lock().unwrap(), [111, 112]);
    assert!(temporary.path().join("stream-history-1.json").exists());

    std::fs::write(temporary.path().join("stream-latest.json"), b"{broken").unwrap();
    assert_eq!(
        DirectRunner
            .start_stream_sink_pair(&left, &right, &config)
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    assert_eq!(*left_values.lock().unwrap(), [22, 24]);
    assert_eq!(*right_values.lock().unwrap(), [111, 112]);
    assert_eq!(visits.load(Ordering::SeqCst), 3);
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
}

#[test]
fn stream_pair_rejects_duplicate_foreign_and_spilled_sinks_before_source_open() {
    let opens = Arc::new(Mutex::new(Vec::new()));
    let (left, right) = pair_fixture(
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::clone(&opens),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        None,
    );
    let foreign = fixture(
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    for other in [&left, &foreign] {
        assert_eq!(
            DirectRunner
                .start_stream_sink_pair(&left, other, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Graph
        );
    }
    assert!(matches!(
        DirectRunner
            .start_stream_sink_pair(
                &left,
                &right,
                &JobConfig {
                    spill: Some(SpillConfig::default()),
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert!(opens.lock().unwrap().is_empty());
}

#[test]
fn stream_pair_rejects_non_idempotent_or_uncommitted_sink_before_source_open() {
    let pipeline = Pipeline::default();
    let opens = Arc::new(AtomicUsize::new(0));
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source-config".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |_| {
                    opens.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(Script {
                        polls: VecDeque::new(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap();
    let make_sink = |key, replay, commit| {
        source
            .sink_named_with_config(
                key,
                SinkSpec {
                    mode: SinkMode::Append,
                    replay,
                    commit,
                },
                format!("{key}-config"),
                || Ok(Box::new(FaultWriter(Fault::SinkWrite))),
            )
            .unwrap()
    };
    let good = make_sink("good", Replay::Idempotent, true);
    let not_idempotent = make_sink("not-idempotent", Replay::AtLeastOnce, true);
    let not_committed = make_sink("not-committed", Replay::Idempotent, false);
    for other in [&not_idempotent, &not_committed] {
        assert!(matches!(
            DirectRunner
                .start_stream_sink_pair(&good, other, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::UnsupportedCapability { .. }
        ));
    }
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[test]
fn stream_pair_rejects_repeated_data_cursor_before_second_commit() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "repeated-cursor-source".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(3, 1, false), batch(4, 1, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let left_values = Arc::new(Mutex::new(Vec::new()));
    let right_values = Arc::new(Mutex::new(Vec::new()));
    let left = source
        .sink_named_with_config(
            "left",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "left-v1".into(),
            {
                let values = Arc::clone(&left_values);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&values),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let right = source
        .sink_named_with_config(
            "right",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "right-v1".into(),
            {
                let values = Arc::clone(&right_values);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&values),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let error = DirectRunner
        .start_stream_sink_pair(&left, &right, &JobConfig::default())
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.message, "data batch did not advance its cursor");
    assert_eq!(*left_values.lock().unwrap(), [3]);
    assert_eq!(*right_values.lock().unwrap(), [3]);
}

#[test]
fn stream_pair_enforces_combined_branch_buffer_before_writing() {
    let left_values = Arc::new(Mutex::new(Vec::new()));
    let right_values = Arc::new(Mutex::new(Vec::new()));
    let (left, right) = pair_fixture(
        Arc::clone(&left_values),
        Arc::clone(&right_values),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        None,
    );
    let error = DirectRunner
        .start_stream_sink_pair(
            &left,
            &right,
            &JobConfig {
                buffer_budget_elements: 1,
                ..JobConfig::default()
            },
        )
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Resource);
    assert_eq!(error.message, "stream branch output exceeds buffer budget");
    assert!(left_values.lock().unwrap().is_empty());
    assert!(right_values.lock().unwrap().is_empty());
}

#[test]
fn stream_pair_replays_first_commit_idempotently_after_second_fails() {
    let temporary = tempfile::tempdir().unwrap();
    let left_values = Arc::new(Mutex::new(Vec::new()));
    let right_values = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let visits = Arc::new(AtomicUsize::new(0));
    let fails = Arc::new(AtomicBool::new(true));
    let (left, right) = pair_fixture(
        Arc::clone(&left_values),
        Arc::clone(&right_values),
        Arc::clone(&opens),
        Arc::clone(&visits),
        Arc::new(AtomicBool::new(false)),
        Arc::clone(&fails),
        None,
    );
    let error = DirectRunner
        .start_stream_sink_pair(&left, &right, &config(temporary.path()))
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(*left_values.lock().unwrap(), [22]);
    assert!(right_values.lock().unwrap().is_empty());
    assert!(!temporary.path().join("stream-latest.json").exists());

    fails.store(false, Ordering::SeqCst);
    let recovered = DirectRunner
        .start_stream_sink_pair(&left, &right, &config(temporary.path()))
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(recovered.status(), JobStatus::Succeeded);
    assert_eq!(*left_values.lock().unwrap(), [22, 24]);
    assert_eq!(*right_values.lock().unwrap(), [111, 112]);
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, None]);
    assert_eq!(visits.load(Ordering::SeqCst), 3);
}

#[test]
fn stream_pair_cancellation_between_sinks_aborts_without_checkpoint() {
    let temporary = tempfile::tempdir().unwrap();
    let left_values = Arc::new(Mutex::new(Vec::new()));
    let right_values = Arc::new(Mutex::new(Vec::new()));
    let cancellation = Arc::new(AtomicBool::new(false));
    let (left, right) = pair_fixture(
        Arc::clone(&left_values),
        Arc::clone(&right_values),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        Some(Arc::clone(&cancellation)),
    );
    let config = JobConfig {
        cancellation,
        ..config(temporary.path())
    };
    let error = DirectRunner
        .start_stream_sink_pair(&left, &right, &config)
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(left_values.lock().unwrap().is_empty());
    assert!(right_values.lock().unwrap().is_empty());
    assert!(!temporary.path().join("stream-latest.json").exists());
}

#[test]
fn stream_resumes_from_committed_cursor_and_skips_completed_job() {
    let temporary = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let hold = Arc::new(AtomicBool::new(true));
    let sink = fixture(
        Arc::clone(&committed),
        Arc::clone(&opens),
        Arc::new(AtomicBool::new(false)),
        Arc::clone(&hold),
    );
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while job
        .progress()
        .as_ref()
        .and_then(|progress| progress.checkpoint_position.as_deref())
        != Some(&[1][..])
    {
        assert!(Instant::now() < deadline, "first batch did not commit");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(job.handle().status(), JobStatus::Running);
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    assert_eq!(*committed.lock().unwrap(), vec![11]);
    hold.store(false, Ordering::SeqCst);
    let resumed = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    assert_eq!(resumed.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(*committed.lock().unwrap(), vec![11, 12]);
    let completed = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    assert!(completed.progress().unwrap().completed);
    assert_eq!(completed.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
}

#[test]
fn failed_commit_does_not_advance_stream_checkpoint() {
    let temporary = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let fail = Arc::new(AtomicBool::new(true));
    let sink = fixture(
        Arc::clone(&committed),
        Arc::clone(&opens),
        Arc::clone(&fail),
        Arc::new(AtomicBool::new(false)),
    );
    let error = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(!temporary.path().join("stream-latest.json").exists());
    fail.store(false, Ordering::SeqCst);
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while job.progress().is_none() {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    job.cancel();
    let _ = job.wait();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, None]);
    assert_eq!(*committed.lock().unwrap(), vec![11]);
}

#[test]
fn stream_cancellation_inside_a_bundle_aborts_without_checkpointing() {
    for (cancel_in_transform, batch_len) in [(true, 2), (false, 2), (false, 1)] {
        let temporary = tempfile::tempdir().unwrap();
        let cancellation = Arc::new(AtomicBool::new(false));
        let committed = Arc::new(Mutex::new(Vec::new()));
        let pipeline = Pipeline::default();
        let source = pipeline
            .unbounded_source_with_factory(
                "events",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "cancel-bundle-source-v1".into(),
                0,
                move |_| {
                    let mut elements = vec![Timestamped {
                        value: 1,
                        timestamp_ns: 100,
                    }];
                    if batch_len == 2 {
                        elements.push(Timestamped {
                            value: 2,
                            timestamp_ns: 200,
                        });
                    }
                    Ok(Box::new(Script {
                        polls: vec![UnboundedPoll::Ready(UnboundedBatch {
                            elements,
                            progress: signal(1, true),
                        })]
                        .into(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap();
        let token = Arc::clone(&cancellation);
        let transformed = if cancel_in_transform {
            source
                .map_named("maybe-cancel", move |value| {
                    token.store(true, Ordering::SeqCst);
                    value
                })
                .unwrap()
                .map_named("after-cancel", |value| value + 10)
                .unwrap()
        } else {
            source
        };
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let values = Arc::clone(&committed);
        let sink_token = (!cancel_in_transform).then(|| Arc::clone(&cancellation));
        let sink = transformed
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "cancel-bundle-sink-v1".into(),
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&values),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: sink_token.clone(),
                    }))
                },
            )
            .unwrap();
        let job_config = JobConfig {
            cancellation,
            ..config(temporary.path())
        };
        let error = DirectRunner
            .start_stream_sink(&sink, &job_config)
            .unwrap()
            .wait()
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Cancelled);
        assert!(committed.lock().unwrap().is_empty());
        assert!(!temporary.path().join("stream-latest.json").exists());
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Covers both panic and ordinary begin failures with abort counts.
fn failed_stream_bundle_begin_aborts_its_open_sink() {
    struct FailingBeginSink {
        aborts: Arc<AtomicUsize>,
        panic_on_begin: bool,
    }
    impl TypedSink<i32> for FailingBeginSink {
        fn begin_bundle(&mut self, _: &str) -> Result<(), Error> {
            assert!(!self.panic_on_begin, "injected begin panic");
            Err(Error::execution("sink", "test", "injected begin failure"))
        }
        fn write(&mut self, _: i32) -> Result<(), Error> {
            panic!("begin failure reached write")
        }
        fn commit(&mut self) -> Result<(), Error> {
            panic!("begin failure reached commit")
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct PairedLeftSink {
        committed: Arc<Mutex<Vec<i32>>>,
        staged: Vec<i32>,
        aborts: Arc<AtomicUsize>,
    }
    impl TypedSink<i32> for PairedLeftSink {
        fn write(&mut self, value: i32) -> Result<(), Error> {
            self.staged.push(value);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            self.committed.lock().unwrap().extend(self.staged.drain(..));
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    for panic_on_begin in [false, true] {
        let temporary = tempfile::tempdir().unwrap();
        let aborts = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::default();
        let source = pipeline
            .unbounded_source_with_factory(
                "events",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "begin-failure-source-v1".into(),
                0,
                |_| {
                    Ok(Box::new(Script {
                        polls: vec![batch(1, 1, true)].into(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap();
        let sink_aborts = Arc::clone(&aborts);
        let sink = source
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "begin-failure-sink-v1".into(),
                move || {
                    Ok(Box::new(FailingBeginSink {
                        aborts: Arc::clone(&sink_aborts),
                        panic_on_begin,
                    }))
                },
            )
            .unwrap();
        let error = DirectRunner
            .start_stream_sink(&sink, &config(temporary.path()))
            .unwrap()
            .wait()
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
        assert_eq!(error.node.as_deref(), Some("sink"));
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        assert!(!temporary.path().join("stream-latest.json").exists());
    }

    let temporary = tempfile::tempdir().unwrap();
    let right_aborts = Arc::new(AtomicUsize::new(0));
    let left_aborts = Arc::new(AtomicUsize::new(0));
    let left_values = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "pair-begin-failure-source-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(1, 1, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let values = Arc::clone(&left_values);
    let aborts = Arc::clone(&left_aborts);
    let left = source
        .sink_named_with_config(
            "left",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "pair-begin-left-v1".into(),
            move || {
                Ok(Box::new(PairedLeftSink {
                    committed: Arc::clone(&values),
                    staged: Vec::new(),
                    aborts: Arc::clone(&aborts),
                }))
            },
        )
        .unwrap();
    let aborts = Arc::clone(&right_aborts);
    let right = source
        .sink_named_with_config(
            "right",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "pair-begin-right-v1".into(),
            move || {
                Ok(Box::new(FailingBeginSink {
                    aborts: Arc::clone(&aborts),
                    panic_on_begin: false,
                }))
            },
        )
        .unwrap();
    let error = DirectRunner
        .start_stream_sink_pair(&left, &right, &config(temporary.path()))
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(left_aborts.load(Ordering::SeqCst), 1);
    assert_eq!(right_aborts.load(Ordering::SeqCst), 1);
    assert!(left_values.lock().unwrap().is_empty());
    assert!(!temporary.path().join("stream-latest.json").exists());
}

#[test]
fn one_job_commits_multiple_batches_and_tracks_progress() {
    let temporary = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source-config-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![
                        UnboundedPoll::Pending,
                        batch(3, 1, false),
                        batch(4, 2, true),
                    ]
                    .into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let sink = source
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sink-config-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    let handle = job.wait().unwrap();
    assert_eq!(handle.status(), JobStatus::Succeeded);
    assert_eq!(handle.processed(), 2);
    assert_eq!(*committed.lock().unwrap(), vec![3, 4]);
    assert!(temporary.path().join("stream-latest.json").exists());
}

#[test]
fn singleton_dynamic_partition_runs_on_unbounded_direct_input() {
    struct Expand;
    impl DynOp for Expand {
        fn apply(&self, input: Partition) -> Partition {
            let [value]: [i32; 1] = (*input.downcast::<Vec<i32>>().unwrap()).try_into().unwrap();
            Box::new(vec![value, -value])
        }
    }
    let temporary = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "dynamic-source-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(3, 1, false), batch(4, 2, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .apply_dyn_transform_with_semantic_named::<i32>(
            "expand",
            1,
            "tests/expand-singleton",
            Version { major: 1, minor: 0 },
            Arc::new(Expand),
        )
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dynamic-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    assert!(
        pipeline
            .snapshot()
            .unwrap()
            .0
            .node("expand/batches")
            .is_none()
    );
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(*committed.lock().unwrap(), [3, -3, 4, -4]);
}

#[test]
fn singleton_typed_partition_runs_on_unbounded_direct_input() {
    let temporary = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "typed-source-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(3, 1, false), batch(4, 2, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .apply_transform_with_semantic_named(
            "typed-expand",
            1,
            "tests/typed-expand-singleton",
            Version { major: 1, minor: 0 },
            |values| {
                values
                    .into_iter()
                    .flat_map(|value| [value, -value])
                    .collect()
            },
        )
        .unwrap()
        .sink_named_with_config(
            "typed-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "typed-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    assert!(
        pipeline
            .snapshot()
            .unwrap()
            .0
            .node("typed-expand/batches")
            .is_none()
    );
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(*committed.lock().unwrap(), [3, -3, 4, -4]);
}

#[test]
fn singleton_batch_helper_runs_on_unbounded_direct_input() {
    let temporary = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "batch-source-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(3, 1, false), batch(4, 2, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap()
        .map_batches_named("batch-expand", 1, |values| vec![values[0], -values[0]])
        .unwrap()
        .sink_named_with_config(
            "batch-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "batch-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    assert!(
        pipeline
            .snapshot()
            .unwrap()
            .0
            .node("batch-expand/batches/collect")
            .is_none()
    );
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(*committed.lock().unwrap(), [3, -3, 4, -4]);
}

#[test]
fn singleton_keyed_batch_map_preserves_unbounded_event_time() {
    struct PairScript {
        polls: VecDeque<UnboundedPoll<Timestamped<(i32, i32)>>>,
    }
    impl UnboundedSource<Timestamped<(i32, i32)>> for PairScript {
        fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
            Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
        }
    }
    let pair_batch = |value, cursor, completed| {
        UnboundedPoll::Ready(UnboundedBatch {
            elements: vec![Timestamped {
                value,
                timestamp_ns: i64::from(cursor) * 100,
            }],
            progress: signal(cursor, completed),
        })
    };
    let temporary = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "keyed-batch-source-v1".into(),
            0,
            move |_| {
                Ok(Box::new(PairScript {
                    polls: vec![pair_batch((1, 3), 1, false), pair_batch((2, 4), 2, true)].into(),
                }) as TypedUnboundedSourceReader<(i32, i32)>)
            },
        )
        .unwrap()
        .map_values_batches_named("keyed-single", 1, |values| vec![values[0] * 2])
        .unwrap()
        .reify_timestamps_named("keyed-time")
        .unwrap()
        .map_named("keyed-capture", |(time, (key, value))| {
            i32::try_from(time).unwrap() + key * 100 + value
        })
        .unwrap()
        .sink_named_with_config(
            "keyed-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "keyed-batch-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(*committed.lock().unwrap(), [206, 408]);
}

#[derive(Clone, Copy)]
enum Fault {
    SourceFactory,
    SourcePoll,
    Transform,
    SinkFactory,
    SinkWrite,
    SinkCommit,
    SinkFinalization,
    ChangedSplit,
    RepeatedCursor,
}

struct FaultReader(Fault);
impl UnboundedSource<Timestamped<i32>> for FaultReader {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        if matches!(self.0, Fault::SourcePoll) {
            panic!("poll panic");
        }
        Ok(batch(7, 1, true))
    }
}

struct FaultWriter(Fault);
impl TypedSink<i32> for FaultWriter {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        if matches!(self.0, Fault::SinkWrite) {
            panic!("write panic");
        }
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        if matches!(self.0, Fault::SinkCommit) {
            panic!("commit panic");
        }
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn requires_bundle_finalization(&self) -> bool {
        matches!(self.0, Fault::SinkFinalization)
    }
}

fn fault_job(fault: Fault) -> ErrorKind {
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source".into(),
            0,
            move |_| {
                if matches!(fault, Fault::SourceFactory) {
                    panic!("factory panic");
                }
                Ok(Box::new(FaultReader(fault)) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let mapped = source
        .map_named("map", move |value| {
            if matches!(fault, Fault::Transform) {
                panic!("transform panic");
            }
            value
        })
        .unwrap();
    let sink = mapped
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sink".into(),
            move || {
                if matches!(fault, Fault::SinkFactory) {
                    panic!("sink factory panic");
                }
                Ok(Box::new(FaultWriter(fault)))
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .err()
        .unwrap()
        .kind
}

#[test]
fn stream_attributes_user_panics_and_rejects_finalization() {
    for fault in [
        Fault::SourceFactory,
        Fault::SourcePoll,
        Fault::Transform,
        Fault::SinkFactory,
        Fault::SinkWrite,
        Fault::SinkCommit,
    ] {
        assert_eq!(fault_job(fault), ErrorKind::Execution);
    }
    assert!(matches!(
        fault_job(Fault::SinkFinalization),
        ErrorKind::UnsupportedCapability { .. }
    ));
}

struct MultiFaultReader {
    fault: Fault,
    source_key: &'static str,
    completed: bool,
}

impl UnboundedSource<Timestamped<i32>> for MultiFaultReader {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        if matches!(self.fault, Fault::SourcePoll) {
            panic!("multi-source poll panic");
        }
        let second_poll = self.completed;
        if second_poll && !matches!(self.fault, Fault::ChangedSplit | Fault::RepeatedCursor) {
            return Ok(UnboundedPoll::Pending);
        }
        self.completed = true;
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: vec![Timestamped {
                value: 7,
                timestamp_ns: 100,
            }],
            progress: ProgressSignal {
                source_key: self.source_key.into(),
                split_id: if second_poll && matches!(self.fault, Fault::ChangedSplit) {
                    "split-1".into()
                } else {
                    "split-0".into()
                },
                checkpoint_position: Some(vec![1]),
                event_time_ns: Some(100),
                completed: !matches!(self.fault, Fault::ChangedSplit | Fault::RepeatedCursor),
            },
        }))
    }
}

fn multi_source_fault_job(fault: Fault) -> ErrorKind {
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: true,
    };
    let first = pipeline
        .unbounded_source_with_factory(
            "first",
            progress.clone(),
            "first-config".into(),
            0,
            move |_| {
                if matches!(fault, Fault::SourceFactory) {
                    panic!("multi-source factory panic");
                }
                Ok(Box::new(MultiFaultReader {
                    fault,
                    source_key: "first",
                    completed: false,
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let second = pipeline
        .unbounded_source_with_factory("second", progress, "second-config".into(), 0, |_| {
            Ok(Box::new(MultiFaultReader {
                fault: Fault::Transform,
                source_key: "second",
                completed: false,
            }) as TypedUnboundedSourceReader<i32>)
        })
        .unwrap();
    let sink = first
        .flatten_named("merge", &[second])
        .unwrap()
        .map_named("map", move |value| {
            if matches!(fault, Fault::Transform) {
                panic!("multi-source transform panic");
            }
            value
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "multi-fault-sink".into(),
            move || {
                if matches!(fault, Fault::SinkFactory) {
                    panic!("multi-source sink factory panic");
                }
                Ok(Box::new(FaultWriter(fault)))
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .err()
        .unwrap()
        .kind
}

#[test]
fn multi_source_stream_attributes_factory_poll_transform_and_sink_panics() {
    for fault in [
        Fault::SourceFactory,
        Fault::SourcePoll,
        Fault::Transform,
        Fault::SinkWrite,
        Fault::SinkCommit,
        Fault::SinkFactory,
        Fault::ChangedSplit,
        Fault::RepeatedCursor,
    ] {
        assert_eq!(multi_source_fault_job(fault), ErrorKind::Execution);
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Multiple invalid configurations share one no-open fixture.
fn stream_validates_config_and_sink_before_opening_source() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let sink = fixture(
        committed,
        Arc::clone(&opens),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(true)),
    );
    let temporary = tempfile::tempdir().unwrap();
    let mut bad_interval = config(temporary.path());
    bad_interval.checkpoint.as_mut().unwrap().interval_batches = 2;
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &bad_interval)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    let mut no_build = config(temporary.path());
    no_build
        .checkpoint
        .as_mut()
        .unwrap()
        .binary_build_id
        .clear();
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &no_build)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    let cancelled = config(temporary.path());
    cancelled.cancellation.store(true, Ordering::Release);
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &cancelled)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    assert!(opens.lock().unwrap().is_empty());
    let with_spill = JobConfig {
        spill: Some(SpillConfig::default()),
        ..JobConfig::default()
    };
    assert!(matches!(
        DirectRunner
            .start_stream_sink(&sink, &with_spill)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));

    let bounded_pipeline = Pipeline::default();
    let bounded = bounded_pipeline.from_vec("events", vec![1_i32]).unwrap();
    let bounded_sink = bounded
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            || Ok(Box::new(FaultWriter(Fault::SinkWrite))),
        )
        .unwrap();
    assert!(matches!(
        DirectRunner
            .start_stream_sink(&bounded_sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));

    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(1, 1, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let non_idempotent = source
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            || Ok(Box::new(FaultWriter(Fault::SinkWrite))),
        )
        .unwrap();
    assert!(matches!(
        DirectRunner
            .start_stream_sink(&non_idempotent, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let no_digest = source
        .sink_named(
            "sink-no-digest",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            || Ok(Box::new(FaultWriter(Fault::SinkWrite))),
        )
        .unwrap();
    assert!(matches!(
        DirectRunner
            .start_stream_sink(&no_digest, &config(temporary.path()))
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
}

#[test]
fn progress_only_poll_and_filtered_batch_keep_watermark_and_cursor() {
    let temporary = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                checkpoint_position: None,
                                event_time_ns: Some(50),
                                ..signal(0, false)
                            },
                        }),
                        batch(1, 1, false),
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                checkpoint_position: None,
                                event_time_ns: None,
                                ..signal(1, true)
                            },
                        }),
                    ]
                    .into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let filtered = source.filter_named("drop", |_| false).unwrap();
    let sink = filtered
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sink".into(),
            {
                let committed = Arc::clone(&committed);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        staged: Vec::new(),
                        fail_commit: Arc::new(AtomicBool::new(false)),
                    }))
                }
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    assert!(committed.lock().unwrap().is_empty());
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temporary.path().join("stream-latest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest["progress"]["checkpoint_position"],
        serde_json::json!([1])
    );
    assert_eq!(
        manifest["progress"]["event_time_ns"],
        serde_json::json!(100)
    );
}

#[test]
fn paired_stream_skips_empty_progress_bundles_and_checkpoints_final_cursor() {
    let temporary = tempfile::tempdir().unwrap();
    let left_values = Arc::new(Mutex::new(Vec::new()));
    let right_values = Arc::new(Mutex::new(Vec::new()));
    let left_accepted = Arc::new(Mutex::new(HashSet::new()));
    let right_accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "pair-progress-source-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                checkpoint_position: None,
                                event_time_ns: Some(50),
                                ..signal(0, false)
                            },
                        }),
                        batch(7, 1, false),
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                checkpoint_position: None,
                                event_time_ns: None,
                                ..signal(1, true)
                            },
                        }),
                    ]
                    .into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let left = source
        .map_named("double", |value| value * 2)
        .unwrap()
        .sink_named_with_config(
            "left-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "pair-progress-left-v1".into(),
            {
                let committed = Arc::clone(&left_values);
                let accepted = Arc::clone(&left_accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let right = source
        .map_named("plus-hundred", |value| value + 100)
        .unwrap()
        .sink_named_with_config(
            "right-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "pair-progress-right-v1".into(),
            {
                let committed = Arc::clone(&right_values);
                let accepted = Arc::clone(&right_accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink_pair(&left, &right, &config(temporary.path()))
        .unwrap();
    assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(*left_values.lock().unwrap(), [14]);
    assert_eq!(*right_values.lock().unwrap(), [107]);
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(temporary.path().join("stream-latest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest["progress"]["checkpoint_position"],
        serde_json::json!([1])
    );
    assert_eq!(
        manifest["progress"]["event_time_ns"],
        serde_json::json!(100)
    );
    assert_eq!(manifest["progress"]["completed"], serde_json::json!(true));
}

#[test]
fn stream_transform_output_obeys_buffer_limit() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(1, 1, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let expanded = source
        .flat_map_named("expand", |value| vec![value, value + 1])
        .unwrap();
    let sink = expanded
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sink".into(),
            || Ok(Box::new(FaultWriter(Fault::SinkWrite))),
        )
        .unwrap();
    let config = JobConfig {
        buffer_budget_elements: 1,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &config)
            .unwrap()
            .wait()
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
}

#[test]
fn stream_flattens_three_unbounded_sources_and_recovers_each_cursor() {
    let temporary = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let hold_remaining = Arc::new(AtomicBool::new(true));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let make_source = |key: &str| {
        let opens = Arc::clone(&opens);
        let hold_remaining = Arc::clone(&hold_remaining);
        let source_key = key.to_owned();
        pipeline
            .unbounded_source_with_factory(
                key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("{key}-config"),
                0,
                move |cursor| {
                    opens
                        .lock()
                        .unwrap()
                        .push((source_key.clone(), cursor.clone()));
                    let ready = |value, position, completed| {
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value,
                                timestamp_ns: i64::from(position),
                            }],
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![position]),
                                event_time_ns: Some(i64::from(position)),
                                completed,
                            },
                        })
                    };
                    let polls = match (source_key.as_str(), cursor.as_deref()) {
                        ("events", None) => vec![ready(1, 1, true)].into(),
                        ("other", None) if hold_remaining.load(Ordering::SeqCst) => {
                            vec![ready(10, 1, false)].into()
                        }
                        ("other", Some([1])) => vec![ready(20, 2, true)].into(),
                        ("third", None) if hold_remaining.load(Ordering::SeqCst) => {
                            vec![ready(100, 1, false)].into()
                        }
                        ("third", Some([1])) => vec![ready(200, 2, true)].into(),
                        _ => return Err(Error::incompatible("unexpected multi-source cursor")),
                    };
                    Ok(Box::new(Script { polls }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap()
    };
    let first = make_source("events")
        .map_named("first-map", |value| value + 100)
        .unwrap();
    let second = make_source("other")
        .map_named("second-map", |value| value + 200)
        .unwrap();
    let third = make_source("third")
        .map_named("third-map", |value| value + 300)
        .unwrap();
    let joined = first.flatten_named("merge", &[second, third]).unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = joined
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "multi-sink-v1".into(),
            move || {
                Ok(Box::new(BundleCapture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    staged: Vec::new(),
                    bundle: None,
                    fail_commit: Arc::new(AtomicBool::new(false)),
                    cancel_on_write: None,
                }))
            },
        )
        .unwrap();
    let job_config = config(temporary.path());
    let first_job = DirectRunner.start_stream_sink(&sink, &job_config).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while first_job.handle().processed() < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(first_job.handle().processed(), 3);
    first_job.cancel();
    assert_eq!(first_job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold_remaining.store(false, Ordering::SeqCst);
    let resumed = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap();
    assert_eq!(resumed.wait().unwrap().status(), JobStatus::Succeeded);
    let mut values = committed.lock().unwrap().clone();
    values.sort_unstable();
    assert_eq!(values, vec![101, 210, 220, 400, 500]);
    let mut opens = opens.lock().unwrap().clone();
    opens.sort();
    let mut expected = vec![
        ("events".into(), None),
        ("other".into(), None),
        ("third".into(), None),
        ("other".into(), Some(vec![1])),
        ("third".into(), Some(vec![1])),
    ];
    expected.sort();
    assert_eq!(opens, expected);
}

#[test]
fn multi_source_stream_retries_only_the_failed_source_bundle() {
    let temporary = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let fail_second = Arc::new(AtomicBool::new(true));
    let make_source = |key: &str, value: i32| {
        let source_key = key.to_owned();
        let opens = Arc::clone(&opens);
        pipeline
            .unbounded_source_with_factory(
                key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: false,
                },
                format!("{key}-v1"),
                0,
                move |cursor| {
                    opens
                        .lock()
                        .unwrap()
                        .push((source_key.clone(), cursor.clone()));
                    if cursor.is_some() {
                        return Err(Error::incompatible(
                            "unexpected failed-source resume cursor",
                        ));
                    }
                    Ok(Box::new(Script {
                        polls: vec![UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value,
                                timestamp_ns: 0,
                            }],
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![1]),
                                event_time_ns: None,
                                completed: true,
                            },
                        })]
                        .into(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap()
    };
    let first = make_source("first", 1);
    let second = make_source("second", 2);
    let merged = first.flatten_named("merge", &[second]).unwrap();
    let sink = merged
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "multi-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                let fail_second = Arc::clone(&fail_second);
                move || {
                    let fail =
                        fail_second.load(Ordering::SeqCst) && !accepted.lock().unwrap().is_empty();
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(fail)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    let error = DirectRunner
        .start_stream_sink(&sink, &config(temporary.path()))
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(*committed.lock().unwrap(), vec![1]);
    fail_second.store(false, Ordering::SeqCst);
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &config(temporary.path()))
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    assert_eq!(*committed.lock().unwrap(), vec![1, 2]);
    assert_eq!(
        *opens.lock().unwrap(),
        vec![
            ("first".into(), None),
            ("second".into(), None),
            ("second".into(), None),
        ]
    );
}

#[test]
fn flattening_one_unbounded_source_twice_keeps_distinct_bundle_ids() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "events-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(7, 1, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let merged = source
        .flatten_named("merge", std::slice::from_ref(&source))
        .unwrap();
    let sink = merged
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(BundleCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                        fail_commit: Arc::new(AtomicBool::new(false)),
                        cancel_on_write: None,
                    }))
                }
            },
        )
        .unwrap();
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    assert_eq!(*committed.lock().unwrap(), vec![7, 7]);
    assert_eq!(accepted.lock().unwrap().len(), 2);
}

#[test]
fn dropping_stream_job_cancels_and_joins_worker() {
    let sink = fixture(
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(true)),
    );
    let job = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    let handle = job.handle().clone();
    drop(job);
    assert_eq!(handle.status(), JobStatus::Cancelled);
}

struct BundleIds(Arc<Mutex<Vec<String>>>);
impl TypedSink<i32> for BundleIds {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        self.0.lock().unwrap().push(id.into());
        Ok(())
    }
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
fn replayed_source_batch_has_a_stable_sink_bundle_id() {
    let ids = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(3, 1, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let sink = source
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            {
                let ids = Arc::clone(&ids);
                move || Ok(Box::new(BundleIds(Arc::clone(&ids))))
            },
        )
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            DirectRunner
                .start_stream_sink(&sink, &JobConfig::default())
                .unwrap()
                .wait()
                .unwrap()
                .status(),
            JobStatus::Succeeded
        );
    }
    let ids = ids.lock().unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1]);
    assert_eq!(ids[0].len(), 64);
}

#[test]
fn source_must_advance_cursor_for_each_data_batch() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "source".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![batch(3, 1, false), batch(4, 1, true)].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let sink = source
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            || Ok(Box::new(FaultWriter(Fault::SourcePoll))),
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
        ErrorKind::Execution
    );
}
