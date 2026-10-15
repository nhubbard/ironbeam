use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
    UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

struct Once(Option<UnboundedBatch<Timestamped<i32>>>);

impl UnboundedSource<Timestamped<i32>> for Once {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(self
            .0
            .take()
            .map_or(UnboundedPoll::Pending, UnboundedPoll::Ready))
    }
}

struct Capture(Arc<Mutex<Vec<i32>>>);

impl TypedSink<i32> for Capture {
    fn write(&mut self, value: i32) -> Result<(), Error> {
        self.0.lock().unwrap().push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

struct Queue(VecDeque<UnboundedBatch<Timestamped<i32>>>);

impl UnboundedSource<Timestamped<i32>> for Queue {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(self
            .0
            .pop_front()
            .map_or(UnboundedPoll::Pending, UnboundedPoll::Ready))
    }
}

struct RetryCapture {
    committed: Arc<Mutex<Vec<i32>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    attempted: Arc<Mutex<Vec<String>>>,
    fail_once: Arc<AtomicBool>,
    bundle: Option<String>,
    staged: Vec<i32>,
}

impl TypedSink<i32> for RetryCapture {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        self.bundle = Some(id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: i32) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let id = self.bundle.take().unwrap();
        self.attempted.lock().unwrap().push(id.clone());
        if self.staged.contains(&4) && self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(Error::execution("sink", "test", "injected commit failure"));
        }
        if self.accepted.lock().unwrap().insert(id) {
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
fn bounded_signals_finish_before_unbounded_main_opens() {
    let pipeline = Pipeline::default();
    let first_ready = Arc::new(AtomicBool::new(false));
    let second_ready = Arc::new(AtomicBool::new(false));
    let first_flag = Arc::clone(&first_ready);
    let first = pipeline
        .from_vec("first-source", vec![1_i32])
        .unwrap()
        .map_named("first-effect", move |value| {
            first_flag.store(true, Ordering::SeqCst);
            value
        })
        .unwrap();
    let second_flag = Arc::clone(&second_ready);
    let second = pipeline
        .from_vec("second-source", vec![2_i32])
        .unwrap()
        .map_named("second-effect", move |value| {
            second_flag.store(true, Ordering::SeqCst);
            value
        })
        .unwrap();
    let opened = Arc::new(AtomicBool::new(false));
    let opened_flag = Arc::clone(&opened);
    let main = pipeline
        .unbounded_source_with_factory(
            "main-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "bounded-wait-main-v1".into(),
            0,
            move |_| {
                assert!(first_ready.load(Ordering::SeqCst));
                assert!(second_ready.load(Ordering::SeqCst));
                opened_flag.store(true, Ordering::SeqCst);
                Ok(Box::new(Once(Some(UnboundedBatch {
                    elements: vec![Timestamped {
                        value: 7,
                        timestamp_ns: 1,
                    }],
                    progress: ProgressSignal {
                        source_key: "main-source".into(),
                        split_id: "split-0".into(),
                        checkpoint_position: Some(vec![1]),
                        event_time_ns: Some(1),
                        completed: true,
                    },
                }))) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let values = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&values);
    let sink = main
        .wait_on_all_named("wait", &[first, second])
        .unwrap()
        .map_named("after-wait", |value| value + 1)
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "bounded-wait-sink-v1".into(),
            move || Ok(Box::new(Capture(Arc::clone(&capture)))),
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    assert_eq!(job.wait().unwrap().status(), JobStatus::Succeeded);
    assert!(opened.load(Ordering::SeqCst));
    assert_eq!(*values.lock().unwrap(), vec![8]);
}

#[test]
fn failed_bounded_signal_never_opens_unbounded_main() {
    let pipeline = Pipeline::default();
    let signal = pipeline
        .from_vec("signal-source", vec![1_i32])
        .unwrap()
        .map_named("fail-signal", |_| -> i32 { panic!("signal failed") })
        .unwrap();
    let opened = Arc::new(AtomicBool::new(false));
    let opened_flag = Arc::clone(&opened);
    let main = pipeline
        .unbounded_source_with_factory(
            "main-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "bounded-wait-failure-main-v1".into(),
            0,
            move |_| {
                opened_flag.store(true, Ordering::SeqCst);
                Ok(Box::new(Once(None)) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let sink = main
        .wait_on_named("wait", &signal)
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "bounded-wait-failure-sink-v1".into(),
            || Ok(Box::new(Capture(Arc::new(Mutex::new(Vec::new()))))),
        )
        .unwrap();
    let job = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Execution);
    assert!(!opened.load(Ordering::SeqCst));
}

#[test]
fn checkpoint_rejects_changed_bounded_signal_before_opening_main() {
    let directory = tempfile::tempdir().unwrap();
    let opened = Arc::new(AtomicBool::new(false));
    let make_sink = |signal_value, opened: Arc<AtomicBool>| {
        let pipeline = Pipeline::default();
        let signal = pipeline
            .from_vec("signal-source", vec![signal_value])
            .unwrap();
        let main = pipeline
            .unbounded_source_with_factory(
                "main-source",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "checkpoint-wait-main-v1".into(),
                0,
                move |_| {
                    opened.store(true, Ordering::SeqCst);
                    Ok(Box::new(Once(Some(UnboundedBatch {
                        elements: Vec::new(),
                        progress: ProgressSignal {
                            source_key: "main-source".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(1),
                            completed: true,
                        },
                    }))) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap();
        main.wait_on_named("wait", &signal)
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "checkpoint-wait-sink-v1".into(),
                || Ok(Box::new(Capture(Arc::new(Mutex::new(Vec::new()))))),
            )
            .unwrap()
    };
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "checkpoint-wait-build-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first = make_sink(1_i32, Arc::clone(&opened));
    DirectRunner
        .start_stream_sink(&first, &config)
        .unwrap()
        .wait()
        .unwrap();
    assert!(opened.load(Ordering::SeqCst));
    opened.store(false, Ordering::SeqCst);
    let changed = make_sink(2_i32, Arc::clone(&opened));
    let error = DirectRunner
        .start_stream_sink(&changed, &config)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Incompatible);
    assert!(!opened.load(Ordering::SeqCst));
}

#[test]
fn bounded_signal_route_resumes_main_cursor_after_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let signal_runs = Arc::new(AtomicUsize::new(0));
    let signal_counter = Arc::clone(&signal_runs);
    let signal = pipeline
        .from_vec("signal-source", vec![1_i32])
        .unwrap()
        .map_named("signal-effect", move |value| {
            signal_counter.fetch_add(1, Ordering::SeqCst);
            value
        })
        .unwrap();
    let release = Arc::new(AtomicBool::new(false));
    let opened = Arc::new(Mutex::new(Vec::new()));
    let source_release = Arc::clone(&release);
    let source_opened = Arc::clone(&opened);
    let main = pipeline
        .unbounded_source_with_factory(
            "main-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "resume-wait-main-v1".into(),
            0,
            move |cursor| {
                source_opened.lock().unwrap().push(cursor.clone());
                let batch = match cursor.as_deref() {
                    None => Some((3, 1, false)),
                    Some([1]) if source_release.load(Ordering::SeqCst) => Some((4, 2, true)),
                    Some([1]) => None,
                    _ => return Err(Error::incompatible("unexpected wait cursor")),
                };
                Ok(Box::new(Once(batch.map(|(value, position, completed)| {
                    UnboundedBatch {
                        elements: vec![Timestamped {
                            value,
                            timestamp_ns: i64::from(position),
                        }],
                        progress: ProgressSignal {
                            source_key: "main-source".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: Some(vec![position]),
                            event_time_ns: Some(i64::from(position)),
                            completed,
                        },
                    }
                }))) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let sink_values = Arc::clone(&committed);
    let sink = main
        .wait_on_named("wait", &signal)
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "resume-wait-sink-v1".into(),
            move || Ok(Box::new(Capture(Arc::clone(&sink_values)))),
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "resume-wait-build-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while first
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(*committed.lock().unwrap(), vec![3]);
    first.cancel();
    assert_eq!(first.wait().err().unwrap().kind, ErrorKind::Cancelled);
    release.store(true, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*committed.lock().unwrap(), vec![3, 4]);
    assert_eq!(signal_runs.load(Ordering::SeqCst), 2);
    assert_eq!(*opened.lock().unwrap(), vec![None, Some(vec![1])]);
}

#[test]
fn failed_second_bundle_retries_from_checkpoint_after_bounded_wait() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let signal_runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&signal_runs);
    let signal = pipeline
        .from_vec("signal-source", vec![1_i32])
        .unwrap()
        .map_named("signal-effect", move |value| {
            counter.fetch_add(1, Ordering::SeqCst);
            value
        })
        .unwrap();
    let cursors = Arc::new(Mutex::new(Vec::new()));
    let opens = Arc::clone(&cursors);
    let main = pipeline
        .unbounded_source_with_factory(
            "main-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "retry-wait-main-v1".into(),
            0,
            move |cursor| {
                opens.lock().unwrap().push(cursor.clone());
                let batch = |value: i32, position: u8, completed| UnboundedBatch {
                    elements: vec![Timestamped {
                        value,
                        timestamp_ns: i64::from(position),
                    }],
                    progress: ProgressSignal {
                        source_key: "main-source".into(),
                        split_id: "split-0".into(),
                        checkpoint_position: Some(vec![position]),
                        event_time_ns: Some(i64::from(position)),
                        completed,
                    },
                };
                let batches = match cursor.as_deref() {
                    None => VecDeque::from([batch(3, 1, false), batch(4, 2, true)]),
                    Some([1]) => VecDeque::from([batch(4, 2, true)]),
                    _ => return Err(Error::incompatible("unexpected retry cursor")),
                };
                Ok(Box::new(Queue(batches)) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let attempted = Arc::new(Mutex::new(Vec::new()));
    let fail_once = Arc::new(AtomicBool::new(true));
    let sink = main
        .wait_on_named("wait", &signal)
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "retry-wait-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                let attempted = Arc::clone(&attempted);
                let fail_once = Arc::clone(&fail_once);
                move || {
                    Ok(Box::new(RetryCapture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        attempted: Arc::clone(&attempted),
                        fail_once: Arc::clone(&fail_once),
                        bundle: None,
                        staged: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "retry-wait-build-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    assert_eq!(first.wait().err().unwrap().kind, ErrorKind::Execution);
    assert_eq!(*committed.lock().unwrap(), vec![3]);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*committed.lock().unwrap(), vec![3, 4]);
    assert_eq!(*cursors.lock().unwrap(), vec![None, Some(vec![1])]);
    assert_eq!(signal_runs.load(Ordering::SeqCst), 2);
    let attempted = attempted.lock().unwrap();
    assert_eq!(attempted.len(), 3);
    assert_eq!(attempted[1], attempted[2]);
}
