use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
    UnboundedSource,
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

#[derive(Clone, Copy)]
enum Fault {
    SourceFactory,
    SourcePoll,
    Transform,
    SinkFactory,
    SinkWrite,
    SinkCommit,
    SinkFinalization,
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

#[test]
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
    assert!(job.wait().unwrap().status() == JobStatus::Succeeded);
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
fn stream_rejects_multiple_unbounded_sources_before_opening_them() {
    let pipeline = Pipeline::default();
    let opens = Arc::new(AtomicUsize::new(0));
    let make_source = |key: &str| {
        let opens = Arc::clone(&opens);
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
                move |_| {
                    opens.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(Script {
                        polls: VecDeque::new(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap()
    };
    let first = make_source("events");
    let second = make_source("other");
    let joined = first.flatten_named("merge", &[second]).unwrap();
    let sink = joined
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
            .start_stream_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(opens.load(Ordering::SeqCst), 0);
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
