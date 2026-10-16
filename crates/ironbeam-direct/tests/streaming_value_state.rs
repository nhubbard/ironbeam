use ironbeam_core::{
    BagStateMutation, ElementMetadata, Error, ErrorKind, Pipeline, ProgressSignal, Replay,
    SinkMode, SinkSpec, SourceProgress, TestStreamEvent, Timestamped, TypedSink,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, UnboundedSource, Version,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct TwoBatchSource {
    cursor: Option<Vec<u8>>,
    emitted: bool,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for TwoBatchSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.emitted {
            return Ok(UnboundedPoll::Pending);
        }
        self.emitted = true;
        let (values, position, completed) = match self.cursor.as_deref() {
            None => (vec![(1, 1), (2, 1)], 1, false),
            Some([1]) => (vec![(1, 1), (2, 1), (1, 1)], 2, true),
            _ => return Err(Error::incompatible("unexpected value-state source cursor")),
        };
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: values
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
                event_time_ns: Some(0),
                completed,
            },
        }))
    }
}

struct IdempotentCapture {
    accepted: Arc<Mutex<HashSet<String>>>,
    pending: Vec<String>,
}

impl TypedSink<String> for IdempotentCapture {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.accepted.lock().unwrap().extend(self.pending.drain(..));
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

#[test]
fn global_value_state_recovers_with_source_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "value-state-two-batches-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(TwoBatchSource {
                        cursor,
                        emitted: false,
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "count",
            "tests/global-value-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let next = prior.unwrap_or(0) + 1;
                Ok((Some(next), next))
            },
        )
        .unwrap()
        .map_named("label", |(key, count)| format!("{key}:{count}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "value-state-idempotent-capture-v1".into(),
            {
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(IdempotentCapture {
                        accepted: Arc::clone(&accepted),
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
            binary_build_id: "global-value-state-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert_ne!(job.handle().status(), JobStatus::Failed);
        assert!(
            Instant::now() < deadline,
            "first stateful batch was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_eq!(
        *accepted.lock().unwrap(),
        HashSet::from([
            "1:1".to_owned(),
            "1:2".to_owned(),
            "1:3".to_owned(),
            "2:1".to_owned(),
            "2:2".to_owned(),
        ])
    );
}

#[test]
fn shared_stream_value_state_recovers_both_sink_branches() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let first_rows = Arc::new(Mutex::new(HashSet::new()));
    let second_rows = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let shared = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "shared-value-state-two-batches-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(TwoBatchSource {
                        cursor,
                        emitted: false,
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "shared-count",
            "tests/shared-value-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let next = prior.unwrap_or(0) + 1;
                Ok((Some(next), next))
            },
        )
        .unwrap()
        .map_named("shared-label", |(key, count)| format!("{key}:{count}"))
        .unwrap();
    let first = shared
        .sink_named_with_config(
            "first",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-state-first-v1".into(),
            {
                let accepted = Arc::clone(&first_rows);
                move || {
                    Ok(Box::new(IdempotentCapture {
                        accepted: Arc::clone(&accepted),
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
            "shared-state-second-v1".into(),
            {
                let accepted = Arc::clone(&second_rows);
                move || {
                    Ok(Box::new(IdempotentCapture {
                        accepted: Arc::clone(&accepted),
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
            binary_build_id: "shared-value-state-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner
        .start_stream_sink_pair(&first, &second, &config())
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert_ne!(job.handle().status(), JobStatus::Failed);
        assert!(
            Instant::now() < deadline,
            "first pair batch was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink_pair(&first, &second, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    let expected = HashSet::from([
        "1:1".to_owned(),
        "1:2".to_owned(),
        "1:3".to_owned(),
        "2:1".to_owned(),
        "2:2".to_owned(),
    ]);
    assert_eq!(*first_rows.lock().unwrap(), expected);
    assert_eq!(
        *second_rows.lock().unwrap(),
        expected
            .into_iter()
            .map(|row| format!("copy:{row}"))
            .collect()
    );
}

#[test]
fn global_bag_state_recovers_with_source_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "bag-state-two-batches-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(TwoBatchSource {
                        cursor,
                        emitted: false,
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .stateful_bag_map_named::<i32, i32>(
            "count",
            "tests/global-bag-state",
            Version { major: 1, minor: 0 },
            |_, value, prior: Vec<i32>| {
                let count = i32::try_from(prior.len()).unwrap() + 1;
                Ok((BagStateMutation::Append(vec![value]), count))
            },
        )
        .unwrap()
        .map_named("label", |(key, count)| format!("{key}:{count}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "bag-state-idempotent-capture-v1".into(),
            {
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(IdempotentCapture {
                        accepted: Arc::clone(&accepted),
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
            binary_build_id: "global-bag-state-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert_ne!(job.handle().status(), JobStatus::Failed);
        assert!(
            Instant::now() < deadline,
            "first bag batch was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_eq!(
        *accepted.lock().unwrap(),
        HashSet::from([
            "1:1".to_owned(),
            "1:2".to_owned(),
            "1:3".to_owned(),
            "2:1".to_owned(),
            "2:2".to_owned(),
        ])
    );
}

struct WindowedSource {
    cursor: Option<Vec<u8>>,
    emitted: bool,
    sliding: bool,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for WindowedSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.emitted {
            return Ok(UnboundedPoll::Pending);
        }
        self.emitted = true;
        let (times, position, watermark, completed) = match self.cursor.as_deref() {
            None if self.sliding => (vec![0, 1_000_000, 20_000_000], 1, 0, false),
            None => (vec![0, 20_000_000], 1, 0, false),
            Some([1]) => (vec![1_000_000, 21_000_000, 40_000_000], 2, 40_000_000, true),
            _ => return Err(Error::incompatible("unexpected windowed state cursor")),
        };
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: times
                .into_iter()
                .map(|timestamp_ns| Timestamped {
                    value: (1, 1),
                    timestamp_ns,
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

struct WindowCapture(IdempotentCapture);

impl TypedSink<String> for WindowCapture {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.0.write(value)
    }
    fn write_with_metadata(
        &mut self,
        value: String,
        metadata: ElementMetadata,
    ) -> Result<(), Error> {
        let start = metadata
            .assigned_window
            .ok_or_else(|| Error::incompatible("window missing"))?
            .start_ns;
        self.0.write(format!("{start}:{value}"))
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.0.commit()
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.0.abort()
    }
}

fn run_windowed_state_recovery(sliding: bool) {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "windowed-value-state-two-batches-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(WindowedSource {
                        cursor,
                        emitted: false,
                        sliding,
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap();
    let windowed = if sliding {
        source
            .sliding_windows("window", 10_000_000, 5_000_000, 0)
            .unwrap()
    } else {
        source.fixed_windows("window", 10_000_000).unwrap()
    };
    let sink = windowed
        .stateful_value_map_named::<i32, i32>(
            "count",
            "tests/windowed-value-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), count))
            },
        )
        .unwrap()
        .map_named("label", |(key, count)| format!("{key}:{count}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "windowed-value-state-capture-v1".into(),
            {
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(WindowCapture(IdempotentCapture {
                        accepted: Arc::clone(&accepted),
                        pending: Vec::new(),
                    })))
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
            binary_build_id: "windowed-value-state-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert_ne!(job.handle().status(), JobStatus::Failed);
        assert!(
            Instant::now() < deadline,
            "first windowed state batch was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_window_counts(&accepted, sliding);
}

fn assert_window_counts(accepted: &Arc<Mutex<HashSet<String>>>, sliding: bool) {
    let expected = if sliding {
        "-5000000:1:1 -5000000:1:2 -5000000:1:3 0:1:1 0:1:2 0:1:3 15000000:1:1 15000000:1:2 20000000:1:1 20000000:1:2 35000000:1:1 40000000:1:1"
    } else {
        "0:1:1 0:1:2 20000000:1:1 20000000:1:2 40000000:1:1"
    }
        .split_whitespace()
        .map(str::to_owned)
        .collect::<HashSet<_>>();
    assert_eq!(*accepted.lock().unwrap(), expected);
}

#[test]
fn fixed_value_state_recovers_each_window_with_source_cursor() {
    run_windowed_state_recovery(false);
}

#[test]
fn sliding_value_state_recovers_each_window_with_source_cursor() {
    run_windowed_state_recovery(true);
}

#[test]
fn global_value_state_obeys_the_stream_memory_budget() {
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 1_i32), 0)])],
        )
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "count",
            "tests/budgeted-value-state",
            Version { major: 1, minor: 0 },
            |_, _, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), count))
            },
        )
        .unwrap()
        .map_named("label", |(key, count)| format!("{key}:{count}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "budgeted-value-state-sink-v1".into(),
            {
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(IdempotentCapture {
                        accepted: Arc::clone(&accepted),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let result = DirectRunner
        .start_stream_sink(
            &sink,
            &JobConfig {
                memory_budget_bytes: 1,
                ..JobConfig::default()
            },
        )
        .unwrap()
        .wait();
    assert_eq!(result.err().unwrap().kind, ErrorKind::Resource);
    assert!(accepted.lock().unwrap().is_empty());
}
