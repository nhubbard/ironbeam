use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll, UnboundedSource, Version,
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
fn unbounded_windowed_value_state_fails_at_graph_construction() {
    let pipeline = Pipeline::default();
    let stream = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "windowed-value-state-preflight-v1".into(),
            0,
            |_| {
                Ok(Box::new(TwoBatchSource {
                    cursor: None,
                    emitted: false,
                }) as TypedUnboundedSourceReader<(i32, i32)>)
            },
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let error = stream
        .stateful_value_map_named::<i32, i32>(
            "count",
            "tests/windowed-value-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let count = prior.unwrap_or(0) + value;
                Ok((Some(count), count))
            },
        )
        .err()
        .unwrap();
    assert!(error.message.contains("global window"));
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
