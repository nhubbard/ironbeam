use ironbeam_core::{
    ElementMetadata, Error, ErrorKind, MapStateMutation, Pipeline, ProgressSignal, Replay,
    SinkMode, SinkSpec, SourceProgress, Timestamped, TypedSink, TypedUnboundedSourceReader,
    UnboundedBatch, UnboundedPoll, UnboundedSource, Version,
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
            None => (vec![(1, 10), (2, 20)], 1, false),
            Some([1]) => (vec![(1, 10), (2, 21)], 2, true),
            _ => return Err(Error::incompatible("unexpected map-state source cursor")),
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
    fn write_with_metadata(
        &mut self,
        value: String,
        metadata: ElementMetadata,
    ) -> Result<(), Error> {
        if let Some(window) = metadata.assigned_window {
            self.write(format!("{}:{}:{value}", window.start_ns, window.end_ns))
        } else {
            self.write(value)
        }
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

struct SlidingMapSource {
    cursor: Option<Vec<u8>>,
    emitted: bool,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for SlidingMapSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.emitted {
            return Ok(UnboundedPoll::Pending);
        }
        self.emitted = true;
        let (values, cursor, watermark, completed) = match self.cursor.as_deref() {
            None => (vec![(11, 6_000_000), (12, 7_000_000)], 1, 8_000_000, false),
            Some([1]) => (
                vec![(11, 12_000_000), (11, 18_000_000)],
                2,
                26_000_000,
                true,
            ),
            _ => return Err(Error::incompatible("unexpected sliding map cursor")),
        };
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: values
                .into_iter()
                .map(|(value, timestamp_ns)| Timestamped {
                    value: (1, value),
                    timestamp_ns,
                })
                .collect(),
            progress: ProgressSignal {
                source_key: "source".into(),
                split_id: "default".into(),
                checkpoint_position: Some(vec![cursor]),
                event_time_ns: Some(watermark),
                completed,
            },
        }))
    }
}

#[test]
fn sliding_map_state_restores_each_window_and_source_cursor() {
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
            "sliding-map-two-batches-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(SlidingMapSource {
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
        .stateful_map_map_named::<i32, i32, (i32, i32, i32)>(
            "map",
            "tests/sliding-map-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                Ok((
                    MapStateMutation::Put(value, value * 10),
                    (
                        value,
                        prior.get(&value).copied().unwrap_or(-1),
                        i32::try_from(prior.len()).unwrap(),
                    ),
                ))
            },
        )
        .unwrap()
        .map_named("label", |(key, (value, old, size))| {
            format!("{key}:{value}:{old}:{size}")
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sliding-map-capture-v1".into(),
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
            binary_build_id: "sliding-map-state-test-build".into(),
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
            "first sliding map batch was not checkpointed"
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
        [
            "0:10000000:1:11:-1:0",
            "0:10000000:1:12:-1:1",
            "5000000:15000000:1:11:-1:0",
            "5000000:15000000:1:12:-1:1",
            "5000000:15000000:1:11:110:2",
            "10000000:20000000:1:11:-1:0",
            "10000000:20000000:1:11:110:1",
            "15000000:25000000:1:11:-1:0",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<HashSet<_>>()
    );
}

fn map_state_sink(
    opens: &Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    accepted: &Arc<Mutex<HashSet<String>>>,
) -> ironbeam_core::Sink<String> {
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "map-state-two-batches-v1".into(),
            0,
            {
                let opens = Arc::clone(opens);
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
        .stateful_map_map_named::<i32, i32, (i32, i32, i32)>(
            "map",
            "tests/global-map-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    value,
                    prior.get(&value).copied().unwrap_or(-1),
                    i32::try_from(prior.len()).unwrap(),
                );
                Ok((MapStateMutation::Put(value, value * 10), observation))
            },
        )
        .unwrap()
        .map_named("label", |(key, (value, found, size))| {
            format!("{key}:{value}:{found}:{size}")
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "map-state-idempotent-capture-v1".into(),
            {
                let accepted = Arc::clone(accepted);
                move || {
                    Ok(Box::new(IdempotentCapture {
                        accepted: Arc::clone(&accepted),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap()
}

#[test]
fn global_map_state_recovers_values_with_source_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let sink = map_state_sink(&opens, &accepted);
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 2,
            recover: true,
            binary_build_id: "global-map-state-test-build".into(),
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
            "first map batch was not checkpointed"
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
            "1:10:-1:0".to_owned(),
            "2:20:-1:0".to_owned(),
            "1:10:100:1".to_owned(),
            "2:21:-1:1".to_owned(),
        ])
    );
}
