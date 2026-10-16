use ironbeam_core::{
    ElementMetadata, Error, ErrorKind, OrderedListStateMutation, Pipeline, ProgressSignal, Replay,
    Sink, SinkMode, SinkSpec, SourceProgress, Timestamped, TypedSink, TypedUnboundedSourceReader,
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
            None => (vec![(1, 20), (1, 10)], 1, false),
            Some([1]) => (vec![(1, 15), (1, -1)], 2, true),
            _ => return Err(Error::incompatible("unexpected ordered-list cursor")),
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

struct SlidingOrderedSource {
    cursor: Option<Vec<u8>>,
    emitted: bool,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for SlidingOrderedSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.emitted {
            return Ok(UnboundedPoll::Pending);
        }
        self.emitted = true;
        let (values, cursor, watermark, completed) = match self.cursor.as_deref() {
            None => (vec![(20, 6_000_000), (10, 7_000_000)], 1, 8_000_000, false),
            Some([1]) => (vec![(15, 12_000_000), (5, 18_000_000)], 2, 26_000_000, true),
            _ => return Err(Error::incompatible("unexpected sliding ordered cursor")),
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
fn sliding_ordered_list_state_restores_each_window_and_source_cursor() {
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
            "sliding-ordered-two-batches-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(SlidingOrderedSource {
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
        .stateful_ordered_list_map_named::<i32, (i32, Vec<i32>)>(
            "ordered",
            "tests/sliding-ordered-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                Ok((
                    OrderedListStateMutation::Append(i64::from(value), value),
                    (value, prior.iter().map(|(_, item)| *item).collect()),
                ))
            },
        )
        .unwrap()
        .map_named("label", |(key, (value, prior))| {
            format!("{key}:{value}:{prior:?}")
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sliding-ordered-capture-v1".into(),
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
            binary_build_id: "sliding-ordered-state-test-build".into(),
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
            "first sliding ordered batch was not checkpointed"
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
            "0:10000000:1:20:[]",
            "0:10000000:1:10:[20]",
            "5000000:15000000:1:20:[]",
            "5000000:15000000:1:10:[20]",
            "5000000:15000000:1:15:[10, 20]",
            "10000000:20000000:1:15:[]",
            "10000000:20000000:1:5:[15]",
            "15000000:25000000:1:5:[]",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<HashSet<_>>()
    );
}

fn ordered_list_pipeline(
    opens: &Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    accepted: &Arc<Mutex<HashSet<String>>>,
) -> Sink<String> {
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "ordered-list-two-batches-v1".into(),
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
        .stateful_ordered_list_map_named::<i32, (i32, Vec<i32>)>(
            "ordered",
            "tests/global-ordered-list-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let seen = prior.iter().map(|(_, item)| *item).collect();
                let mutation = if value == -1 {
                    OrderedListStateMutation::ClearRange {
                        start_ms: 10,
                        end_ms: 16,
                    }
                } else {
                    OrderedListStateMutation::Append(i64::from(value), value)
                };
                Ok((mutation, (value, seen)))
            },
        )
        .unwrap()
        .map_named("label", |(key, (value, seen))| {
            format!("{key}:{value}:{seen:?}")
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "ordered-list-idempotent-capture-v1".into(),
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
fn ordered_list_recovers_sorted_values_with_source_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let sink = ordered_list_pipeline(&opens, &accepted);
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 2,
            recover: true,
            binary_build_id: "global-ordered-list-test-build".into(),
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
            "first ordered-list batch was not checkpointed"
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
            "1:20:[]".to_owned(),
            "1:10:[20]".to_owned(),
            "1:15:[10, 20]".to_owned(),
            "1:-1:[10, 15, 20]".to_owned(),
        ])
    );
}
