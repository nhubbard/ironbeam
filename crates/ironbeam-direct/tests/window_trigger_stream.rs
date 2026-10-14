use ironbeam_core::{
    ElementMetadata, Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec,
    SourceProgress, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Row = (String, i32);

struct Script {
    polls: VecDeque<UnboundedPoll<Timestamped<Row>>>,
}

impl UnboundedSource<Timestamped<Row>> for Script {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<Row>>, Error> {
        Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

struct Capture {
    committed: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    staged: Vec<String>,
    bundle: Option<String>,
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

    fn write_with_metadata(
        &mut self,
        value: String,
        metadata: ElementMetadata,
    ) -> Result<(), Error> {
        let pane = metadata.pane;
        let suffix = format!(
            ":{:?}:{}:{}:{}:{}",
            pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
        );
        if !value.ends_with(&suffix) {
            return Err(Error::incompatible(
                "Direct sink pane differs from reified pane",
            ));
        }
        self.write(value)
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

fn batch(
    cursor: u8,
    watermark: i64,
    completed: bool,
    value: Option<(i32, i64)>,
) -> UnboundedPoll<Timestamped<Row>> {
    UnboundedPoll::Ready(UnboundedBatch {
        elements: value
            .into_iter()
            .map(|(value, timestamp_ns)| Timestamped {
                value: ("a".to_owned(), value),
                timestamp_ns,
            })
            .collect(),
        progress: ProgressSignal {
            source_key: "values".into(),
            split_id: "split-0".into(),
            checkpoint_position: Some(vec![cursor]),
            event_time_ns: Some(watermark),
            completed,
        },
    })
}

#[test]
fn early_count_pane_indices_survive_checkpoint_recovery() {
    run_count_pane_recovery(false, false);
}

#[test]
fn accumulating_count_values_survive_checkpoint_recovery() {
    run_count_pane_recovery(true, false);
}

#[test]
fn latest_in_pane_discarding_times_survive_checkpoint_recovery() {
    run_count_pane_recovery(false, true);
}

#[test]
fn latest_in_pane_accumulating_times_survive_checkpoint_recovery() {
    run_count_pane_recovery(true, true);
}

#[allow(clippy::too_many_lines)] // The source, sink, restart, and pane assertions form one scenario.
fn run_count_pane_recovery(accumulating: bool, latest_time: bool) {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let values = pipeline
        .unbounded_source_with_factory(
            "values",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "count-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let polls = match position.as_deref() {
                        None => vec![batch(1, 2_000_000, false, Some((2, 1_000_000)))],
                        Some([1]) => vec![
                            batch(2, 4_000_000, false, Some((3, 3_000_000))),
                            batch(3, 10_000_000, true, None),
                        ],
                        _ => return Err(Error::incompatible("unexpected count resume cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<Row>)
                }
            },
        )
        .unwrap();
    let values = values.fixed_windows("windows", 10_000_000).unwrap();
    let values = if latest_time {
        values
            .latest_in_pane_output_time_named("output-time")
            .unwrap()
    } else {
        values
    };
    let values = if accumulating {
        values.accumulating_count_trigger_named("count-trigger", 1)
    } else {
        values.early_count_trigger_named("count-trigger", 1)
    }
    .unwrap();
    let sink = values
        .group_by_key_named("groups")
        .unwrap()
        .reify_timestamps_named("timestamp")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", move |(pane, (timestamp, (key, values)))| {
            let timestamp = if latest_time {
                format!(":{timestamp}")
            } else {
                String::new()
            };
            format!(
                "{key}:{values:?}{timestamp}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
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
            "count-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: None,
                    }))
                }
            },
        )
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "count-test-build".into(),
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
        assert!(
            Instant::now() < deadline,
            "first early pane was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let first = if latest_time {
        "a:[2]:1000000:Early:true:false:0:-1"
    } else {
        "a:[2]:Early:true:false:0:-1"
    };
    assert_eq!(committed.lock().unwrap().as_slice(), [first]);
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    let expected = if accumulating && latest_time {
        vec![
            "a:[2]:1000000:Early:true:false:0:-1",
            "a:[2, 3]:3000000:Early:false:false:1:-1",
            "a:[2, 3]:3000000:OnTime:false:true:2:0",
        ]
    } else if accumulating {
        vec![
            "a:[2]:Early:true:false:0:-1",
            "a:[2, 3]:Early:false:false:1:-1",
            "a:[2, 3]:OnTime:false:true:2:0",
        ]
    } else if latest_time {
        vec![
            "a:[2]:1000000:Early:true:false:0:-1",
            "a:[3]:3000000:Early:false:false:1:-1",
        ]
    } else {
        vec![
            "a:[2]:Early:true:false:0:-1",
            "a:[3]:Early:false:false:1:-1",
        ]
    };
    assert_eq!(committed.lock().unwrap().as_slice(), expected);
}
