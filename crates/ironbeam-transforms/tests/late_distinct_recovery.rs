use ironbeam_core::{
    ElementMetadata, Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec,
    SourceProgress, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use ironbeam_transforms::ElementwiseExt;
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
#[allow(clippy::too_many_lines)] // The checkpoint boundary and recovered pane form one scenario.
fn late_distinct_resumes_partly_filled_threshold_without_replaying_on_time_pane() {
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
            "late-distinct-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let polls = match position.as_deref() {
                        None => vec![
                            batch(1, 1_000_000, false, Some((2, 1_000_000))),
                            batch(2, 10_000_000, false, None),
                            batch(3, 11_000_000, false, Some((2, 2_000_000))),
                        ],
                        Some([3]) => vec![
                            batch(4, 12_000_000, false, Some((2, 3_000_000))),
                            batch(5, 15_000_000, false, None),
                            batch(6, 20_000_000, true, None),
                        ],
                        _ => return Err(Error::incompatible("unexpected distinct resume cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<Row>)
                }
            },
        )
        .unwrap();
    let sink = values
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .late_count_trigger_named("late-count", 2)
        .unwrap()
        .distinct_per_window_named("distinct")
        .unwrap()
        .reify_timestamps_named("timestamp")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (timestamp, (key, value)))| {
            format!(
                "{key}:{value}:{timestamp}:{:?}:{}:{}:{}:{}",
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
            "late-distinct-sink-v1".into(),
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
            binary_build_id: "late-distinct-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![3])
    {
        if job.handle().status() == JobStatus::Failed {
            panic!("late distinct job failed: {}", job.wait().err().unwrap());
        }
        assert!(
            Instant::now() < deadline,
            "late distinct pane was not checkpointed: status={:?}, progress={:?}, committed={:?}",
            job.handle().status(),
            job.progress(),
            committed.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["a:2:1000000:OnTime:true:false:0:0"]
    );
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![3])]);
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "a:2:1000000:OnTime:true:false:0:0",
            "a:2:2000000:Late:false:false:1:1"
        ]
    );
}
