use ironbeam_core::{
    Element, Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec,
    SourceProgress, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll, UnboundedSource, Version,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Row = (String, (u8, i32));

struct Script(VecDeque<UnboundedPoll<Timestamped<Row>>>);

impl UnboundedSource<Timestamped<Row>> for Script {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<Row>>, Error> {
        Ok(self.0.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn batch(
    cursor: u8,
    watermark: i64,
    completed: bool,
    values: &[(u8, i32, i64)],
) -> UnboundedPoll<Timestamped<Row>> {
    UnboundedPoll::Ready(UnboundedBatch {
        elements: values
            .iter()
            .map(|(shard, value, timestamp_ns)| Timestamped {
                value: ("hot".into(), (*shard, *value)),
                timestamp_ns: *timestamp_ns,
            })
            .collect(),
        progress: ProgressSignal {
            source_key: "source".into(),
            split_id: "split-0".into(),
            checkpoint_position: Some(vec![cursor]),
            event_time_ns: Some(watermark),
            completed,
        },
    })
}

struct Capture {
    rows: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    staged: Vec<String>,
    bundle: String,
}

impl TypedSink<String> for Capture {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        id.clone_into(&mut self.bundle);
        Ok(())
    }

    fn write(&mut self, row: String) -> Result<(), Error> {
        self.staged.push(row);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
            self.rows.lock().unwrap().append(&mut self.staged);
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

#[test]
fn sharded_batches_restore_both_open_shards_without_replaying_the_source() {
    run_sharded_batch_recovery(false);
}

#[test]
fn sharded_byte_batches_restore_both_open_shards_without_replaying_the_source() {
    run_sharded_batch_recovery(true);
}

#[expect(
    clippy::too_many_lines,
    reason = "the source restart checks both open shard folds and committed batches"
)]
fn run_sharded_batch_recovery(byte_limited: bool) {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(Vec::new()));
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
            "sharded-batch-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    let polls = match cursor.as_deref() {
                        None => vec![batch(
                            1,
                            2_000_000,
                            false,
                            &[(1, 10, 1_000_000), (2, 20, 2_000_000)],
                        )],
                        Some([1]) => vec![
                            batch(
                                2,
                                4_000_000,
                                false,
                                &[(1, 30, 3_000_000), (2, 40, 4_000_000)],
                            ),
                            batch(3, 10_000_000, true, &[]),
                        ],
                        _ => return Err(Error::incompatible("unexpected shard source cursor")),
                    };
                    Ok(Box::new(Script(polls.into())) as TypedUnboundedSourceReader<Row>)
                }
            },
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let batches = if byte_limited {
        source.group_into_batches_with_limits_and_sharded_key_named(
            "batches",
            10,
            (1_u8, 10_i32).encoded_len().unwrap() * 2,
            "tests/recovery-shard",
            Version { major: 1, minor: 0 },
            |_, (shard, _)| vec![*shard],
        )
    } else {
        source.group_into_batches_with_sharded_key_named(
            "batches",
            2,
            "tests/recovery-shard",
            Version { major: 1, minor: 0 },
            |_, (shard, _)| vec![*shard],
        )
    };
    let sink = batches
        .unwrap()
        .map_named("row", |(key, values)| {
            let mut values = values
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            values.sort_unstable();
            format!("{}:{:?}:{values:?}", key.key, key.shard_id)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sharded-batch-sink-v1".into(),
            {
                let rows = Arc::clone(&rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
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
            binary_build_id: "sharded-batch-recovery-build".into(),
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
            "first shard state was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(rows.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);

    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    let mut committed = rows.lock().unwrap().clone();
    committed.sort();
    assert_eq!(committed, ["hot:[1]:[10, 30]", "hot:[2]:[20, 40]"]);
}
