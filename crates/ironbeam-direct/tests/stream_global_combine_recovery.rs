use ironbeam_core::{
    CombineFn, Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec,
    SourceProgress, TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader,
    UnboundedBatch, UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Script {
    polls: VecDeque<UnboundedPoll<Timestamped<i32>>>,
}

impl UnboundedSource<Timestamped<i32>> for Script {
    fn poll(&mut self, _max_elements: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn batch(
    cursor: u8,
    watermark: i64,
    completed: bool,
    values: &[(i32, i64)],
) -> UnboundedPoll<Timestamped<i32>> {
    UnboundedPoll::Ready(UnboundedBatch {
        elements: values
            .iter()
            .map(|(value, timestamp_ns)| Timestamped {
                value: *value,
                timestamp_ns: *timestamp_ns,
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
}

struct FailOnceSink {
    committed: Arc<Mutex<Vec<i32>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    commit_attempts: Arc<AtomicUsize>,
    fail_at_commit: usize,
    bundle: Option<String>,
    pending: Vec<i32>,
}

struct Sum;

impl CombineFn<i32, i32, i32> for Sum {
    fn create(&self) -> i32 {
        0
    }

    fn add_input(&self, accumulator: &mut i32, value: i32) {
        *accumulator += value;
    }

    fn merge(&self, accumulator: &mut i32, other: i32) {
        *accumulator += other;
    }

    fn finish(&self, accumulator: i32) -> i32 {
        accumulator
    }
}

impl TypedSink<i32> for FailOnceSink {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.bundle = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: i32) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if self.commit_attempts.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at_commit {
            return Err(Error::execution("sink", "direct", "commit failed once"));
        }
        let bundle = self.bundle.take().expect("bundle ID was bound");
        if self.accepted.lock().unwrap().insert(bundle) {
            self.committed
                .lock()
                .unwrap()
                .extend(self.pending.drain(..));
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

#[test]
fn global_combine_restores_open_window_after_failed_sink_commit() {
    for lifted in [false, true] {
        run_restore_case(lifted, false);
    }
}

#[test]
fn global_trigger_combine_restores_pane_index_after_failed_sink_commit() {
    for lifted in [false, true] {
        run_restore_case(lifted, true);
    }
}

#[allow(clippy::too_many_lines)] // The fixture exercises both combine modes through the same recovery path.
fn run_restore_case(lifted: bool, global_trigger: bool) {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let commit_attempts = Arc::new(AtomicUsize::new(0));
    let fail_at_commit = if global_trigger { 2 } else { 1 };
    let pipeline = Pipeline::default();
    let opens_for_source = Arc::clone(&opens);
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "global-combine-recovery-source-v1".into(),
            0,
            move |cursor| {
                opens_for_source.lock().unwrap().push(cursor.clone());
                let final_batch = batch(2, 10_000_000, true, &[(3, 6_000_000)]);
                let polls = match cursor.as_deref() {
                    None => vec![
                        batch(1, 5_000_000, false, &[(1, 1_000_000), (2, 3_000_000)]),
                        final_batch,
                    ],
                    Some([1]) => vec![final_batch],
                    _ => return Err(Error::incompatible("unexpected global combine cursor")),
                };
                Ok(Box::new(Script {
                    polls: polls.into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let windowed = if global_trigger {
        source
            .early_count_trigger_named("global-trigger", 2)
            .unwrap()
    } else {
        source.fixed_windows("window", 10_000_000).unwrap()
    };
    let combined = if lifted {
        windowed.combine_globally_lifted_named("sum", Sum)
    } else {
        windowed.combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
    }
    .unwrap();
    let combined = if global_trigger {
        combined
            .reify_panes_named("pane")
            .unwrap()
            .map_named("pane-and-value", |(pane, value)| {
                i32::try_from(pane.index).unwrap() * 100 + value
            })
            .unwrap()
    } else {
        combined
    };
    let sink = combined
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "global-combine-recovery-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                let commit_attempts = Arc::clone(&commit_attempts);
                move || {
                    Ok(Box::new(FailOnceSink {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        commit_attempts: Arc::clone(&commit_attempts),
                        fail_at_commit,
                        bundle: None,
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "global-combine-recovery-test".into(),
        }),
        ..JobConfig::default()
    };
    let error = DirectRunner
        .start_stream_sink(&sink, &config)
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    if global_trigger {
        assert_eq!(*committed.lock().unwrap(), [3]);
    } else {
        assert!(committed.lock().unwrap().is_empty());
    }
    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &config)
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    if global_trigger {
        assert_eq!(*committed.lock().unwrap(), [3, 103]);
    } else {
        assert_eq!(*committed.lock().unwrap(), [6]);
    }
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
}

#[test]
fn keyed_latest_restores_global_trigger_panes_after_failed_sink_commit() {
    let directory = tempfile::tempdir().unwrap();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let commit_attempts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "keyed-latest-source",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, 3_i32), 1_000_000),
                    ((1, 2), 2_000_000),
                    ((2, 9), 3_000_000),
                ]),
                TestStreamEvent::Elements(vec![((1, 4), 4_000_000)]),
            ],
        )
        .unwrap()
        .early_count_trigger_named("keyed-latest-trigger", 2)
        .unwrap()
        .latest_per_key_named("keyed-latest")
        .unwrap()
        .reify_panes_named("keyed-latest-pane")
        .unwrap()
        .map_named("keyed-latest-row", |(pane, (key, value))| {
            i32::try_from(pane.index).unwrap() * 1_000 + key * 100 + value
        })
        .unwrap()
        .sink_named_with_config(
            "keyed-latest-sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "keyed-latest-recovery-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                let commit_attempts = Arc::clone(&commit_attempts);
                move || {
                    Ok(Box::new(FailOnceSink {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        commit_attempts: Arc::clone(&commit_attempts),
                        fail_at_commit: 2,
                        bundle: None,
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "keyed-latest-recovery-test".into(),
        }),
        ..JobConfig::default()
    };
    let error = DirectRunner
        .start_stream_sink(&sink, &config)
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(*committed.lock().unwrap(), [102]);

    assert_eq!(
        DirectRunner
            .start_stream_sink(&sink, &config)
            .unwrap()
            .wait()
            .unwrap()
            .status(),
        JobStatus::Succeeded
    );
    let mut rows = committed.lock().unwrap().clone();
    rows.sort_unstable();
    assert_eq!(rows, [102, 209, 1104]);
}
