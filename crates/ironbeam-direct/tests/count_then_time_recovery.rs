mod processing_time_support;

use ironbeam_core::{
    Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress, Timestamped,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use processing_time_support::{Capture, TimedScript, Value, progress};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[test]
#[allow(clippy::too_many_lines)] // Source restart, saved trigger phase, and pane assertions form one scenario.
fn count_then_time_phase_and_deadline_survive_checkpoint() {
    for accumulating in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let opens = Arc::new(Mutex::new(Vec::new()));
        let committed = Arc::new(Mutex::new(Vec::new()));
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
                format!("count-then-time-source-{accumulating}-v1"),
                0,
                {
                    let opens = Arc::clone(&opens);
                    move |cursor| {
                        opens.lock().unwrap().push(cursor.clone());
                        let polls = match cursor.as_deref() {
                            None => VecDeque::from([
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: vec![
                                            Timestamped {
                                                value: (1, 2),
                                                timestamp_ns: 1_000_000,
                                            },
                                            Timestamped {
                                                value: (1, 3),
                                                timestamp_ns: 2_000_000,
                                            },
                                        ],
                                        progress: progress(1, 2_000_000, false),
                                    }),
                                    0,
                                ),
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: vec![Timestamped {
                                            value: (1, 4),
                                            timestamp_ns: 3_000_000,
                                        }],
                                        progress: progress(2, 3_000_000, false),
                                    }),
                                    0,
                                ),
                            ]),
                            Some([2]) => VecDeque::from([
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: Vec::new(),
                                        progress: progress(3, 3_000_000, false),
                                    }),
                                    5_000_000,
                                ),
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: Vec::new(),
                                        progress: progress(4, 10_000_000, true),
                                    }),
                                    5_000_000,
                                ),
                            ]),
                            _ => {
                                return Err(Error::incompatible(
                                    "unexpected sequential trigger cursor",
                                ));
                            }
                        };
                        Ok(Box::new(TimedScript { polls, clock_ns: 0 })
                            as TypedUnboundedSourceReader<Value>)
                    }
                },
            )
            .unwrap()
            .fixed_windows("window", 10_000_000)
            .unwrap();
        let triggered = if accumulating {
            source.accumulating_count_then_processing_time_trigger_named("trigger", 2, 5_000_000)
        } else {
            source.count_then_processing_time_trigger_named("trigger", 2, 5_000_000)
        }
        .unwrap();
        let sink = triggered
            .group_by_key_named("group")
            .unwrap()
            .reify_panes_named("pane")
            .unwrap()
            .map_named("row", |(pane, (key, values))| {
                format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
            })
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("count-then-time-sink-{accumulating}-v1"),
                {
                    let committed = Arc::clone(&committed);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(Capture {
                            committed: Arc::clone(&committed),
                            accepted: Arc::clone(&accepted),
                            fail_once: None,
                            panic_once: None,
                            fail_write_once: None,
                            bundle_ids: None,
                            staged: Vec::new(),
                            bundle: None,
                        }))
                    }
                },
            )
            .unwrap();
        let config = || JobConfig {
            checkpoint: Some(CheckpointConfig {
                directory: directory.path().into(),
                interval_batches: 1,
                max_checkpoints: 1,
                recover: true,
                binary_build_id: "count-then-time-test-build".into(),
            }),
            ..JobConfig::default()
        };
        let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while job
            .progress()
            .and_then(|progress| progress.checkpoint_position)
            != Some(vec![2])
        {
            assert_ne!(
                job.handle().status(),
                JobStatus::Failed,
                "sequential trigger failed before checkpoint"
            );
            assert!(
                Instant::now() < deadline,
                "sequential phase was not checkpointed"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(committed.lock().unwrap().as_slice(), ["1:[2, 3]:Early:0"]);
        job.cancel();
        assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
        DirectRunner
            .start_stream_sink(&sink, &config())
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![2])]);
        let expected: &[&str] = if accumulating {
            &[
                "1:[2, 3]:Early:0",
                "1:[2, 3, 4]:Early:1",
                "1:[2, 3, 4]:OnTime:2",
            ]
        } else {
            &["1:[2, 3]:Early:0", "1:[4]:Early:1"]
        };
        assert_eq!(committed.lock().unwrap().as_slice(), expected);
    }
}
