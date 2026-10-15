#![allow(clippy::too_many_lines)] // The open timer, saved cursor, and resumed firing form one fixture.

use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
    UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

type Value = (i32, i32);

struct TimedScript {
    polls: VecDeque<(UnboundedPoll<Timestamped<Value>>, i64)>,
    clock_ns: i64,
}

impl UnboundedSource<Timestamped<Value>> for TimedScript {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<Value>>, Error> {
        let Some((poll, clock_ns)) = self.polls.pop_front() else {
            return Ok(UnboundedPoll::Pending);
        };
        self.clock_ns = clock_ns;
        Ok(poll)
    }

    fn processing_time_ns(&self) -> Option<i64> {
        Some(self.clock_ns)
    }
}

struct Capture {
    committed: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    staged: Vec<String>,
    bundle: Option<String>,
}

impl TypedSink<String> for Capture {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        self.bundle = Some(id.to_owned());
        Ok(())
    }
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
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

fn progress(cursor: u8, watermark_ns: i64, completed: bool) -> ProgressSignal {
    ProgressSignal {
        source_key: "source".into(),
        split_id: "split-0".into(),
        checkpoint_position: Some(vec![cursor]),
        event_time_ns: Some(watermark_ns),
        completed,
    }
}

#[test]
fn processing_timer_deadline_survives_checkpoint_before_first_firing() {
    let directory = tempfile::tempdir().unwrap();
    let hold = Arc::new(AtomicBool::new(true));
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
            "processing-timer-source-v1".into(),
            0,
            {
                let hold = Arc::clone(&hold);
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    let polls = match cursor.as_deref() {
                        None => VecDeque::from([(
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: vec![
                                    Timestamped {
                                        value: (1, 2),
                                        timestamp_ns: 1_000_000,
                                    },
                                    Timestamped {
                                        value: (3, 4),
                                        timestamp_ns: 1_000_000,
                                    },
                                ],
                                progress: progress(1, 1_000_000, false),
                            }),
                            0,
                        )]),
                        Some([1]) if hold.load(Ordering::SeqCst) => VecDeque::new(),
                        Some([1]) => VecDeque::from([
                            (
                                UnboundedPoll::Ready(UnboundedBatch {
                                    elements: Vec::new(),
                                    progress: progress(2, 1_000_000, false),
                                }),
                                5_000_000,
                            ),
                            (
                                UnboundedPoll::Ready(UnboundedBatch {
                                    elements: Vec::new(),
                                    progress: progress(3, 10_000_000, true),
                                }),
                                5_000_000,
                            ),
                        ]),
                        _ => return Err(Error::incompatible("unexpected timer source cursor")),
                    };
                    Ok(Box::new(TimedScript { polls, clock_ns: 0 })
                        as TypedUnboundedSourceReader<Value>)
                }
            },
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .accumulating_processing_time_trigger_named("trigger", 5_000_000)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = source
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
            "processing-timer-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    staged: Vec::new(),
                    bundle: None,
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "processing-timer-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first_job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while first_job
        .progress()
        .and_then(|signal| signal.checkpoint_position)
        != Some(vec![1])
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        first_job
            .progress()
            .and_then(|signal| signal.checkpoint_position),
        Some(vec![1])
    );
    assert!(committed.lock().unwrap().is_empty());
    first_job.cancel();
    assert_eq!(first_job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    let resumed = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    assert_eq!(resumed.wait().unwrap().status(), JobStatus::Succeeded);
    let mut rows = committed.lock().unwrap().clone();
    rows.sort();
    assert_eq!(
        rows,
        [
            "1:[2]:Early:0",
            "1:[2]:OnTime:1",
            "3:[4]:Early:0",
            "3:[4]:OnTime:1",
        ]
    );
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
}

#[test]
fn processing_timer_rejects_a_negative_source_clock() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "negative-clock-source-v1".into(),
            0,
            |_| {
                Ok(Box::new(TimedScript {
                    polls: VecDeque::from([(
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value: (1, 2),
                                timestamp_ns: 1_000_000,
                            }],
                            progress: progress(1, 1_000_000, false),
                        }),
                        -1,
                    )]),
                    clock_ns: 0,
                }) as TypedUnboundedSourceReader<Value>)
            },
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .early_processing_time_trigger_named("trigger", 5_000_000)
        .unwrap();
    let sink = source
        .group_by_key_named("group")
        .unwrap()
        .map_named("row", |(key, values)| format!("{key}:{values:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "negative-clock-sink-v1".into(),
            || {
                Ok(Box::new(Capture {
                    committed: Arc::new(Mutex::new(Vec::new())),
                    accepted: Arc::new(Mutex::new(HashSet::new())),
                    staged: Vec::new(),
                    bundle: None,
                }))
            },
        )
        .unwrap();
    let error = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Incompatible);
}
