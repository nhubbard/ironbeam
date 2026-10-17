mod processing_time_support;

use ironbeam_core::{
    Error, ErrorKind, PaneTiming, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress,
    TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use processing_time_support::{Capture as IdempotentCapture, TimedScript, Value, progress};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Capture {
    rows: Arc<Mutex<Vec<String>>>,
    staged: Vec<String>,
}

impl TypedSink<String> for Capture {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.rows.lock().unwrap().append(&mut self.staged);
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

fn run(accumulating: bool) -> Vec<String> {
    let pipeline = Pipeline::default();
    let mut events = Vec::new();
    for values in [[1, 2].as_slice(), &[3, 4], &[5, 6], &[7], &[8, 9]] {
        events.push(TestStreamEvent::Elements(
            values
                .iter()
                .map(|&value| (("a".to_owned(), value), i64::from(value) * 1_000_000))
                .collect(),
        ));
    }
    events.push(TestStreamEvent::AdvanceWatermark(10_000_000));
    let values = pipeline
        .test_stream_source("source", events)
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap();
    let values = if accumulating {
        values.accumulating_count_or_finally_trigger_named("trigger", 2, 7)
    } else {
        values.count_or_finally_trigger_named("trigger", 2, 7)
    }
    .unwrap();
    let rows = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&rows);
    let sink = values
        .group_by_key_named("group")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, values))| {
            let timing = match pane.timing {
                PaneTiming::Early => "Early",
                PaneTiming::OnTime => "OnTime",
                PaneTiming::Late => "Late",
                PaneTiming::Unknown => "Unknown",
            };
            format!("{key}:{values:?}:{timing}:{}", pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("or-finally-{accumulating}-v1"),
            move || {
                Ok(Box::new(Capture {
                    rows: Arc::clone(&capture),
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    rows.lock().unwrap().clone()
}

#[test]
fn count_or_finally_stops_early_firing_but_keeps_later_input_for_on_time() {
    assert_eq!(
        run(false),
        [
            "a:[1, 2]:Early:0",
            "a:[3, 4]:Early:1",
            "a:[5, 6]:Early:2",
            "a:[7]:Early:3",
            "a:[8, 9]:OnTime:4",
        ]
    );
    assert_eq!(
        run(true),
        [
            "a:[1, 2]:Early:0",
            "a:[1, 2, 3, 4]:Early:1",
            "a:[1, 2, 3, 4, 5, 6]:Early:2",
            "a:[1, 2, 3, 4, 5, 6, 7]:Early:3",
            "a:[1, 2, 3, 4, 5, 6, 7, 8, 9]:OnTime:4",
        ]
    );
}

#[test]
fn count_or_finally_keeps_key_and_window_cursors_independent() {
    let pipeline = Pipeline::default();
    let batches = [
        vec![("a", 1), ("a", 2)],
        vec![("b", 10), ("b", 11)],
        vec![("a", 3)],
        vec![("a", 4), ("a", 5)],
        vec![("b", 12)],
        vec![("a", 20), ("a", 21)],
        vec![("b", 30)],
    ];
    let mut events = batches
        .into_iter()
        .map(|batch| {
            TestStreamEvent::Elements(
                batch
                    .into_iter()
                    .map(|(key, value)| {
                        let timestamp_ns = if value >= 20 { 11_000_000 } else { 1_000_000 };
                        ((key.to_owned(), value), timestamp_ns)
                    })
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    events.push(TestStreamEvent::AdvanceWatermark(10_000_000));
    events.push(TestStreamEvent::AdvanceWatermark(20_000_000));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let capture = Arc::clone(&rows);
    let sink = pipeline
        .test_stream_source("source", events)
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .count_or_finally_trigger_named("trigger", 2, 3)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .with_window_named("interval")
        .unwrap()
        .map_named("row", |(window, (pane, (key, values)))| {
            format!(
                "{}..{}:{key}:{values:?}:{:?}:{}",
                window.start_ns, window.end_ns, pane.timing, pane.index
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
            "or-finally-key-window-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    rows: Arc::clone(&capture),
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut actual = rows.lock().unwrap().clone();
    actual.sort();
    let mut expected = [
        "0..10000000:a:[1, 2]:Early:0",
        "0..10000000:a:[3]:Early:1",
        "0..10000000:a:[4, 5]:OnTime:2",
        "0..10000000:b:[10, 11]:Early:0",
        "0..10000000:b:[12]:Early:1",
        "10000000..20000000:a:[20, 21]:Early:0",
        "10000000..20000000:b:[30]:OnTime:0",
    ]
    .map(str::to_owned)
    .to_vec();
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn count_or_finally_cursor_survives_checkpoint() {
    for accumulating in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let opens = Arc::new(Mutex::new(Vec::new()));
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let pipeline = Pipeline::default();
        let values = pipeline
            .unbounded_source_with_factory(
                "source",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("or-finally-source-{accumulating}-v1"),
                0,
                {
                    let opens = Arc::clone(&opens);
                    move |cursor| {
                        opens.lock().unwrap().push(cursor.clone());
                        let batches: VecDeque<_> = match cursor.as_deref() {
                            None => VecDeque::from([
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: vec![(1, 1), (1, 2)]
                                            .into_iter()
                                            .map(|value| Timestamped {
                                                timestamp_ns: i64::from(value.1) * 1_000_000,
                                                value,
                                            })
                                            .collect(),
                                        progress: progress(1, 2_000_000, false),
                                    }),
                                    0,
                                ),
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: vec![(1, 3), (1, 4)]
                                            .into_iter()
                                            .map(|value| Timestamped {
                                                timestamp_ns: i64::from(value.1) * 1_000_000,
                                                value,
                                            })
                                            .collect(),
                                        progress: progress(2, 4_000_000, false),
                                    }),
                                    0,
                                ),
                            ]),
                            Some([2]) => VecDeque::from([
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: vec![(1, 5), (1, 6), (1, 7)]
                                            .into_iter()
                                            .map(|value| Timestamped {
                                                timestamp_ns: i64::from(value.1) * 1_000_000,
                                                value,
                                            })
                                            .collect(),
                                        progress: progress(3, 7_000_000, false),
                                    }),
                                    0,
                                ),
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: vec![(1, 8), (1, 9)]
                                            .into_iter()
                                            .map(|value| Timestamped {
                                                timestamp_ns: i64::from(value.1) * 1_000_000,
                                                value,
                                            })
                                            .collect(),
                                        progress: progress(4, 9_000_000, false),
                                    }),
                                    0,
                                ),
                                (
                                    UnboundedPoll::Ready(UnboundedBatch {
                                        elements: Vec::new(),
                                        progress: progress(5, 10_000_000, true),
                                    }),
                                    0,
                                ),
                            ]),
                            _ => return Err(Error::incompatible("unexpected OrFinally cursor")),
                        };
                        Ok(Box::new(TimedScript {
                            polls: batches,
                            clock_ns: 0,
                        })
                            as TypedUnboundedSourceReader<Value>)
                    }
                },
            )
            .unwrap()
            .fixed_windows("windows", 10_000_000)
            .unwrap();
        let values = if accumulating {
            values.accumulating_count_or_finally_trigger_named("trigger", 2, 7)
        } else {
            values.count_or_finally_trigger_named("trigger", 2, 7)
        }
        .unwrap();
        let sink = values
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
                format!("or-finally-recovery-sink-{accumulating}-v1"),
                {
                    let committed = Arc::clone(&committed);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(IdempotentCapture {
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
                binary_build_id: "or-finally-recovery-build".into(),
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
            assert_ne!(job.handle().status(), JobStatus::Failed);
            assert!(
                Instant::now() < deadline,
                "OrFinally cursor was not checkpointed"
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
        assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![2])]);
        // Count fires on row 6; the final threshold fires separately on row 7.
        let expected: &[&str] = if accumulating {
            &[
                "1:[1, 2]:Early:0",
                "1:[1, 2, 3, 4]:Early:1",
                "1:[1, 2, 3, 4, 5, 6]:Early:2",
                "1:[1, 2, 3, 4, 5, 6, 7]:Early:3",
                "1:[1, 2, 3, 4, 5, 6, 7, 8, 9]:OnTime:4",
            ]
        } else {
            &[
                "1:[1, 2]:Early:0",
                "1:[3, 4]:Early:1",
                "1:[5, 6]:Early:2",
                "1:[7]:Early:3",
                "1:[8, 9]:OnTime:4",
            ]
        };
        assert_eq!(committed.lock().unwrap().as_slice(), expected);
    }
}
