use ironbeam_core::{
    Element, Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec,
    SourceProgress, TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader,
    UnboundedBatch, UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectCollectExt, DirectRunner, JobConfig};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

struct Rows {
    committed: Arc<Mutex<Vec<String>>>,
    pending: Vec<String>,
}

impl TypedSink<String> for Rows {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        self.committed
            .lock()
            .unwrap()
            .extend(self.pending.drain(..));
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

struct RecoveringByteSource {
    cursor: Option<Vec<u8>>,
    hold: Arc<AtomicBool>,
}

impl UnboundedSource<Timestamped<(i32, String)>> for RecoveringByteSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, String)>>, Error> {
        let (value, position, timestamp, completed) = match self.cursor.as_deref() {
            None => ("a", 1_u8, 1_000_000, false),
            Some([1]) if self.hold.load(Ordering::SeqCst) => return Ok(UnboundedPoll::Pending),
            Some([1]) => ("b", 2, 2_000_000, true),
            _ => return Ok(UnboundedPoll::Pending),
        };
        self.cursor = Some(vec![position]);
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: vec![Timestamped {
                value: (1, value.to_owned()),
                timestamp_ns: timestamp,
            }],
            progress: ProgressSignal {
                source_key: "values".into(),
                split_id: "default".into(),
                checkpoint_position: self.cursor.clone(),
                event_time_ns: Some(if completed { 10_000_000 } else { timestamp }),
                completed,
            },
        }))
    }
}

#[test]
fn bounded_keyed_batches_obey_count_and_encoded_byte_limits() {
    let pipeline = Pipeline::default();
    let short_bytes = "a".to_owned().encoded_len().unwrap();
    let batches = pipeline
        .from_vec(
            "values",
            vec![
                (1_i32, "a".to_owned()),
                (1, "bbb".to_owned()),
                (1, "c".to_owned()),
                (1, "d".to_owned()),
                (1, "oversized".to_owned()),
                (2, "e".to_owned()),
            ],
        )
        .unwrap()
        .group_into_batches_with_limits_named("batches", 2, short_bytes * 2)
        .unwrap();
    assert_eq!(
        batches.collect_seq().unwrap(),
        [
            (1, vec!["a".to_owned()]),
            (1, vec!["bbb".to_owned()]),
            (1, vec!["c".to_owned(), "d".to_owned()]),
            (1, vec!["oversized".to_owned()]),
            (2, vec!["e".to_owned()]),
        ]
    );
}

#[test]
fn byte_limited_batches_reject_invalid_limits_and_build_unbounded_state() {
    let pipeline = Pipeline::default();
    let bounded = pipeline.from_vec("bounded", vec![(1_i32, 2_i32)]).unwrap();
    for (key, count, bytes) in [("count", 0, 1), ("bytes", 1, 0)] {
        let error = bounded
            .group_into_batches_with_limits_named(key, count, bytes)
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Graph);
    }
    let unbounded = pipeline
        .test_stream_source(
            "unbounded",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap();
    unbounded
        .group_into_batches_with_limits_named("unbounded-batches", 2, 8)
        .unwrap();
    let unaligned = unbounded
        .fixed_windows("nanosecond-window", 1_000_001)
        .unwrap();
    let error = unaligned
        .group_into_batches_with_limits_named("unaligned-batches", 2, 8)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Graph);
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(graph.node("count").is_none());
    assert!(graph.node("bytes").is_none());
    assert!(graph.node("unbounded-batches").is_some());
    assert!(graph.node("unbounded-batches/byte-state").is_some());
    assert!(graph.node("unaligned-batches").is_none());
}

#[test]
fn unbounded_byte_batches_validate_timer_window_alignment() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap();
    let fixed_offset = source
        .fixed_windows_with_offset("fixed-offset", 10_000_000, 2_000_000)
        .unwrap();
    fixed_offset
        .group_into_batches_with_limits_named("valid-fixed-offset", 2, 8)
        .unwrap();
    let sliding = source
        .sliding_windows("sliding", 10_000_000, 5_000_000, 0)
        .unwrap();
    sliding
        .group_into_batches_with_limits_named("valid-sliding", 2, 8)
        .unwrap();
    for (name, candidate) in [
        (
            "short-fixed",
            source.fixed_windows("short-fixed-window", 500_000).unwrap(),
        ),
        (
            "short-offset",
            source
                .fixed_windows_with_offset("short-offset-window", 500_000, 0)
                .unwrap(),
        ),
        (
            "unaligned-offset",
            source
                .fixed_windows_with_offset("unaligned-offset-window", 10_000_000, 1)
                .unwrap(),
        ),
        (
            "short-sliding",
            source
                .sliding_windows("short-sliding-window", 500_000, 5_000_000, 0)
                .unwrap(),
        ),
        (
            "unaligned-period",
            source
                .sliding_windows("unaligned-period-window", 10_000_000, 5_000_001, 0)
                .unwrap(),
        ),
        (
            "unaligned-slide-offset",
            source
                .sliding_windows("unaligned-slide-offset-window", 10_000_000, 5_000_000, 1)
                .unwrap(),
        ),
        (
            "session",
            source
                .session_windows_named("session-window", 5_000_000)
                .unwrap(),
        ),
    ] {
        assert_eq!(
            candidate
                .group_into_batches_with_limits_named(name, 2, 8)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Graph
        );
    }
}

#[test]
fn unbounded_byte_batches_flush_on_limits_and_window_end() {
    run_unbounded_byte_batches(false);
    run_unbounded_byte_batches(true);
}

#[test]
fn unbounded_byte_batch_restores_partial_state_with_source_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let hold = Arc::new(AtomicBool::new(true));
    let opened = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source_hold = Arc::clone(&hold);
    let source_opened = Arc::clone(&opened);
    let source = pipeline
        .unbounded_source_with_factory(
            "values",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "tests/byte-batch-recovery-source-v1".into(),
            0,
            move |cursor| {
                source_opened.lock().unwrap().push(cursor.clone());
                Ok(Box::new(RecoveringByteSource {
                    cursor,
                    hold: Arc::clone(&source_hold),
                })
                    as TypedUnboundedSourceReader<(i32, String)>)
            },
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let captured = Arc::clone(&rows);
    let short_bytes = "a".to_owned().encoded_len().unwrap();
    let sink = source
        .group_into_batches_with_limits_named("batches", 10, short_bytes * 2)
        .unwrap()
        .reify_timestamps_named("timestamps")
        .unwrap()
        .map_named("row", |(timestamp, (key, values))| {
            format!("{key}:{timestamp}:{values:?}")
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/byte-batch-recovery-sink-v1".into(),
            move || {
                Ok(Box::new(Rows {
                    committed: Arc::clone(&captured),
                    pending: Vec::new(),
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 2,
            recover: true,
            binary_build_id: "tests/byte-batch-recovery-build-v1".into(),
        }),
        ..JobConfig::default()
    };
    let first = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while first
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert!(
            Instant::now() < deadline,
            "partial byte batch was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(rows.lock().unwrap().is_empty());
    first.cancel();
    assert!(first.wait().is_err());
    hold.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*opened.lock().unwrap(), [None, Some(vec![1])]);
    assert_eq!(*rows.lock().unwrap(), ["1:1000000:[\"a\", \"b\"]"]);
}

#[test]
fn unbounded_byte_batch_timer_holds_earliest_out_of_order_timestamp() {
    let pipeline = Pipeline::default();
    let rows = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&rows);
    let sink = pipeline
        .test_stream_source(
            "values",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, "later".to_owned()), 3_000_000),
                    ((1, "earlier".to_owned()), 1_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .group_into_batches_with_limits_named("batches", 10, 1_000)
        .unwrap()
        .reify_timestamps_named("timestamps")
        .unwrap()
        .map_named("row", |(timestamp, (key, values))| {
            format!("{key}:{timestamp}:{values:?}")
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/out-of-order-byte-batch-sink-v1".into(),
            move || {
                Ok(Box::new(Rows {
                    committed: Arc::clone(&captured),
                    pending: Vec::new(),
                }))
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        *rows.lock().unwrap(),
        ["1:1000000:[\"later\", \"earlier\"]"]
    );
}

#[test]
fn unbounded_byte_batches_keep_overlapping_sliding_windows_separate() {
    let pipeline = Pipeline::default();
    let rows = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&rows);
    let sink = pipeline
        .test_stream_source(
            "values",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, "a".to_owned()), 1_000_000),
                    ((1, "b".to_owned()), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(9_000_000),
            ],
        )
        .unwrap()
        .sliding_windows("window", 6_000_000, 3_000_000, 0)
        .unwrap()
        .group_into_batches_with_limits_named("batches", 10, 1_000)
        .unwrap()
        .reify_timestamps_named("timestamps")
        .unwrap()
        .with_window_named("interval")
        .unwrap()
        .map_named("row", |(window, (timestamp, (key, values)))| {
            format!(
                "{}..{}:{key}:{timestamp}:{values:?}",
                window.start_ns, window.end_ns
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
            "tests/sliding-byte-batch-sink-v1".into(),
            move || {
                Ok(Box::new(Rows {
                    committed: Arc::clone(&captured),
                    pending: Vec::new(),
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
    assert_eq!(
        actual,
        [
            "-3000000..3000000:1:1000000:[\"a\"]",
            "0..6000000:1:1000000:[\"a\", \"b\"]",
            "3000000..9000000:1:4000000:[\"b\"]",
        ]
    );
}

fn run_unbounded_byte_batches(windowed: bool) {
    let pipeline = Pipeline::default();
    let short_bytes = "a".to_owned().encoded_len().unwrap();
    let source = pipeline
        .test_stream_source(
            "values",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, "a".to_owned()), 1_000_000),
                    ((1, "bbb".to_owned()), 2_000_000),
                    ((1, "c".to_owned()), 3_000_000),
                    ((1, "d".to_owned()), 4_000_000),
                    ((1, "oversized".to_owned()), 5_000_000),
                    ((1, "e".to_owned()), 6_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap();
    let source = if windowed {
        source.fixed_windows("window", 10_000_000).unwrap()
    } else {
        source
    };
    let rows = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&rows);
    let sink = source
        .group_into_batches_with_limits_named("batches", 2, short_bytes * 2)
        .unwrap()
        .reify_timestamps_named("timestamps")
        .unwrap()
        .map_named("row", |(timestamp, (key, values))| {
            format!("{key}:{timestamp}:{values:?}")
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "tests/unbounded-byte-batch-sink-v1".into(),
            move || {
                Ok(Box::new(Rows {
                    committed: Arc::clone(&captured),
                    pending: Vec::new(),
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
    let mut expected = vec![
        "1:1000000:[\"a\"]",
        "1:2000000:[\"bbb\"]",
        "1:3000000:[\"c\", \"d\"]",
        "1:5000000:[\"oversized\"]",
    ];
    if windowed {
        expected.push("1:6000000:[\"e\"]");
    }
    assert_eq!(actual, expected);
}

#[test]
fn keyed_batch_timestamp_follows_earliest_value_in_each_batch() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .from_vec(
            "events",
            vec![
                (1_000_000_i64, (1_i32, "a".to_owned())),
                (2_000_000, (1, "b".to_owned())),
                (8_000_000, (1, "c".to_owned())),
            ],
        )
        .unwrap()
        .timestamps_from_pairs_named("event-time")
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let count = source
        .group_into_batches_named("count-batches", 2)
        .unwrap()
        .reify_timestamps_named("count-times")
        .unwrap()
        .collect_seq()
        .unwrap();
    let bytes = source
        .group_into_batches_with_limits_named("byte-batches", 2, 1024)
        .unwrap()
        .reify_timestamps_named("byte-times")
        .unwrap()
        .collect_seq()
        .unwrap();
    let expected = [
        (1_000_000, (1, vec!["a".to_owned(), "b".to_owned()])),
        (8_000_000, (1, vec!["c".to_owned()])),
    ];
    assert_eq!(count, expected);
    assert_eq!(bytes, expected);
}
