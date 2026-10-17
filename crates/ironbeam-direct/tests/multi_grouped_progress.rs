mod processing_time_support;

use ironbeam_core::{
    ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress, Timestamped,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
};
use ironbeam_direct::{DirectRunner, JobConfig};
use processing_time_support::{Capture, TimedScript, Value};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy)]
enum InvalidProgress {
    SourceKey,
    EmptySplit,
    ChangedSplit,
    ReusedCursor,
    BackwardWatermark,
}

fn signal(
    source_key: &str,
    split_id: &str,
    cursor: u8,
    watermark_ns: i64,
    completed: bool,
) -> ProgressSignal {
    ProgressSignal {
        source_key: source_key.into(),
        split_id: split_id.into(),
        checkpoint_position: Some(vec![cursor]),
        event_time_ns: Some(watermark_ns),
        completed,
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Every invalid progress record traverses the same two-source route.
fn multi_source_group_rejects_invalid_progress_before_committing_output() {
    for (case, detail) in [
        (
            InvalidProgress::SourceKey,
            "source progress identity differs",
        ),
        (
            InvalidProgress::EmptySplit,
            "source progress identity differs",
        ),
        (
            InvalidProgress::ChangedSplit,
            "source split changed during a session",
        ),
        (
            InvalidProgress::ReusedCursor,
            "data batch did not advance its cursor",
        ),
        (
            InvalidProgress::BackwardWatermark,
            "source watermark regressed",
        ),
    ] {
        let pipeline = Pipeline::default();
        let first = pipeline
            .unbounded_source_with_factory(
                "first",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "multi-progress-first-v1".into(),
                0,
                move |_cursor| {
                    let second_progress = match case {
                        InvalidProgress::SourceKey => {
                            signal("wrong", "split-0", 2, 2_000_000, false)
                        }
                        InvalidProgress::EmptySplit => signal("first", "", 2, 2_000_000, false),
                        InvalidProgress::ChangedSplit => {
                            signal("first", "split-1", 2, 2_000_000, false)
                        }
                        InvalidProgress::ReusedCursor => {
                            signal("first", "split-0", 1, 2_000_000, false)
                        }
                        InvalidProgress::BackwardWatermark => {
                            signal("first", "split-0", 2, 0, false)
                        }
                    };
                    let polls = VecDeque::from([
                        (
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: vec![Timestamped {
                                    value: (1, 2),
                                    timestamp_ns: 1_000_000,
                                }],
                                progress: signal("first", "split-0", 1, 1_000_000, false),
                            }),
                            0,
                        ),
                        (
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: vec![Timestamped {
                                    value: (1, 3),
                                    timestamp_ns: 2_000_000,
                                }],
                                progress: second_progress,
                            }),
                            0,
                        ),
                    ]);
                    Ok(Box::new(TimedScript { polls, clock_ns: 0 })
                        as TypedUnboundedSourceReader<Value>)
                },
            )
            .unwrap()
            .fixed_windows("first-window", 10_000_000)
            .unwrap();
        let second = pipeline
            .unbounded_source_with_factory(
                "second",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "multi-progress-second-v1".into(),
                0,
                move |_cursor| {
                    let polls = VecDeque::from([(
                        UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: signal("second", "split-0", 1, 0, false),
                        }),
                        0,
                    )]);
                    Ok(Box::new(TimedScript { polls, clock_ns: 0 })
                        as TypedUnboundedSourceReader<Value>)
                },
            )
            .unwrap()
            .fixed_windows("second-window", 10_000_000)
            .unwrap();
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let sink = first
            .co_group_homogeneous_named("group", &[second])
            .unwrap()
            .map_named("row", |row| format!("{row:?}"))
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "multi-progress-sink-v1".into(),
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
        let error = DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
        assert!(
            error.to_string().contains(detail),
            "unexpected error for {detail}: {error}"
        );
        assert!(
            committed.lock().unwrap().is_empty(),
            "invalid progress committed output"
        );
    }
}
