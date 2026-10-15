use ironbeam_core::{
    CombineFn, ElementMetadata, Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode,
    SinkSpec, SourceProgress, TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader,
    UnboundedBatch, UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, StreamJob};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Row = (String, i32);

#[test]
fn processing_time_trigger_fires_repeated_early_panes() {
    for accumulating in [false, true] {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let pipeline = Pipeline::default();
        let source = pipeline
            .test_stream_source(
                "processing-source",
                vec![
                    TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)]),
                    TestStreamEvent::AdvanceProcessingTime(5_000_000),
                    TestStreamEvent::Elements(vec![((1, 3), 2_000_000)]),
                    TestStreamEvent::AdvanceProcessingTime(5_000_000),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                ],
            )
            .unwrap()
            .fixed_windows("processing-window", 10_000_000)
            .unwrap();
        let triggered = if accumulating {
            source.accumulating_processing_time_trigger_named("processing-trigger", 5_000_000)
        } else {
            source.early_processing_time_trigger_named("processing-trigger", 5_000_000)
        }
        .unwrap();
        let sink = triggered
            .group_by_key_named("processing-group")
            .unwrap()
            .reify_panes_named("processing-pane")
            .unwrap()
            .map_named("processing-row", |(pane, (key, values))| {
                format!(
                    "{key}:{values:?}:{:?}:{}:{}:{}:{}",
                    pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
                )
            })
            .unwrap()
            .sink_named_with_config(
                "processing-sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("processing-sink-{accumulating}-v1"),
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
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let rows = committed.lock().unwrap().clone();
        if accumulating {
            assert_eq!(
                rows,
                [
                    "1:[2]:Early:true:false:0:-1",
                    "1:[2, 3]:Early:false:false:1:-1",
                    "1:[2, 3]:OnTime:false:true:2:0",
                ]
            );
        } else {
            assert_eq!(
                rows,
                [
                    "1:[2]:Early:true:false:0:-1",
                    "1:[3]:Early:false:false:1:-1"
                ]
            );
        }
    }
}

#[test]
fn count_and_processing_time_waits_for_both_conditions() {
    for accumulating in [false, true] {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let pipeline = Pipeline::default();
        let source = pipeline
            .test_stream_source(
                "source",
                vec![
                    TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)]),
                    TestStreamEvent::AdvanceProcessingTime(5_000_000),
                    TestStreamEvent::Elements(vec![((1, 3), 2_000_000)]),
                    TestStreamEvent::Elements(vec![((1, 4), 3_000_000)]),
                    TestStreamEvent::AdvanceProcessingTime(5_000_000),
                    TestStreamEvent::Elements(vec![((1, 5), 4_000_000)]),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                ],
            )
            .unwrap()
            .fixed_windows("window", 10_000_000)
            .unwrap();
        let triggered = if accumulating {
            source.accumulating_count_and_processing_time_trigger_named("trigger", 2, 5_000_000)
        } else {
            source.count_and_processing_time_trigger_named("trigger", 2, 5_000_000)
        }
        .unwrap();
        let sink = triggered
            .group_by_key_named("group")
            .unwrap()
            .reify_panes_named("pane")
            .unwrap()
            .map_named("row", |(pane, (key, values))| {
                format!(
                    "{key}:{values:?}:{:?}:{}:{}:{}:{}",
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
                format!("count-and-time-{accumulating}-v1"),
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
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let rows = committed.lock().unwrap().clone();
        if accumulating {
            assert_eq!(
                rows,
                [
                    "1:[2, 3]:Early:true:false:0:-1",
                    "1:[2, 3, 4, 5]:Early:false:false:1:-1",
                    "1:[2, 3, 4, 5]:OnTime:false:true:2:0",
                ]
            );
        } else {
            assert_eq!(
                rows,
                [
                    "1:[2, 3]:Early:true:false:0:-1",
                    "1:[4, 5]:Early:false:false:1:-1",
                ]
            );
        }
    }
}

#[test]
fn processing_time_trigger_rejects_deadline_overflow() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .test_stream_source(
            "clock-source",
            vec![
                TestStreamEvent::AdvanceProcessingTime(i64::MAX - 1),
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)]),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .early_processing_time_trigger_named("timer", 5_000_000)
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
            "overflow-sink-v1".into(),
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
    assert_eq!(error.kind, ErrorKind::Resource);
}

#[test]
fn heterogeneous_cogroup_retains_late_pane_until_expiry() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let main = pipeline
        .test_stream_source(
            "late-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("late-main-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("late-main-policy", 5_000_000)
        .unwrap();
    let side = pipeline
        .test_stream_source(
            "late-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, "right".to_owned()), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("late-side-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("late-side-policy", 5_000_000)
        .unwrap();
    let sink = ironbeam_core::cogroup_by_key_2_named("late-group", &main, &side)
        .unwrap()
        .reify_panes_named("late-pane")
        .unwrap()
        .map_named("late-row", |(pane, (key, buckets))| {
            format!(
                "{key}:{buckets:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
        .unwrap()
        .sink_named_with_config(
            "late-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "late-heterogeneous-group-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "1:([2], [\"right\"]):OnTime:true:false:0:0",
            "1:([3], []):Late:false:false:1:1",
        ]
    );
}

#[test]
fn heterogeneous_cogroup_fires_late_count_after_two_new_values() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let main = pipeline
        .test_stream_source(
            "count-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
                TestStreamEvent::Elements(vec![((1, 5), 5_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("count-main-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("count-main-policy", 5_000_000)
        .unwrap()
        .late_count_trigger_named("count-main-late", 2)
        .unwrap();
    let side = pipeline
        .test_stream_source(
            "count-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, "right".to_owned()), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("count-side-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("count-side-policy", 5_000_000)
        .unwrap()
        .late_count_trigger_named("count-side-late", 2)
        .unwrap();
    let sink = ironbeam_core::cogroup_by_key_2_named("count-group", &main, &side)
        .unwrap()
        .reify_panes_named("count-pane")
        .unwrap()
        .map_named("count-row", |(pane, (key, buckets))| {
            format!(
                "{key}:{buckets:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
        .unwrap()
        .sink_named_with_config(
            "count-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "late-heterogeneous-count-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "1:([2], [\"right\"]):OnTime:true:false:0:0",
            "1:([3, 5], []):Late:false:false:1:1",
        ]
    );
}

#[test]
fn typed_full_join_retains_late_pane_until_expiry() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let main = pipeline
        .test_stream_source(
            "positive-late-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::Elements(vec![((1, 5), 5_000_000)]),
            ],
        )
        .unwrap()
        .fixed_windows("positive-late-main-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("positive-late-main-policy", 5_000_000)
        .unwrap();
    let side = pipeline
        .test_stream_source(
            "positive-late-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("positive-late-side-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("positive-late-side-policy", 5_000_000)
        .unwrap();
    let sink = main
        .join_full_named("positive-late-join", &side)
        .unwrap()
        .reify_panes_named("positive-late-pane")
        .unwrap()
        .map_named("positive-late-row", |(pane, (key, pair))| {
            format!(
                "{key}:{pair:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
        .unwrap()
        .sink_named_with_config(
            "positive-late-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "positive-late-direct-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "1:(Some(2), Some(4)):OnTime:true:false:0:0",
            "1:(Some(3), None):Late:false:false:1:1",
        ]
    );
}

#[test]
fn unbounded_global_group_into_batches_fires_full_and_final_partial_batches() {
    assert_eq!(
        run_unbounded_keyed_batches(false, false),
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[4]:OnTime:false:true:1:0",
        ]
    );
}

#[test]
fn keyed_batch_overrides_an_upstream_accumulating_count_trigger() {
    assert_eq!(
        run_unbounded_keyed_batches(false, true),
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[4]:OnTime:false:true:1:0",
        ]
    );
}

#[test]
fn keyed_batches_flush_late_values_at_their_own_count() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "values",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 2_i32), 1_000_000),
                    (("a".to_owned(), 3), 2_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 4), 3_000_000),
                    (("a".to_owned(), 5), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .accumulating_early_and_late_count_trigger_named("upstream", 1, 1)
        .unwrap()
        .group_into_batches_named("batches", 2)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
            format!(
                "{key}:{values:?}:{:?}:{}:{}:{}:{}",
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
            "late-keyed-batches-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[4, 5]:Late:false:false:1:0",
        ]
    );
}

#[test]
fn keyed_batches_flush_on_either_time_or_count() {
    let rows = run_count_or_time_keyed_batches(vec![
        TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 1_000_000)]),
        TestStreamEvent::AdvanceProcessingTime(5_000_000),
        TestStreamEvent::Elements(vec![
            (('a'.to_string(), 3), 2_000_000),
            (('a'.to_string(), 4), 3_000_000),
        ]),
        TestStreamEvent::Elements(vec![(('a'.to_string(), 5), 4_000_000)]),
        TestStreamEvent::AdvanceWatermark(10_000_000),
    ]);
    assert_eq!(
        rows,
        [
            "a:[2]:Early:true:false:0:-1",
            "a:[3, 4]:Early:false:false:1:-1",
            "a:[5]:OnTime:false:true:2:0",
        ]
    );
}

#[test]
fn keyed_batch_count_firing_cancels_its_old_timer() {
    let rows = run_count_or_time_keyed_batches(vec![
        TestStreamEvent::Elements(vec![
            (('a'.to_string(), 2_i32), 1_000_000),
            (('a'.to_string(), 3), 2_000_000),
        ]),
        TestStreamEvent::AdvanceProcessingTime(5_000_000),
        TestStreamEvent::Elements(vec![(('a'.to_string(), 4), 3_000_000)]),
        TestStreamEvent::AdvanceProcessingTime(5_000_000),
        TestStreamEvent::AdvanceWatermark(10_000_000),
    ]);
    assert_eq!(
        rows,
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[4]:Early:false:false:1:-1",
        ]
    );
}

#[test]
fn accumulating_count_or_time_retains_values_across_both_early_firings() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .test_stream_source(
            "values",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 2_i32), 1_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
                TestStreamEvent::Elements(vec![
                    (('a'.to_string(), 3), 2_000_000),
                    (('a'.to_string(), 4), 3_000_000),
                ]),
                TestStreamEvent::Elements(vec![(('a'.to_string(), 5), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .accumulating_count_or_processing_time_trigger_named("trigger", 2, 5_000_000)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
            format!(
                "{key}:{values:?}:{:?}:{}:{}:{}:{}",
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
            "accumulating-count-or-time-v1".to_owned(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "a:[2]:Early:true:false:0:-1",
            "a:[2, 3, 4]:Early:false:false:1:-1",
            "a:[2, 3, 4, 5]:OnTime:false:true:2:0",
        ]
    );
}

fn run_count_or_time_keyed_batches(events: Vec<TestStreamEvent<Row>>) -> Vec<String> {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let source = pipeline.test_stream_source("values", events).unwrap();
    let sink = source
        .group_into_batches_with_max_buffering_duration_named("batches", 2, 5_000_000)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
            format!(
                "{key}:{values:?}:{:?}:{}:{}:{}:{}",
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
            "count-or-time-batches-v1".to_owned(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    committed.lock().unwrap().clone()
}

#[test]
fn unbounded_fixed_window_group_into_batches_flushes_partial_batch_at_watermark() {
    assert_eq!(
        run_unbounded_keyed_batches(true, false),
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[4]:OnTime:false:true:1:0",
        ]
    );
}

fn run_unbounded_keyed_batches(windowed: bool, upstream_trigger: bool) -> Vec<String> {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .test_stream_source(
            "values",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 2_i32), 2_000_000),
                    (("a".to_owned(), 3), 3_000_000),
                ]),
                TestStreamEvent::Elements(vec![(("a".to_owned(), 4), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap();
    let source = if windowed {
        source.fixed_windows("window", 10_000_000).unwrap()
    } else {
        source
    };
    let source = if upstream_trigger {
        source
            .accumulating_count_trigger_named("upstream-count", 1)
            .unwrap()
    } else {
        source
    };
    let sink = source
        .group_into_batches_named("batches", 2)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
            format!(
                "{key}:{values:?}:{:?}:{}:{}:{}:{}",
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
            "group-batches-global-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    committed.lock().unwrap().clone()
}

#[test]
fn keyed_batch_restores_a_partial_group_and_source_cursor() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "values",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "keyed-batch-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let polls = match position.as_deref() {
                        None => vec![batch(1, 2_000_000, false, Some((2, 1_000_000)))],
                        Some([1]) => vec![
                            batch(2, 4_000_000, false, Some((3, 3_000_000))),
                            batch(3, 10_000_000, true, Some((4, 4_000_000))),
                        ],
                        _ => return Err(Error::incompatible("unexpected keyed batch cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<Row>)
                }
            },
        )
        .unwrap();
    let sink = source
        .group_into_batches_named("batches", 2)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, values))| {
            format!(
                "{key}:{values:?}:{:?}:{}:{}:{}:{}",
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
            "keyed-batch-sink-v1".into(),
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
            binary_build_id: "keyed-batch-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    wait_for_checkpoint_position(&job, &[1]);
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "a:[2, 3]:Early:true:false:0:-1",
            "a:[4]:OnTime:false:true:1:0"
        ]
    );
}

fn wait_for_checkpoint_position(job: &StreamJob, expected: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while job
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        .as_deref()
        != Some(expected)
    {
        assert!(Instant::now() < deadline, "batch was not checkpointed");
        std::thread::sleep(Duration::from_millis(1));
    }
}

struct Sum;

impl CombineFn<i32, i32, i32> for Sum {
    fn create(&self) -> i32 {
        0
    }
    fn add_input(&self, total: &mut i32, value: i32) {
        *total += value;
    }
    fn merge(&self, total: &mut i32, other: i32) {
        *total += other;
    }
    fn finish(&self, total: i32) -> i32 {
        total
    }
}

#[test]
fn unbounded_global_keyed_combine_uses_count_panes() {
    for accumulating in [false, true] {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let pipeline = Pipeline::default();
        let source = pipeline
            .test_stream_source(
                "values",
                vec![
                    TestStreamEvent::Elements(vec![
                        (("a".to_owned(), 2_i32), 2_000_000),
                        (("a".to_owned(), 3), 3_000_000),
                    ]),
                    TestStreamEvent::Elements(vec![(("a".to_owned(), 4), 4_000_000)]),
                ],
            )
            .unwrap();
        let source = if accumulating {
            source
                .accumulating_count_trigger_named("trigger", 2)
                .unwrap()
        } else {
            source.early_count_trigger_named("trigger", 2).unwrap()
        };
        let sink = source
            .combine_values_lifted_named("combine", Sum)
            .unwrap()
            .reify_panes_named("pane")
            .unwrap()
            .map_named("row", |(pane, (key, sum))| {
                format!(
                    "{key}:{sum}:{:?}:{}:{}:{}:{}",
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
                "global-combine-v1".into(),
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
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(
            committed.lock().unwrap().as_slice(),
            [
                "a:5:Early:true:false:0:-1",
                if accumulating {
                    "a:9:OnTime:false:true:1:0"
                } else {
                    "a:4:OnTime:false:true:1:0"
                },
            ]
        );
    }
}

#[test]
fn unbounded_global_count_trigger_emits_early_and_final_panes() {
    for accumulating in [false, true] {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let pipeline = Pipeline::default();
        let source = pipeline
            .test_stream_source(
                "values",
                vec![
                    TestStreamEvent::Elements(vec![
                        (("a".to_owned(), 2_i32), 2_000_000),
                        (("a".to_owned(), 3), 3_000_000),
                    ]),
                    TestStreamEvent::Elements(vec![(("a".to_owned(), 4), 4_000_000)]),
                ],
            )
            .unwrap();
        let source = if accumulating {
            source
                .accumulating_count_trigger_named("trigger", 2)
                .unwrap()
        } else {
            source.early_count_trigger_named("trigger", 2).unwrap()
        };
        let sink = source
            .group_by_key_named("group")
            .unwrap()
            .reify_panes_named("pane")
            .unwrap()
            .map_named("row", |(pane, (key, values))| {
                format!(
                    "{key}:{values:?}:{:?}:{}:{}:{}:{}",
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
                "global-count-v1".into(),
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
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let rows = committed.lock().unwrap().clone();
        assert_eq!(rows[0], "a:[2, 3]:Early:true:false:0:-1");
        assert_eq!(
            rows[1],
            if accumulating {
                "a:[2, 3, 4]:OnTime:false:true:1:0"
            } else {
                "a:[4]:OnTime:false:true:1:0"
            }
        );
    }
}

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
    run_count_pane_recovery(false, false, false);
}

#[test]
fn accumulating_count_values_survive_checkpoint_recovery() {
    run_count_pane_recovery(true, false, false);
}

#[test]
fn global_count_pane_indices_survive_checkpoint_recovery() {
    run_count_pane_recovery(false, false, true);
}

#[test]
fn accumulating_global_count_values_survive_checkpoint_recovery() {
    run_count_pane_recovery(true, false, true);
}

#[test]
fn latest_in_pane_discarding_times_survive_checkpoint_recovery() {
    run_count_pane_recovery(false, true, false);
}

#[test]
fn latest_in_pane_accumulating_times_survive_checkpoint_recovery() {
    run_count_pane_recovery(true, true, false);
}

#[test]
#[allow(clippy::too_many_lines)] // Source restart, pane assertions, and expiry form one scenario.
fn late_pane_and_expiry_survive_checkpoint_recovery() {
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
            "late-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let polls = match position.as_deref() {
                        None => vec![batch(1, 10_000_000, false, Some((2, 2_000_000)))],
                        Some([1]) => vec![
                            batch(2, 11_000_000, false, Some((3, 3_000_000))),
                            batch(3, 12_000_000, false, Some((5, 5_000_000))),
                            batch(4, 15_000_000, false, None),
                            batch(5, 20_000_000, false, Some((4, 4_000_000))),
                            batch(6, 20_000_000, true, None),
                        ],
                        _ => return Err(Error::incompatible("unexpected late resume cursor")),
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
        .group_by_key_named("groups")
        .unwrap()
        .reify_timestamps_named("timestamp")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (timestamp, (key, values)))| {
            format!(
                "{key}:{values:?}:{timestamp}:{:?}:{}:{}:{}:{}",
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
            "late-sink-v1".into(),
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
            binary_build_id: "late-test-build".into(),
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
            "on-time pane was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["a:[2]:2000000:OnTime:true:false:0:0"]
    );
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[3]:3000000:Late:false:false:1:1",
            "a:[5]:5000000:Late:false:false:2:2",
        ]
    );
}

#[test]
fn late_count_buffers_across_checkpoint_and_flushes_at_expiry() {
    run_late_count_recovery(LateCountMode::Discarding);
}

#[test]
fn accumulating_late_count_retains_on_time_values_across_recovery() {
    run_late_count_recovery(LateCountMode::Accumulating);
}

#[test]
fn combined_early_and_late_counts_restore_non_speculative_index() {
    run_late_count_recovery(LateCountMode::Combined);
}

#[test]
fn accumulating_combined_counts_restore_values_and_pane_indices() {
    run_late_count_recovery(LateCountMode::CombinedAccumulating);
}

#[test]
fn combined_count_matches_portable_on_time_schedule() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let events = vec![
        TestStreamEvent::Elements(vec![
            (("a".to_owned(), 2_i32), 2_000_000),
            (("a".to_owned(), 3), 3_000_000),
        ]),
        TestStreamEvent::AdvanceWatermark(4_000_000),
        TestStreamEvent::Elements(vec![(("a".to_owned(), 4), 4_000_000)]),
        TestStreamEvent::AdvanceWatermark(10_000_000),
        TestStreamEvent::Elements(vec![(("a".to_owned(), 5), 5_000_000)]),
        TestStreamEvent::AdvanceWatermark(11_000_000),
        TestStreamEvent::Elements(vec![(("a".to_owned(), 7), 7_000_000)]),
        TestStreamEvent::AdvanceWatermark(12_000_000),
        TestStreamEvent::Elements(vec![(("a".to_owned(), 11), 8_000_000)]),
        TestStreamEvent::AdvanceWatermark(15_000_000),
    ];
    let sink = pipeline
        .test_stream_source("values", events)
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .early_and_late_count_trigger_named("combined", 2, 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_timestamps_named("timestamp")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (timestamp, (key, values)))| {
            format!(
                "{key}:{values:?}:{timestamp}:{:?}:{}:{}:{}:{}",
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
            "combined-count-sink-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "a:[2, 3]:2000000:Early:true:false:0:-1",
            "a:[4]:4000000:OnTime:false:false:1:0",
            "a:[5, 7]:5000000:Late:false:false:2:1",
            "a:[11]:8000000:Late:false:true:3:2",
        ]
    );
}

#[derive(Clone, Copy)]
enum LateCountMode {
    Discarding,
    Accumulating,
    Combined,
    CombinedAccumulating,
}

#[allow(clippy::too_many_lines)] // Restart across a partial late pane and expiry.
fn run_late_count_recovery(mode: LateCountMode) {
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
            "late-count-source-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |position| {
                    opens.lock().unwrap().push(position.clone());
                    let polls = match position.as_deref() {
                        None => vec![
                            batch(1, 10_000_000, false, Some((2, 2_000_000))),
                            batch(2, 11_000_000, false, Some((3, 3_000_000))),
                        ],
                        Some([2]) => vec![
                            batch(3, 12_000_000, false, Some((5, 5_000_000))),
                            batch(4, 13_000_000, false, Some((7, 7_000_000))),
                            batch(5, 15_000_000, false, None),
                            batch(6, 20_000_000, true, None),
                        ],
                        _ => {
                            return Err(Error::incompatible("unexpected late count resume cursor"));
                        }
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<Row>)
                }
            },
        )
        .unwrap();
    let windowed = values
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap();
    let triggered = match mode {
        LateCountMode::Discarding => windowed.late_count_trigger_named("late-count", 2).unwrap(),
        LateCountMode::Accumulating => windowed
            .accumulating_late_count_trigger_named("late-count", 2)
            .unwrap(),
        LateCountMode::Combined => windowed
            .early_and_late_count_trigger_named("late-count", 1, 2)
            .unwrap(),
        LateCountMode::CombinedAccumulating => windowed
            .accumulating_early_and_late_count_trigger_named("late-count", 1, 2)
            .unwrap(),
    };
    let sink = triggered
        .group_by_key_named("groups")
        .unwrap()
        .reify_timestamps_named("timestamp")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (timestamp, (key, values)))| {
            format!(
                "{key}:{values:?}:{timestamp}:{:?}:{}:{}:{}:{}",
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
            "late-count-sink-v1".into(),
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
            binary_build_id: "late-count-test-build".into(),
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
        assert!(
            Instant::now() < deadline,
            "partial late pane was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let first_timing = if matches!(
        mode,
        LateCountMode::Combined | LateCountMode::CombinedAccumulating
    ) {
        "Early"
    } else {
        "OnTime"
    };
    let first_non_speculative = if matches!(
        mode,
        LateCountMode::Combined | LateCountMode::CombinedAccumulating
    ) {
        -1
    } else {
        0
    };
    let first = format!("a:[2]:2000000:{first_timing}:true:false:0:{first_non_speculative}");
    if matches!(mode, LateCountMode::CombinedAccumulating) {
        assert_eq!(
            committed.lock().unwrap().as_slice(),
            [first.as_str(), "a:[2]:2000000:OnTime:false:false:1:0"]
        );
    } else {
        assert_eq!(committed.lock().unwrap().as_slice(), [first]);
    }
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![2])]);
    let expected: &[&str] = match mode {
        LateCountMode::Accumulating => &[
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[2, 3, 5]:2000000:Late:false:false:1:1",
            "a:[2, 3, 5, 7]:2000000:Late:false:true:2:2",
        ],
        LateCountMode::Discarding => &[
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[3, 5]:3000000:Late:false:false:1:1",
            "a:[7]:7000000:Late:false:true:2:2",
        ],
        LateCountMode::Combined => &[
            "a:[2]:2000000:Early:true:false:0:-1",
            "a:[3, 5]:3000000:Late:false:false:1:0",
            "a:[7]:7000000:Late:false:true:2:1",
        ],
        LateCountMode::CombinedAccumulating => &[
            "a:[2]:2000000:Early:true:false:0:-1",
            "a:[2]:2000000:OnTime:false:false:1:0",
            "a:[2, 3, 5]:2000000:Late:false:false:2:1",
            "a:[2, 3, 5, 7]:2000000:Late:false:true:3:2",
        ],
    };
    assert_eq!(committed.lock().unwrap().as_slice(), expected);
}

#[test]
#[allow(clippy::too_many_lines)] // Two overlapping windows need their own on-time and late panes.
fn sliding_windows_keep_independent_late_pane_indices() {
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let values = pipeline
        .unbounded_source_with_factory(
            "values",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "sliding-late-source-v1".into(),
            0,
            |_| {
                Ok(Box::new(Script {
                    polls: vec![
                        batch(1, 10_000_000, false, Some((2, 6_000_000))),
                        batch(2, 12_000_000, false, Some((3, 7_000_000))),
                        batch(3, 15_000_000, false, None),
                        batch(4, 17_000_000, false, Some((4, 8_000_000))),
                        batch(5, 20_000_000, true, None),
                    ]
                    .into(),
                }) as TypedUnboundedSourceReader<Row>)
            },
        )
        .unwrap();
    let sink = values
        .sliding_windows("windows", 10_000_000, 5_000_000, 0)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .reify_timestamps_named("timestamp")
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .with_window_named("interval")
        .unwrap()
        .map_named("row", |(interval, (pane, (timestamp, (key, values))))| {
            format!(
                "{}..{}:{key}:{values:?}:{timestamp}:{:?}:{}:{}:{}:{}",
                interval.start_ns,
                interval.end_ns,
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
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
            "sliding-late-sink-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        [
            "0..10000000:a:[2]:6000000:OnTime:true:false:0:0",
            "0..10000000:a:[3]:7000000:Late:false:false:1:1",
            "5000000..15000000:a:[2, 3]:6000000:OnTime:true:false:0:0",
            "5000000..15000000:a:[4]:8000000:Late:false:false:1:1",
        ]
    );
}

#[allow(clippy::too_many_lines)] // The source, sink, restart, and pane assertions form one scenario.
fn run_count_pane_recovery(accumulating: bool, latest_time: bool, global: bool) {
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
    let values = if global {
        values
    } else {
        values.fixed_windows("windows", 10_000_000).unwrap()
    };
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
