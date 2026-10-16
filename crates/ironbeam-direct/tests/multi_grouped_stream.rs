#![allow(clippy::too_many_lines)] // End-to-end checkpoint fixtures keep setup and assertions together.

use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::{HashSet, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

struct Script {
    polls: VecDeque<UnboundedPoll<Timestamped<(i32, i32)>>>,
}

#[test]
fn bounded_multi_source_group_finishes_during_materialization() {
    let pipeline = Pipeline::default();
    let make = |key: &str, value| {
        pipeline
            .from_vec(key, vec![(1_000_000_i64, (1_i32, value))])
            .unwrap()
            .timestamps_from_pairs_named(&format!("{key}-time"))
            .unwrap()
            .fixed_windows(&format!("{key}-window"), 10_000_000)
            .unwrap()
            .early_count_trigger_named(&format!("{key}-trigger"), 1)
            .unwrap()
    };
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let sink = make("first", 2)
        .co_group_homogeneous_named("group", &[make("second", 4)])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, values))| {
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
            "bounded-multi-early-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        staged: Vec::new(),
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
        ["1:[[2], [4]]:Early:0"]
    );
}

#[test]
fn multi_source_early_count_emits_panes_from_both_inputs() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let make = |name: &str, value: i32| {
        pipeline
            .test_stream_source(
                name,
                vec![
                    TestStreamEvent::Elements(vec![((1_i32, value), 1_000_000)]),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                ],
            )
            .unwrap()
            .fixed_windows(&format!("{name}-window"), 10_000_000)
            .unwrap()
            .early_count_trigger_named(&format!("{name}-trigger"), 1)
            .unwrap()
    };
    let sink = make("first", 2)
        .co_group_homogeneous_named("group", &[make("second", 4)])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, buckets))| {
            format!("{key}:{buckets:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "multi-early-count-test-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        staged: Vec::new(),
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
        ["1:[[2], []]:Early:0", "1:[[], [4]]:Early:1",]
    );
}

#[test]
fn bounded_branch_fires_early_before_unbounded_source_polls_and_recovers() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let hold = Arc::new(AtomicBool::new(true));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let bounded = pipeline
        .from_vec("bounded", vec![(1_000_000_i64, (1_i32, 2_i32))])
        .unwrap()
        .timestamps_from_pairs_named("bounded-time")
        .unwrap()
        .fixed_windows("bounded-window", 10_000_000)
        .unwrap()
        .accumulating_count_trigger_named("bounded-trigger", 1)
        .unwrap();
    let stream = pipeline
        .unbounded_source_with_factory(
            "stream",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "pending-early-source-v1".into(),
            0,
            {
                let hold = Arc::clone(&hold);
                move |_| {
                    let polls = if hold.load(Ordering::SeqCst) {
                        VecDeque::new()
                    } else {
                        VecDeque::from([UnboundedPoll::Ready(UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                source_key: "stream".into(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![1]),
                                event_time_ns: Some(10_000_000),
                                completed: true,
                            },
                        })])
                    };
                    Ok(Box::new(Script { polls }) as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .fixed_windows("stream-window", 10_000_000)
        .unwrap()
        .accumulating_count_trigger_named("stream-trigger", 1)
        .unwrap();
    let sink = bounded
        .co_group_homogeneous_named("group", &[stream])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, buckets))| {
            format!("{key}:{buckets:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "mixed-early-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        staged: Vec::new(),
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
            binary_build_id: "mixed-early-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while committed.lock().unwrap().is_empty() {
        assert!(Instant::now() < deadline, "bounded early pane did not fire");
        std::thread::sleep(Duration::from_millis(1));
    }
    while job.progress().and_then(|signal| signal.checkpoint_position)
        != Some(1_u64.to_le_bytes().to_vec())
    {
        assert!(
            Instant::now() < deadline,
            "bounded early pane was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["1:[[2], []]:Early:0"]
    );
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.lock().unwrap().as_slice(),
        ["1:[[2], []]:Early:0", "1:[[2], []]:OnTime:1"]
    );
}

#[test]
fn multi_source_group_retains_late_rows_until_expiry() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let first = pipeline
        .test_stream_source(
            "first",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 2_000_000)]),
            ],
        )
        .unwrap()
        .fixed_windows("first-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("first-lateness", 5_000_000)
        .unwrap();
    let second = pipeline
        .test_stream_source(
            "second",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("second-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("second-lateness", 5_000_000)
        .unwrap();
    let sink = first
        .co_group_homogeneous_named("group", &[second])
        .unwrap()
        .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "multi-source-lateness-test-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        staged: Vec::new(),
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
        ["1:[[2], [4]]", "1:[[3], []]"]
    );
}

#[test]
fn multi_source_late_count_waits_for_threshold_and_flushes_at_expiry() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let first = pipeline
        .test_stream_source(
            "first",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(11_000_000),
                TestStreamEvent::Elements(vec![((1, 5), 5_000_000)]),
                TestStreamEvent::AdvanceWatermark(12_000_000),
                TestStreamEvent::Elements(vec![((1, 7), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("first-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("first-lateness", 5_000_000)
        .unwrap()
        .late_count_trigger_named("first-trigger", 2)
        .unwrap();
    let second = pipeline
        .test_stream_source(
            "second",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("second-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("second-lateness", 5_000_000)
        .unwrap()
        .late_count_trigger_named("second-trigger", 2)
        .unwrap();
    let sink = first
        .co_group_homogeneous_named("group", &[second])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, buckets))| {
            format!("{key}:{buckets:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "multi-late-count-test-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        staged: Vec::new(),
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
            "1:[[2], [4]]:OnTime:0",
            "1:[[3, 5], []]:Late:1",
            "1:[[7], []]:Late:2",
        ]
    );
}

#[test]
fn multi_source_late_group_restores_cursors_and_panes() {
    assert_multi_source_late_group_restores_cursors_and_panes(LateMode::Default);
}

#[test]
fn multi_source_late_count_group_restores_cursors_and_panes() {
    assert_multi_source_late_group_restores_cursors_and_panes(LateMode::DiscardingCount);
}

#[test]
fn multi_source_accumulating_late_count_restores_values_cursors_and_panes() {
    assert_multi_source_late_group_restores_cursors_and_panes(LateMode::AccumulatingCount);
}

#[test]
fn multi_source_early_count_restores_cursors_and_pane_indices() {
    assert_multi_source_late_group_restores_cursors_and_panes(LateMode::EarlyCount);
}

#[test]
fn multi_source_accumulating_early_count_restores_values_and_panes() {
    assert_multi_source_late_group_restores_cursors_and_panes(LateMode::EarlyAccumulating);
}

#[test]
fn global_multi_source_accumulating_count_restores_values_and_panes() {
    assert_multi_source_late_group_restores_cursors_and_panes(LateMode::GlobalEarlyAccumulating);
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LateMode {
    Default,
    DiscardingCount,
    AccumulatingCount,
    EarlyCount,
    EarlyAccumulating,
    GlobalEarlyAccumulating,
}

fn assert_multi_source_late_group_restores_cursors_and_panes(mode: LateMode) {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let hold = Arc::new(AtomicBool::new(true));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let make_source = |index: i32| {
        let source_key = format!("source-{index}");
        let factory_key = source_key.clone();
        let hold = Arc::clone(&hold);
        let opens = Arc::clone(&opens);
        let windowed = pipeline
            .unbounded_source_with_factory(
                &source_key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("late-source-{index}-v1"),
                0,
                move |cursor| {
                    opens.lock().unwrap().push((index, cursor.clone()));
                    let (rows, position, completed) = match cursor.as_deref() {
                        None => (vec![if index == 0 { 2 } else { 4 }], 1, false),
                        Some([1]) if !hold.load(Ordering::SeqCst) => (
                            if index == 0 {
                                if matches!(
                                    mode,
                                    LateMode::Default
                                        | LateMode::EarlyCount
                                        | LateMode::EarlyAccumulating
                                        | LateMode::GlobalEarlyAccumulating
                                ) {
                                    vec![3]
                                } else {
                                    vec![3, 5]
                                }
                            } else {
                                Vec::new()
                            },
                            2,
                            true,
                        ),
                        Some([1]) => {
                            return Ok(Box::new(Script {
                                polls: VecDeque::new(),
                            })
                                as TypedUnboundedSourceReader<(i32, i32)>);
                        }
                        _ => return Err(Error::incompatible("unexpected late source cursor")),
                    };
                    let elements = rows
                        .into_iter()
                        .map(|value| Timestamped {
                            value: (1_i32, value),
                            timestamp_ns: i64::from(value) * 1_000_000,
                        })
                        .collect();
                    Ok(Box::new(Script {
                        polls: vec![UnboundedPoll::Ready(UnboundedBatch {
                            elements,
                            progress: ProgressSignal {
                                source_key: factory_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![position]),
                                event_time_ns: Some(
                                    if position == 1
                                        && matches!(
                                            mode,
                                            LateMode::EarlyCount
                                                | LateMode::EarlyAccumulating
                                                | LateMode::GlobalEarlyAccumulating
                                        )
                                    {
                                        0
                                    } else {
                                        10_000_000
                                    },
                                ),
                                completed,
                            },
                        })]
                        .into(),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                },
            )
            .unwrap();
        let windowed = if mode == LateMode::GlobalEarlyAccumulating {
            windowed
        } else {
            windowed
                .fixed_windows(&format!("window-{index}"), 10_000_000)
                .unwrap()
        };
        match mode {
            LateMode::Default => windowed
                .allowed_lateness_named(&format!("lateness-{index}"), 5_000_000)
                .unwrap(),
            LateMode::DiscardingCount => windowed
                .allowed_lateness_named(&format!("lateness-{index}"), 5_000_000)
                .unwrap()
                .late_count_trigger_named(&format!("trigger-{index}"), 2)
                .unwrap(),
            LateMode::AccumulatingCount => windowed
                .allowed_lateness_named(&format!("lateness-{index}"), 5_000_000)
                .unwrap()
                .accumulating_late_count_trigger_named(&format!("trigger-{index}"), 2)
                .unwrap(),
            LateMode::EarlyCount => windowed
                .early_count_trigger_named(&format!("trigger-{index}"), 1)
                .unwrap(),
            LateMode::EarlyAccumulating | LateMode::GlobalEarlyAccumulating => windowed
                .accumulating_count_trigger_named(&format!("trigger-{index}"), 1)
                .unwrap(),
        }
    };
    let first = make_source(0);
    let second = make_source(1);
    let sink = first
        .co_group_homogeneous_named("group", &[second])
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("format", |(pane, (key, buckets))| {
            format!("{key}:{buckets:?}:{:?}:{}", pane.timing, pane.index)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "multi-late-recovery-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        staged: Vec::new(),
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
            binary_build_id: "multi-late-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.progress().and_then(|signal| signal.checkpoint_position)
        != Some(2_u64.to_le_bytes().to_vec())
    {
        assert!(
            Instant::now() < deadline,
            "on-time pane was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let initial = match mode {
        LateMode::EarlyCount => vec!["1:[[2], []]:Early:0", "1:[[], [4]]:Early:1"],
        LateMode::EarlyAccumulating | LateMode::GlobalEarlyAccumulating => {
            vec!["1:[[2], []]:Early:0", "1:[[2], [4]]:Early:1"]
        }
        _ => vec!["1:[[2], [4]]:OnTime:0"],
    };
    assert_eq!(committed.lock().unwrap().as_slice(), initial);
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    let late_row = match mode {
        LateMode::Default => "1:[[3], []]:Late:1",
        LateMode::DiscardingCount => "1:[[3, 5], []]:Late:1",
        LateMode::AccumulatingCount => "1:[[2, 3, 5], [4]]:Late:1",
        LateMode::EarlyCount => "1:[[3], []]:Early:2",
        LateMode::EarlyAccumulating | LateMode::GlobalEarlyAccumulating => {
            "1:[[2, 3], [4]]:Early:2"
        }
    };
    let mut expected = initial;
    expected.push(late_row);
    if matches!(
        mode,
        LateMode::EarlyAccumulating | LateMode::GlobalEarlyAccumulating
    ) {
        expected.push("1:[[2, 3], [4]]:OnTime:3");
    }
    assert_eq!(committed.lock().unwrap().as_slice(), expected);
    assert_eq!(
        opens.lock().unwrap().as_slice(),
        [(0, None), (1, None), (0, Some(vec![1])), (1, Some(vec![1])),]
    );
}

#[test]
fn one_unbounded_input_uses_one_group_bucket() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let source = pipeline
        .test_stream_source(
            "single",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 5_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = source
        .co_group_homogeneous_named("group", &[])
        .unwrap()
        .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "single-group-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
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
    assert_eq!(*committed.lock().unwrap(), ["1:[[5]]".to_owned()]);
}

#[test]
fn shared_unbounded_source_runs_common_transform_once_before_three_branches() {
    let pipeline = Pipeline::default();
    let visits = Arc::new(AtomicUsize::new(0));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let common_visits = Arc::clone(&visits);
    let source = pipeline
        .test_stream_source(
            "shared",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, (0_i32, 10_i32)), 1_000_000),
                    ((1, (1, 20)), 2_000_000),
                    ((1, (2, 30)), 3_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap()
        .map_named("common", move |value| {
            common_visits.fetch_add(1, Ordering::SeqCst);
            value
        })
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let inputs = (0..3)
        .map(|index| {
            source
                .filter_named(&format!("branch-{index}"), move |(_, (branch, _))| {
                    *branch == index
                })
                .unwrap()
                .map_named(&format!("value-{index}"), |(key, (_, value))| (key, value))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = inputs[0]
        .co_group_homogeneous_named("group", &inputs[1..])
        .unwrap()
        .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-group-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
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
    assert_eq!(
        *committed.lock().unwrap(),
        ["1:[[10], [20], [30]]".to_owned()]
    );
    assert_eq!(visits.load(Ordering::SeqCst), 3);
}

#[test]
fn shared_unbounded_source_restores_one_cursor_and_open_group() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let hold = Arc::new(AtomicBool::new(true));
    let visits = Arc::new(AtomicUsize::new(0));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let factory_hold = Arc::clone(&hold);
    let factory_opens = Arc::clone(&opens);
    let common_visits = Arc::clone(&visits);
    let source = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "shared-recovery-v1".into(),
            0,
            move |cursor| {
                factory_opens.lock().unwrap().push(cursor.clone());
                let batch = match cursor.as_deref() {
                    None => UnboundedBatch {
                        elements: vec![
                            Timestamped {
                                value: (1_i32, 10_i32),
                                timestamp_ns: 1_000_000,
                            },
                            Timestamped {
                                value: (1, 20),
                                timestamp_ns: 2_000_000,
                            },
                        ],
                        progress: ProgressSignal {
                            source_key: "source".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(2_000_000),
                            completed: false,
                        },
                    },
                    Some([1]) if !factory_hold.load(Ordering::SeqCst) => UnboundedBatch {
                        elements: Vec::new(),
                        progress: ProgressSignal {
                            source_key: "source".into(),
                            split_id: "split-0".into(),
                            checkpoint_position: Some(vec![2]),
                            event_time_ns: Some(10_000_000),
                            completed: true,
                        },
                    },
                    Some([1]) => {
                        return Ok(Box::new(Script {
                            polls: VecDeque::new(),
                        })
                            as TypedUnboundedSourceReader<(i32, i32)>);
                    }
                    _ => return Err(Error::incompatible("unexpected shared cursor")),
                };
                Ok(Box::new(Script {
                    polls: vec![UnboundedPoll::Ready(batch)].into(),
                }) as TypedUnboundedSourceReader<(i32, i32)>)
            },
        )
        .unwrap()
        .map_named("common", move |row| {
            common_visits.fetch_add(1, Ordering::SeqCst);
            row
        })
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap();
    let first = source
        .filter_named("first", |(_, value)| *value == 10)
        .unwrap();
    let second = source
        .filter_named("second", |(_, value)| *value == 20)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = first
        .co_group_homogeneous_named("group", &[second])
        .unwrap()
        .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-recovery-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "shared-recovery-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let expected_progress = 1_u64.to_le_bytes().to_vec();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.progress().and_then(|signal| signal.checkpoint_position)
        != Some(expected_progress.clone())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        job.progress().and_then(|signal| signal.checkpoint_position),
        Some(expected_progress)
    );
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*committed.lock().unwrap(), ["1:[[10], [20]]".to_owned()]);
    assert_eq!(visits.load(Ordering::SeqCst), 2);
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
}

#[test]
fn three_unbounded_sources_merge_a_keyed_session_across_inputs() {
    let pipeline = Pipeline::default();
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let first = pipeline
        .test_stream_source(
            "first",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 1_i32), 0),
                    (("a".to_owned(), 2), 8_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .session_windows_named("first-window", 5_000_000)
        .unwrap();
    let second = pipeline
        .test_stream_source(
            "second",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 10_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .session_windows_named("second-window", 5_000_000)
        .unwrap();
    let third = pipeline
        .test_stream_source(
            "third",
            vec![
                TestStreamEvent::Elements(vec![(('b'.to_string(), 99_i32), 0)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .session_windows_named("third-window", 5_000_000)
        .unwrap();
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = first
        .co_group_homogeneous_named("group", &[second, third])
        .unwrap()
        .with_window_named("with-window")
        .unwrap()
        .map_named("format", |(window, (key, mut buckets))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!("{}..{}:{key}:{buckets:?}", window.start_ns, window.end_ns)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "session-group-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
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
    let mut rows = committed.lock().unwrap().clone();
    rows.sort();
    assert_eq!(
        rows,
        [
            "0..13000000:a:[[1, 2], [10], []]".to_owned(),
            "0..5000000:b:[[], [], [99]]".to_owned(),
        ]
    );
}

#[test]
fn bounded_side_is_materialized_once_and_restored_with_unbounded_group() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let hold = Arc::new(AtomicBool::new(true));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let side_visits = Arc::new(AtomicUsize::new(0));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let main = pipeline
        .unbounded_source_with_factory(
            "main",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "mixed-main-v1".into(),
            0,
            {
                let hold = Arc::clone(&hold);
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    let polls = match cursor.as_deref() {
                        None => vec![UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value: (1_i32, 5_i32),
                                timestamp_ns: 1_000_000,
                            }],
                            progress: ProgressSignal {
                                source_key: "main".into(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![1]),
                                event_time_ns: Some(0),
                                completed: false,
                            },
                        })],
                        Some([1]) if !hold.load(Ordering::SeqCst) => {
                            vec![UnboundedPoll::Ready(UnboundedBatch {
                                elements: Vec::new(),
                                progress: ProgressSignal {
                                    source_key: "main".into(),
                                    split_id: "split-0".into(),
                                    checkpoint_position: Some(vec![2]),
                                    event_time_ns: Some(10_000_000),
                                    completed: true,
                                },
                            })]
                        }
                        Some([1]) => Vec::new(),
                        _ => return Err(Error::incompatible("unexpected mixed source cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: polls.into(),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .fixed_windows("main-window", 10_000_000)
        .unwrap();
    let side = pipeline
        .from_vec(
            "side",
            vec![(2_000_000_i64, (1_i32, 10_i32)), (3_000_000, (2, 20))],
        )
        .unwrap()
        .timestamps_from_pairs_named("side-time")
        .unwrap()
        .fixed_windows("side-window", 10_000_000)
        .unwrap()
        .map_named("side-count", {
            let visits = Arc::clone(&side_visits);
            move |row| {
                visits.fetch_add(1, Ordering::SeqCst);
                row
            }
        })
        .unwrap();
    let sink = main
        .co_group_homogeneous_named("group", &[side])
        .unwrap()
        .map_named("format", |(key, mut buckets)| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!("{key}:{buckets:?}")
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "mixed-sink-v1".into(),
            {
                let committed = Arc::clone(&committed);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&committed),
                        accepted: Arc::clone(&accepted),
                        bundle: None,
                        staged: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "mixed-group-test-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let expected_progress = 2_u64.to_le_bytes().to_vec();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.progress().and_then(|signal| signal.checkpoint_position)
        != Some(expected_progress.clone())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        job.progress().and_then(|signal| signal.checkpoint_position),
        Some(expected_progress)
    );
    assert_eq!(side_visits.load(Ordering::SeqCst), 2);
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    let mut rows = committed.lock().unwrap().clone();
    rows.sort();
    assert_eq!(
        rows,
        ["1:[[5], [10]]".to_owned(), "2:[[], [20]]".to_owned()]
    );
    assert_eq!(side_visits.load(Ordering::SeqCst), 2);
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
}

#[test]
fn three_source_session_expired_bridge_arrives_after_checkpoint_restart() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let hold = Arc::new(AtomicBool::new(true));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let make_source = |index: i32| {
        let source_key = format!("source-{index}");
        let factory_key = source_key.clone();
        let hold = Arc::clone(&hold);
        let opens = Arc::clone(&opens);
        pipeline
            .unbounded_source_with_factory(
                &source_key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("session-source-{index}-v1"),
                0,
                move |cursor| {
                    opens.lock().unwrap().push((index, cursor.clone()));
                    let (rows, position, watermark, completed) = match cursor.as_deref() {
                        None => {
                            let rows = match index {
                                0 => vec![(1, 1, 0), (1, 2, 4_000_000)],
                                1 => Vec::new(),
                                _ => vec![(2, 99, 6_000_000)],
                            };
                            (rows, 1, 6_000_000, false)
                        }
                        Some([1]) if !hold.load(Ordering::SeqCst) => {
                            // Key 1 joins its live [0, 9) session. Key 2's
                            // [0, 5) candidate cannot reach its [6, 11)
                            // session and must be dropped after the restart.
                            let rows = if index == 1 {
                                vec![(1, 10, 0), (2, 88, 0)]
                            } else {
                                Vec::new()
                            };
                            (rows, 2, 20_000_000, true)
                        }
                        Some([1]) => {
                            return Ok(Box::new(Script {
                                polls: VecDeque::new(),
                            })
                                as TypedUnboundedSourceReader<(i32, i32)>);
                        }
                        _ => return Err(Error::incompatible("unexpected session source cursor")),
                    };
                    let elements = rows
                        .into_iter()
                        .map(|(key, value, timestamp_ns)| Timestamped {
                            value: (key, value),
                            timestamp_ns,
                        })
                        .collect();
                    Ok(Box::new(Script {
                        polls: vec![UnboundedPoll::Ready(UnboundedBatch {
                            elements,
                            progress: ProgressSignal {
                                source_key: factory_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![position]),
                                event_time_ns: Some(watermark),
                                completed,
                            },
                        })]
                        .into(),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                },
            )
            .unwrap()
            .session_windows_named(&format!("session-{index}"), 5_000_000)
            .unwrap()
    };
    let first = make_source(0);
    let second = make_source(1);
    let third = make_source(2);
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = first
        .co_group_homogeneous_named("group", &[second, third])
        .unwrap()
        .with_window_named("with-window")
        .unwrap()
        .map_named("format", |(window, (key, mut buckets))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!("{}..{}:{key}:{buckets:?}", window.start_ns, window.end_ns)
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "session-recovery-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "session-recovery-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let expected_progress = 3_u64.to_le_bytes().to_vec();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.progress().and_then(|signal| signal.checkpoint_position)
        != Some(expected_progress.clone())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        job.progress().and_then(|signal| signal.checkpoint_position),
        Some(expected_progress)
    );
    assert!(committed.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    let mut rows = committed.lock().unwrap().clone();
    rows.sort();
    assert_eq!(
        rows,
        [
            "0..9000000:1:[[1, 2], [10], []]".to_owned(),
            "6000000..11000000:2:[[], [], [99]]".to_owned(),
        ]
    );
    let opens = opens.lock().unwrap();
    assert_eq!(opens.len(), 6);
    assert_eq!(
        opens.iter().filter(|(_, cursor)| cursor.is_none()).count(),
        3
    );
    assert_eq!(
        opens
            .iter()
            .filter(|(_, cursor)| cursor.as_deref() == Some([1].as_slice()))
            .count(),
        3
    );
}

#[test]
fn grouped_stream_rejects_wrong_source_identity_and_regressing_watermark() {
    for wrong_identity in [true, false] {
        let pipeline = Pipeline::default();
        let committed = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let source = pipeline
            .unbounded_source_with_factory(
                "source",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "invalid-progress-v1".into(),
                0,
                move |_| {
                    let progress = |cursor, watermark, key: &str| ProgressSignal {
                        source_key: key.into(),
                        split_id: "split-0".into(),
                        checkpoint_position: Some(vec![cursor]),
                        event_time_ns: Some(watermark),
                        completed: false,
                    };
                    let first = UnboundedPoll::Ready(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: (1_i32, 5_i32),
                            timestamp_ns: 1_000_000,
                        }],
                        progress: progress(
                            1,
                            5_000_000,
                            if wrong_identity { "wrong" } else { "source" },
                        ),
                    });
                    let second = UnboundedPoll::Ready(UnboundedBatch {
                        elements: Vec::new(),
                        progress: progress(2, 4_000_000, "source"),
                    });
                    Ok(Box::new(Script {
                        polls: vec![first, second].into(),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                },
            )
            .unwrap()
            .fixed_windows("window", 10_000_000)
            .unwrap();
        let sink_committed = Arc::clone(&committed);
        let sink_accepted = Arc::clone(&accepted);
        let sink = source
            .co_group_homogeneous_named("group", &[])
            .unwrap()
            .map_named("format", |(key, buckets)| format!("{key}:{buckets:?}"))
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "invalid-progress-sink-v1".into(),
                move || {
                    Ok(Box::new(Capture {
                        committed: Arc::clone(&sink_committed),
                        accepted: Arc::clone(&sink_accepted),
                        bundle: None,
                        staged: Vec::new(),
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
        assert_eq!(error.kind, ErrorKind::Execution);
        assert!(committed.lock().unwrap().is_empty());
    }
}

impl UnboundedSource<Timestamped<(i32, i32)>> for Script {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

struct Capture {
    committed: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    bundle: Option<String>,
    staged: Vec<String>,
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

#[test]
fn three_unbounded_sources_restore_open_group_and_independent_cursors() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let hold = Arc::new(AtomicBool::new(true));
    let opens = Arc::new(Mutex::new(Vec::new()));
    let committed = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let make_source = |index: i32| {
        let key = format!("source-{index}");
        let source_key = key.clone();
        let hold = Arc::clone(&hold);
        let opens = Arc::clone(&opens);
        pipeline
            .unbounded_source_with_factory(
                &key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("source-{index}-v1"),
                0,
                move |cursor| {
                    opens
                        .lock()
                        .unwrap()
                        .push((source_key.clone(), cursor.clone()));
                    let batch = match cursor.as_deref() {
                        None => UnboundedBatch {
                            elements: vec![Timestamped {
                                value: (1, index),
                                timestamp_ns: i64::from(index) * 1_000_000,
                            }],
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![1]),
                                event_time_ns: Some(i64::from(index) * 1_000_000),
                                completed: false,
                            },
                        },
                        Some([1]) if !hold.load(Ordering::SeqCst) => UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                source_key: source_key.clone(),
                                split_id: "split-0".into(),
                                checkpoint_position: Some(vec![2]),
                                event_time_ns: Some(10_000_000),
                                completed: true,
                            },
                        },
                        Some([1]) => {
                            return Ok(Box::new(Script {
                                polls: VecDeque::new(),
                            })
                                as TypedUnboundedSourceReader<(i32, i32)>);
                        }
                        _ => return Err(Error::incompatible("unexpected source cursor")),
                    };
                    Ok(Box::new(Script {
                        polls: vec![UnboundedPoll::Ready(batch)].into(),
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                },
            )
            .unwrap()
            .fixed_windows(&format!("window-{index}"), 10_000_000)
            .unwrap()
    };
    let first = make_source(0);
    let second = make_source(1);
    let third = make_source(2);
    let sink_committed = Arc::clone(&committed);
    let sink_accepted = Arc::clone(&accepted);
    let sink = first
        .co_group_homogeneous_named("group", &[second, third])
        .unwrap()
        .with_window_named("with-window")
        .unwrap()
        .reify_timestamps_named("with-time")
        .unwrap()
        .map_named("format", |(time, (window, (key, mut buckets)))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!(
                "{}..{}:{time}:{key}:{buckets:?}",
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
            "group-sink-v1".into(),
            move || {
                Ok(Box::new(Capture {
                    committed: Arc::clone(&sink_committed),
                    accepted: Arc::clone(&sink_accepted),
                    bundle: None,
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "multi-group-test-build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let first_job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let expected_progress = 3_u64.to_le_bytes().to_vec();
    let deadline = Instant::now() + Duration::from_secs(5);
    while first_job
        .progress()
        .and_then(|signal| signal.checkpoint_position)
        != Some(expected_progress.clone())
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        first_job
            .progress()
            .and_then(|signal| signal.checkpoint_position),
        Some(expected_progress)
    );
    assert!(committed.lock().unwrap().is_empty());
    first_job.cancel();
    assert_eq!(first_job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    hold.store(false, Ordering::SeqCst);
    let resumed = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    assert_eq!(resumed.wait().unwrap().status(), JobStatus::Succeeded);
    assert_eq!(
        *committed.lock().unwrap(),
        ["0..10000000:0:1:[[0], [1], [2]]".to_owned()]
    );
    let opens = opens.lock().unwrap();
    assert_eq!(opens.len(), 6);
    assert_eq!(
        opens.iter().filter(|(_, cursor)| cursor.is_none()).count(),
        3
    );
    assert_eq!(
        opens
            .iter()
            .filter(|(_, cursor)| cursor.as_deref() == Some([1].as_slice()))
            .count(),
        3
    );
}
