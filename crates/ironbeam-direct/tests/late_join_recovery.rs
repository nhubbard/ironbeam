use ironbeam_core::{
    ElementMetadata, Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec,
    SourceProgress, TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader,
    UnboundedBatch, UnboundedPoll, UnboundedSource, WindowOutputTime,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Row = (i32, i32);

#[test]
fn typed_late_join_fires_at_count_and_flushes_partial_at_expiry() {
    let rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let main = pipeline
        .test_stream_source(
            "main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
                TestStreamEvent::Elements(vec![((1, 5), 5_000_000)]),
                TestStreamEvent::Elements(vec![((1, 7), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::Elements(vec![((1, 9), 9_000_000)]),
            ],
        )
        .unwrap()
        .fixed_windows("main-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("main-lateness", 5_000_000)
        .unwrap()
        .late_count_trigger_named("main-trigger", 2)
        .unwrap();
    let side = pipeline
        .test_stream_source(
            "side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(11_000_000),
                TestStreamEvent::AdvanceWatermark(12_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )
        .unwrap()
        .fixed_windows("side-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("side-lateness", 5_000_000)
        .unwrap()
        .late_count_trigger_named("side-trigger", 2)
        .unwrap();
    let sink = main
        .join_full_named("join", &side)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, pair))| {
            format!(
                "{key}:{pair:?}:{:?}:{}:{}:{}:{}",
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
            "late-count-join-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut actual = rows.lock().unwrap().clone();
    actual.sort_unstable();
    let mut expected = vec![
        "1:(Some(2), Some(4)):OnTime:true:false:0:0",
        "1:(Some(3), None):Late:false:false:1:1",
        "1:(Some(5), None):Late:false:false:1:1",
        "1:(Some(7), None):Late:false:true:2:2",
    ];
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn sliding_late_join_keeps_overlapping_panes_independent() {
    let rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let main = pipeline
        .test_stream_source(
            "main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 8_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .sliding_windows("main-window", 10_000_000, 5_000_000, 0)
        .unwrap()
        .allowed_lateness_named("main-lateness", 5_000_000)
        .unwrap();
    let side = pipeline
        .test_stream_source(
            "side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .sliding_windows("side-window", 10_000_000, 5_000_000, 0)
        .unwrap()
        .allowed_lateness_named("side-lateness", 5_000_000)
        .unwrap();
    let sink = main
        .join_full_named("join", &side)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .with_window_named("window")
        .unwrap()
        .map_named("row", |(window, (pane, (key, pair)))| {
            format!(
                "{}..{}:{key}:{pair:?}:{:?}:{}:{}:{}:{}",
                window.start_ns,
                window.end_ns,
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
            "sliding-late-join-v1".into(),
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
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut actual = rows.lock().unwrap().clone();
    actual.sort_unstable();
    let mut expected = vec![
        "0..10000000:1:(Some(2), Some(4)):OnTime:true:false:0:0",
        "0..10000000:1:(Some(3), None):Late:false:false:1:1",
        "5000000..15000000:1:(Some(2), Some(4)):OnTime:true:false:0:0",
        "5000000..15000000:1:(Some(3), Some(4)):OnTime:true:false:0:0",
    ];
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn typed_late_join_uses_each_panes_output_time_policy() {
    for (policy, on_time, late) in [
        (WindowOutputTime::EndOfWindow, 9_000_000, 9_000_000),
        (WindowOutputTime::LatestInPane, 4_000_000, 3_000_000),
    ] {
        let rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let pipeline = Pipeline::default();
        let main = pipeline
            .test_stream_source(
                "main",
                vec![
                    TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                    TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
                    TestStreamEvent::AdvanceWatermark(15_000_000),
                ],
            )
            .unwrap()
            .fixed_windows("main-window", 10_000_000)
            .unwrap()
            .allowed_lateness_named("main-lateness", 5_000_000)
            .unwrap();
        let side = pipeline
            .test_stream_source(
                "side",
                vec![
                    TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                    TestStreamEvent::AdvanceWatermark(15_000_000),
                ],
            )
            .unwrap()
            .fixed_windows("side-window", 10_000_000)
            .unwrap()
            .allowed_lateness_named("side-lateness", 5_000_000)
            .unwrap();
        let (main, side) = match policy {
            WindowOutputTime::EndOfWindow => (
                main.end_of_window_output_time_named("main-policy").unwrap(),
                side.end_of_window_output_time_named("side-policy").unwrap(),
            ),
            WindowOutputTime::LatestInPane => (
                main.latest_in_pane_output_time_named("main-policy")
                    .unwrap(),
                side.latest_in_pane_output_time_named("side-policy")
                    .unwrap(),
            ),
            WindowOutputTime::EarliestInPane => unreachable!(),
        };
        let sink = main
            .join_full_named("join", &side)
            .unwrap()
            .reify_panes_named("pane")
            .unwrap()
            .reify_timestamps_named("timestamp")
            .unwrap()
            .map_named("row", |(time, (pane, (key, pair)))| {
                format!(
                    "{time}:{key}:{pair:?}:{:?}:{}:{}:{}:{}",
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
                format!("late-time-{policy:?}-v1"),
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
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        assert_eq!(
            rows.lock().unwrap().as_slice(),
            [
                format!("{on_time}:1:(Some(2), Some(4)):OnTime:true:false:0:0"),
                format!("{late}:1:(Some(3), None):Late:false:false:1:1"),
            ],
            "policy: {policy:?}"
        );
    }
}

struct Script(VecDeque<UnboundedPoll<Timestamped<Row>>>);

impl UnboundedSource<Timestamped<Row>> for Script {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<Row>>, Error> {
        Ok(self.0.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn batch(
    source: &str,
    cursor: u8,
    watermark: i64,
    completed: bool,
    values: &[(i32, i32, i64)],
) -> UnboundedPoll<Timestamped<Row>> {
    UnboundedPoll::Ready(UnboundedBatch {
        elements: values
            .iter()
            .map(|&(key, value, timestamp_ns)| Timestamped {
                value: (key, value),
                timestamp_ns,
            })
            .collect(),
        progress: ProgressSignal {
            source_key: source.into(),
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
        self.bundle = id.into();
        Ok(())
    }
    fn write(&mut self, row: String) -> Result<(), Error> {
        self.staged.push(row);
        Ok(())
    }
    fn write_with_metadata(&mut self, row: String, metadata: ElementMetadata) -> Result<(), Error> {
        let pane = metadata.pane;
        let suffix = format!(
            ":{:?}:{}:{}:{}:{}",
            pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
        );
        if !row.ends_with(&suffix) {
            return Err(Error::incompatible("sink pane differs from reified pane"));
        }
        self.write(row)
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
#[expect(
    clippy::too_many_lines,
    reason = "the fixture checks all four join kinds and their late panes"
)]
fn every_typed_join_kind_fires_the_correct_late_pane() {
    for kind in ["inner", "left", "right", "full"] {
        let rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let pipeline = Pipeline::default();
        let main = pipeline
            .test_stream_source(
                "main",
                vec![
                    TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                    TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
                    TestStreamEvent::AdvanceWatermark(15_000_000),
                ],
            )
            .unwrap()
            .fixed_windows("main-window", 10_000_000)
            .unwrap()
            .allowed_lateness_named("main-lateness", 5_000_000)
            .unwrap();
        let side = pipeline
            .test_stream_source(
                "side",
                vec![
                    TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                    TestStreamEvent::AdvanceWatermark(15_000_000),
                ],
            )
            .unwrap()
            .fixed_windows("side-window", 10_000_000)
            .unwrap()
            .allowed_lateness_named("side-lateness", 5_000_000)
            .unwrap();
        let joined = match kind {
            "inner" => main
                .join_inner_named("join", &side)
                .unwrap()
                .map_named("normalize", |(key, (left, right))| {
                    (key, (Some(left), Some(right)))
                })
                .unwrap(),
            "left" => main
                .join_left_named("join", &side)
                .unwrap()
                .map_named("normalize", |(key, (left, right))| {
                    (key, (Some(left), right))
                })
                .unwrap(),
            "right" => main
                .join_right_named("join", &side)
                .unwrap()
                .map_named("normalize", |(key, (left, right))| {
                    (key, (left, Some(right)))
                })
                .unwrap(),
            "full" => main.join_full_named("join", &side).unwrap(),
            _ => unreachable!(),
        };
        let sink = joined
            .reify_panes_named("pane")
            .unwrap()
            .map_named("row", |(pane, (key, pair))| {
                format!(
                    "{key}:{pair:?}:{:?}:{}:{}:{}:{}",
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
                format!("late-{kind}-sink-v1"),
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
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let mut actual = rows.lock().unwrap().clone();
        actual.sort_unstable();
        let mut expected = vec!["1:(Some(2), Some(4)):OnTime:true:false:0:0"];
        if matches!(kind, "left" | "full") {
            expected.push("1:(Some(3), None):Late:false:false:1:1");
        }
        expected.sort_unstable();
        assert_eq!(actual, expected, "kind: {kind}");
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the recovery fixture retains both source cursors and every keyed pane"
)]
fn typed_late_join_restores_each_key_pane_and_both_source_cursors() {
    let main_opens = Arc::new(Mutex::new(Vec::new()));
    let side_opens = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: true,
    };
    let main = pipeline
        .unbounded_source_with_factory("main", progress.clone(), "late-main-v1".into(), 0, {
            let opens = Arc::clone(&main_opens);
            move |position| {
                opens.lock().unwrap().push(position.clone());
                let polls = match position.as_deref() {
                    None => vec![
                        batch(
                            "main",
                            1,
                            4_000_000,
                            false,
                            &[(1, 2, 2_000_000), (2, 20, 2_000_000)],
                        ),
                        batch("main", 2, 10_000_000, false, &[]),
                    ],
                    Some([2]) => vec![
                        batch("main", 3, 10_000_000, false, &[(1, 3, 3_000_000)]),
                        batch("main", 4, 10_000_000, false, &[(2, 21, 5_000_000)]),
                        batch("main", 5, 10_000_000, false, &[(3, 30, 6_000_000)]),
                        batch("main", 6, 15_000_000, false, &[]),
                        batch("main", 7, 15_000_000, true, &[(1, 99, 7_000_000)]),
                    ],
                    _ => return Err(Error::incompatible("unexpected main cursor")),
                };
                Ok(Box::new(Script(polls.into())) as TypedUnboundedSourceReader<Row>)
            }
        })
        .unwrap()
        .fixed_windows("main-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("main-lateness", 5_000_000)
        .unwrap();
    let side = pipeline
        .unbounded_source_with_factory("side", progress, "late-side-v1".into(), 0, {
            let opens = Arc::clone(&side_opens);
            move |position| {
                opens.lock().unwrap().push(position.clone());
                let polls = match position.as_deref() {
                    None => vec![
                        batch(
                            "side",
                            1,
                            4_000_000,
                            false,
                            &[(1, 4, 4_000_000), (2, 40, 4_000_000)],
                        ),
                        batch("side", 2, 10_000_000, false, &[]),
                    ],
                    Some([2]) => vec![batch("side", 3, 15_000_000, true, &[])],
                    _ => return Err(Error::incompatible("unexpected side cursor")),
                };
                Ok(Box::new(Script(polls.into())) as TypedUnboundedSourceReader<Row>)
            }
        })
        .unwrap()
        .fixed_windows("side-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("side-lateness", 5_000_000)
        .unwrap();
    let sink = main
        .join_full_named("join", &side)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, pair))| {
            format!(
                "{key}:{pair:?}:{:?}:{}:{}:{}:{}",
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
            "late-join-sink-v1".into(),
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
    let directory = tempfile::tempdir().unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "late-join-recovery-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while rows.lock().unwrap().len() != 2
        || job
            .progress()
            .and_then(|progress| progress.checkpoint_position)
            .is_none()
    {
        assert!(
            Instant::now() < deadline,
            "on-time panes were not checkpointed"
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
    assert_eq!(
        main_opens.lock().unwrap().as_slice(),
        &[None, Some(vec![2])]
    );
    assert_eq!(
        side_opens.lock().unwrap().as_slice(),
        &[None, Some(vec![2])]
    );
    let mut actual = rows.lock().unwrap().clone();
    actual.sort_unstable();
    let mut expected = vec![
        "1:(Some(2), Some(4)):OnTime:true:false:0:0",
        "2:(Some(20), Some(40)):OnTime:true:false:0:0",
        "1:(Some(3), None):Late:false:false:1:1",
        "2:(Some(21), None):Late:false:false:1:1",
        "3:(Some(30), None):Late:true:false:0:0",
    ];
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[test]
fn typed_late_count_join_restores_partial_pane_and_source_cursors() {
    assert_typed_late_count_join_restores_partial_pane_and_source_cursors(false);
}

#[test]
fn typed_accumulating_late_count_join_restores_retained_values_and_cursors() {
    assert_typed_late_count_join_restores_partial_pane_and_source_cursors(true);
}

#[expect(
    clippy::too_many_lines,
    reason = "the count-trigger recovery fixture checks both source cursors and panes"
)]
fn assert_typed_late_count_join_restores_partial_pane_and_source_cursors(accumulating: bool) {
    let main_opens = Arc::new(Mutex::new(Vec::new()));
    let side_opens = Arc::new(Mutex::new(Vec::new()));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: true,
    };
    let main = pipeline
        .unbounded_source_with_factory("main", progress.clone(), "late-count-main-v1".into(), 0, {
            let opens = Arc::clone(&main_opens);
            move |position| {
                opens.lock().unwrap().push(position.clone());
                let polls = match position.as_deref() {
                    None => vec![
                        batch("main", 1, 4_000_000, false, &[(1, 2, 2_000_000)]),
                        batch("main", 2, 10_000_000, false, &[]),
                        batch("main", 3, 10_000_000, false, &[(1, 3, 3_000_000)]),
                    ],
                    Some([3]) => vec![
                        batch("main", 4, 10_000_000, false, &[(1, 5, 5_000_000)]),
                        batch("main", 5, 10_000_000, false, &[(1, 7, 7_000_000)]),
                        batch("main", 6, 15_000_000, false, &[]),
                        batch("main", 7, 15_000_000, true, &[(1, 9, 9_000_000)]),
                    ],
                    _ => return Err(Error::incompatible("unexpected main cursor")),
                };
                Ok(Box::new(Script(polls.into())) as TypedUnboundedSourceReader<Row>)
            }
        })
        .unwrap()
        .fixed_windows("main-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("main-lateness", 5_000_000)
        .unwrap();
    let main = if accumulating {
        main.accumulating_late_count_trigger_named("main-trigger", 2)
    } else {
        main.late_count_trigger_named("main-trigger", 2)
    }
    .unwrap();
    let side = pipeline
        .unbounded_source_with_factory("side", progress, "late-count-side-v1".into(), 0, {
            let opens = Arc::clone(&side_opens);
            move |position| {
                opens.lock().unwrap().push(position.clone());
                let polls = match position.as_deref() {
                    None => vec![
                        batch("side", 1, 4_000_000, false, &[(1, 4, 4_000_000)]),
                        batch("side", 2, 10_000_000, false, &[]),
                    ],
                    Some([2]) => vec![
                        batch("side", 3, 11_000_000, false, &[]),
                        batch("side", 4, 12_000_000, false, &[]),
                        batch("side", 5, 15_000_000, true, &[]),
                    ],
                    _ => return Err(Error::incompatible("unexpected side cursor")),
                };
                Ok(Box::new(Script(polls.into())) as TypedUnboundedSourceReader<Row>)
            }
        })
        .unwrap()
        .fixed_windows("side-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("side-lateness", 5_000_000)
        .unwrap();
    let side = if accumulating {
        side.accumulating_late_count_trigger_named("side-trigger", 2)
    } else {
        side.late_count_trigger_named("side-trigger", 2)
    }
    .unwrap();
    let sink = main
        .join_full_named("join", &side)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, pair))| {
            format!(
                "{key}:{pair:?}:{:?}:{}:{}:{}:{}",
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
            "late-count-recovery-sink-v1".into(),
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
    let directory = tempfile::tempdir().unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 1,
            recover: true,
            binary_build_id: "late-count-recovery-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while job
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        .is_none_or(|position| position != 5_u64.to_le_bytes())
    {
        assert!(
            Instant::now() < deadline,
            "partial late pane was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(rows.lock().unwrap().len(), 1);
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        main_opens.lock().unwrap().as_slice(),
        &[None, Some(vec![3])]
    );
    assert_eq!(
        side_opens.lock().unwrap().as_slice(),
        &[None, Some(vec![2])]
    );
    let mut actual = rows.lock().unwrap().clone();
    actual.sort_unstable();
    let mut expected = if accumulating {
        vec![
            "1:(Some(2), Some(4)):OnTime:true:false:0:0",
            "1:(Some(2), Some(4)):Late:false:false:1:1",
            "1:(Some(3), Some(4)):Late:false:false:1:1",
            "1:(Some(5), Some(4)):Late:false:false:1:1",
            "1:(Some(2), Some(4)):Late:false:true:2:2",
            "1:(Some(3), Some(4)):Late:false:true:2:2",
            "1:(Some(5), Some(4)):Late:false:true:2:2",
            "1:(Some(7), Some(4)):Late:false:true:2:2",
        ]
    } else {
        vec![
            "1:(Some(2), Some(4)):OnTime:true:false:0:0",
            "1:(Some(3), None):Late:false:false:1:1",
            "1:(Some(5), None):Late:false:false:1:1",
            "1:(Some(7), None):Late:false:true:2:2",
        ]
    };
    expected.sort_unstable();
    assert_eq!(actual, expected);
}
