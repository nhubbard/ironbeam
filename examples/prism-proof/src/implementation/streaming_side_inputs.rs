/// An unbounded main stream consumes bounded side values in each fixed window.
pub fn streaming_windowed_side_input_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "stream-window-side-main",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::Elements(vec![(2_i32, 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::Elements(vec![(3_i32, 5_000_000)]),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("stream-window-side-main-window", 2_000_000)?;
    let side = pipeline
        .from_vec(
            "stream-window-side-values",
            vec![(0_i64, 10_i32), (2_000_000, 20)],
        )?
        .timestamps_from_pairs_named("stream-window-side-values-time")?
        .fixed_windows("stream-window-side-values-window", 2_000_000)?;
    main.map_with_windowed_side_named("stream-window-side-add", &side, |value, values| {
        value + values.iter().sum::<i32>()
    })?
    .reify_timestamps_named("stream-window-side-result-time")?
    .with_window_named("stream-window-side-result-window")?
    .map_named("stream-window-side-row", |(window, (time, value))| {
        format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
    })
}

pub fn streaming_windowed_side_input_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    streaming_windowed_side_input_collection(pipeline)?.sink_named_with_config(
        "stream-window-side-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("stream-window-side-contract:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Two watermarked streams produce one side view when each fixed window closes.
pub fn dual_stream_windowed_side_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "dual-window-side-main",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000), (2, 3_000_000), (3, 5_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("dual-window-side-main-window", 2_000_000)?;
    let side = pipeline
        .test_stream_source(
            "dual-window-side-values",
            vec![
                TestStreamEvent::Elements(vec![(10_i32, 0), (20, 2_000_000), (21, 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("dual-window-side-values-window", 2_000_000)?;
    main.map_with_windowed_side_named("dual-window-side-add", &side, |value, values| {
        value + values.iter().sum::<i32>()
    })?
    .reify_timestamps_named("dual-window-side-result-time")?
    .with_window_named("dual-window-side-result-window")?
    .map_named("dual-window-side-row", |(window, (time, value))| {
        format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
    })
}

pub fn dual_stream_windowed_side_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_stream_windowed_side_collection(pipeline)?.sink_named_with_config(
        "dual-window-side-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-window-side-contract:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// A two-stream completion barrier releases each main window at closure.
pub fn dual_stream_windowed_wait_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "stream-wait-main",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000), (2, 3_000_000), (3, 5_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("stream-wait-main-window", 2_000_000)?;
    let signal = pipeline
        .test_stream_source(
            "stream-wait-signal",
            vec![
                TestStreamEvent::Elements(vec![(10_i32, 0), (20, 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("stream-wait-signal-window", 2_000_000)?;
    main.wait_on_windowed_named("stream-wait-barrier", &signal)?
        .reify_timestamps_named("stream-wait-time")?
        .with_window_named("stream-wait-window")?
        .map_named("stream-wait-row", |(window, (time, value))| {
            format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
        })
}

pub fn dual_stream_windowed_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_stream_windowed_wait_collection(pipeline)?.sink_named_with_config(
        "stream-wait-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("stream-wait-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

struct EarlyWaitReader {
    batch: Option<UnboundedBatch<Timestamped<i32>>>,
}

impl UnboundedSource<Timestamped<i32>> for EarlyWaitReader {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(self
            .batch
            .take()
            .map_or(UnboundedPoll::Pending, UnboundedPoll::Ready))
    }
}

/// The source advances a fixed-window watermark, then remains open without
/// data. A portable test observes the grouped output before cancellation.
pub fn idle_source_window_group_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let source = pipeline.unbounded_source_with_factory(
        "idle-window-source",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: true,
        },
        "idle-window-source-v1".into(),
        0,
        |cursor| {
            let first = UnboundedBatch {
                elements: vec![Timestamped {
                    value: 7_i32,
                    timestamp_ns: 1_000_000,
                }],
                progress: ProgressSignal {
                    source_key: "idle-window-source".into(),
                    split_id: "source-split".into(),
                    checkpoint_position: Some(vec![1]),
                    event_time_ns: Some(1_000_000),
                    completed: false,
                },
            };
            let second = UnboundedBatch {
                elements: Vec::new(),
                progress: ProgressSignal {
                    source_key: "idle-window-source".into(),
                    split_id: "source-split".into(),
                    checkpoint_position: Some(vec![2]),
                    event_time_ns: Some(11_000_000),
                    completed: false,
                },
            };
            let polls = match cursor.as_deref() {
                None => vec![UnboundedPoll::Ready(first), UnboundedPoll::Ready(second)],
                Some([1]) => vec![UnboundedPoll::Ready(second)],
                Some([2]) => Vec::new(),
                _ => return Err(Error::incompatible("idle source cursor changed")),
            };
            Ok(Box::new(ProductionSource(polls.into())) as TypedUnboundedSourceReader<i32>)
        },
    )?;
    let sink_path = path.clone();
    source
        .fixed_windows("idle-window", 10_000_000)?
        .map_named("idle-key", |value| (0_i32, value))?
        .group_by_key_named("idle-group")?
        .with_window_named("idle-result-window")?
        .map_named("idle-format", |(window, (_, values))| {
            format!("{}..{}:{values:?}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "idle-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("idle-result:{}", path.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_path.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Keep the main source open after one value while an empty signal window
/// closes. A portable test observes the sink before the job is cancelled.
pub fn early_windowed_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    early_windowed_wait_fixture_for(pipeline, path, false, false, false)
}

/// Keep the main source open while a completed signal closes two overlapping
/// windows that contain the main value.
pub fn early_sliding_windowed_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    early_windowed_wait_fixture_for(pipeline, path, true, false, false)
}

/// Keep the main source open until two independent signal watermarks close its window.
pub fn early_multi_windowed_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    early_windowed_wait_fixture_for(pipeline, path, false, true, false)
}

/// Keep the main source open while a signal emits a value and completes.
pub fn early_value_windowed_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    early_windowed_wait_fixture_for(pipeline, path, false, false, true)
}

fn early_windowed_wait_fixture_for(
    pipeline: &Pipeline,
    path: PathBuf,
    sliding: bool,
    multi: bool,
    signal_value: bool,
) -> Result<Sink<String>, Error> {
    let main_timestamp = if sliding { 7_000_000 } else { 1_000_000 };
    let main = pipeline
        .unbounded_source_with_factory(
            "early-wait-main",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "early-wait-main-v1".into(),
            0,
            move |cursor| {
                let batch = match cursor.as_deref() {
                    None => Some(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: 7_i32,
                            timestamp_ns: main_timestamp,
                        }],
                        progress: ProgressSignal {
                            source_key: "early-wait-main".into(),
                            split_id: "main-split".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(main_timestamp),
                            completed: false,
                        },
                    }),
                    Some([1]) => None,
                    _ => return Err(Error::incompatible("early wait main cursor changed")),
                };
                Ok(Box::new(EarlyWaitReader { batch }) as TypedUnboundedSourceReader<i32>)
            },
        )?;
    let main = if sliding {
        main.sliding_windows("early-wait-main-window", 10_000_000, 5_000_000, 0)?
    } else {
        main.fixed_windows("early-wait-main-window", 10_000_000)?
    };
    let signal = pipeline
        .unbounded_source_with_factory(
            "early-wait-signal",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "early-wait-signal-v1".into(),
            0,
            move |cursor| {
                let batch = match cursor.as_deref() {
                    None => Some(UnboundedBatch {
                        elements: if signal_value {
                            vec![Timestamped {
                                value: 99,
                                timestamp_ns: 1_000_000,
                            }]
                        } else {
                            Vec::new()
                        },
                        progress: ProgressSignal {
                            source_key: "early-wait-signal".into(),
                            split_id: "signal-split".into(),
                            checkpoint_position: Some(vec![1]),
                            event_time_ns: Some(if sliding { 16_000_000 } else { 10_000_000 }),
                            completed: true,
                        },
                    }),
                    _ => return Err(Error::incompatible("early wait signal cursor changed")),
                };
                Ok(Box::new(EarlyWaitReader { batch }) as TypedUnboundedSourceReader<i32>)
            },
        )?;
    let signal = if sliding {
        signal.sliding_windows("early-wait-signal-window", 10_000_000, 5_000_000, 0)?
    } else {
        signal.fixed_windows("early-wait-signal-window", 10_000_000)?
    };
    let waited = if multi {
        let second = pipeline
            .unbounded_source_with_factory(
                "early-wait-second-signal",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "early-wait-second-signal-v1".into(),
                0,
                |cursor| {
                    let batch = match cursor.as_deref() {
                        None => Some(UnboundedBatch {
                            elements: Vec::new(),
                            progress: ProgressSignal {
                                source_key: "early-wait-second-signal".into(),
                                split_id: "second-signal-split".into(),
                                checkpoint_position: Some(vec![1]),
                                event_time_ns: Some(10_000_000),
                                completed: true,
                            },
                        }),
                        _ => return Err(Error::incompatible("early wait second signal cursor changed")),
                    };
                    Ok(Box::new(EarlyWaitReader { batch }) as TypedUnboundedSourceReader<i32>)
                },
            )?
            .fixed_windows("early-wait-second-signal-window", 10_000_000)?;
        main.wait_on_all_windowed_named("early-wait-barrier", &[signal, second])?
    } else {
        main.wait_on_windowed_named("early-wait-barrier", &signal)?
    };
    let sink_path = path.clone();
    waited
        .with_window_named("early-wait-window")?
        .reshuffle_named("early-wait-sink-boundary")?
        .map_named("early-wait-format", |(window, value)| {
            format!("{}..{}:{value}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "early-wait-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("early-wait-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A three-source barrier with two independently watermarked signal streams.
pub fn triple_stream_windowed_wait_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "triple-wait-main",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000), (2, 3_000_000), (3, 5_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("triple-wait-main-window", 2_000_000)?;
    let first = pipeline
        .test_stream_source(
            "triple-wait-first",
            vec![
                TestStreamEvent::Elements(vec![(10_i32, 0)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("triple-wait-first-window", 2_000_000)?;
    let second = pipeline
        .test_stream_source(
            "triple-wait-second",
            vec![
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::Elements(vec![(20_i32, 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("triple-wait-second-window", 2_000_000)?;
    main.wait_on_all_windowed_named("triple-wait-barrier", &[first, second])?
        .reify_timestamps_named("triple-wait-time")?
        .with_window_named("triple-wait-window")?
        .map_named("triple-wait-row", |(window, (time, value))| {
            format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
        })
}

pub fn triple_stream_windowed_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    triple_stream_windowed_wait_collection(pipeline)?.sink_named_with_config(
        "triple-wait-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("triple-wait-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Three independent watermarked sources with differently typed signals.
pub fn heterogeneous_windowed_wait_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "heterogeneous-wait-main",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000), (2, 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
            ],
        )?
        .fixed_windows("heterogeneous-wait-main-window", 2_000_000)?;
    let text = pipeline
        .test_stream_source(
            "heterogeneous-wait-text",
            vec![
                TestStreamEvent::Elements(vec![(10_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
            ],
        )?
        .fixed_windows("heterogeneous-wait-text-window", 2_000_000)?
        .map_named("heterogeneous-wait-text-value", |_| "ready".to_owned())?
        .windowed_wait_signal_named("heterogeneous-wait-text-marker")?;
    let flag = pipeline
        .test_stream_source(
            "heterogeneous-wait-flag",
            vec![
                TestStreamEvent::Elements(vec![(20_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceWatermark(4_000_000),
            ],
        )?
        .fixed_windows("heterogeneous-wait-flag-window", 2_000_000)?
        .map_named("heterogeneous-wait-flag-value", |_| true)?
        .windowed_wait_signal_named("heterogeneous-wait-flag-marker")?;
    main.wait_on_windowed_signals_named("heterogeneous-wait-barrier", &[text, flag])?
        .reify_timestamps_named("heterogeneous-wait-time")?
        .with_window_named("heterogeneous-wait-window")?
        .map_named("heterogeneous-wait-row", |(window, (time, value))| {
            format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
        })
}

pub fn heterogeneous_windowed_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    heterogeneous_windowed_wait_collection(pipeline)?.sink_named_with_config(
        "heterogeneous-wait-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("heterogeneous-wait-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

type KeyedStream = PCollection<(String, i32)>;

fn dual_unbounded_sources(pipeline: &Pipeline) -> Result<(KeyedStream, KeyedStream), Error> {
    let main = pipeline
        .test_stream_source(
            "dual-main",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 1_i32), 1_000_000),
                    (("c".to_owned(), 3), 5_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(8_000_000),
            ],
        )?
        .fixed_windows("dual-main-window", 4_000_000)?;
    let side = pipeline
        .test_stream_source(
            "dual-side",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 10_i32), 2_000_000),
                    (("b".to_owned(), 20), 5_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(8_000_000),
            ],
        )?
        .fixed_windows("dual-side-window", 4_000_000)?;
    Ok((main, side))
}

/// Timestamped full join and heterogeneous co-group over two watermarked inputs.
pub fn dual_unbounded_fanin_output_time_collection(
    pipeline: &Pipeline,
    policy: ironbeam_core::WindowOutputTime,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    let (main, side) = match policy {
        ironbeam_core::WindowOutputTime::EndOfWindow => (
            main.end_of_window_output_time_named("dual-time-main-policy")?,
            side.end_of_window_output_time_named("dual-time-side-policy")?,
        ),
        ironbeam_core::WindowOutputTime::LatestInPane => (
            main.latest_in_pane_output_time_named("dual-time-main-policy")?,
            side.latest_in_pane_output_time_named("dual-time-side-policy")?,
        ),
        ironbeam_core::WindowOutputTime::EarliestInPane => (main, side),
    };
    let joined = main
        .join_full_named("dual-time-join", &side)?
        .reify_timestamps_named("dual-time-join-stamp")?
        .map_named("dual-time-join-row", |(time, (key, pair))| {
            format!("J:{key}:{time}:{pair:?}")
        })?;
    let grouped = ironbeam_core::cogroup_by_key_2_named("dual-time-group", &main, &side)?
        .reify_timestamps_named("dual-time-group-stamp")?
        .map_named("dual-time-group-row", |(time, (key, groups))| {
            format!("C:{key}:{time}:{groups:?}")
        })?;
    joined.flatten_named("dual-time-merge", &[grouped])
}

pub fn dual_unbounded_fanin_output_time_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    policy: ironbeam_core::WindowOutputTime,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_fanin_output_time_collection(pipeline, policy)?
        .sink_named_with_config(
            "dual-time-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("dual-time-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A typed map side view joins two independent watermarked sources at closure.
pub fn dual_unbounded_map_side_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    main.map_with_windowed_map_side_named(
        "dual-typed-side-map",
        &side,
        |(key, value), view| {
            let lookup = view.get(&key).copied().unwrap_or_default();
            format!("{key}:{}", value + lookup)
        },
    )?
    .reify_timestamps_named("dual-typed-side-time")?
    .with_window_named("dual-typed-side-window")?
    .map_named("dual-typed-side-row", |(window, (time, value))| {
        format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
    })
}

pub fn dual_unbounded_map_side_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_map_side_collection(pipeline)?.sink_named_with_config(
        "dual-typed-side-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-typed-side-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Two tagged unbounded TestStream outputs meet at a fixed-window full join.
pub fn dual_unbounded_join_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    main.join_full_named("dual-join", &side)?
        .reify_timestamps_named("dual-join-time")?
        .with_window_named("dual-join-window")?
        .map_named("dual-join-row", |(window, (time, (key, (main, side))))| {
            format!(
                "{}..{}:{time}:{key}:{main:?}:{side:?}",
                window.start_ns, window.end_ns
            )
        })
}

/// Run one join kind against two unbounded sources and finite event-time windows.
pub fn dual_unbounded_join_kind_collection(
    pipeline: &Pipeline,
    kind: &str,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    match kind {
        "inner" => main
            .join_inner_named("dual-kind-inner", &side)?
            .reify_timestamps_named("dual-kind-inner-time")?
            .with_window_named("dual-kind-inner-window")?
            .map_named("dual-kind-inner-row", |(window, (time, (key, (l, r))))| {
                format!(
                    "I|{key}|{l}|{r}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            }),
        "left" => main
            .join_left_named("dual-kind-left", &side)?
            .reify_timestamps_named("dual-kind-left-time")?
            .with_window_named("dual-kind-left-window")?
            .map_named("dual-kind-left-row", |(window, (time, (key, (l, r))))| {
                format!(
                    "L|{key}|{l}|{r:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            }),
        "right" => main
            .join_right_named("dual-kind-right", &side)?
            .reify_timestamps_named("dual-kind-right-time")?
            .with_window_named("dual-kind-right-window")?
            .map_named("dual-kind-right-row", |(window, (time, (key, (l, r))))| {
                format!(
                    "R|{key}|{l:?}|{r}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            }),
        "full" => main
            .join_full_named("dual-kind-full", &side)?
            .reify_timestamps_named("dual-kind-full-time")?
            .with_window_named("dual-kind-full-window")?
            .map_named("dual-kind-full-row", |(window, (time, (key, (l, r))))| {
                format!(
                    "F|{key}|{l:?}|{r:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            }),
        _ => Err(Error::graph("dual-join-kind", "unknown join kind")),
    }
}

pub fn dual_unbounded_join_kind_fixture(
    pipeline: &Pipeline,
    kind: &str,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_join_kind_collection(pipeline, kind)?.sink_named_with_config(
        "dual-kind-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-kind-{kind}-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Four joins share the same two unbounded source cursors and one sink.
pub fn dual_unbounded_join_fanout_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    let inner = main.join_inner_named("dual-fanout-inner", &side)?
        .map_named("dual-fanout-inner-value", |(_, (left, right))| 10 + left + right)?;
    let left = main.join_left_named("dual-fanout-left", &side)?
        .map_named("dual-fanout-left-value", |(_, (left, right))| 20 + left + right.unwrap_or_default())?;
    let right = main.join_right_named("dual-fanout-right", &side)?
        .map_named("dual-fanout-right-value", |(_, (left, right))| 30 + left.unwrap_or_default() + right)?;
    let full = main.join_full_named("dual-fanout-full", &side)?
        .map_named("dual-fanout-full-value", |(_, (left, right))| {
            40 + left.unwrap_or_default() + right.unwrap_or_default()
        })?;
    let sink_directory = directory.clone();
    inner.flatten_named("dual-fanout-merge", &[left, right, full])?
        .reify_timestamps_named("dual-fanout-time")?
        .with_window_named("dual-fanout-window")?
        .map_named("dual-fanout-row", |(window, (time, value))| {
            if let Some(path) = std::env::var_os("IRONBEAM_TEST_KILL_DUAL_FANOUT_CHILD") {
                if let Ok(file) = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                {
                    let _ = file.sync_all();
                    std::process::exit(88);
                }
            }
            format!("{value}|{time}|{}..{}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "dual-fanout-capture",
            SinkSpec { mode: SinkMode::Append, replay: Replay::Idempotent, commit: true },
            format!("dual-fanout:{}", directory.display()),
            move || Ok(Box::new(IdempotentRowSink { directory: sink_directory.clone(), pending: Vec::new() })),
        )
}

/// A late event arrives after both source watermarks have closed its window.
pub fn dual_unbounded_late_join_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "late-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 10_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 11), 1_000_000)]),
            ],
        )?
        .fixed_windows("late-main-window", 10_000_000)?;
    let side = pipeline
        .test_stream_source(
            "late-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 20_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("late-side-window", 10_000_000)?;
    main.join_inner_named("late-join", &side)?
        .map_named("late-row", |(key, (left, right))| {
            format!("{key}:{left}:{right}")
        })
}

pub fn dual_unbounded_late_join_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_late_join_collection(pipeline)?.sink_named_with_config(
        "late-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("late-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

type DualLateInputs = (PCollection<(i32, i32)>, PCollection<(i32, i32)>);

fn dual_positive_late_join_sources(pipeline: &Pipeline) -> Result<DualLateInputs, Error> {
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
        )?
        .fixed_windows("positive-late-main-window", 10_000_000)?
        .allowed_lateness_named("positive-late-main-policy", 5_000_000)?;
    let side = pipeline
        .test_stream_source(
            "positive-late-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )?
        .fixed_windows("positive-late-side-window", 10_000_000)?
        .allowed_lateness_named("positive-late-side-policy", 5_000_000)?;
    Ok((main, side))
}

/// A typed full join retains its window for a main-only late pane.
pub fn dual_positive_late_full_join_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_positive_late_join_sources(pipeline)?;
    main.join_full_named("positive-late-join", &side)?
        .reify_panes_named("positive-late-pane")?
        .map_named("positive-late-row", |(pane, (key, pair))| {
            format!(
                "{key}:{pair:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
}

/// Apply Beam's output-time policy independently to on-time and late panes.
pub fn dual_positive_late_join_time_collection(
    pipeline: &Pipeline,
    policy: ironbeam_core::WindowOutputTime,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_positive_late_join_sources(pipeline)?;
    let (main, side) = match policy {
        ironbeam_core::WindowOutputTime::EndOfWindow => (
            main.end_of_window_output_time_named("positive-late-main-time")?,
            side.end_of_window_output_time_named("positive-late-side-time")?,
        ),
        ironbeam_core::WindowOutputTime::LatestInPane => (
            main.latest_in_pane_output_time_named("positive-late-main-time")?,
            side.latest_in_pane_output_time_named("positive-late-side-time")?,
        ),
        ironbeam_core::WindowOutputTime::EarliestInPane => (main, side),
    };
    main.join_full_named("positive-late-time-join", &side)?
        .reify_panes_named("positive-late-time-pane")?
        .reify_timestamps_named("positive-late-time-stamp")?
        .map_named("positive-late-time-row", |(time, (pane, (key, pair)))| {
            format!(
                "{time}:{key}:{pair:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
}

pub fn dual_positive_late_join_time_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    policy: ironbeam_core::WindowOutputTime,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_positive_late_join_time_collection(pipeline, policy)?.sink_named_with_config(
        "positive-late-time-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("positive-late-time-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

pub fn dual_positive_late_full_join_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_positive_late_full_join_collection(pipeline)?.sink_named_with_config(
        "positive-late-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("positive-late-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// A heterogeneous co-group emits the matching side only in the on-time pane.
pub fn dual_positive_late_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    heterogeneous_late_cogroup_collection(pipeline, false)
}

pub fn heterogeneous_late_count_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    heterogeneous_late_cogroup_collection(pipeline, true)
}

fn heterogeneous_late_cogroup_collection(
    pipeline: &Pipeline,
    count_trigger: bool,
) -> Result<PCollection<String>, Error> {
    let mut events = vec![
        TestStreamEvent::Elements(vec![
            ((1_i32, 0_u8, 2_i32), 2_000_000),
            ((1, 1, 4), 4_000_000),
        ]),
        TestStreamEvent::AdvanceWatermark(10_000_000),
        TestStreamEvent::Elements(vec![((1, 0, 3), 3_000_000)]),
    ];
    if count_trigger {
        events.push(TestStreamEvent::Elements(vec![((1, 0, 5), 5_000_000)]));
    }
    events.push(TestStreamEvent::AdvanceWatermark(15_000_000));
    let source = pipeline
        .test_stream_source(
            "heterogeneous-late-source",
            events,
        )?
        .fixed_windows("heterogeneous-late-window", 10_000_000)?
        .allowed_lateness_named("heterogeneous-late-policy", 5_000_000)?;
    let source = if count_trigger {
        source.late_count_trigger_named("heterogeneous-late-trigger", 2)?
    } else {
        source
    };
    let main = source
        .filter_named("heterogeneous-main-filter", |(_, tag, _)| *tag == 0)?
        .map_named("heterogeneous-main", |(key, _, value)| (key, value))?;
    let side = source
        .filter_named("heterogeneous-side-filter", |(_, tag, _)| *tag == 1)?
        .map_named("heterogeneous-side", |(key, _, _)| (key, "right".to_owned()))?;
    ironbeam_core::cogroup_by_key_2_named("heterogeneous-late-group", &main, &side)?
        .reify_panes_named("heterogeneous-late-pane")?
        .map_named("heterogeneous-late-row", |(pane, (key, buckets))| {
            format!(
                "{key}:{buckets:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
}

pub fn dual_positive_late_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    heterogeneous_late_cogroup_fixture(pipeline, path, false)
}

pub fn heterogeneous_late_count_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    heterogeneous_late_cogroup_fixture(pipeline, path, true)
}

fn heterogeneous_late_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    count_trigger: bool,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    heterogeneous_late_cogroup_collection(pipeline, count_trigger)?.sink_named_with_config(
        "heterogeneous-late-result",
        SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
        format!("heterogeneous-late-result:{}", path.display()),
        move || Ok(Box::new(GlobalFoldSink { path: sink_path.clone(), values: Vec::new() })),
    )
}

/// The same two unbounded sources also exercise portable co-group tags.
pub fn dual_unbounded_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    ironbeam_core::cogroup_by_key_2_named("dual-cogroup", &main, &side)?
        .reify_timestamps_named("dual-cogroup-time")?
        .with_window_named("dual-cogroup-window")?
        .map_named(
            "dual-cogroup-row",
            |(window, (time, (key, (main, side))))| {
                format!(
                    "{}..{}:{time}:{key}:{main:?}:{side:?}",
                    window.start_ns, window.end_ns
                )
            },
        )
}
