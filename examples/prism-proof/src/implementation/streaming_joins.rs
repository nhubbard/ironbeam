fn dual_unbounded_session_sources(pipeline: &Pipeline) -> Result<(KeyedStream, KeyedStream), Error> {
    let main = pipeline.test_stream_source("dual-session-main", vec![
        TestStreamEvent::Elements(vec![
            (("a".to_owned(), 1_i32), 0),
            (("b".to_owned(), 99_i32), 0),
        ]),
        TestStreamEvent::Elements(vec![(("a".to_owned(), 2_i32), 8_000_000)]),
        TestStreamEvent::AdvanceWatermark(20_000_000),
        TestStreamEvent::Elements(vec![(("a".to_owned(), 3_i32), 0)]),
    ])?.session_windows_named("dual-session-main-window", 5_000_000)?;
    let side = pipeline.test_stream_source("dual-session-side", vec![
        TestStreamEvent::Elements(vec![(("a".to_owned(), 10_i32), 4_000_000)]),
        TestStreamEvent::AdvanceWatermark(20_000_000),
    ])?.session_windows_named("dual-session-side-window", 5_000_000)?;
    Ok((main, side))
}

/// One unbounded and two bounded keyed inputs share a fixed window.
pub fn mixed_homogeneous_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "mixed-group-main",
            vec![
                TestStreamEvent::Elements(vec![(('a'.to_string(), 1_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("mixed-group-main-window", 10_000_000)?;
    let side = pipeline
        .from_vec(
            "mixed-group-side",
            vec![
                (2_000_000_i64, ('a'.to_string(), 10_i32)),
                (3_000_000, ('b'.to_string(), 20)),
            ],
        )?
        .timestamps_from_pairs_named("mixed-group-side-time")?
        .fixed_windows("mixed-group-side-window", 10_000_000)?;
    let empty = pipeline
        .from_vec("mixed-group-empty", Vec::<(String, i32)>::new())?
        .fixed_windows("mixed-group-empty-window", 10_000_000)?;
    main.co_group_homogeneous_named("mixed-group", &[side, empty])?
        .with_window_named("mixed-group-with-window")?
        .map_named("mixed-group-row", |(window, (key, mut buckets))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!("{}..{}:{key}:{buckets:?}", window.start_ns, window.end_ns)
        })
}

pub fn mixed_homogeneous_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    mixed_homogeneous_cogroup_collection(pipeline)?.sink_named_with_config(
        "mixed-group-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("mixed-group-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Stateless Flatten accepts a bounded first input and a live stream.
pub fn mixed_flatten_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    let bounded = pipeline
        .from_vec("mixed-flat-bounded", vec![(1_000_000_i64, 10_i32), (3_000_000, 20)])?
        .timestamps_from_pairs_named("mixed-flat-bounded-time")?
        .fixed_windows("mixed-flat-bounded-window", 2_000_000)?;
    let stream = pipeline
        .test_stream_source(
            "mixed-flat-stream",
            vec![
                TestStreamEvent::Elements(vec![(30_i32, 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .fixed_windows("mixed-flat-stream-window", 2_000_000)?;
    bounded
        .flatten_named("mixed-flat-merge", &[stream])?
        .reify_timestamps_named("mixed-flat-time")?
        .with_window_named("mixed-flat-window")?
        .map_named("mixed-flat-row", |(window, (time, value))| {
            format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
        })
}

pub fn mixed_flatten_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    mixed_flatten_collection(pipeline)?.sink_named_with_config(
        "mixed-flat-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("mixed-flat-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// A side event bridges two unbounded keyed session intervals across inputs.
pub fn dual_unbounded_session_join_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_session_sources(pipeline)?;
    main.join_inner_named("dual-session-join", &side)?
        .with_window_named("dual-session-window")?
        .map_named("dual-session-row", |(window, (key, (left, right)))| {
            format!("{}..{}:{key}:{left}:{right}", window.start_ns, window.end_ns)
        })
}

pub fn dual_unbounded_session_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_session_sources(pipeline)?;
    ironbeam_core::cogroup_by_key_2_named("dual-session-cogroup", &main, &side)?
        .with_window_named("dual-session-cogroup-window")?
        .map_named("dual-session-cogroup-row", |(window, (key, (mut main, mut side)))| {
            main.sort_unstable();
            side.sort_unstable();
            format!("{}..{}:{key}:{main:?}:{side:?}", window.start_ns, window.end_ns)
        })
}

/// Three independent unbounded sources bridge a keyed session across inputs.
pub fn triple_unbounded_session_homogeneous_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_session_sources(pipeline)?;
    let third = pipeline
        .test_stream_source(
            "triple-session-third",
            vec![
                TestStreamEvent::Elements(vec![(('c'.to_string(), 77_i32), 0)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .session_windows_named("triple-session-third-window", 5_000_000)?;
    main.co_group_homogeneous_named("triple-session-group", &[side, third])?
        .with_window_named("triple-session-window")?
        .map_named("triple-session-row", |(window, (key, mut buckets))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!("{}..{}:{key}:{buckets:?}", window.start_ns, window.end_ns)
        })
}

pub fn triple_unbounded_session_homogeneous_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    triple_unbounded_session_homogeneous_collection(pipeline)?.sink_named_with_config(
        "triple-session-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("triple-session-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

pub fn dual_unbounded_session_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_session_cogroup_collection(pipeline)?.sink_named_with_config(
        "dual-session-cogroup-result",
        SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
        format!("dual-session-cogroup-result:{}", path.display()),
        move || Ok(Box::new(GlobalFoldSink { path: sink_path.clone(), values: Vec::new() })),
    )
}

pub fn dual_unbounded_session_join_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_session_join_collection(pipeline)?.sink_named_with_config(
        "dual-session-result",
        SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
        format!("dual-session-result:{}", path.display()),
        move || Ok(Box::new(GlobalFoldSink { path: sink_path.clone(), values: Vec::new() })),
    )
}

pub fn dual_unbounded_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_cogroup_collection(pipeline)?.sink_named_with_config(
        "dual-cogroup-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-cogroup-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Stateless fan-in keeps both sources' values and event-time metadata.
pub fn dual_unbounded_flatten_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    main.flatten_named("single-stream-flatten", &[])?
        .flatten_named("dual-flatten", &[side])?
        .reify_timestamps_named("dual-flatten-time")?
        .with_window_named("dual-flatten-window")?
        .map_named("dual-flatten-row", |(window, (time, (key, value)))| {
            format!(
                "{}..{}:{time}:{key}:{value}",
                window.start_ns, window.end_ns
            )
        })
}

pub fn dual_unbounded_flatten_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_flatten_collection(pipeline)?.sink_named_with_config(
        "dual-flatten-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-flatten-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Three independent watermarked sources share a native Flatten boundary.
pub fn triple_unbounded_flatten_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    let third = pipeline
        .test_stream_source(
            "third-flatten-source",
            vec![
                TestStreamEvent::Elements(vec![
                    (("d".to_owned(), 30_i32), 2_000_000),
                    (("e".to_owned(), 40), 5_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::AdvanceWatermark(8_000_000),
            ],
        )?
        .fixed_windows("third-flatten-window", 4_000_000)?;
    main.flatten_named("triple-flatten", &[side, third])?
        .reify_timestamps_named("triple-flatten-time")?
        .with_window_named("triple-flatten-window-out")?
        .map_named("triple-flatten-row", |(window, (time, (key, value)))| {
            format!(
                "{}..{}:{time}:{key}:{value}",
                window.start_ns, window.end_ns
            )
        })
}

pub fn triple_unbounded_flatten_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    triple_unbounded_flatten_collection(pipeline)?.sink_named_with_config(
        "triple-flatten-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("triple-flatten-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Three independently watermarked sources co-group at fixed-window closure.
pub fn triple_unbounded_homogeneous_cogroup_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    let (main, side) = dual_unbounded_sources(pipeline)?;
    let third = pipeline
        .test_stream_source(
            "third-homogeneous-source",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 30_i32), 3_000_000),
                    (("b".to_owned(), 40), 5_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
                TestStreamEvent::Elements(vec![(("late".to_owned(), 99), 0)]),
                TestStreamEvent::AdvanceWatermark(8_000_000),
            ],
        )?
        .fixed_windows("third-homogeneous-window", 4_000_000)?;
    main.co_group_homogeneous_named("triple-homogeneous-cogroup", &[side, third])?
        .reify_timestamps_named("triple-homogeneous-time")?
        .with_window_named("triple-homogeneous-window-out")?
        .map_named("triple-homogeneous-row", |(window, (time, (key, mut buckets)))| {
            for bucket in &mut buckets {
                bucket.sort_unstable();
            }
            format!(
                "{}..{}:{time}:{key}:{buckets:?}",
                window.start_ns, window.end_ns
            )
        })
}

pub fn triple_unbounded_homogeneous_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    triple_unbounded_homogeneous_cogroup_collection(pipeline)?.sink_named_with_config(
        "triple-homogeneous-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("triple-homogeneous-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

pub fn dual_unbounded_join_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    dual_unbounded_join_collection(pipeline)?.sink_named_with_config(
        "dual-join-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("dual-join-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// The signal must finish before fixed-window main rows leave the wait node.
pub fn bounded_wait_on_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    marker: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    let signal = completion_signal(pipeline, marker.clone())?;
    pipeline
        .from_vec("wait-main", vec![(0_i64, 1_i32), (2_000_000, 2)])?
        .timestamps_from_pairs_named("wait-main-time")?
        .fixed_windows("wait-main-window", 2_000_000)?
        .wait_on_named("wait-for-completion", &signal)?
        .map_named("wait-check-completion", move |value| {
            assert!(
                marker.exists(),
                "wait signal did not finish before main data"
            );
            value
        })?
        .with_window_named("wait-row-window")?
        .map_named("wait-row-value", |(window, value)| {
            format!("{}..{}:{value}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "wait-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("wait-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A bounded completion signal gates an unbounded main source.
pub fn unbounded_main_bounded_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    marker: PathBuf,
) -> Result<Sink<String>, Error> {
    let signal = completion_signal(pipeline, marker.clone())?;
    let sink_directory = path.clone();
    pipeline
        .test_stream_source(
            "stream-wait-main",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000), (2, 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(3_000_000),
            ],
        )?
        .wait_on_named("stream-wait-barrier", &signal)?
        .map_named("stream-wait-check", move |value| {
            assert!(marker.exists(), "bounded signal did not complete");
            value
        })?
        .reify_timestamps_named("stream-wait-time")?
        .map_named("stream-wait-row", |(time, value)| format!("{time}:{value}"))?
        .sink_named_with_config(
            "stream-wait-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("stream-wait-contract:{}", path.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Both independent bounded producers must finish before main output begins.
pub fn bounded_multi_wait_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    first_marker: PathBuf,
    second_marker: PathBuf,
) -> Result<Sink<i32>, Error> {
    let first = completion_signal_named(pipeline, "multi-wait-first", first_marker.clone())?;
    let second = completion_signal_named(pipeline, "multi-wait-second", second_marker.clone())?
        .map_named("multi-wait-second-text", |value| value.to_string())?;
    let sink_path = path.clone();
    pipeline
        .from_vec("multi-wait-main", vec![1_i32, 2])?
        .flatten_named("multi-wait-single-flatten", &[])?
        .wait_on_signals_named("multi-wait", &[first.wait_signal(), second.wait_signal()])?
        .map_named("multi-wait-check", move |value| {
            assert!(first_marker.exists(), "first signal did not finish");
            assert!(second_marker.exists(), "second signal did not finish");
            value
        })?
        .sink_named_with_config(
            "multi-wait-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("multi-wait-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A partially emitted signal must fail the job before the main sink commits.
pub fn failing_wait_on_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    let signal = pipeline.bounded_source_with_semantic(
        "wait-failing-signal",
        "tests/prism-failing-wait-signal",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        "wait-failing-signal/v1".into(),
        Some(2),
        || {
            Ok(Box::new(
                vec![
                    Ok(1_i32),
                    Err(Error::execution(
                        "wait-failing-signal",
                        "runner",
                        "injected signal read failure",
                    )),
                ]
                .into_iter(),
            ))
        },
    )?;
    pipeline
        .from_vec("wait-failing-main", vec![1_i32, 2])?
        .wait_on_named("wait-for-failing-signal", &signal)?
        .sink_named_with_config(
            "wait-failing-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("wait-failure-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Two independent sources meet at Flatten and GroupByKey on both runners.
pub fn bounded_multi_source_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    let left = pipeline.from_vec("multi-source-left", vec![(1_i32, 2_i32), (2, 3)])?;
    let right = pipeline.from_vec("multi-source-right", vec![(1_i32, 5_i32), (3, 7)])?;
    left.flatten_named("multi-source-flatten", &[right])?
        .group_by_key_named("multi-source-group")?
        .map_named("multi-source-result", |(key, values)| {
            key * 100 + values.into_iter().sum::<i32>()
        })?
        .sink_named_with_config(
            "multi-source-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("multi-source-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub fn group_time_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    group_time_fixture_with_policy(pipeline, path, None)
}

pub fn group_end_time_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    group_time_fixture_with_policy(pipeline, path, Some(ironbeam_core::WindowOutputTime::EndOfWindow))
}

pub fn group_latest_time_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    group_time_fixture_with_policy(pipeline, path, Some(ironbeam_core::WindowOutputTime::LatestInPane))
}

/// Compare vector, lifted global, and lifted keyed combines in one window.
pub fn combine_output_time_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    policy: ironbeam_core::WindowOutputTime,
) -> Result<Sink<String>, Error> {
    let keyed = pipeline
        .from_vec("combine-time-events", vec![(7_000_000_i64, (1_i32, 7_i32)), (2_000_000, (1, 2))])?
        .timestamps_from_pairs_named("combine-time-assign")?
        .fixed_windows("combine-time-window", 10_000_000)?;
    let keyed = match policy {
        ironbeam_core::WindowOutputTime::EndOfWindow => keyed.end_of_window_output_time_named("combine-time-policy")?,
        ironbeam_core::WindowOutputTime::LatestInPane => keyed.latest_in_pane_output_time_named("combine-time-policy")?,
        ironbeam_core::WindowOutputTime::EarliestInPane => keyed,
    };
    let values = keyed.map_named("combine-time-values", |(_, value)| value)?;
    let vector = values.combine_globally_named("combine-time-vector", |rows| rows.into_iter().sum::<i32>())?
        .reify_timestamps_named("combine-time-vector-stamp")?
        .map_named("combine-time-vector-row", |(time, value)| format!("V:{time}:{value}"))?;
    let lifted = values.combine_globally_lifted_named("combine-time-lifted", ProductionSum)?
        .reify_timestamps_named("combine-time-lifted-stamp")?
        .map_named("combine-time-lifted-row", |(time, value)| format!("G:{time}:{value}"))?;
    let keyed = keyed.combine_values_lifted_named("combine-time-keyed", ProductionSum)?
        .reify_timestamps_named("combine-time-keyed-stamp")?
        .map_named("combine-time-keyed-row", |(time, (key, value))| format!("K:{time}:{key}:{value}"))?;
    let sink_path = path.clone();
    vector.flatten_named("combine-time-merge", &[lifted, keyed])?
        .sink_named_with_config(
            "combine-time-capture",
            SinkSpec { mode: SinkMode::Append, replay: Replay::AtLeastOnce, commit: true },
            format!("combine-time:{}", path.display()),
            move || Ok(Box::new(GlobalFoldSink { path: sink_path.clone(), values: Vec::new() })),
        )
}

fn group_time_fixture_with_policy(
    pipeline: &Pipeline,
    path: PathBuf,
    output_time: Option<ironbeam_core::WindowOutputTime>,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    let values = pipeline
        .from_vec(
            "group-time-events",
            vec![
                (5_000_000_i64, (1_i32, 5_i32)),
                (2_000_000, (1, 2)),
                (7_000_000, (1, 7)),
            ],
        )?
        .timestamps_from_pairs_named("group-time-assign")?
        .fixed_windows("group-time-window", 10_000_000)?;
    let values = match output_time {
        Some(ironbeam_core::WindowOutputTime::EndOfWindow) => {
            values.end_of_window_output_time_named("group-time-output-policy")?
        }
        Some(ironbeam_core::WindowOutputTime::LatestInPane) => {
            values.latest_in_pane_output_time_named("group-time-output-policy")?
        }
        _ => values,
    };
    values
        .group_by_key_named("group-time-group")?
        .reify_timestamps_named("group-time-reify")?
        .with_window_named("group-time-window-context")?
        .map_named("group-time-result", |(window, (timestamp, (_, values)))| {
            i32::try_from(window.end_ns + timestamp).unwrap() + values.into_iter().sum::<i32>()
        })?
        .sink_named_with_config(
            "group-time-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("group-time:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub fn global_fold_fixture(
    pipeline: &Pipeline,
    nonempty: PathBuf,
    empty: PathBuf,
) -> Result<(), Error> {
    let nonempty_sink = nonempty.clone();
    pipeline
        .from_vec("global-numbers", vec![2_i32, 3_i32])?
        .combine_globally_lifted_named("global-sum", ProductionSum)?
        .sink_named_with_config(
            "global-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("global-fold:{}", nonempty.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: nonempty_sink.clone(),
                    values: Vec::new(),
                }))
            },
        )?;
    let empty_sink = empty.clone();
    pipeline
        .from_vec("global-empty", Vec::<i32>::new())?
        .combine_globally_lifted_named("global-empty-sum", ProductionSum)?
        .sink_named_with_config(
            "global-empty-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("global-fold-empty:{}", empty.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: empty_sink.clone(),
                    values: Vec::new(),
                }))
            },
        )?;
    Ok(())
}
