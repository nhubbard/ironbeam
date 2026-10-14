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
