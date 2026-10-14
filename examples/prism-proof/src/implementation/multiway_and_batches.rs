/// Negative event times and an offset fixed window share exact grouped metadata.
pub fn offset_window_group_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "offset-window-source",
            vec![
                (-1_000_000_i64, ("a".to_owned(), 1_i32)),
                (0, ("a".to_owned(), 2)),
                (1_000_000, ("a".to_owned(), 3)),
                (4_000_000, ("a".to_owned(), 4)),
                (7_000_000, ("a".to_owned(), 5)),
            ],
        )?
        .timestamps_from_pairs_named("offset-window-time")?
        .fixed_windows_with_offset("offset-window-assign", 4_000_000, 1_000_000)?
        .group_by_key_named("offset-window-group")?
        .reify_timestamps_named("offset-window-group-time")?
        .with_window_named("offset-window-group-window")?
        .map_named(
            "offset-window-row",
            |(window, (time, (key, mut values)))| {
                values.sort_unstable();
                format!(
                    "{key}|{values:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?
        .sink_named_with_config(
            "offset-window-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("offset-window-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Overlapping windows assign each row to two intervals on both runners.
pub fn sliding_window_group_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "sliding-window-source",
            vec![
                (-1_000_000_i64, ("a".to_owned(), 1_i32)),
                (0, ("a".to_owned(), 2)),
                (1_000_000, ("a".to_owned(), 3)),
                (4_000_000, ("a".to_owned(), 4)),
                (7_000_000, ("a".to_owned(), 5)),
            ],
        )?
        .timestamps_from_pairs_named("sliding-window-time")?
        .sliding_windows("sliding-window-assign", 4_000_000, 2_000_000, 0)?
        .group_by_key_named("sliding-window-group")?
        .reify_timestamps_named("sliding-window-group-time")?
        .with_window_named("sliding-window-group-window")?
        .map_named("sliding-window-row", |(window, (time, (key, mut values)))| {
            values.sort_unstable();
            format!("{key}|{values:?}|{time}|{}|{}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "sliding-window-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("sliding-window-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Rows on different inputs bridge a bounded keyed session before the join.
pub fn bounded_session_join_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    let left = pipeline
        .from_vec(
            "session-join-left-source",
            vec![
                (0_i64, ("a".to_owned(), 1_i32)),
                (8_000_000, ("a".to_owned(), 2)),
                (30_000_000, ("b".to_owned(), 3)),
            ],
        )?
        .timestamps_from_pairs_named("session-join-left-time")?
        .session_windows_named("session-join-left-window", 5_000_000)?;
    let right = pipeline
        .from_vec(
            "session-join-right-source",
            vec![
                (4_000_000_i64, ("a".to_owned(), 10_i32)),
                (20_000_000, ("a".to_owned(), 11)),
                (31_000_000, ("b".to_owned(), 12)),
            ],
        )?
        .timestamps_from_pairs_named("session-join-right-time")?
        .session_windows_named("session-join-right-window", 5_000_000)?;
    left.join_inner_named("session-join", &right)?
        .with_window_named("session-join-bounds")?
        .map_named("session-join-row", |(window, (key, (left, right)))| {
            format!("{key}:{left}:{right}:{}..{}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "session-join-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("session-join-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A late-arriving bridge joins two prior sessions for one key.
pub fn session_group_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "session-source",
            vec![
                (0_i64, ("a".to_owned(), 1_i32)),
                (8_000_000, ("a".to_owned(), 2)),
                (4_000_000, ("a".to_owned(), 3)),
                (13_000_000, ("a".to_owned(), 4)),
                (4_000_000, ("b".to_owned(), 5)),
                (-4_000_000, ("c".to_owned(), 6)),
                (-1_000_000, ("c".to_owned(), 7)),
            ],
        )?
        .timestamps_from_pairs_named("session-time")?
        .session_windows_named("session-group/session", 5_000_000)?
        .group_by_key_named("session-group")?
        .reify_timestamps_named("session-group-time")?
        .with_window_named("session-group-window")?
        .map_named("session-row", |(window, (time, (key, mut values)))| {
            values.sort_unstable();
            format!("{key}|{values:?}|{time}|{}|{}", window.start_ns, window.end_ns)
        })?
        .sink_named_with_config(
            "session-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("session-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Three tagged inputs exercise the portable co-group coder beyond join arity.
pub fn bounded_three_way_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let first = pipeline
        .from_vec(
            "three-cogroup-first",
            vec![
                (1_000_000_i64, ("a".to_owned(), 1_i32)),
                (5_000_000, ("b".to_owned(), 2)),
            ],
        )?
        .timestamps_from_pairs_named("three-cogroup-first-time")?
        .fixed_windows("three-cogroup-first-window", 4_000_000)?;
    let second = pipeline
        .from_vec(
            "three-cogroup-second",
            vec![
                (2_000_000_i64, ("a".to_owned(), 10_i32)),
                (6_000_000, ("c".to_owned(), 30)),
            ],
        )?
        .timestamps_from_pairs_named("three-cogroup-second-time")?
        .fixed_windows("three-cogroup-second-window", 4_000_000)?;
    let third = pipeline
        .from_vec(
            "three-cogroup-third",
            vec![
                (3_000_000_i64, ("a".to_owned(), 100_i32)),
                (7_000_000, ("c".to_owned(), 300)),
            ],
        )?
        .timestamps_from_pairs_named("three-cogroup-third-time")?
        .fixed_windows("three-cogroup-third-window", 4_000_000)?;
    let sink_path = path.clone();
    ironbeam_core::cogroup_by_key_3_named("three-cogroup", &first, &second, &third)?
        .reify_timestamps_named("three-cogroup-time")?
        .with_window_named("three-cogroup-window")?
        .map_named(
            "three-cogroup-label",
            |(window, (time, (key, (a, b, c))))| {
                format!(
                    "{key}|{a:?}|{b:?}|{c:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?
        .sink_named_with_config(
            "three-cogroup-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("three-cogroup-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// The maximum public co-group arity, including one empty side and a side-only key.
pub fn bounded_ten_way_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let a = pipeline.from_vec("ten-cogroup-a", vec![("x".to_owned(), 1_i32)])?;
    let b = pipeline.from_vec("ten-cogroup-b", vec![("x".to_owned(), 2_i32)])?;
    let c = pipeline.from_vec("ten-cogroup-c", vec![("x".to_owned(), 3_i32)])?;
    let d = pipeline.from_vec("ten-cogroup-d", vec![("x".to_owned(), 4_i32)])?;
    let e = pipeline.from_vec("ten-cogroup-e", Vec::<(String, i32)>::new())?;
    let f = pipeline.from_vec("ten-cogroup-f", vec![("x".to_owned(), 6_i32)])?;
    let g = pipeline.from_vec("ten-cogroup-g", vec![("x".to_owned(), 7_i32)])?;
    let h = pipeline.from_vec("ten-cogroup-h", vec![("x".to_owned(), 8_i32)])?;
    let i = pipeline.from_vec("ten-cogroup-i", vec![("x".to_owned(), 9_i32)])?;
    let j = pipeline.from_vec(
        "ten-cogroup-j",
        vec![("x".to_owned(), 10_i32), ("y".to_owned(), 11)],
    )?;
    let sink_path = path.clone();
    ironbeam_core::cogroup_by_key_10_named("ten-cogroup", &a, &b, &c, &d, &e, &f, &g, &h, &i, &j)?
        .map_named("ten-cogroup-counts", |(key, groups)| {
            let counts = [
                groups.0.len(),
                groups.1.len(),
                groups.2.len(),
                groups.3.len(),
                groups.4.len(),
                groups.5.len(),
                groups.6.len(),
                groups.7.len(),
                groups.8.len(),
                groups.9.len(),
            ];
            format!("{key}|{counts:?}")
        })?
        .sink_named_with_config(
            "ten-cogroup-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ten-cogroup-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A bounded side joins and co-groups with a finite-window TestStream.
pub fn mixed_boundedness_join_cogroup_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let live = pipeline
        .test_stream_source(
            "mixed-live",
            vec![
                TestStreamEvent::Elements(vec![(("a".to_owned(), 1_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
            ],
        )?
        .fixed_windows("mixed-live-window", 4_000_000)?;
    let bounded = pipeline
        .from_vec("mixed-bounded", vec![(0_i64, ("a".to_owned(), 10_i32))])?
        .timestamps_from_pairs_named("mixed-bounded-time")?
        .fixed_windows("mixed-bounded-window", 4_000_000)?;
    let grouped = ironbeam_core::cogroup_by_key_2_named("mixed-cogroup", &live, &bounded)?
        .reify_timestamps_named("mixed-cogroup-time")?
        .with_window_named("mixed-cogroup-window")?
        .map_named(
            "mixed-cogroup-row",
            |(window, (time, (key, (left, right))))| {
                format!(
                    "C|{key}|{left:?}|{right:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?;
    let joined = live
        .join_full_named("mixed-join", &bounded)?
        .reify_timestamps_named("mixed-join-time")?
        .with_window_named("mixed-join-window")?
        .map_named(
            "mixed-join-row",
            |(window, (time, (key, (left, right))))| {
                format!(
                    "J|{key}|{left:?}|{right:?}|{time}|{}|{}",
                    window.start_ns, window.end_ns
                )
            },
        )?;
    let sink_path = path.clone();
    grouped
        .flatten_named("mixed-merge", &[joined])?
        .sink_named_with_config(
            "mixed-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("mixed:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// The bounded runner-contract case, shared by the driver and Fn API worker.
struct ReverseBatch;

impl DynOp for ReverseBatch {
    fn apply(&self, input: Partition) -> Partition {
        let mut batch = *input.downcast::<Vec<i32>>().expect("i32 batch");
        batch.reverse();
        Box::new(batch)
    }
}
