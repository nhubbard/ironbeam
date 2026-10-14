/// Exercise singleton, map, and multimap views through one portable graph.
pub fn side_view_shapes_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let main = pipeline.from_vec(
        "side-shapes-main",
        vec!["a".to_owned(), "b".to_owned(), "missing".to_owned()],
    )?;
    let singleton = pipeline.from_vec("side-shapes-singleton", vec![100_i32])?;
    let map = pipeline.from_vec(
        "side-shapes-map",
        vec![("a".to_owned(), 10_i32), ("b".to_owned(), 20)],
    )?;
    let multimap = pipeline.from_vec(
        "side-shapes-multimap",
        vec![
            ("a".to_owned(), 1_i32),
            ("a".to_owned(), 2),
            ("b".to_owned(), 3),
        ],
    )?;
    let single_rows =
        main.map_with_singleton_side_named("side-shapes-single", &singleton, |key, value| {
            format!("single:{key}:{value}")
        })?;
    let map_rows = main.map_with_map_side_named("side-shapes-lookup", &map, |key, values| {
        format!(
            "map:{key}:{}",
            values.get(&key).copied().unwrap_or_default()
        )
    })?;
    let multimap_rows =
        main.map_with_multimap_side_named("side-shapes-many", &multimap, |key, values| {
            let mut values = values.get(&key).cloned().unwrap_or_default();
            values.sort_unstable();
            format!("multi:{key}:{values:?}")
        })?;
    let sink_path = path.clone();
    single_rows
        .flatten_named("side-shapes-merge", &[map_rows, multimap_rows])?
        .sink_named_with_config(
            "side-shapes-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("side-shapes-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Apply three typed side views independently in each fixed event-time window.
pub fn windowed_side_view_shapes_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let main = pipeline
        .from_vec(
            "window-shapes-main",
            vec![
                (1_000_000_i64, ("a".to_owned(), 1_i32)),
                (3_000_000, ("a".to_owned(), 2)),
                (5_000_000, ("b".to_owned(), 3)),
            ],
        )?
        .timestamps_from_pairs_named("window-shapes-main-time")?
        .fixed_windows("window-shapes-main-window", 2_000_000)?;
    let singleton = pipeline
        .from_vec(
            "window-shapes-singleton",
            vec![(0_i64, 10_i32), (2_000_000, 20), (4_000_000, 30)],
        )?
        .timestamps_from_pairs_named("window-shapes-singleton-time")?
        .fixed_windows("window-shapes-singleton-window", 2_000_000)?;
    let map = pipeline
        .from_vec(
            "window-shapes-map",
            vec![
                (0_i64, ("a".to_owned(), 10_i32)),
                (2_000_000, ("a".to_owned(), 20)),
                (4_000_000, ("b".to_owned(), 30)),
            ],
        )?
        .timestamps_from_pairs_named("window-shapes-map-time")?
        .fixed_windows("window-shapes-map-window", 2_000_000)?;
    let multimap = pipeline
        .from_vec(
            "window-shapes-multimap",
            vec![
                (0_i64, ("a".to_owned(), 1_i32)),
                (0, ("a".to_owned(), 2)),
                (2_000_000, ("a".to_owned(), 3)),
                (4_000_000, ("b".to_owned(), 4)),
            ],
        )?
        .timestamps_from_pairs_named("window-shapes-multimap-time")?
        .fixed_windows("window-shapes-multimap-window", 2_000_000)?;
    let single_rows = main.map_with_windowed_singleton_side_named(
        "window-shapes-single",
        &singleton,
        |(key, value), side| format!("single:{key}:{value}:{side}"),
    )?;
    let map_rows = main.map_with_windowed_map_side_named(
        "window-shapes-lookup",
        &map,
        |(key, value), view| {
            format!(
                "map:{key}:{value}:{}",
                view.get(&key).copied().unwrap_or_default()
            )
        },
    )?;
    let multimap_rows = main.map_with_windowed_multimap_side_named(
        "window-shapes-many",
        &multimap,
        |(key, value), view| {
            let mut values = view.get(&key).cloned().unwrap_or_default();
            values.sort_unstable();
            format!("multi:{key}:{value}:{values:?}")
        },
    )?;
    let sink_path = path.clone();
    single_rows
        .flatten_named("window-shapes-merge", &[map_rows, multimap_rows])?
        .sink_named_with_config(
            "window-shapes-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("window-shapes-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A duplicate key in one window must fail before a sink commits.
pub fn windowed_duplicate_map_side_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let main = pipeline
        .from_vec(
            "duplicate-window-main",
            vec![(1_000_000_i64, "a".to_owned())],
        )?
        .timestamps_from_pairs_named("duplicate-window-main-time")?
        .fixed_windows("duplicate-window-main-window", 2_000_000)?;
    let side = pipeline
        .from_vec(
            "duplicate-window-side",
            vec![(0_i64, ("a".to_owned(), 1_i32)), (0, ("a".to_owned(), 2))],
        )?
        .timestamps_from_pairs_named("duplicate-window-side-time")?
        .fixed_windows("duplicate-window-side-window", 2_000_000)?;
    let sink_path = path.clone();
    main.map_with_windowed_map_side_named("duplicate-window-lookup", &side, |key, view| {
        format!("{key}:{}", view.get(&key).copied().unwrap_or_default())
    })?
    .sink_named_with_config(
        "duplicate-window-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("duplicate-window-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
