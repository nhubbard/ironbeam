pub fn ordered_global_duplicate_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("ordered-duplicate-source", vec![(1_i32, 2_i32), (1, 3)])?
        .fold_globally_ordered_by_named("ordered-duplicate-fold", |value| value.0, OrderedDigits)?
        .sink_named_with_config(
            "ordered-duplicate-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ordered-duplicate:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub fn ordered_keyed_duplicate_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "ordered-keyed-duplicate-source",
            vec![(1_i32, (1_i32, 2_i32)), (1, (1, 3))],
        )?
        .fold_values_ordered_by_named(
            "ordered-keyed-duplicate-fold",
            |value| value.0,
            OrderedDigits,
        )?
        .map_named("ordered-keyed-duplicate-value", |(_, value)| value)?
        .sink_named_with_config(
            "ordered-keyed-duplicate-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ordered-keyed-duplicate:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub fn ordered_batch_duplicate_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec(
            "ordered-batch-duplicate-source",
            vec![(1_i32, (1_i32, 2_i32)), (1, (1, 3))],
        )?
        .map_values_batches_ordered_by_named(
            "ordered-batch-duplicate",
            2,
            |value| value.0,
            |values| values.to_vec(),
        )?
        .map_named("ordered-batch-duplicate-value", |(_, value)| value.1)?
        .sink_named_with_config(
            "ordered-batch-duplicate-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("ordered-batch-duplicate:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

pub fn latest_tie_fixture(
    pipeline: &Pipeline,
    keyed_path: PathBuf,
    global_path: PathBuf,
) -> Result<(Sink<i32>, Sink<i32>), Error> {
    let keyed_sink_path = keyed_path.clone();
    let keyed = pipeline
        .from_vec(
            "latest-tie-keyed-source",
            vec![
                (9_000_000_i64, (1_i32, (1_i32, 1_i32))),
                (9_000_000, (1, (3, 3))),
                (4_000_000, (1, (8, 8))),
                (12_000_000, (1, (2, 5))),
                (9_000_000, (2, (2, 9))),
            ],
        )?
        .timestamps_from_pairs_named("latest-tie-keyed-time")?
        .fixed_windows("latest-tie-keyed-window", 10_000_000)?
        .latest_per_key_by_named("latest-tie-keyed", |value| value.0)?
        .reify_timestamps_named("latest-tie-keyed-result-time")?
        .with_window_named("latest-tie-keyed-result-window")?
        .map_named(
            "latest-tie-keyed-capture-value",
            |(window, (timestamp, (key, (tie, digit))))| {
                i32::try_from(window.start_ns / 1_000_000).unwrap() * 1_000_000
                    + i32::try_from(timestamp / 1_000_000).unwrap() * 10_000
                    + key * 1_000
                    + tie * 100
                    + digit
            },
        )?
        .sink_named_with_config(
            "latest-tie-keyed-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("latest-tie-keyed:{}", keyed_path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: keyed_sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )?;
    let global_sink_path = global_path.clone();
    let global = pipeline
        .from_vec(
            "latest-tie-global-source",
            vec![
                (9_000_000_i64, (1_i32, 1_i32)),
                (9_000_000, (3, 3)),
                (4_000_000, (8, 8)),
                (12_000_000, (2, 5)),
            ],
        )?
        .timestamps_from_pairs_named("latest-tie-global-time")?
        .fixed_windows("latest-tie-global-window", 10_000_000)?
        .latest_globally_by_named("latest-tie-global", |value| value.0)?
        .reify_timestamps_named("latest-tie-global-result-time")?
        .with_window_named("latest-tie-global-result-window")?
        .map_named(
            "latest-tie-global-capture-value",
            |(window, (timestamp, (tie, digit)))| {
                i32::try_from(window.start_ns / 1_000_000).unwrap() * 1_000_000
                    + i32::try_from(timestamp / 1_000_000).unwrap() * 10_000
                    + tie * 100
                    + digit
            },
        )?
        .sink_named_with_config(
            "latest-tie-global-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("latest-tie-global:{}", global_path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: global_sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )?;
    Ok((keyed, global))
}

pub fn latest_tie_duplicate_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    keyed: bool,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    let values = if keyed {
        pipeline
            .from_vec(
                "latest-tie-duplicate-keyed-source",
                vec![
                    (9_000_000_i64, (1_i32, (2_i32, 1_i32))),
                    (9_000_000, (1, (2, 3))),
                ],
            )?
            .timestamps_from_pairs_named("latest-tie-duplicate-keyed-time")?
            .latest_per_key_by_named("latest-tie-duplicate-keyed", |value| value.0)?
            .map_named("latest-tie-duplicate-keyed-value", |(_, value)| value.1)?
    } else {
        pipeline
            .from_vec(
                "latest-tie-duplicate-global-source",
                vec![(9_000_000_i64, (2_i32, 1_i32)), (9_000_000, (2, 3))],
            )?
            .timestamps_from_pairs_named("latest-tie-duplicate-global-time")?
            .latest_globally_by_named("latest-tie-duplicate-global", |value| value.0)?
            .map_named("latest-tie-duplicate-global-value", |value| value.1)?
    };
    values.sink_named_with_config(
        "latest-tie-duplicate-capture",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("latest-tie-duplicate:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

pub fn latest_tie_empty_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("latest-tie-empty-source", Vec::<(i64, (i32, i32))>::new())?
        .timestamps_from_pairs_named("latest-tie-empty-time")?
        .latest_globally_by_named("latest-tie-empty", |value| value.0)?
        .map_named("latest-tie-empty-value", |value| value.1)?
        .sink_named_with_config(
            "latest-tie-empty-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("latest-tie-empty:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
