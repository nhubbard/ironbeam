/// A user map panic must be a typed execution failure with no committed output.
pub fn panicking_map_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("panicking-map-input", vec![1_i32, 2, 3])?
        .map_named("panicking-map", |value| {
            assert_ne!(value, 2, "injected map failure");
            value * 10
        })?
        .sink_named_with_config(
            "panicking-map-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("panicking-map-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A side-view callback panic must abort the main sink on both runners.
pub fn panicking_side_input_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    let side = pipeline.from_vec("panicking-side-values", vec![10_i32])?;
    pipeline
        .from_vec("panicking-side-main", vec![1_i32, 2])?
        .map_with_side_named("panicking-side-apply", &side, |value, _| {
            assert_ne!(value, 2, "injected side-view failure");
            value
        })?
        .sink_named_with_config(
            "panicking-side-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("panicking-side-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A global combine callback panic must abort its sink after Beam grouping.
pub fn panicking_global_combine_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("panicking-global-values", vec![1_i32, 2])?
        .combine_globally_named("panicking-global-combine", |_: Vec<i32>| -> i32 {
            panic!("injected global combine failure")
        })?
        .sink_named_with_config(
            "panicking-global-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("panicking-global-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A keyed incremental fold callback panic must abort its sink after Beam grouping.
pub fn panicking_keyed_fold_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("panicking-fold-values", vec![(1_i32, 1_i32), (1, 2)])?
        .combine_values_lifted_named("panicking-keyed-fold", PanickingSum)?
        .map_named("panicking-fold-label", |(key, sum)| format!("{key}:{sum}"))?
        .sink_named_with_config(
            "panicking-fold-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("panicking-fold-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Batch boundaries and output values are stable even if the runner shuffles inputs.
pub fn bounded_batch_helpers_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    let byte_rows = pipeline
        .from_vec("batch-byte-input", vec!["aa".to_owned(); 5])?
        .batch_by_size_named("batch-by-bytes", 4, String::len)?
        .map_named("batch-byte-label", |values| {
            format!("bytes:{}", values.join("+"))
        })?;
    let keyed_rows = pipeline
        .from_vec(
            "batch-keyed-input",
            vec![(1_i32, 1_i32), (1, 1), (1, 1), (2, 9), (2, 9)],
        )?
        .group_into_batches_named("batch-by-key", 2)?
        .map_named("batch-key-label", |(key, values)| {
            format!(
                "key:{key}:{}",
                values
                    .into_iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join("+")
            )
        })?;
    byte_rows
        .flatten_named("batch-merge", &[keyed_rows])?
        .sink_named_with_config(
            "batch-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("batch-helper-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// A size-one keyed batch map must reject a changed output length before commit.
pub fn invalid_keyed_batch_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("invalid-keyed-batch-input", vec![(1_i32, 2_i32)])?
        .map_values_batches_named("invalid-keyed-batch", 1, |_| Vec::<i32>::new())?
        .map_named("invalid-keyed-batch-value", |(_, value)| value)?
        .sink_named_with_config(
            "invalid-keyed-batch-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("invalid-keyed-batch-contract:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

/// Fixed-window main rows use one global bounded side view on both runners.
pub fn bounded_side_input_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    let main = pipeline
        .from_vec(
            "side-main",
            vec![(0_i64, 1_i32), (1_000_000, 2), (2_000_000, 3)],
        )?
        .timestamps_from_pairs_named("side-main-time")?
        .fixed_windows("side-main-window", 2_000_000)?;
    let side = pipeline
        .from_vec("side-values", vec![(0_i64, 10_i32), (2_000_000, 20)])?
        .timestamps_from_pairs_named("side-values-time")?
        .fixed_windows("side-values-window", 2_000_000)?;
    main.map_with_side_named("side-add", &side, |value, values| {
        value + values.iter().sum::<i32>()
    })?
    .with_window_named("side-window")?
    .map_named("side-row", |(window, value)| {
        format!("{}..{}:{value}", window.start_ns, window.end_ns)
    })?
    .sink_named_with_config(
        "side-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("side-contract:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// A per-window side view restores each main row's original event timestamp.
pub fn windowed_side_input_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    let main = pipeline
        .from_vec(
            "window-side-main",
            vec![
                (0_i64, 1_i32),
                (1_000_000, 2),
                (2_000_000, 3),
                (4_000_000, 4),
            ],
        )?
        .timestamps_from_pairs_named("window-side-main-time")?
        .fixed_windows("window-side-main-window", 2_000_000)?;
    let side = pipeline
        .from_vec("window-side-values", vec![(0_i64, 10_i32), (2_000_000, 20)])?
        .timestamps_from_pairs_named("window-side-values-time")?
        .fixed_windows("window-side-values-window", 2_000_000)?;
    main.map_with_windowed_side_named("window-side-add", &side, |value, values| {
        value + values.iter().sum::<i32>()
    })?
    .reify_timestamps_named("window-side-result-time")?
    .with_window_named("window-side-result-window")?
    .map_named("window-side-row", |(window, (time, value))| {
        format!("{}..{}:{time}:{value}", window.start_ns, window.end_ns)
    })?
    .sink_named_with_config(
        "window-side-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("window-side-contract:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
