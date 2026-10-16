use super::*;

/// Compare fixed, shifted fixed, and sliding side windows with the same main values.
pub fn mapped_side_view_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let main = pipeline
        .from_vec(
            "mapped-side-main-source",
            vec![
                (1_000_000_i64, 10_i32),
                (5_000_000, 20),
                (9_000_000, 30),
                (13_000_000, 40),
                (25_000_000, 50),
            ],
        )?
        .timestamps_from_pairs_named("mapped-side-main-time")?
        .fixed_windows("mapped-side-main-windows", 4_000_000)?;
    let side = pipeline
        .from_vec(
            "mapped-side-values-source",
            vec![
                (1_000_000_i64, 1_i32),
                (6_000_000, 6),
                (11_000_000, 11),
                (16_000_000, 16),
            ],
        )?
        .timestamps_from_pairs_named("mapped-side-values-time")?;
    let fixed_side = side.fixed_windows("mapped-side-fixed-windows", 10_000_000)?;
    let offset_side =
        side.fixed_windows_with_offset("mapped-side-offset-windows", 10_000_000, 2_000_000)?;
    let sliding_side =
        side.sliding_windows("mapped-side-sliding-windows", 10_000_000, 5_000_000, 0)?;
    let gapped_side =
        side.sliding_windows("mapped-side-gapped-windows", 5_000_000, 10_000_000, 0)?;
    let fixed = main
        .map_with_mapped_window_side_named("mapped-side-fixed", &fixed_side, |value, side| {
            format!("fixed:{value}:{}", side.iter().sum::<i32>())
        })?
        .reify_timestamps_named("mapped-side-fixed-time")?
        .map_named("mapped-side-fixed-label", |(timestamp, value)| {
            format!("{timestamp}:{value}")
        })?;
    let sliding = main
        .map_with_mapped_window_side_named("mapped-side-sliding", &sliding_side, |value, side| {
            format!("sliding:{value}:{}", side.iter().sum::<i32>())
        })?
        .reify_timestamps_named("mapped-side-sliding-time")?
        .map_named("mapped-side-sliding-label", |(timestamp, value)| {
            format!("{timestamp}:{value}")
        })?;
    let offset = main
        .map_with_mapped_window_side_named("mapped-side-offset", &offset_side, |value, side| {
            format!("offset:{value}:{}", side.iter().sum::<i32>())
        })?
        .reify_timestamps_named("mapped-side-offset-time")?
        .map_named("mapped-side-offset-label", |(timestamp, value)| {
            format!("{timestamp}:{value}")
        })?;
    let gapped = main
        .map_with_mapped_window_side_named("mapped-side-gapped", &gapped_side, |value, side| {
            format!("gapped:{value}:{}", side.iter().sum::<i32>())
        })?
        .reify_timestamps_named("mapped-side-gapped-time")?
        .map_named("mapped-side-gapped-label", |(timestamp, value)| {
            format!("{timestamp}:{value}")
        })?;
    let sink_path = path.clone();
    fixed
        .flatten_named("mapped-side-flatten", &[sliding, offset, gapped])?
        .sink_named_with_config(
            "mapped-side-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("mapped-side-result:{}", path.display()),
            move || {
                Ok(Box::new(GlobalFoldSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
