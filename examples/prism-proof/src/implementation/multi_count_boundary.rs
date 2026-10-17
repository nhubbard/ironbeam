use super::*;

/// Fire two early panes from two independent watermarked sources.
pub fn two_source_count_boundary_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let left = pipeline
        .test_stream_source(
            "count-boundary-left",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, 2_i32), 1_000_000),
                    ((1, 3), 2_000_000),
                    ((1, 6), 3_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("count-boundary-left-window", 10_000_000)?
        .early_count_trigger_named("count-boundary-left-trigger", 2)?;
    let right = pipeline
        .test_stream_source(
            "count-boundary-right",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("count-boundary-right-window", 10_000_000)?
        .early_count_trigger_named("count-boundary-right-trigger", 2)?;
    let sink_directory = directory.clone();
    left.co_group_homogeneous_named("count-boundary-group", &[right])?
        .reify_panes_named("count-boundary-pane")?
        .map_named("count-boundary-row", |(pane, (key, values))| {
            format!(
                "{key}:{}:{}:{:?}:{}",
                values[0].len(),
                values[1].len(),
                pane.timing,
                pane.index
            )
        })?
        .sink_named_with_config(
            "count-boundary-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("count-boundary:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
