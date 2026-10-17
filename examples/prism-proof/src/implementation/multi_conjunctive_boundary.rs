use super::*;

/// Group two sources after a timer becomes ready before a count threshold.
pub fn two_source_count_and_time_boundary_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let first = pipeline
        .test_stream_source(
            "conjunctive-boundary-first",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(5_000_000),
                TestStreamEvent::Elements(vec![
                    ((1, 3), 2_000_000),
                    ((1, 4), 3_000_000),
                    ((1, 5), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("conjunctive-boundary-first-window", 10_000_000)?
        .count_and_processing_time_trigger_named(
            "conjunctive-boundary-first-trigger",
            2,
            5_000_000,
        )?;
    let second = pipeline
        .test_stream_source(
            "conjunctive-boundary-second",
            vec![TestStreamEvent::AdvanceWatermark(10_000_000)],
        )?
        .fixed_windows("conjunctive-boundary-second-window", 10_000_000)?
        .count_and_processing_time_trigger_named(
            "conjunctive-boundary-second-trigger",
            2,
            5_000_000,
        )?;
    let sink_directory = directory.clone();
    first
        .co_group_homogeneous_named("conjunctive-boundary-group", &[second])?
        .reify_panes_named("conjunctive-boundary-pane")?
        .map_named("conjunctive-boundary-row", |(pane, (key, values))| {
            format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
        })?
        .sink_named_with_config(
            "conjunctive-boundary-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("conjunctive-boundary:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
