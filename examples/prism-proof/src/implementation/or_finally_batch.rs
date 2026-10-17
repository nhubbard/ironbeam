use super::*;

/// Place a repeated count and its final threshold in one TestStream batch.
pub fn or_finally_same_batch_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "or-finally-batch-source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 1_i32), 1_000_000), ((1, 2), 2_000_000)]),
                TestStreamEvent::Elements(vec![((1, 3), 3_000_000), ((1, 4), 4_000_000)]),
                TestStreamEvent::Elements(vec![
                    ((1, 5), 5_000_000),
                    ((1, 6), 6_000_000),
                    ((1, 7), 7_000_000),
                ]),
                TestStreamEvent::Elements(vec![((1, 8), 8_000_000), ((1, 9), 9_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("or-finally-batch-window", 10_000_000)?
        .count_or_finally_trigger_named("or-finally-batch-trigger", 2, 7)?;
    let sink_directory = directory.clone();
    source
        .group_by_key_named("or-finally-batch-group")?
        .reify_panes_named("or-finally-batch-pane")?
        .map_named("or-finally-batch-row", |(pane, (key, values))| {
            format!("{key}:{values:?}:{:?}:{}", pane.timing, pane.index)
        })?
        .sink_named_with_config(
            "or-finally-batch-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("or-finally-batch:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
