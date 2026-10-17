use super::*;

/// Place a repeated count and its final threshold in one TestStream batch.
pub fn or_finally_same_batch_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
    accumulating: bool,
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
        .fixed_windows("or-finally-batch-window", 10_000_000)?;
    let source = if accumulating {
        source.accumulating_count_or_finally_trigger_named("or-finally-batch-trigger", 2, 7)?
    } else {
        source.count_or_finally_trigger_named("or-finally-batch-trigger", 2, 7)?
    };
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
            format!("or-finally-batch:{accumulating}:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Keep final-count cursors independent across keys and fixed windows.
pub fn or_finally_multi_scope_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "or-finally-scope-source",
            vec![
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 1_i32), 1_000_000),
                    (("b".to_owned(), 10), 1_000_000),
                    (("a".to_owned(), 2), 2_000_000),
                    (("b".to_owned(), 11), 2_000_000),
                ]),
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 3), 3_000_000),
                    (("a".to_owned(), 4), 4_000_000),
                    (("b".to_owned(), 12), 3_000_000),
                ]),
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 5), 5_000_000),
                    (("a".to_owned(), 6), 6_000_000),
                    (("a".to_owned(), 7), 7_000_000),
                ]),
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 8), 8_000_000),
                    (("a".to_owned(), 9), 9_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![
                    (("a".to_owned(), 20), 11_000_000),
                    (("a".to_owned(), 21), 12_000_000),
                    (("a".to_owned(), 22), 13_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("or-finally-scope-window", 10_000_000)?
        .count_or_finally_trigger_named("or-finally-scope-trigger", 2, 7)?;
    let sink_directory = directory.clone();
    source
        .group_by_key_named("or-finally-scope-group")?
        .with_window_named("or-finally-scope-interval")?
        .reify_panes_named("or-finally-scope-pane")?
        .map_named("or-finally-scope-row", |(pane, (window, (key, values)))| {
            format!(
                "{}..{}:{key}:{values:?}:{:?}:{}",
                window.start_ns, window.end_ns, pane.timing, pane.index
            )
        })?
        .sink_named_with_config(
            "or-finally-scope-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("or-finally-scope:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
