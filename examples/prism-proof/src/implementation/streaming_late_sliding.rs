/// Two overlapping windows close at different watermarks. The first receives
/// a late row while the second still holds that same row for its on-time pane.
pub fn sliding_late_join_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "sliding-late-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 8_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .sliding_windows("sliding-late-main-window", 10_000_000, 5_000_000, 0)?
        .allowed_lateness_named("sliding-late-main-policy", 5_000_000)?;
    let side = pipeline
        .test_stream_source(
            "sliding-late-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .sliding_windows("sliding-late-side-window", 10_000_000, 5_000_000, 0)?
        .allowed_lateness_named("sliding-late-side-policy", 5_000_000)?;
    main.join_full_named("sliding-late-join", &side)?
        .reify_panes_named("sliding-late-pane")?
        .with_window_named("sliding-late-interval")?
        .map_named("sliding-late-row", |(window, (pane, (key, pair)))| {
            format!(
                "{}..{}:{key}:{pair:?}:{:?}:{}:{}:{}:{}",
                window.start_ns,
                window.end_ns,
                pane.timing,
                pane.is_first,
                pane.is_last,
                pane.index,
                pane.nonspeculative_index
            )
        })
}

pub fn sliding_late_join_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    sliding_late_join_collection(pipeline)?.sink_named_with_config(
        "sliding-late-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("sliding-late-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
