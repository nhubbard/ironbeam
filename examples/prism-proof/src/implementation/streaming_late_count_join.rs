/// A count trigger fires a typed join's late pane after two new main values.
pub fn late_count_join_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    let main = pipeline
        .test_stream_source(
            "late-count-main",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![((1, 3), 3_000_000)]),
                TestStreamEvent::Elements(vec![((1, 5), 5_000_000)]),
                TestStreamEvent::Elements(vec![((1, 7), 7_000_000)]),
                TestStreamEvent::AdvanceWatermark(15_000_000),
                TestStreamEvent::Elements(vec![((1, 9), 9_000_000)]),
            ],
        )?
        .fixed_windows("late-count-main-window", 10_000_000)?
        .allowed_lateness_named("late-count-main-policy", 5_000_000)?
        .late_count_trigger_named("late-count-main-trigger", 2)?;
    let side = pipeline
        .test_stream_source(
            "late-count-side",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::AdvanceWatermark(11_000_000),
                TestStreamEvent::AdvanceWatermark(12_000_000),
                TestStreamEvent::AdvanceWatermark(15_000_000),
            ],
        )?
        .fixed_windows("late-count-side-window", 10_000_000)?
        .allowed_lateness_named("late-count-side-policy", 5_000_000)?
        .late_count_trigger_named("late-count-side-trigger", 2)?;
    main.join_full_named("late-count-join", &side)?
        .reify_panes_named("late-count-join-pane")?
        .map_named("late-count-join-row", |(pane, (key, pair))| {
            format!(
                "{key}:{pair:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
}

pub fn late_count_join_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    late_count_join_collection(pipeline)?.sink_named_with_config(
        "late-count-join-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("late-count-join-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
