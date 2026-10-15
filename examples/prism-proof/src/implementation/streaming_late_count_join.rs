/// A count trigger fires a typed join's late pane after two new main values.
pub fn late_count_join_collection(pipeline: &Pipeline) -> Result<PCollection<String>, Error> {
    late_count_join_kind_collection(pipeline, "full")
}

pub fn late_count_join_kind_collection(
    pipeline: &Pipeline,
    kind: &str,
) -> Result<PCollection<String>, Error> {
    late_count_join_kind_collection_with_mode(pipeline, kind, false)
}

pub fn accumulating_late_join_collection(
    pipeline: &Pipeline,
) -> Result<PCollection<String>, Error> {
    late_count_join_kind_collection_with_mode(pipeline, "full", true)
}

fn late_count_join_kind_collection_with_mode(
    pipeline: &Pipeline,
    kind: &str,
    accumulating: bool,
) -> Result<PCollection<String>, Error> {
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
        .allowed_lateness_named("late-count-main-policy", 5_000_000)?;
    let main = if accumulating {
        main.accumulating_late_count_trigger_named("late-count-main-trigger", 2)?
    } else {
        main.late_count_trigger_named("late-count-main-trigger", 2)?
    };
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
        .allowed_lateness_named("late-count-side-policy", 5_000_000)?;
    let side = if accumulating {
        side.accumulating_late_count_trigger_named("late-count-side-trigger", 2)?
    } else {
        side.late_count_trigger_named("late-count-side-trigger", 2)?
    };
    match kind {
        "inner" => main.join_inner_named("late-count-inner", &side)?
            .reify_panes_named("late-count-inner-pane")?
            .map_named("late-count-inner-row", |(pane, (key, pair))| {
                format!("{key}:{pair:?}:{:?}:{}:{}:{}:{}", pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index)
            }),
        "left" => main.join_left_named("late-count-left", &side)?
            .reify_panes_named("late-count-left-pane")?
            .map_named("late-count-left-row", |(pane, (key, pair))| {
                format!("{key}:{pair:?}:{:?}:{}:{}:{}:{}", pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index)
            }),
        "right" => main.join_right_named("late-count-right", &side)?
            .reify_panes_named("late-count-right-pane")?
            .map_named("late-count-right-row", |(pane, (key, pair))| {
                format!("{key}:{pair:?}:{:?}:{}:{}:{}:{}", pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index)
            }),
        "full" => main.join_full_named("late-count-join", &side)?
            .reify_panes_named("late-count-join-pane")?
            .map_named("late-count-join-row", |(pane, (key, pair))| {
                format!("{key}:{pair:?}:{:?}:{}:{}:{}:{}", pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index)
            }),
        _ => Err(Error::graph("late-count-join-kind", "unknown join kind")),
    }
}

pub fn accumulating_late_join_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    accumulating_late_join_collection(pipeline)?.sink_named_with_config(
        "accumulating-late-join-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("accumulating-late-join-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
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


pub fn late_count_join_kind_fixture(
    pipeline: &Pipeline,
    kind: &str,
    path: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_path = path.clone();
    late_count_join_kind_collection(pipeline, kind)?.sink_named_with_config(
        "late-count-kind-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("late-count-{kind}-result:{}", path.display()),
        move || {
            Ok(Box::new(GlobalFoldSink {
                path: sink_path.clone(),
                values: Vec::new(),
            }))
        },
    )
}
