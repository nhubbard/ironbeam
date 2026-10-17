use super::*;

/// Fire two early panes from two independent watermarked sources.
pub fn two_source_count_boundary_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    count_boundary_fixture(pipeline, directory, CountBoundaryMode::Plain)
}

/// Fire a count-or-processing-time pane at the count boundary within one batch.
pub fn two_source_count_or_time_boundary_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    count_boundary_fixture(pipeline, directory, CountBoundaryMode::OrTime)
}

/// Fire the first sequential count pane at the boundary within one batch.
pub fn two_source_count_then_time_boundary_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    count_boundary_fixture(pipeline, directory, CountBoundaryMode::ThenTime)
}

/// Fire exact two-element panes across three independent watermarked sources.
pub fn three_source_count_boundary_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let source = |name: &str, values: Vec<((i32, i32), i64)>| {
        pipeline
            .test_stream_source(
                name,
                vec![
                    TestStreamEvent::Elements(values),
                    TestStreamEvent::AdvanceWatermark(10_000_000),
                ],
            )?
            .fixed_windows(&format!("{name}-window"), 10_000_000)?
            .early_count_trigger_named(&format!("{name}-trigger"), 2)
    };
    let first = source(
        "triple-boundary-first",
        vec![
            ((1, 2), 1_000_000),
            ((1, 3), 2_000_000),
            ((1, 6), 3_000_000),
        ],
    )?;
    let second = source("triple-boundary-second", vec![((1, 4), 4_000_000)])?;
    let third = source(
        "triple-boundary-third",
        vec![((1, 5), 5_000_000), ((1, 7), 6_000_000)],
    )?;
    let sink_directory = directory.clone();
    first
        .co_group_homogeneous_named("triple-boundary-group", &[second, third])?
        .reify_panes_named("triple-boundary-pane")?
        .map_named("triple-boundary-row", |(pane, (key, values))| {
            format!(
                "{key}:{}:{}:{}:{:?}:{}",
                values[0].len(),
                values[1].len(),
                values[2].len(),
                pane.timing,
                pane.index,
            )
        })?
        .sink_named_with_config(
            "triple-boundary-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("triple-boundary:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentRowSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

#[derive(Clone, Copy)]
enum CountBoundaryMode {
    Plain,
    OrTime,
    ThenTime,
}

fn count_boundary_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
    mode: CountBoundaryMode,
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
        .fixed_windows("count-boundary-left-window", 10_000_000)?;
    let left = match mode {
        CountBoundaryMode::Plain => {
            left.early_count_trigger_named("count-boundary-left-trigger", 2)?
        }
        CountBoundaryMode::OrTime => left.count_or_processing_time_trigger_named(
            "count-boundary-left-trigger",
            2,
            5_000_000,
        )?,
        CountBoundaryMode::ThenTime => left.count_then_processing_time_trigger_named(
            "count-boundary-left-trigger",
            2,
            5_000_000,
        )?,
    };
    let right = pipeline
        .test_stream_source(
            "count-boundary-right",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("count-boundary-right-window", 10_000_000)?;
    let right = match mode {
        CountBoundaryMode::Plain => {
            right.early_count_trigger_named("count-boundary-right-trigger", 2)?
        }
        CountBoundaryMode::OrTime => right.count_or_processing_time_trigger_named(
            "count-boundary-right-trigger",
            2,
            5_000_000,
        )?,
        CountBoundaryMode::ThenTime => right.count_then_processing_time_trigger_named(
            "count-boundary-right-trigger",
            2,
            5_000_000,
        )?,
    };
    let sink_directory = directory.clone();
    left.co_group_homogeneous_named("count-boundary-group", &[right])?
        .reify_panes_named("count-boundary-pane")?
        .reify_timestamps_named("count-boundary-time")?
        .map_named("count-boundary-row", |(time, (pane, (key, values)))| {
            format!(
                "{key}:{}:{}:{:?}:{}:{time}",
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
