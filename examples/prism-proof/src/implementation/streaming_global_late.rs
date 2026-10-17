use super::IdempotentRowSink;
use ironbeam_core::{Error, Pipeline, Replay, Sink, SinkMode, SinkSpec, TestStreamEvent};
use ironbeam_transforms::ElementwiseExt;
use std::path::PathBuf;

/// Select one late global combine or their merged output.
#[derive(Clone, Copy)]
pub enum LateGlobalCombineRoute {
    All,
    Count,
    Sum,
}

/// Exercise on-time and late panes from finite-window global combines.
pub fn streaming_late_global_combine_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    route: LateGlobalCombineRoute,
) -> Result<Sink<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "late-global-source",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000), (2, 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(3, 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("late-global-window", 10_000_000)?
        .allowed_lateness_named("late-global-lateness", 10_000_000)?
        .late_count_trigger_named("late-global-trigger", 1)?;
    let count = source
        .count_globally_named("late-global-count")?
        .reify_timestamps_named("late-global-count-time")?
        .reify_panes_named("late-global-count-pane")?
        .map_named("late-global-count-label", |(pane, (time, value))| {
            format!(
                "count:{:?}:{}:{}:{value}",
                pane.timing,
                pane.index,
                time / 1_000_000
            )
        })?;
    let sum = source
        .sum_globally_named("late-global-sum")?
        .reify_timestamps_named("late-global-sum-time")?
        .reify_panes_named("late-global-sum-pane")?
        .map_named("late-global-sum-label", |(pane, (time, value))| {
            format!(
                "sum:{:?}:{}:{}:{value}",
                pane.timing,
                pane.index,
                time / 1_000_000
            )
        })?;
    let output = match route {
        LateGlobalCombineRoute::All => count.flatten_named("late-global-flatten", &[sum])?,
        LateGlobalCombineRoute::Count => count,
        LateGlobalCombineRoute::Sum => sum,
    };
    let directory = path.clone();
    output.sink_named_with_config(
        "late-global-sink",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("late-global:{}", path.display()),
        move || {
            Ok(Box::new(IdempotentRowSink {
                directory: directory.clone(),
                pending: Vec::new(),
            }))
        },
    )
}
