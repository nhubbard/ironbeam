use super::IdempotentRowSink;
use ironbeam_core::{Error, Pipeline, Replay, Sink, SinkMode, SinkSpec, TestStreamEvent};
use ironbeam_transforms::ElementwiseExt;
use std::path::PathBuf;

/// Select a global-window early-trigger branch or their merged output.
#[derive(Clone, Copy)]
pub enum GlobalTriggerRoute {
    All,
    Vector,
    Count,
    Sum,
    Ordered,
    Latest,
}

/// Choose how the global window emits its first pane.
#[derive(Clone, Copy)]
pub enum GlobalTriggerPolicy {
    Count,
    ProcessingTime,
}

/// Exercise finite panes in an unbounded global window with an early trigger.
pub fn streaming_global_trigger_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    route: GlobalTriggerRoute,
    trigger: GlobalTriggerPolicy,
) -> Result<Sink<String>, Error> {
    let source = pipeline.test_stream_source(
        "global-trigger-source",
        vec![
            TestStreamEvent::Elements(vec![(2_i32, 2_000_000), (3, 3_000_000)]),
            TestStreamEvent::AdvanceProcessingTime(1_000_000),
            TestStreamEvent::Elements(vec![(4, 4_000_000)]),
        ],
    )?;
    let source = match trigger {
        GlobalTriggerPolicy::Count => source.early_count_trigger_named("global-trigger", 2)?,
        GlobalTriggerPolicy::ProcessingTime => {
            source.early_processing_time_trigger_named("global-trigger", 1_000_000)?
        }
    };
    let vector = source
        .combine_globally_named("global-trigger-vector", |mut values| {
            values.sort();
            values
        })?
        .reify_timestamps_named("global-trigger-vector-time")?
        .reify_panes_named("global-trigger-vector-pane")?
        .map_named("global-trigger-vector-label", |(pane, (time, values))| {
            format!(
                "vector:{:?}:{}:{}:{values:?}",
                pane.timing,
                pane.index,
                time / 1_000_000
            )
        })?;
    let count = source
        .count_globally_named("global-trigger-count")?
        .reify_timestamps_named("global-trigger-count-time")?
        .reify_panes_named("global-trigger-count-pane")?
        .map_named("global-trigger-count-label", |(pane, (time, value))| {
            format!(
                "count:{:?}:{}:{}:{value}",
                pane.timing,
                pane.index,
                time / 1_000_000
            )
        })?;
    let sum = source
        .sum_globally_named("global-trigger-sum")?
        .reify_timestamps_named("global-trigger-sum-time")?
        .reify_panes_named("global-trigger-sum-pane")?
        .map_named("global-trigger-sum-label", |(pane, (time, value))| {
            format!(
                "sum:{:?}:{}:{}:{value}",
                pane.timing,
                pane.index,
                time / 1_000_000
            )
        })?;
    let ordered = source
        .to_list_ordered_by_named("global-trigger-ordered", |value| *value)?
        .reify_timestamps_named("global-trigger-ordered-time")?
        .reify_panes_named("global-trigger-ordered-pane")?
        .map_named("global-trigger-ordered-label", |(pane, (time, values))| {
            format!(
                "ordered:{:?}:{}:{}:{values:?}",
                pane.timing,
                pane.index,
                time / 1_000_000
            )
        })?;
    let latest = source
        .latest_globally_by_named("global-trigger-latest", |value| *value)?
        .reify_timestamps_named("global-trigger-latest-time")?
        .reify_panes_named("global-trigger-latest-pane")?
        .map_named("global-trigger-latest-label", |(pane, (time, value))| {
            format!(
                "latest:{:?}:{}:{}:{value}",
                pane.timing,
                pane.index,
                time / 1_000_000
            )
        })?;
    let output = match route {
        GlobalTriggerRoute::All => {
            vector.flatten_named("global-trigger-flatten", &[count, sum, ordered, latest])?
        }
        GlobalTriggerRoute::Vector => vector,
        GlobalTriggerRoute::Count => count,
        GlobalTriggerRoute::Sum => sum,
        GlobalTriggerRoute::Ordered => ordered,
        GlobalTriggerRoute::Latest => latest,
    };
    let directory = path.clone();
    output.sink_named_with_config(
        "global-trigger-sink",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("global-trigger:{}", path.display()),
        move || {
            Ok(Box::new(IdempotentRowSink {
                directory: directory.clone(),
                pending: Vec::new(),
            }))
        },
    )
}
