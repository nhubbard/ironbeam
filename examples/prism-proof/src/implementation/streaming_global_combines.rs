use super::IdempotentRowSink;
use ironbeam_core::{Error, Pipeline, Replay, Sink, SinkMode, SinkSpec, TestStreamEvent};
use ironbeam_transforms::ElementwiseExt;
use std::path::PathBuf;

/// Choose a single global combine or their merged output for the fixture.
#[derive(Clone, Copy)]
pub enum StreamingGlobalCombineRoute {
    All,
    Vector,
    Count,
    Sum,
}

/// Exercise vector and lifted combines in two closed TestStream windows.
pub fn streaming_global_combine_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    route: StreamingGlobalCombineRoute,
) -> Result<Sink<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "streaming-global-combine-source",
            vec![
                TestStreamEvent::Elements(vec![(3_i32, 3_000_000), (1, 1_000_000), (2, 2_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
                TestStreamEvent::Elements(vec![(5, 12_000_000), (6, 13_000_000), (4, 11_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )?
        .fixed_windows("streaming-global-combine-window", 10_000_000)?;
    let vector = source
        .combine_globally_named("streaming-global-combine-vector", |mut values| {
            values.sort();
            values
        })?
        .reify_timestamps_named("streaming-global-combine-vector-time")?
        .with_window_named("streaming-global-combine-vector-window")?
        .map_named(
            "streaming-global-combine-vector-label",
            |(window, (time, values))| {
                format!(
                    "vector:{}:{}:{values:?}",
                    window.start_ns / 1_000_000,
                    time / 1_000_000
                )
            },
        )?;
    let count = source
        .count_globally_named("streaming-global-combine-count")?
        .reify_timestamps_named("streaming-global-combine-count-time")?
        .with_window_named("streaming-global-combine-count-window")?
        .map_named(
            "streaming-global-combine-count-label",
            |(window, (time, value))| {
                format!(
                    "count:{}:{}:{value}",
                    window.start_ns / 1_000_000,
                    time / 1_000_000
                )
            },
        )?;
    let sum = source
        .sum_globally_named("streaming-global-combine-sum")?
        .reify_timestamps_named("streaming-global-combine-sum-time")?
        .with_window_named("streaming-global-combine-sum-window")?
        .map_named(
            "streaming-global-combine-sum-label",
            |(window, (time, value))| {
                format!(
                    "sum:{}:{}:{value}",
                    window.start_ns / 1_000_000,
                    time / 1_000_000
                )
            },
        )?;
    let output = match route {
        StreamingGlobalCombineRoute::All => {
            vector.flatten_named("streaming-global-combine-flatten", &[count, sum])?
        }
        StreamingGlobalCombineRoute::Vector => vector,
        StreamingGlobalCombineRoute::Count => count,
        StreamingGlobalCombineRoute::Sum => sum,
    };
    let directory = path.clone();
    output.sink_named_with_config(
        "streaming-global-combine-sink",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("streaming-global-combine:{}", path.display()),
        move || {
            Ok(Box::new(IdempotentRowSink {
                directory: directory.clone(),
                pending: Vec::new(),
            }))
        },
    )
}

/// Exercise early and on-time count and sum panes with latest-in-pane time.
pub fn streaming_global_combine_panes_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    route: StreamingGlobalCombineRoute,
    accumulating: bool,
) -> Result<Sink<String>, Error> {
    let source = pipeline
        .test_stream_source(
            "streaming-global-panes-source",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000), (2, 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(5_000_000),
                TestStreamEvent::Elements(vec![(3, 6_000_000)]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .fixed_windows("streaming-global-panes-window", 10_000_000)?
        .latest_in_pane_output_time_named("streaming-global-panes-output-time")?;
    let source = if accumulating {
        source.accumulating_count_trigger_named("streaming-global-panes-trigger", 2)?
    } else {
        source.early_count_trigger_named("streaming-global-panes-trigger", 2)?
    };
    let count = source
        .count_globally_named("streaming-global-panes-count")?
        .reify_timestamps_named("streaming-global-panes-count-time")?
        .reify_panes_named("streaming-global-panes-count-pane")?
        .map_named(
            "streaming-global-panes-count-label",
            |(pane, (time, value))| {
                format!(
                    "count:{:?}:{}:{}:{value}",
                    pane.timing,
                    pane.index,
                    time / 1_000_000
                )
            },
        )?;
    let sum = source
        .sum_globally_named("streaming-global-panes-sum")?
        .reify_timestamps_named("streaming-global-panes-sum-time")?
        .reify_panes_named("streaming-global-panes-sum-pane")?
        .map_named(
            "streaming-global-panes-sum-label",
            |(pane, (time, value))| {
                format!(
                    "sum:{:?}:{}:{}:{value}",
                    pane.timing,
                    pane.index,
                    time / 1_000_000
                )
            },
        )?;
    let output = match route {
        StreamingGlobalCombineRoute::All => {
            count.flatten_named("streaming-global-panes-flatten", &[sum])?
        }
        StreamingGlobalCombineRoute::Count => count,
        StreamingGlobalCombineRoute::Sum => sum,
        StreamingGlobalCombineRoute::Vector => {
            return Err(Error::graph(
                "streaming-global-panes",
                "vector route is not defined",
            ));
        }
    };
    let directory = path.clone();
    output.sink_named_with_config(
        "streaming-global-panes-sink",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("streaming-global-panes:{accumulating}:{}", path.display()),
        move || {
            Ok(Box::new(IdempotentRowSink {
                directory: directory.clone(),
                pending: Vec::new(),
            }))
        },
    )
}
