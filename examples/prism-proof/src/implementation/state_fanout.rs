use super::{production_state_source, IdempotentStateLabelSink};
use ironbeam_core::{Error, MapStateMutation, Pipeline, Replay, Sink, SinkMode, SinkSpec, Version};
use std::path::PathBuf;

/// Feed independent value and map cells from one replayable unbounded source.
pub fn production_state_fanout_fixture(
    pipeline: &Pipeline,
    value_directory: PathBuf,
    map_directory: PathBuf,
) -> Result<(Sink<String>, Sink<String>), Error> {
    let source = production_state_source(pipeline)?;
    let value = source
        .stateful_value_map_named::<i32, (i32, i32)>(
            "fanout-value-state",
            "prism-proof/fanout-value-state",
            Version { major: 1, minor: 0 },
            |_, item, prior| {
                let count = prior.unwrap_or(0) + 1;
                Ok((Some(count), (item, count)))
            },
        )?
        .map_named("fanout-value-label", |(key, (item, count))| {
            format!("{key}:{item}:{count}")
        })?;
    let map = source
        .stateful_map_map_named::<i32, i32, (i32, i32)>(
            "fanout-map-state",
            "prism-proof/fanout-map-state",
            Version { major: 1, minor: 0 },
            |_, item, prior| {
                let previous = prior.get(&0).copied().unwrap_or(-1);
                Ok((MapStateMutation::Put(0, item), (item, previous)))
            },
        )?
        .map_named("fanout-map-label", |(key, (item, previous))| {
            format!("{key}:{item}:{previous}")
        })?;
    let value_sink_directory = value_directory.clone();
    let value_sink = value.sink_named_with_config(
        "fanout-value-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("fanout-value:{}", value_directory.display()),
        move || {
            Ok(Box::new(IdempotentStateLabelSink {
                directory: value_sink_directory.clone(),
                pending: Vec::new(),
            }))
        },
    )?;
    let map_sink_directory = map_directory.clone();
    let map_sink = map.sink_named_with_config(
        "fanout-map-result",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("fanout-map:{}", map_directory.display()),
        move || {
            Ok(Box::new(IdempotentStateLabelSink {
                directory: map_sink_directory.clone(),
                pending: Vec::new(),
            }))
        },
    )?;
    Ok((value_sink, map_sink))
}
