use super::{production_state_source, IdempotentStateLabelSink, WindowedStateLabelSink};
use ironbeam_core::{
    Error, MultimapStateMutation, Pipeline, Replay, Sink, SinkMode, SinkSpec, TestStreamEvent,
    Version,
};
use std::collections::HashMap;
use std::path::PathBuf;

/// Shared bounded multimap-state mutation fixture for Direct and Prism.
pub fn bounded_multimap_mutations_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .from_vec(
            "multimap-input",
            vec![
                (1_i32, 10_i32),
                (1, 10),
                (1, 20),
                (2, 10),
                (1, -4),
                (1, -1),
                (1, -2),
                (1, -3),
                (1, 11),
            ],
        )?
        .stateful_multimap_map_named::<i32, i32, (i32, i32, i32)>(
            "multimap-mutate",
            "prism-proof/key-multimap-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    value,
                    i32::try_from(prior.get(&10).map_or(0, Vec::len)).expect("fixture fits i32"),
                    i32::try_from(prior.values().map(Vec::len).sum::<usize>())
                        .expect("fixture fits i32"),
                );
                let mutation = match value {
                    -1 => MultimapStateMutation::Remove(10),
                    -2 => MultimapStateMutation::Replace(HashMap::from([(10, vec![7, 7, 9])])),
                    -3 => MultimapStateMutation::Clear,
                    -4 => MultimapStateMutation::Keep,
                    value => MultimapStateMutation::Append(10, value),
                };
                Ok((mutation, observation))
            },
        )?
        .map_named("multimap-label", |(key, (value, count, total))| {
            format!("{key}:{value}:{count}:{total}")
        })?
        .sink_named_with_config(
            "multimap-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("multimap-mutations:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// One multimap per pipeline key across two unbounded TestStream batches.
pub fn streaming_multimap_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-multimap-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 10_i32), 0), ((2, 17), 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![
                    ((1, 10), 2_000_000),
                    ((2, 19), 2_000_000),
                    ((1, 20), 2_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(3_000_000),
            ],
        )?
        .stateful_multimap_map_named::<i32, i32, (i32, i32, i32)>(
            "streaming-multimap-count",
            "prism-proof/streaming-key-multimap-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let map_key = value % 10;
                let observation = (
                    value,
                    i32::try_from(prior.get(&map_key).map_or(0, Vec::len))
                        .expect("fixture fits i32"),
                    i32::try_from(prior.values().map(Vec::len).sum::<usize>())
                        .expect("fixture fits i32"),
                );
                Ok((MultimapStateMutation::Append(map_key, value), observation))
            },
        )?
        .map_named(
            "streaming-multimap-label",
            |(key, (value, count, total))| format!("{key}:{value}:{count}:{total}"),
        )?
        .sink_named_with_config(
            "streaming-multimap-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("streaming-multimap-state:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Two fixed windows keep distinct multimaps for the same pipeline key.
pub fn streaming_windowed_multimap_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-windowed-multimap-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 11_i32), 0), ((1, 17), 20_000_000)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 11), 2_000_000), ((1, 17), 22_000_000)]),
                TestStreamEvent::AdvanceWatermark(30_000_000),
            ],
        )?
        .fixed_windows("streaming-windowed-multimap-window", 10_000_000)?
        .stateful_multimap_map_named::<i32, i32, (i32, i32)>(
            "streaming-windowed-multimap-count",
            "prism-proof/streaming-windowed-key-multimap-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    value,
                    i32::try_from(prior.get(&value).map_or(0, Vec::len)).expect("fixture fits i32"),
                );
                Ok((MultimapStateMutation::Append(value, value), observation))
            },
        )?
        .map_named(
            "streaming-windowed-multimap-label",
            |(key, (value, count))| format!("{key}:{value}:{count}"),
        )?
        .sink_named_with_config(
            "streaming-windowed-multimap-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("streaming-windowed-multimap-state:{}", directory.display()),
            move || {
                Ok(Box::new(WindowedStateLabelSink(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                })))
            },
        )
}

/// Production unbounded source with duplicate-preserving state and idempotent output.
pub fn production_multimap_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    production_state_source(pipeline)?
        .stateful_multimap_map_named::<i32, i32, (i32, i32)>(
            "production-multimap-state",
            "prism-proof/production-key-multimap-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let map_key = value % 2;
                let observation = (
                    value,
                    i32::try_from(prior.get(&map_key).map_or(0, Vec::len))
                        .expect("fixture fits i32"),
                );
                Ok((MultimapStateMutation::Append(map_key, value), observation))
            },
        )?
        .map_named("production-multimap-label", |(key, (value, count))| {
            format!("{key}:{value}:{count}")
        })?
        .sink_named_with_config(
            "production-multimap-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("production-multimap-state:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
