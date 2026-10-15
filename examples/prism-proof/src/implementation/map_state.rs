use super::IdempotentStateLabelSink;
use ironbeam_core::{
    Error, MapStateMutation, Pipeline, Replay, Sink, SinkMode, SinkSpec, TestStreamEvent, Version,
};
use std::collections::HashMap;
use std::path::PathBuf;

/// Shared bounded map-state mutation fixture for Direct and Prism.
pub fn bounded_map_mutations_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .from_vec(
            "map-input",
            vec![
                (1_i32, 10_i32),
                (1, 20),
                (1, 10),
                (2, 10),
                (1, 0),
                (1, -3),
                (1, -2),
                (1, -1),
                (1, 11),
            ],
        )?
        .stateful_map_map_named::<i32, i32, (i32, i32, i32)>(
            "map-mutate",
            "prism-proof/key-map-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    value,
                    prior.get(&10).copied().unwrap_or(-1),
                    i32::try_from(prior.len()).expect("fixture map fits i32"),
                );
                let mutation = match value {
                    -1 => MapStateMutation::Clear,
                    -2 => MapStateMutation::Replace(HashMap::from([(10, 100), (30, 300)])),
                    -3 => MapStateMutation::Remove(10),
                    0 => MapStateMutation::Keep,
                    value => MapStateMutation::Put(value, value * 10),
                };
                Ok((mutation, observation))
            },
        )?
        .map_named("map-label", |(key, (value, found, size))| {
            format!("{key}:{value}:{found}:{size}")
        })?
        .sink_named_with_config(
            "map-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("map-mutations:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// One map per pipeline key across two unbounded TestStream batches.
pub fn streaming_map_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-map-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 11_i32), 0), ((2, 17), 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![
                    ((1, 11), 2_000_000),
                    ((2, 19), 2_000_000),
                    ((1, 23), 2_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(3_000_000),
            ],
        )?
        .stateful_map_map_named::<i32, i32, (i32, i32, i32)>(
            "streaming-map-membership",
            "prism-proof/streaming-key-map-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let map_key = value % 10;
                let observation = (
                    value,
                    prior.get(&map_key).copied().unwrap_or(-1),
                    i32::try_from(prior.len()).expect("fixture map fits i32"),
                );
                Ok((MapStateMutation::Put(map_key, value), observation))
            },
        )?
        .map_named("streaming-map-label", |(key, (value, old, size))| {
            format!("{key}:{value}:{old}:{size}")
        })?
        .sink_named_with_config(
            "streaming-map-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("streaming-map-state:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
