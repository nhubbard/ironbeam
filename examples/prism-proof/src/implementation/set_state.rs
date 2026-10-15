use super::{production_state_source, IdempotentStateLabelSink, WindowedStateLabelSink};
use ironbeam_core::TestStreamEvent;
use ironbeam_core::{Error, Pipeline, Replay, SetStateMutation, Sink, SinkMode, SinkSpec, Version};
use std::path::PathBuf;

/// Shared bounded set-state mutation fixture for Direct and Prism.
pub fn bounded_set_mutations_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .from_vec(
            "set-input",
            vec![
                (1_i32, 10_i32),
                (1, 10),
                (1, 20),
                (2, 10),
                (1, -1),
                (1, 11),
                (1, -2),
                (1, 10),
                (1, -3),
                (1, 0),
                (1, 10),
            ],
        )?
        .stateful_set_map_named::<i32, (i32, i32, i32)>(
            "set-mutate",
            "prism-proof/key-set-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    value,
                    i32::from(prior.contains(&value)),
                    i32::try_from(prior.len()).expect("fixture set fits i32"),
                );
                let mutation = match value {
                    -1 => SetStateMutation::Clear,
                    -2 => SetStateMutation::Replace(vec![10, 10, 30]),
                    -3 => SetStateMutation::Remove(10),
                    0 => SetStateMutation::Keep,
                    value => SetStateMutation::Insert(value),
                };
                Ok((mutation, observation))
            },
        )?
        .map_named("set-label", |(key, (value, found, size))| {
            format!("{key}:{value}:{found}:{size}")
        })?
        .sink_named_with_config(
            "set-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("set-mutations:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// One keyed set per key across two unbounded TestStream element batches.
pub fn streaming_set_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-set-input",
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
        .stateful_set_map_named::<i32, (i32, i32, i32)>(
            "streaming-set-membership",
            "prism-proof/streaming-key-set-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    value,
                    i32::from(prior.contains(&value)),
                    i32::try_from(prior.len()).expect("fixture set fits i32"),
                );
                Ok((SetStateMutation::Insert(value), observation))
            },
        )?
        .map_named("streaming-set-label", |(key, (value, found, size))| {
            format!("{key}:{value}:{found}:{size}")
        })?
        .sink_named_with_config(
            "streaming-set-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("streaming-set-state:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Two fixed windows keep distinct sets for the same key across unbounded batches.
pub fn streaming_windowed_set_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-windowed-set-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 11_i32), 0), ((1, 17), 20_000_000)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 11), 2_000_000), ((1, 17), 22_000_000)]),
                TestStreamEvent::AdvanceWatermark(30_000_000),
            ],
        )?
        .fixed_windows("streaming-windowed-set-window", 10_000_000)?
        .stateful_set_map_named::<i32, (i32, i32, i32)>(
            "streaming-windowed-set-membership",
            "prism-proof/streaming-windowed-key-set-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    value,
                    i32::from(prior.contains(&value)),
                    i32::try_from(prior.len()).expect("fixture set fits i32"),
                );
                Ok((SetStateMutation::Insert(value), observation))
            },
        )?
        .map_named(
            "streaming-windowed-set-label",
            |(key, (value, found, size))| format!("{key}:{value}:{found}:{size}"),
        )?
        .sink_named_with_config(
            "streaming-windowed-set-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("streaming-windowed-set-state:{}", directory.display()),
            move || {
                Ok(Box::new(WindowedStateLabelSink(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                })))
            },
        )
}

/// Production unbounded source with a keyed set and idempotent output.
pub fn production_set_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    production_state_source(pipeline)?
        .stateful_set_map_named::<i32, (i32, i32, i32)>(
            "production-state-count",
            "prism-proof/production-key-set-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    value,
                    i32::from(prior.contains(&value)),
                    i32::try_from(prior.len()).expect("fixture set fits i32"),
                );
                Ok((SetStateMutation::Insert(value), observation))
            },
        )?
        .map_named("production-state-label", |(key, (value, found, size))| {
            format!("{key}:{value}:{found}:{size}")
        })?
        .sink_named_with_config(
            "production-state-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("production-set-state:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
