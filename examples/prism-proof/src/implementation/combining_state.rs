use super::{production_state_source, IdempotentStateLabelSink, WindowedStateLabelSink};
use ironbeam_core::{
    CombiningFunction, CombiningStateMutation, Error, Pipeline, Replay, Sink, SinkMode, SinkSpec,
    TestStreamEvent, Version,
};
use std::path::PathBuf;

/// Shared bounded CombiningState fixture for Direct and pinned local Prism.
pub fn bounded_combining_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .from_vec(
            "combining-state-input",
            vec![(1_i32, 10_i32), (1, 20), (2, 30), (1, -1), (1, -2), (1, 5)],
        )?
        .stateful_combining_map_named::<i32, String>(
            "combining-state-sum",
            "prism-proof/combining-state-sum",
            Version { major: 1, minor: 0 },
            CombiningFunction {
                semantic_id: "prism-proof/checked-sum",
                version: Version { major: 1, minor: 0 },
            },
            |left, right| {
                left.checked_add(right)
                    .ok_or_else(|| Error::incompatible("combining sum overflow"))
            },
            |key, value, prior| {
                let mutation = match value {
                    -1 => CombiningStateMutation::Clear,
                    -2 => CombiningStateMutation::Replace(50),
                    value => CombiningStateMutation::Add(value),
                };
                Ok((
                    mutation,
                    format!("{key}:{value}:{}", prior.copied().unwrap_or_default()),
                ))
            },
        )?
        .map_named("combining-state-label", |(_, label)| label)?
        .sink_named_with_config(
            "combining-state-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("combining-state-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Replay-safe unbounded source with one combining cell across child replacement.
pub fn production_combining_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    production_state_source(pipeline)?
        .stateful_combining_map_named::<i32, String>(
            "production-combining-state",
            "prism-proof/production-combining-state",
            Version { major: 1, minor: 0 },
            CombiningFunction {
                semantic_id: "prism-proof/checked-sum",
                version: Version { major: 1, minor: 0 },
            },
            |left, right| {
                left.checked_add(right)
                    .ok_or_else(|| Error::incompatible("combining sum overflow"))
            },
            |key, value, prior| {
                Ok((
                    CombiningStateMutation::Add(value),
                    format!("{key}:{value}:{}", prior.copied().unwrap_or_default()),
                ))
            },
        )?
        .map_named("production-combining-label", |(_, label)| label)?
        .sink_named_with_config(
            "production-combining-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("production-combining-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Two fixed windows retain independent accumulators across source batches.
pub fn streaming_windowed_combining_state_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-combining-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 10_i32), 0), ((1, 30), 20_000_000)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 5), 2_000_000), ((1, 7), 22_000_000)]),
                TestStreamEvent::AdvanceWatermark(30_000_000),
            ],
        )?
        .fixed_windows("streaming-combining-window", 10_000_000)?
        .stateful_combining_map_named::<i32, String>(
            "streaming-combining-sum",
            "prism-proof/streaming-combining-sum",
            Version { major: 1, minor: 0 },
            CombiningFunction {
                semantic_id: "prism-proof/checked-sum",
                version: Version { major: 1, minor: 0 },
            },
            |left, right| {
                left.checked_add(right)
                    .ok_or_else(|| Error::incompatible("combining sum overflow"))
            },
            |key, value, prior| {
                Ok((
                    CombiningStateMutation::Add(value),
                    format!("{key}:{value}:{}", prior.copied().unwrap_or_default()),
                ))
            },
        )?
        .map_named("streaming-combining-label", |(_, label)| label)?
        .sink_named_with_config(
            "streaming-combining-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("streaming-combining-result:{}", directory.display()),
            move || {
                Ok(Box::new(WindowedStateLabelSink(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                })))
            },
        )
}
