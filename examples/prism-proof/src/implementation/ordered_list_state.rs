use super::{production_state_source, IdempotentStateLabelSink, WindowedStateLabelSink};
use ironbeam_core::{
    BagStateMutation, Error, OrderedListStateMutation, Pipeline, Replay, Sink, SinkMode, SinkSpec,
    TestStreamEvent, Version,
};
use std::path::PathBuf;

/// A shared bag-state stage feeds two bounded sinks through coded fanout.
pub fn bounded_stateful_fanout_fixture(
    pipeline: &Pipeline,
    first: PathBuf,
    second: PathBuf,
) -> Result<(), Error> {
    let shared = pipeline
        .from_vec(
            "stateful-fanout-pairs",
            vec![(1_i32, 10_i32), (1, 20), (2, 30), (1, 40)],
        )?
        .stateful_bag_map_named::<i32, String>(
            "stateful-fanout-bag",
            "prism-proof/stateful-fanout-bag",
            Version { major: 1, minor: 0 },
            |key, value, prior| {
                Ok((
                    BagStateMutation::Append(vec![value]),
                    format!("{key}:{value}:{}", prior.len()),
                ))
            },
        )?
        .map_named("stateful-fanout-label", |(_, label)| label)?;
    let first_directory = first.clone();
    shared.sink_named_with_config(
        "stateful-fanout-first",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("stateful-fanout-first:{}", first.display()),
        move || {
            Ok(Box::new(IdempotentStateLabelSink {
                directory: first_directory.clone(),
                pending: Vec::new(),
            }))
        },
    )?;
    let second_directory = second.clone();
    shared
        .map_named("stateful-fanout-second-label", |label| {
            format!("copy:{label}")
        })?
        .sink_named_with_config(
            "stateful-fanout-second",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("stateful-fanout-second:{}", second.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: second_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )?;
    Ok(())
}

/// Shared ordered-list mutation fixture for Direct and the local Prism worker.
pub fn bounded_ordered_list_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .from_vec(
            "ordered-list-input",
            vec![
                (1_i32, 20_i32),
                (1, -10),
                (1, 10),
                (2, 30),
                (1, 10),
                (1, -1),
                (1, 15),
            ],
        )?
        .stateful_ordered_list_map_named::<i32, (i32, Vec<i32>)>(
            "ordered-list-mutate",
            "prism-proof/key-ordered-list-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let seen = prior.iter().map(|(_, item)| *item).collect();
                let mutation = if value == -1 {
                    OrderedListStateMutation::ClearRange {
                        start_ms: 10,
                        end_ms: 11,
                    }
                } else {
                    OrderedListStateMutation::Append(i64::from(value), value)
                };
                Ok((mutation, (value, seen)))
            },
        )?
        .map_named("ordered-list-label", |(key, (value, seen))| {
            format!("{key}:{value}:{seen:?}")
        })?
        .sink_named_with_config(
            "ordered-list-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("ordered-list-mutations:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Two event-time windows retain independent ordered lists across input batches.
pub fn streaming_windowed_ordered_list_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "streaming-ordered-list-input",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 20_i32), 0), ((1, 30), 20_000_000)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![((1, 10), 2_000_000), ((1, 25), 22_000_000)]),
                TestStreamEvent::AdvanceWatermark(30_000_000),
            ],
        )?
        .fixed_windows("streaming-ordered-list-window", 10_000_000)?
        .stateful_ordered_list_map_named::<i32, (i32, Vec<i32>)>(
            "streaming-ordered-list-mutate",
            "prism-proof/streaming-ordered-list-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let seen = prior.iter().map(|(_, item)| *item).collect();
                Ok((
                    OrderedListStateMutation::Append(i64::from(value), value),
                    (value, seen),
                ))
            },
        )?
        .map_named("streaming-ordered-list-label", |(key, (value, seen))| {
            format!("{key}:{value}:{seen:?}")
        })?
        .sink_named_with_config(
            "streaming-ordered-list-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("streaming-windowed-ordered-list:{}", directory.display()),
            move || {
                Ok(Box::new(WindowedStateLabelSink(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                })))
            },
        )
}

/// Production source with an ordered-list cell and idempotent output.
pub fn production_ordered_list_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    production_state_source(pipeline)?
        .stateful_ordered_list_map_named::<i32, (i32, Vec<i32>)>(
            "production-ordered-list-state",
            "prism-proof/production-ordered-list-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let seen = prior.iter().map(|(_, item)| *item).collect();
                Ok((
                    OrderedListStateMutation::Append(i64::from(value), value),
                    (value, seen),
                ))
            },
        )?
        .map_named("production-ordered-list-label", |(key, (value, seen))| {
            format!("{key}:{value}:{seen:?}")
        })?
        .sink_named_with_config(
            "production-ordered-list-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("production-ordered-list:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
