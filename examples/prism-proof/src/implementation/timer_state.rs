use super::{production_state_source, IdempotentStateLabelSink, IdempotentValueSink};
use ironbeam_core::{
    Error, Pipeline, Replay, Sink, SinkMode, SinkSpec, TestStreamEvent, TimedStateResult,
    UserTimerDomain, UserTimerRequest, Version,
};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Two keyed event timers shared by Direct and the pinned local Prism worker.
pub fn streaming_event_timer_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "event-timer-source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 3_i32), 0), ((2, 4), 0)]),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )?
        .stateful_timer_map_named::<i32, i32>(
            "event-timer-state",
            "prism-proof/event-timer-state",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                if event.dynamic_tag != "flush" || event.fire_timestamp_ns != 5_000_000 {
                    return Err(Error::incompatible("unexpected event timer firing"));
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )?
        .map_named("event-timer-label", |(key, value)| format!("{key}:{value}"))?
        .sink_named_with_config(
            "event-timer-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("event-timer-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Processing-clock timer shared by Direct and the pinned local Prism worker.
pub fn streaming_processing_timer_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "processing-timer-source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 3_i32), 2_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(6_000_000),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .stateful_timer_map_named::<i32, i32>(
            "processing-timer-state",
            "prism-proof/processing-timer-state",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "process".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                if event.domain != UserTimerDomain::ProcessingTime
                    || event.output_timestamp_ns != 2_000_000
                {
                    return Err(Error::incompatible("unexpected processing timer firing"));
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )?
        .map_named("processing-timer-label", |(key, value)| {
            format!("{key}:{value}")
        })?
        .sink_named_with_config(
            "processing-timer-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("processing-timer-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Production event timer armed before a supervised execution child is replaced.
pub fn production_recovered_event_timer_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    production_state_source(pipeline)?
        .stateful_timer_map_named::<i32, i32>(
            "production-event-timer-state",
            "prism-proof/production-recovered-event-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, prior, _| {
                Ok(TimedStateResult {
                    state: Some(prior.unwrap_or(0) + value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 2_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                if event.dynamic_tag != "flush" || event.fire_timestamp_ns != 2_000_000 {
                    return Err(Error::incompatible("unexpected recovered event timer"));
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )?
        .map_named("production-event-timer-label", |(key, total)| {
            format!("{key}:{total}")
        })?
        .sink_named_with_config(
            "production-event-timer-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("production-event-timer-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Fixed-window timers with an output timestamp before the firing deadline.
pub fn streaming_windowed_timer_hold_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    windowed_timer_hold_fixture(pipeline, directory, false)
}

/// One event in two overlapping sliding windows, each with its own timer.
pub fn streaming_sliding_timer_hold_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    windowed_timer_hold_fixture(pipeline, directory, true)
}

fn windowed_timer_hold_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
    sliding: bool,
) -> Result<Sink<(i64, i32)>, Error> {
    let sink_directory = directory.clone();
    let events = if sliding {
        vec![
            TestStreamEvent::Elements(vec![((1_i32, 3_i32), 7_000_000)]),
            TestStreamEvent::AdvanceWatermark(20_000_000),
        ]
    } else {
        vec![
            TestStreamEvent::Elements(vec![((1_i32, 3_i32), 0), ((1, 4), 20_000_000)]),
            TestStreamEvent::AdvanceWatermark(30_000_000),
        ]
    };
    let source = pipeline.test_stream_source("windowed-timer-source", events)?;
    let windowed = if sliding {
        source.sliding_windows("windowed-timer-assign", 10_000_000, 5_000_000, 0)?
    } else {
        source.fixed_windows("windowed-timer-assign", 10_000_000)?
    };
    let semantic_id = if sliding {
        "prism-proof/sliding-timer-hold"
    } else {
        "prism-proof/windowed-timer-hold"
    };
    windowed
        .stateful_timer_map_named::<i32, (i64, i32)>(
            "windowed-timer-state",
            semantic_id,
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            move |_, value, _, metadata| {
                let window = metadata
                    .assigned_window
                    .ok_or_else(|| Error::incompatible("windowed timer has no interval"))?;
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: if sliding {
                            window.end_ns - 1_000_000
                        } else {
                            window.start_ns + 5_000_000
                        },
                        output_timestamp_ns: Some(if sliding {
                            7_000_000
                        } else {
                            window.start_ns + 2_000_000
                        }),
                    }],
                })
            },
            move |_, state, event| {
                let window = event
                    .window
                    .ok_or_else(|| Error::incompatible("windowed timer firing has no interval"))?;
                let hold = if sliding {
                    7_000_000
                } else {
                    window.start_ns + 2_000_000
                };
                let fire = if sliding {
                    window.end_ns - 1_000_000
                } else {
                    window.start_ns + 5_000_000
                };
                if event.fire_timestamp_ns != fire || event.output_timestamp_ns != hold {
                    return Err(Error::incompatible("windowed timer metadata differs"));
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs: state
                        .into_iter()
                        .map(|value| {
                            let value = if sliding {
                                value + i32::try_from(window.start_ns / 5_000_000).unwrap()
                            } else {
                                value
                            };
                            (hold, value)
                        })
                        .collect(),
                    timers: Vec::new(),
                })
            },
        )?
        .map_named("windowed-timer-unkey", |(_, value)| value)?
        .sink_named_with_config(
            "windowed-timer-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("windowed-timer-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
