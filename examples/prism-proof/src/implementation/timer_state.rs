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
                if std::env::var_os("IRONBEAM_V5_PRISM_KILL_TIMER_CALLBACK").is_some() {
                    super::await_supervised_source_replacement(Some(&[1]))?;
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

/// One key replaces its event timer while another clears its timer. Only the
/// replacement should fire after the watermark passes both old deadlines.
pub fn streaming_replaced_event_timer_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "replaced-timer-source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 1_i32), 0), ((2, 4), 0)]),
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 1_000_000), ((2, -4), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(3_000_000),
                TestStreamEvent::AdvanceWatermark(5_000_000),
            ],
        )?
        .stateful_timer_map_named::<i32, String>(
            "replaced-timer-state",
            "prism-proof/replaced-event-timer-state",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, prior, _| {
                let timers = if value < 0 {
                    vec![UserTimerRequest::Clear {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                    }]
                } else {
                    vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: if value == 1 { 3_000_000 } else { 5_000_000 },
                        output_timestamp_ns: Some(0),
                    }]
                };
                Ok(TimedStateResult {
                    state: Some(prior.unwrap_or(0) + value),
                    outputs: Vec::new(),
                    timers,
                })
            },
            |key, state, event| {
                if *key != 1
                    || event.dynamic_tag != "flush"
                    || event.fire_timestamp_ns != 5_000_000
                    || event.output_timestamp_ns != 0
                {
                    return Err(Error::incompatible(
                        "cancelled or replaced event timer fired",
                    ));
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![format!(
                        "{}:{}",
                        state.unwrap_or_default(),
                        event.fire_timestamp_ns
                    )],
                    timers: Vec::new(),
                })
            },
        )?
        .map_named("replaced-timer-label", |(key, value)| {
            format!("{key}:{value}")
        })?
        .sink_named_with_config(
            "replaced-timer-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("replaced-timer-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// One shared event timer feeds two idempotent sink branches.
pub fn streaming_event_timer_fanout_fixture(
    pipeline: &Pipeline,
    first: PathBuf,
    second: PathBuf,
) -> Result<(Sink<String>, Sink<String>), Error> {
    let shared = pipeline
        .test_stream_source(
            "event-timer-fanout-source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 3_i32), 0)]),
                TestStreamEvent::Elements(vec![((1_i32, 4_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(5_000_000),
            ],
        )?
        .stateful_timer_map_named::<i32, i32>(
            "event-timer-fanout-state",
            "prism-proof/event-timer-fanout-state",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, state, _| {
                Ok(TimedStateResult {
                    state: Some(state.unwrap_or(0) + value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: Some(1_000_000),
                    }],
                })
            },
            |_, state, event| {
                if event.dynamic_tag != "flush" || event.output_timestamp_ns != 1_000_000 {
                    return Err(Error::incompatible("unexpected fanout timer firing"));
                }
                if std::env::var_os("IRONBEAM_V5_PRISM_KILL_FANOUT_TIMER_CALLBACK").is_some() {
                    super::await_supervised_source_replacement(Some(&[1]))?;
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )?
        .map_named("event-timer-fanout-label", |(key, value)| {
            format!("{key}:{value}")
        })?;
    let first_directory = first.clone();
    let first_sink = shared.sink_named_with_config(
        "event-timer-fanout-first",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        format!("event-timer-fanout-first:{}", first.display()),
        move || {
            Ok(Box::new(IdempotentStateLabelSink {
                directory: first_directory.clone(),
                pending: Vec::new(),
            }))
        },
    )?;
    let second_directory = second.clone();
    let second_sink = shared
        .map_named("event-timer-fanout-copy", |label| format!("copy:{label}"))?
        .sink_named_with_config(
            "event-timer-fanout-second",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("event-timer-fanout-second:{}", second.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: second_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )?;
    Ok((first_sink, second_sink))
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
                if std::env::var_os("IRONBEAM_V5_PRISM_KILL_PROCESSING_TIMER_CALLBACK").is_some() {
                    super::await_supervised_source_replacement(Some(&[1]))?;
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

/// A processing timer starts four milliseconds after the first element's
/// runner clock. A second element arrives after three milliseconds and must
/// enter the same state cell before the timer fires.
pub fn streaming_relative_processing_timer_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "relative-processing-source",
            vec![
                TestStreamEvent::AdvanceProcessingTime(2_000_000),
                TestStreamEvent::Elements(vec![((1_i32, 1_i32), 1_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(3_000_000),
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 2_000_000)]),
                TestStreamEvent::AdvanceProcessingTime(2_000_000),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )?
        .stateful_timer_map_with_clock_named::<i32, i32>(
            "relative-processing-state",
            "prism-proof/relative-processing-state",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, value, prior, _, processing_time_ns| {
                let timers = if prior.is_none() {
                    let now = processing_time_ns.ok_or_else(|| {
                        Error::incompatible("processing clock missing from element callback")
                    })?;
                    let deadline = now
                        .checked_add(4_000_000)
                        .ok_or_else(|| Error::incompatible("processing timer deadline overflow"))?;
                    let remainder = deadline.rem_euclid(1_000_000);
                    let deadline = if remainder == 0 {
                        deadline
                    } else {
                        deadline.checked_add(1_000_000 - remainder).ok_or_else(|| {
                            Error::incompatible("processing timer deadline overflow")
                        })?
                    };
                    vec![UserTimerRequest::Set {
                        family_id: "process".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: deadline,
                        output_timestamp_ns: Some(1_000_000),
                    }]
                } else {
                    Vec::new()
                };
                Ok(TimedStateResult {
                    state: Some(prior.unwrap_or(0) + value),
                    outputs: Vec::new(),
                    timers,
                })
            },
            |_, state, event| {
                if event.domain != UserTimerDomain::ProcessingTime
                    || event.output_timestamp_ns != 1_000_000
                    || state != Some(3)
                {
                    return Err(Error::incompatible("relative processing timer fired early"));
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![3],
                    timers: Vec::new(),
                })
            },
        )?
        .map_named("relative-processing-label", |(key, value)| {
            format!("{key}:{value}")
        })?
        .sink_named_with_config(
            "relative-processing-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("relative-processing-result:{}", directory.display()),
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

/// Two same-key events in two overlapping sliding windows, each with one timer.
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
            TestStreamEvent::Elements(vec![
                ((1_i32, 3_i32), 7_000_000),
                ((1_i32, 4_i32), 8_000_000),
            ]),
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
            move |_, value, state, metadata| {
                let window = metadata
                    .assigned_window
                    .ok_or_else(|| Error::incompatible("windowed timer has no interval"))?;
                Ok(TimedStateResult {
                    state: Some(if sliding {
                        state.unwrap_or(0) + value
                    } else {
                        value
                    }),
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
                if std::env::var_os("IRONBEAM_V5_PRISM_KILL_WINDOWED_TIMER_CALLBACK").is_some() {
                    super::await_supervised_source_replacement(Some(&[1]))?;
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

/// Runner-owned expiration callback when a finite global stream completes.
pub fn streaming_global_window_expiration_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<String>, Error> {
    let sink_directory = directory.clone();
    pipeline
        .test_stream_source(
            "global-expiration-source",
            vec![
                TestStreamEvent::AdvanceWatermark(5_000_000),
                TestStreamEvent::Elements(vec![((1_i32, 3_i32), 6_000_000)]),
            ],
        )?
        .stateful_timer_map_with_window_expiration_named::<(i64, i32), (i64, i32)>(
            "global-expiration-state",
            "prism-proof/global-window-expiration-state",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("expiry".into(), UserTimerDomain::EventTime)]),
            "expiry",
            |_, value, _, metadata| {
                Ok(TimedStateResult {
                    state: Some((metadata.timestamp_ns, value)),
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
            |_, state, event| {
                if event.family_id != "expiry" || event.window.is_some() {
                    return Err(Error::incompatible("global expiration metadata differs"));
                }
                let value = state.ok_or_else(|| {
                    Error::incompatible("global expiration callback lost its keyed state")
                })?;
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![value],
                    timers: Vec::new(),
                })
            },
        )?
        .map_named(
            "global-expiration-time-pair",
            |(key, (timestamp, value))| (timestamp, (key, value)),
        )?
        .timestamps_from_pairs_in_window_named("global-expiration-timestamp")?
        .map_named("global-expiration-label", |(key, value)| {
            format!("{key}:{value}")
        })?
        .sink_named_with_config(
            "global-expiration-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("global-expiration-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentStateLabelSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}

/// Runner-owned expiration callbacks for two disjoint fixed windows.
pub fn streaming_window_expiration_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    fixed_window_expiration_fixture(pipeline, directory, 0, false, None)
}

/// Runner-owned expiration callbacks for two offset fixed windows.
pub fn streaming_offset_window_expiration_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    fixed_window_expiration_fixture(pipeline, directory, 2_000_000, false, None)
}

/// Runner-owned expiration callbacks for two keys in each fixed window.
pub fn streaming_multi_key_window_expiration_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    fixed_window_expiration_fixture(pipeline, directory, 0, true, None)
}

/// Two distinct keys in different fixed windows from one source batch.
pub fn streaming_same_batch_distinct_key_expiration_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    fixed_window_expiration_fixture(pipeline, directory, 0, false, Some(2))
}

/// One key in two fixed windows from one source batch.
pub fn streaming_same_key_batch_expiration_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
) -> Result<Sink<(i64, i32)>, Error> {
    fixed_window_expiration_fixture(pipeline, directory, 0, false, Some(1))
}

fn fixed_window_expiration_fixture(
    pipeline: &Pipeline,
    directory: PathBuf,
    offset_ns: i64,
    multiple_keys: bool,
    same_batch_second_key: Option<i32>,
) -> Result<Sink<(i64, i32)>, Error> {
    let sink_directory = directory.clone();
    let events = if let Some(second_key) = same_batch_second_key {
        vec![
            TestStreamEvent::Elements(vec![((1_i32, 3_i32), 0), ((second_key, 6_i32), 20_000_000)]),
            TestStreamEvent::AdvanceWatermark(31_000_000),
        ]
    } else {
        vec![
            TestStreamEvent::Elements(if multiple_keys {
                vec![((1_i32, 3_i32), 0), ((2_i32, 5_i32), 0)]
            } else {
                vec![((1_i32, 3_i32), 0)]
            }),
            TestStreamEvent::AdvanceWatermark(if offset_ns == 0 {
                11_000_000
            } else {
                3_000_000
            }),
            TestStreamEvent::Elements(if multiple_keys {
                vec![((1_i32, 4_i32), 20_000_000), ((2_i32, 6_i32), 20_000_000)]
            } else {
                vec![((1_i32, 4_i32), 20_000_000)]
            }),
            TestStreamEvent::AdvanceWatermark(if offset_ns == 0 {
                31_000_000
            } else {
                23_000_000
            }),
        ]
    };
    let source = pipeline.test_stream_source("window-expiration-source", events)?;
    let windowed = if offset_ns == 0 {
        source.fixed_windows("window-expiration-assign", 10_000_000)?
    } else {
        source.fixed_windows_with_offset("window-expiration-assign", 10_000_000, offset_ns)?
    };
    let semantic_id = if let Some(second_key) = same_batch_second_key {
        if second_key == 1 {
            "prism-proof/same-batch-same-key-expiration-state"
        } else {
            "prism-proof/same-batch-distinct-key-expiration-state"
        }
    } else if multiple_keys {
        "prism-proof/multi-key-window-expiration-state"
    } else if offset_ns == 0 {
        "prism-proof/window-expiration-state"
    } else {
        "prism-proof/offset-window-expiration-state"
    };
    windowed
        .stateful_timer_map_with_window_expiration_named::<i32, (i64, i32)>(
            "window-expiration-state",
            semantic_id,
            Version { major: 1, minor: 0 },
            BTreeMap::from([("expiry".into(), UserTimerDomain::EventTime)]),
            "expiry",
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
            |_, state, event| {
                let window = event
                    .window
                    .ok_or_else(|| Error::incompatible("expiration has no interval"))?;
                let timestamp = window.end_ns - 1_000_000;
                if event.family_id != "expiry"
                    || event.fire_timestamp_ns != timestamp
                    || event.output_timestamp_ns != timestamp
                {
                    return Err(Error::incompatible("window-expiration metadata differs"));
                }
                if std::env::var_os("IRONBEAM_V5_PRISM_KILL_EXPIRATION_CALLBACK").is_some() {
                    super::await_supervised_source_replacement(Some(&[1]))?;
                }
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().map(|value| (timestamp, value)).collect(),
                    timers: Vec::new(),
                })
            },
        )?
        .map_named("window-expiration-unkey", |(_, value)| value)?
        .sink_named_with_config(
            "window-expiration-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("window-expiration-result:{}", directory.display()),
            move || {
                Ok(Box::new(IdempotentValueSink {
                    directory: sink_directory.clone(),
                    pending: Vec::new(),
                }))
            },
        )
}
