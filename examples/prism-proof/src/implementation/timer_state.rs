use super::IdempotentStateLabelSink;
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
