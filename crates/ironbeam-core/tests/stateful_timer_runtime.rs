use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};

use ironbeam_core::{
    Element, ElementMetadata, Envelope, PCollection, PaneInfo, Pipeline, TestStreamEvent,
    TimedStateResult, TimerFiring, TimerMutation, UserTimerDomain, UserTimerRequest, Version,
    Window,
};

fn input(value: i32) -> Envelope {
    Envelope {
        value: Box::new((1_i32, value)),
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
    }
}

#[test]
fn clock_aware_timer_callbacks_receive_runner_time_without_changing_legacy_invocation() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .stateful_timer_map_with_clock_named::<i32, i64>(
            "timer",
            "tests/clock-aware-timer",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, _, _, _, clock| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![clock.unwrap_or(-1)],
                    timers: Vec::new(),
                })
            },
            |_, _, _| unreachable!(),
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let output = registry
        .invoke_timed_element_at("timer", input(2), None, Some(6_000_000))
        .unwrap();
    assert_eq!(
        *output.outputs[0]
            .value
            .downcast_ref::<(i32, i64)>()
            .unwrap(),
        (1, 6_000_000)
    );
    let output = registry
        .apply_many_at("timer", input(2), Some(7_000_000))
        .unwrap();
    assert_eq!(
        *output[0].value.downcast_ref::<(i32, i64)>().unwrap(),
        (1, 7_000_000)
    );
    let legacy = registry.apply_many("timer", input(2)).unwrap();
    assert_eq!(
        *legacy[0].value.downcast_ref::<(i32, i64)>().unwrap(),
        (1, -1)
    );
}

#[test]
#[allow(clippy::too_many_lines)] // The checkpoint and recovery assertions share one stateful pipeline.
fn timed_state_and_timer_restore_atomically_and_fire_once() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/timer-runtime-recovery",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _: ElementMetadata| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "fire".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, event| {
                assert_eq!(event.dynamic_tag, "fire");
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![state.unwrap()],
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let before = registry.checkpoint_stateful_value("timer").unwrap();
    let change = registry
        .invoke_timed_element("timer", input(2), None)
        .unwrap();
    assert_eq!(change.state, Some(2_i32.encode().unwrap()));
    assert!(change.outputs.is_empty());
    let TimerMutation::Set(identity, schedule) = &change.timers[0] else {
        panic!("element callback did not arm a timer");
    };
    assert!(
        registry
            .invoke_timed_firing(
                "timer",
                &TimerFiring {
                    identity: identity.clone(),
                    schedule: *schedule,
                    domain: UserTimerDomain::ProcessingTime,
                },
                change.state.as_deref(),
            )
            .is_err()
    );
    let fired = registry
        .invoke_timed_firing(
            "timer",
            &TimerFiring {
                identity: identity.clone(),
                schedule: *schedule,
                domain: UserTimerDomain::EventTime,
            },
            change.state.as_deref(),
        )
        .unwrap();
    assert_eq!(fired.state, None);
    assert_eq!(fired.outputs.len(), 1);
    assert_eq!(
        *fired.outputs[0].value.downcast_ref::<(i32, i32)>().unwrap(),
        (1, 2)
    );
    assert_eq!(fired.timers.len(), 0);
    assert_eq!(registry.checkpoint_stateful_value("timer").unwrap(), before);
    assert!(registry.apply_many("timer", input(2)).unwrap().is_empty());
    let snapshot = registry.checkpoint_stateful_value("timer").unwrap();

    let (_, restored) = pipeline.snapshot().unwrap();
    restored.restore_stateful_value("timer", &snapshot).unwrap();
    assert_eq!(
        restored.checkpoint_stateful_value("timer").unwrap(),
        snapshot
    );
    assert!(restored.restore_stateful_value("timer", b"{").is_err());
    for variant in 0..5 {
        let mut corrupt: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
        match variant {
            0 => corrupt["version"] = serde_json::json!(2),
            1 => corrupt["cells"][0][0] = serde_json::json!({"start_ns": 0, "end_ns": 1}),
            2 => corrupt["cells"][0][1] = serde_json::json!([0]),
            3 => {
                let duplicate = corrupt["cells"][0].clone();
                corrupt["cells"].as_array_mut().unwrap().push(duplicate);
            }
            4 => corrupt["timers"] = serde_json::json!([0]),
            _ => unreachable!(),
        }
        assert!(
            restored
                .restore_stateful_value("timer", &serde_json::to_vec(&corrupt).unwrap())
                .is_err(),
            "corrupt checkpoint variant {variant} was accepted"
        );
        assert_eq!(
            restored.checkpoint_stateful_value("timer").unwrap(),
            snapshot
        );
    }
    let mut malformed: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
    malformed["cells"][0][2] = serde_json::json!([1]);
    assert!(
        restored
            .restore_stateful_value("timer", &serde_json::to_vec(&malformed).unwrap())
            .is_err()
    );
    assert_eq!(
        restored.checkpoint_stateful_value("timer").unwrap(),
        snapshot
    );

    let fired = restored
        .drain_stateful_timers("timer", Some(5_000_000), None)
        .unwrap();
    assert_eq!(fired.len(), 1);
    assert_eq!(
        *fired[0].value.downcast_ref::<(i32, i32)>().unwrap(),
        (1, 2)
    );
    assert_eq!(fired[0].timestamp_ns, 5_000_000);
    assert!(
        restored
            .drain_stateful_timers("timer", Some(5_000_000), None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn invalid_timer_request_does_not_change_value_cell() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/invalid-timer-request",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: vec![value],
                    timers: vec![UserTimerRequest::Set {
                        family_id: "missing".into(),
                        dynamic_tag: String::new(),
                        fire_timestamp_ns: 1_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, _, _| unreachable!(),
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let before = registry.checkpoint_stateful_value("timer").unwrap();
    assert!(registry.apply_many("timer", input(2)).is_err());
    assert_eq!(registry.checkpoint_stateful_value("timer").unwrap(), before);
}

#[test]
#[allow(clippy::too_many_lines)] // Valid and invalid firings use the same registered callback.
fn registered_processing_timer_callbacks_keep_explicit_holds_and_validate_firings() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/processing-timer-callback",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("process".into(), UserTimerDomain::ProcessingTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: vec![value],
                    timers: vec![
                        UserTimerRequest::Clear {
                            family_id: "process".into(),
                            dynamic_tag: "old".into(),
                        },
                        UserTimerRequest::Set {
                            family_id: "process".into(),
                            dynamic_tag: "new".into(),
                            fire_timestamp_ns: 5_000_000,
                            output_timestamp_ns: Some(2_000_000),
                        },
                    ],
                })
            },
            |_, state, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![state.unwrap()],
                    timers: vec![UserTimerRequest::Clear {
                        family_id: "process".into(),
                        dynamic_tag: "new".into(),
                    }],
                })
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(
        registry
            .invoke_timed_element(
                "timer",
                Envelope {
                    value: Box::new(2_i32),
                    ..input(2)
                },
                None,
            )
            .is_err()
    );
    assert!(
        registry
            .invoke_timed_element("timer", input(2), Some(&[1]))
            .is_err()
    );
    let change = registry
        .invoke_timed_element("timer", input(2), None)
        .unwrap();
    assert_eq!(change.outputs.len(), 1);
    assert_eq!(change.outputs[0].timestamp_ns, 0);
    assert!(matches!(change.timers[0], TimerMutation::Clear(_)));
    let TimerMutation::Set(identity, schedule) = &change.timers[1] else {
        panic!("processing timer was not set");
    };
    assert_eq!(identity.dynamic_tag, "new");
    assert_eq!(schedule.hold_timestamp_ns, 2_000_000);
    let firing = TimerFiring {
        identity: identity.clone(),
        schedule: *schedule,
        domain: UserTimerDomain::ProcessingTime,
    };
    assert!(
        registry
            .invoke_timed_firing("timer", &firing, Some(&[1]))
            .is_err()
    );
    let fired = registry
        .invoke_timed_firing("timer", &firing, change.state.as_deref())
        .unwrap();
    assert_eq!(fired.outputs.len(), 1);
    assert_eq!(fired.outputs[0].timestamp_ns, 2_000_000);
    assert!(matches!(fired.timers[0], TimerMutation::Clear(_)));

    for variant in 0..3 {
        let mut invalid = firing.clone();
        match variant {
            0 => invalid.identity.family_id = "undeclared".into(),
            1 => invalid.identity.encoded_key = vec![0],
            2 => invalid.schedule.fire_timestamp_ns = 1,
            _ => unreachable!(),
        }
        assert!(
            registry
                .invoke_timed_firing("timer", &invalid, change.state.as_deref())
                .is_err(),
            "invalid firing variant {variant} was accepted"
        );
    }
}

#[test]
fn windowed_timer_state_requires_window_and_expires_at_watermark() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/windowed-timer-state",
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
            |_, _, _| unreachable!(),
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(
        registry
            .invoke_timed_element("timer", input(2), None)
            .is_err()
    );
    let windowed = registry.apply_many("window", input(2)).unwrap();
    assert_eq!(windowed.len(), 1);
    assert!(
        registry
            .apply_many("timer", windowed.into_iter().next().unwrap())
            .unwrap()
            .is_empty()
    );
    let snapshot = registry.checkpoint_stateful_value("timer").unwrap();
    let saved: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
    assert_eq!(saved["cells"].as_array().unwrap().len(), 1);
    let mut malformed = saved;
    malformed["cells"][0][0] = serde_json::json!({"start_ns": 5, "end_ns": 5});
    assert!(
        registry
            .restore_stateful_value("timer", &serde_json::to_vec(&malformed).unwrap())
            .is_err()
    );
    assert_eq!(
        registry.checkpoint_stateful_value("timer").unwrap(),
        snapshot
    );
    registry.expire_stateful_value("timer", Some(0)).unwrap();
    assert_eq!(
        registry.checkpoint_stateful_value("timer").unwrap(),
        snapshot
    );
    registry
        .expire_stateful_value("timer", Some(10_000_000))
        .unwrap();
    let expired: serde_json::Value =
        serde_json::from_slice(&registry.checkpoint_stateful_value("timer").unwrap()).unwrap();
    assert_eq!(expired["cells"].as_array().unwrap().len(), 0);
}

#[test]
fn timer_key_coder_handles_long_and_malformed_length_prefixes() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![(
                ("a".repeat(200), 2_i32),
                0,
            )])],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/long-timer-key",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: String::new(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, _, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let change = registry
        .invoke_timed_element(
            "timer",
            Envelope {
                value: Box::new(("a".repeat(200), 2_i32)),
                ..input(2)
            },
            None,
        )
        .unwrap();
    let TimerMutation::Set(identity, schedule) = &change.timers[0] else {
        panic!("long timer key was not encoded");
    };
    assert_ne!(identity.encoded_key[0] & 0x80, 0);
    for invalid_key in [vec![0x80; 10], vec![2, 1], vec![0x80]] {
        let mut invalid = identity.clone();
        invalid.encoded_key = invalid_key;
        assert!(
            registry
                .invoke_timed_firing(
                    "timer",
                    &TimerFiring {
                        identity: invalid,
                        schedule: *schedule,
                        domain: UserTimerDomain::EventTime,
                    },
                    change.state.as_deref(),
                )
                .is_err()
        );
    }
}

#[test]
fn timer_registration_rejects_unsupported_input_time_contracts() {
    let register = |input: &PCollection<(i32, i32)>, families| {
        input.stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/invalid-timer-time-contract",
            Version { major: 1, minor: 0 },
            families,
            |_, _, _, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
            |_, _, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
        )
    };
    let family = || BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]);
    let pipeline = Pipeline::default();
    let bounded = pipeline.from_vec("bounded", vec![(1_i32, 2_i32)]).unwrap();
    assert!(register(&bounded, family()).is_err());
    let stream = pipeline
        .test_stream_source("source", vec![TestStreamEvent::Elements(vec![((1, 2), 0)])])
        .unwrap();
    assert!(register(&stream, BTreeMap::new()).is_err());
    let sessions = stream
        .session_windows_named("sessions", 10_000_000)
        .unwrap();
    assert!(register(&sessions, family()).is_err());
    let fixed = stream.fixed_windows("fixed", 10_000_000).unwrap();
    let triggered = fixed.early_count_trigger_named("early", 1).unwrap();
    assert!(register(&triggered, family()).is_err());
    let late = fixed.allowed_lateness_named("late", 1_000_000).unwrap();
    assert!(register(&late, family()).is_err());
}

#[test]
fn empty_value_cell_and_rejected_timer_callback_return_typed_errors() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/rejected-timer-callback",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: vec![value],
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, _, _| {
                Err(ironbeam_core::Error::execution(
                    "timer",
                    "test",
                    "callback refused",
                ))
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(registry.apply_many("timer", input(2)).unwrap().len(), 1);
    let checkpoint: serde_json::Value =
        serde_json::from_slice(&registry.checkpoint_stateful_value("timer").unwrap()).unwrap();
    assert_eq!(checkpoint["cells"].as_array().unwrap().len(), 0);
    let error = registry
        .drain_stateful_timers("timer", Some(5_000_000), None)
        .err()
        .expect("timer callback should fail");
    assert!(error.to_string().contains("callback refused"));
}

#[test]
fn poisoned_windowed_timer_cell_returns_recovery_errors() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "timer",
            "tests/poisoned-timer-cell",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, _, _, _| panic!("injected timed cell panic"),
            |_, _, _| unreachable!(),
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let initial = registry.checkpoint_stateful_value("timer").unwrap();
    let windowed = registry.apply_many("window", input(2)).unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            registry.apply_many("timer", windowed.into_iter().next().unwrap())
        }))
        .is_err()
    );
    let windowed = registry.apply_many("window", input(2)).unwrap();
    assert!(
        registry
            .apply_many("timer", windowed.into_iter().next().unwrap())
            .err()
            .expect("poisoned state should fail")
            .to_string()
            .contains("lock poisoned")
    );
    assert!(registry.checkpoint_stateful_value("timer").is_err());
    assert!(registry.restore_stateful_value("timer", &initial).is_err());
    assert!(
        registry
            .expire_stateful_value("timer", Some(10_000_000))
            .is_err()
    );
}

#[test]
fn expiration_timer_and_value_cell_restore_together_before_window_cleanup() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 3_i32), 0)])],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .stateful_timer_map_with_window_expiration_named::<i32, i32>(
            "expiry",
            "tests/checkpointed-window-expiration",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("expire".into(), UserTimerDomain::EventTime)]),
            "expire",
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
            |_, state, event| {
                assert_eq!(event.family_id, "expire");
                assert_eq!(event.fire_timestamp_ns, 9_000_000);
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let windowed = registry.apply_many("window", input(3)).unwrap();
    assert!(
        registry
            .apply_many("expiry", windowed.into_iter().next().unwrap())
            .unwrap()
            .is_empty()
    );
    let snapshot = registry.checkpoint_stateful_value("expiry").unwrap();
    let (_, restored) = pipeline.snapshot().unwrap();
    restored
        .restore_stateful_value("expiry", &snapshot)
        .unwrap();
    assert!(
        restored
            .drain_stateful_timers("expiry", Some(9_000_000), None)
            .unwrap()
            .is_empty()
    );
    let output = restored
        .drain_stateful_timers("expiry", Some(10_000_000), None)
        .unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].timestamp_ns, 9_000_000);
    assert_eq!(
        *output[0].value.downcast_ref::<(i32, i32)>().unwrap(),
        (1, 3)
    );
    restored
        .expire_stateful_value("expiry", Some(10_000_000))
        .unwrap();
    let finished: serde_json::Value =
        serde_json::from_slice(&restored.checkpoint_stateful_value("expiry").unwrap()).unwrap();
    assert_eq!(finished["cells"].as_array().unwrap().len(), 0);
    assert!(
        restored
            .drain_stateful_timers("expiry", Some(11_000_000), None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn expiration_registration_requires_aligned_finite_windows_and_event_family() {
    let register = |input: &PCollection<(i32, i32)>, families, expiration_family_id, key: &str| {
        input.stateful_timer_map_with_window_expiration_named::<i32, i32>(
            key,
            &format!("tests/expiration-contract/{key}"),
            Version { major: 1, minor: 0 },
            families,
            expiration_family_id,
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: Vec::new(),
                })
            },
            |_, state, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
    };
    let pipeline = Pipeline::default();
    let stream = pipeline
        .test_stream_source("source", vec![TestStreamEvent::Elements(vec![((1, 2), 0)])])
        .unwrap();
    let event = || BTreeMap::from([("expire".into(), UserTimerDomain::EventTime)]);
    assert!(register(&stream, event(), "expire", "global").is_err());
    let submillisecond = stream.fixed_windows("tiny", 500_000).unwrap();
    assert!(register(&submillisecond, event(), "expire", "tiny-expiry").is_err());
    let fixed = stream.fixed_windows("fixed", 10_000_000).unwrap();
    assert!(register(&fixed, event(), "missing", "missing-expiry").is_err());
    assert!(
        register(
            &fixed,
            BTreeMap::from([("expire".into(), UserTimerDomain::ProcessingTime)]),
            "expire",
            "process-expiry",
        )
        .is_err()
    );
    let offset = stream
        .fixed_windows_with_offset("offset", 10_000_000, 2_000_000)
        .unwrap();
    assert!(register(&offset, event(), "expire", "offset-expiry").is_ok());
    let unaligned_offset = stream
        .fixed_windows_with_offset("unaligned-offset", 10_000_000, 500_000)
        .unwrap();
    assert!(register(&unaligned_offset, event(), "expire", "bad-offset-expiry").is_err());
    let sliding = stream
        .sliding_windows("sliding", 10_000_000, 5_000_000, 0)
        .unwrap();
    assert!(register(&sliding, event(), "expire", "sliding-expiry").is_err());
    let unaligned_slide = stream
        .sliding_windows("unaligned-slide", 10_000_000, 500_000, 0)
        .unwrap();
    assert!(register(&unaligned_slide, event(), "expire", "bad-slide-expiry").is_err());
}

#[test]
fn user_callback_cannot_set_runner_owned_expiration_timer() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .stateful_timer_map_with_window_expiration_named::<i32, i32>(
            "expiry",
            "tests/reject-user-expiration-mutation",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("expire".into(), UserTimerDomain::EventTime)]),
            "expire",
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "expire".into(),
                        dynamic_tag: String::new(),
                        fire_timestamp_ns: 9_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, _, _| unreachable!(),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let before = registry.checkpoint_stateful_value("expiry").unwrap();
    let windowed = registry.apply_many("window", input(2)).unwrap();
    let error = registry
        .apply_many("expiry", windowed.into_iter().next().unwrap())
        .err()
        .expect("runner-owned family must reject user mutations");
    assert!(error.to_string().contains("runner owned"));
    assert_eq!(
        registry.checkpoint_stateful_value("expiry").unwrap(),
        before
    );
}
