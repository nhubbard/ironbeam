use std::collections::BTreeMap;

use ironbeam_core::{
    Element, ElementMetadata, Envelope, PaneInfo, Pipeline, TestStreamEvent, TimedStateResult,
    TimerFiring, TimerMutation, UserTimerDomain, UserTimerRequest, Version, Window,
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
    assert!(fired.timers.is_empty());
    assert_eq!(registry.checkpoint_stateful_value("timer").unwrap(), before);
    assert!(registry.apply_many("timer", input(2)).unwrap().is_empty());
    let snapshot = registry.checkpoint_stateful_value("timer").unwrap();

    let (_, restored) = pipeline.snapshot().unwrap();
    restored.restore_stateful_value("timer", &snapshot).unwrap();
    assert_eq!(
        restored.checkpoint_stateful_value("timer").unwrap(),
        snapshot
    );
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
