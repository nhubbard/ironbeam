use std::collections::BTreeMap;

use ironbeam_core::{
    Error, PaneInfo, StatefulParDoContract, TimerIdentity, TimerMutation, TimerQueue,
    TimerSchedule, UserStateKind, UserTimerDomain, UserTimerFamily, Version, WindowInterval,
};

fn contract() -> StatefulParDoContract {
    StatefulParDoContract {
        semantic_id: "test/timer-callback".into(),
        version: Version { major: 1, minor: 0 },
        key_coder_id: "v5_lp_bytes".into(),
        window_coder_id: "c_iw".into(),
        states: BTreeMap::from([(
            "value".into(),
            UserStateKind::Value {
                coder_id: "v5_lp_bytes".into(),
            },
        )]),
        timers: BTreeMap::from([
            (
                "event".into(),
                UserTimerFamily {
                    domain: UserTimerDomain::EventTime,
                    coder_id: "event_timer".into(),
                },
            ),
            (
                "process".into(),
                UserTimerFamily {
                    domain: UserTimerDomain::ProcessingTime,
                    coder_id: "process_timer".into(),
                },
            ),
        ]),
        on_window_expiration_timer: None,
    }
}

fn identity(family: &str, key: u8, tag: &str) -> TimerIdentity {
    TimerIdentity {
        family_id: family.into(),
        dynamic_tag: tag.into(),
        encoded_key: vec![1, key],
        window: Some(WindowInterval {
            start_ns: 0,
            end_ns: 10_000_000,
        }),
    }
}

const fn schedule(fire_timestamp_ns: i64) -> TimerSchedule {
    TimerSchedule {
        fire_timestamp_ns,
        hold_timestamp_ns: 1_000_000,
        pane: PaneInfo::NO_FIRING,
    }
}

fn validate_key(encoded: &[u8]) -> Result<(), Error> {
    if encoded.len() == 2 && encoded[0] == 1 {
        Ok(())
    } else {
        Err(Error::incompatible("invalid timer key coder bytes"))
    }
}

#[test]
fn timers_replace_clear_and_fire_in_independent_domains() {
    let mut queue = TimerQueue::new(contract(), false).unwrap();
    queue
        .set(identity("event", 2, "b"), schedule(4_000_000))
        .unwrap();
    queue
        .set(identity("event", 1, "a"), schedule(4_000_000))
        .unwrap();
    queue
        .set(identity("event", 0, "z"), schedule(4_000_000))
        .unwrap();
    queue
        .set(identity("event", 2, "b"), schedule(5_000_000))
        .unwrap();
    queue
        .set(identity("process", 3, ""), schedule(2_000_000))
        .unwrap();
    queue
        .set(identity("process", 4, "clear"), schedule(2_000_000))
        .unwrap();
    queue.clear(&identity("process", 4, "clear")).unwrap();
    queue.clear(&identity("event", 9, "absent")).unwrap();

    assert_eq!(queue.len(), 4);
    assert_eq!(
        queue.next_deadline(UserTimerDomain::EventTime),
        Some(4_000_000)
    );
    assert_eq!(
        queue.next_deadline(UserTimerDomain::ProcessingTime),
        Some(2_000_000)
    );
    assert_eq!(queue.take_ready(Some(3_000_000), None).len(), 0);
    let first = queue.take_ready(Some(4_000_000), None);
    assert_eq!(first.len(), 2);
    assert_eq!(first[0].identity, identity("event", 0, "z"));
    assert_eq!(first[1].identity, identity("event", 1, "a"));
    assert_eq!(first[0].domain, UserTimerDomain::EventTime);
    let second = queue.take_ready(None, Some(2_000_000));
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].identity, identity("process", 3, ""));
    assert_eq!(second[0].domain, UserTimerDomain::ProcessingTime);
    assert_eq!(queue.next_deadline(UserTimerDomain::ProcessingTime), None);
    assert_eq!(
        queue.take_ready(Some(5_000_000), None)[0].identity,
        identity("event", 2, "b")
    );
    assert!(queue.is_empty());
}

#[test]
fn checkpoint_restores_slots_and_rejects_corruption_atomically() {
    let mut queue = TimerQueue::new(contract(), false).unwrap();
    queue
        .set(identity("event", 1, "a"), schedule(5_000_000))
        .unwrap();
    queue
        .set(identity("process", 2, "b"), schedule(2_000_000))
        .unwrap();
    let snapshot = queue.checkpoint().unwrap();

    let mut restored = TimerQueue::new(contract(), false).unwrap();
    restored.restore(&snapshot, validate_key).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), snapshot);
    let mut changed_contract = contract();
    changed_contract.version.minor += 1;
    assert!(
        TimerQueue::new(changed_contract, false)
            .unwrap()
            .restore(&snapshot, validate_key)
            .is_err()
    );
    for field in ["version", "global_window"] {
        let mut incompatible: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
        incompatible[field] = if field == "version" {
            serde_json::json!(2)
        } else {
            serde_json::json!(true)
        };
        assert!(
            restored
                .restore(&serde_json::to_vec(&incompatible).unwrap(), validate_key)
                .is_err(),
            "incompatible {field} was accepted"
        );
        assert_eq!(restored.checkpoint().unwrap(), snapshot);
    }

    let mut duplicate: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
    let first = duplicate["entries"][0].clone();
    duplicate["entries"].as_array_mut().unwrap().push(first);
    assert!(
        restored
            .restore(&serde_json::to_vec(&duplicate).unwrap(), validate_key)
            .is_err()
    );
    assert_eq!(restored.checkpoint().unwrap(), snapshot);

    let mut malformed: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
    malformed["entries"][0][0]["window"]["end_ns"] = serde_json::json!(0);
    assert!(
        restored
            .restore(&serde_json::to_vec(&malformed).unwrap(), validate_key)
            .is_err()
    );
    assert_eq!(restored.checkpoint().unwrap(), snapshot);

    let mut malformed_key: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
    malformed_key["entries"][0][0]["encoded_key"] = serde_json::json!([0]);
    assert!(
        restored
            .restore(&serde_json::to_vec(&malformed_key).unwrap(), validate_key)
            .is_err()
    );
    assert_eq!(restored.checkpoint().unwrap(), snapshot);
}

#[test]
fn timer_slots_require_declared_family_window_and_portable_timestamps() {
    let mut finite = TimerQueue::new(contract(), false).unwrap();
    assert!(
        finite
            .set(identity("missing", 1, ""), schedule(1_000_000))
            .is_err()
    );
    assert!(finite.set(identity("event", 1, ""), schedule(1)).is_err());
    let mut fractional_hold = schedule(1_000_000);
    fractional_hold.hold_timestamp_ns = 1;
    assert!(
        finite
            .set(identity("event", 1, ""), fractional_hold)
            .is_err()
    );
    let mut invalid_pane = schedule(1_000_000);
    invalid_pane.pane.index = -1;
    assert!(finite.set(identity("event", 1, ""), invalid_pane).is_err());
    let mut no_window = identity("event", 1, "");
    no_window.window = None;
    assert!(finite.set(no_window.clone(), schedule(1_000_000)).is_err());
    let mut empty_window = identity("event", 1, "");
    empty_window.window = Some(WindowInterval {
        start_ns: 10_000_000,
        end_ns: 10_000_000,
    });
    assert!(finite.clear(&empty_window).is_err());
    assert!(finite.clear(&identity("missing", 1, "")).is_err());

    let mut global = TimerQueue::new(contract(), true).unwrap();
    assert!(
        global
            .set(identity("event", 1, ""), schedule(1_000_000))
            .is_err()
    );
    global.set(no_window, schedule(1_000_000)).unwrap();
    assert_eq!(global.take_ready(Some(1_000_000), None).len(), 1);
}

#[test]
fn callback_mutations_validate_before_changing_any_timer() {
    let mut queue = TimerQueue::new(contract(), false).unwrap();
    queue
        .set(identity("event", 1, "old"), schedule(2_000_000))
        .unwrap();
    let before = queue.checkpoint().unwrap();
    assert!(
        queue
            .apply_mutations([
                TimerMutation::Clear(identity("event", 1, "old")),
                TimerMutation::Set(identity("event", 2, "new"), schedule(3_000_000)),
                TimerMutation::Set(identity("missing", 3, "bad"), schedule(4_000_000)),
            ])
            .is_err()
    );
    assert_eq!(queue.checkpoint().unwrap(), before);

    queue
        .apply_mutations([
            TimerMutation::Clear(identity("event", 1, "old")),
            TimerMutation::Set(identity("event", 2, "new"), schedule(3_000_000)),
            TimerMutation::Set(identity("event", 2, "new"), schedule(5_000_000)),
        ])
        .unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.take_ready(Some(3_000_000), None).len(), 0);
    assert_eq!(
        queue.take_ready(Some(5_000_000), None)[0].identity,
        identity("event", 2, "new")
    );
}
