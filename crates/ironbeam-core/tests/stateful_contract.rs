use ironbeam_core::{
    StatefulParDoContract, UserStateKind, UserTimerDomain, UserTimerFamily, Version,
};
use std::collections::BTreeMap;

fn contract() -> StatefulParDoContract {
    StatefulParDoContract {
        semantic_id: "acme/keyed-counter".into(),
        version: Version { major: 1, minor: 0 },
        key_coder_id: "coder:key:v1".into(),
        window_coder_id: "coder:window:v1".into(),
        states: BTreeMap::from([
            (
                "value".into(),
                UserStateKind::Value {
                    coder_id: "coder:i64:v1".into(),
                },
            ),
            (
                "bag".into(),
                UserStateKind::Bag {
                    element_coder_id: "coder:i32:v1".into(),
                },
            ),
            (
                "set".into(),
                UserStateKind::Set {
                    element_coder_id: "coder:string:v1".into(),
                },
            ),
            (
                "ordered".into(),
                UserStateKind::OrderedList {
                    element_coder_id: "coder:i32:v1".into(),
                },
            ),
            (
                "sum".into(),
                UserStateKind::Combining {
                    accumulator_coder_id: "coder:i64:v1".into(),
                    combine_semantic_id: "acme/sum".into(),
                    combine_version: Version { major: 2, minor: 0 },
                },
            ),
            (
                "map".into(),
                UserStateKind::Map {
                    key_coder_id: "coder:string:v1".into(),
                    value_coder_id: "coder:i32:v1".into(),
                },
            ),
            (
                "multi".into(),
                UserStateKind::Multimap {
                    key_coder_id: "coder:string:v1".into(),
                    value_coder_id: "coder:i32:v1".into(),
                },
            ),
        ]),
        timers: BTreeMap::from([
            (
                "event".into(),
                UserTimerFamily {
                    domain: UserTimerDomain::EventTime,
                    coder_id: "coder:event-timer:v1".into(),
                },
            ),
            (
                "process".into(),
                UserTimerFamily {
                    domain: UserTimerDomain::ProcessingTime,
                    coder_id: "coder:processing-timer:v1".into(),
                },
            ),
        ]),
        on_window_expiration_timer: Some("event".into()),
    }
}

#[test]
fn state_and_timer_families_have_a_stable_serialized_contract() {
    let first = contract();
    first.validate().unwrap();
    for (id, kind) in &first.states {
        let expected = match id.as_str() {
            "map" | "multi" => "beam:user_state:multimap:v1",
            "ordered" => "beam:user_state:ordered_list:v1",
            _ => "beam:user_state:bag:v1",
        };
        assert_eq!(kind.protocol_urn(), expected);
    }
    let encoded = serde_json::to_string(&first).unwrap();
    let restored: StatefulParDoContract = serde_json::from_str(&encoded).unwrap();
    assert_eq!(restored, first);
    assert_eq!(serde_json::to_string(&restored).unwrap(), encoded);
    let mut changed = restored;
    changed.version.major += 1;
    assert_ne!(serde_json::to_string(&changed).unwrap(), encoded);
}

#[test]
fn stateful_contract_rejects_missing_identity_coders_and_timer_references() {
    let mut invalid = contract();
    invalid.semantic_id = "local-only".into();
    assert!(invalid.validate().is_err());
    let mut invalid = contract();
    invalid.version.major = 0;
    assert!(invalid.validate().is_err());
    let mut invalid = contract();
    invalid.key_coder_id.clear();
    assert!(invalid.validate().is_err());
    let mut invalid = contract();
    invalid.window_coder_id.clear();
    assert!(invalid.validate().is_err());
    let mut invalid = contract();
    invalid.states.clear();
    invalid.timers.clear();
    assert!(invalid.validate().is_err());
    let mut invalid = contract();
    invalid.on_window_expiration_timer = Some("missing".into());
    assert!(invalid.validate().is_err());
    let mut invalid = contract();
    invalid.on_window_expiration_timer = Some("process".into());
    assert!(invalid.validate().is_err());
}

#[test]
fn stateful_contract_rejects_malformed_state_and_timer_families() {
    for state in [
        UserStateKind::Value {
            coder_id: String::new(),
        },
        UserStateKind::Bag {
            element_coder_id: String::new(),
        },
        UserStateKind::Set {
            element_coder_id: String::new(),
        },
        UserStateKind::OrderedList {
            element_coder_id: String::new(),
        },
        UserStateKind::Map {
            key_coder_id: String::new(),
            value_coder_id: "value".into(),
        },
        UserStateKind::Multimap {
            key_coder_id: "key".into(),
            value_coder_id: String::new(),
        },
        UserStateKind::Combining {
            accumulator_coder_id: "acc".into(),
            combine_semantic_id: "unqualified".into(),
            combine_version: Version { major: 1, minor: 0 },
        },
    ] {
        let mut invalid = contract();
        invalid.states.insert("fault".into(), state);
        assert!(invalid.validate().is_err());
    }
    let mut invalid = contract();
    invalid.states.insert(
        String::new(),
        UserStateKind::Value {
            coder_id: "coder".into(),
        },
    );
    assert!(invalid.validate().is_err());
    let mut invalid = contract();
    invalid.timers.insert(
        "bad".into(),
        UserTimerFamily {
            domain: UserTimerDomain::EventTime,
            coder_id: String::new(),
        },
    );
    assert!(invalid.validate().is_err());
}
