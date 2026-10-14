use beam_proto::pipeline_v1::{state_spec, time_domain, ParDoPayload};
use ironbeam_core::{
    StatefulParDoContract, UserStateKind, UserTimerDomain, UserTimerFamily, Version,
};
use ironbeam_graphx::v5::attach_stateful_contract;
use std::collections::BTreeMap;

fn contract() -> StatefulParDoContract {
    StatefulParDoContract {
        semantic_id: "acme/stateful-fn".into(),
        version: Version { major: 1, minor: 2 },
        key_coder_id: "key".into(),
        window_coder_id: "window".into(),
        states: BTreeMap::from([
            (
                "value".into(),
                UserStateKind::Value {
                    coder_id: "v".into(),
                },
            ),
            (
                "bag".into(),
                UserStateKind::Bag {
                    element_coder_id: "v".into(),
                },
            ),
            (
                "set".into(),
                UserStateKind::Set {
                    element_coder_id: "v".into(),
                },
            ),
            (
                "ordered".into(),
                UserStateKind::OrderedList {
                    element_coder_id: "v".into(),
                },
            ),
            (
                "map".into(),
                UserStateKind::Map {
                    key_coder_id: "map-key".into(),
                    value_coder_id: "v".into(),
                },
            ),
            (
                "multimap".into(),
                UserStateKind::Multimap {
                    key_coder_id: "map-key".into(),
                    value_coder_id: "v".into(),
                },
            ),
            (
                "sum".into(),
                UserStateKind::Combining {
                    accumulator_coder_id: "acc".into(),
                    combine_semantic_id: "acme/sum".into(),
                    combine_version: Version { major: 2, minor: 1 },
                },
            ),
        ]),
        timers: BTreeMap::from([
            (
                "event".into(),
                UserTimerFamily {
                    domain: UserTimerDomain::EventTime,
                    coder_id: "timer-event".into(),
                },
            ),
            (
                "process".into(),
                UserTimerFamily {
                    domain: UserTimerDomain::ProcessingTime,
                    coder_id: "timer-process".into(),
                },
            ),
        ]),
        on_window_expiration_timer: Some("event".into()),
    }
}

#[test]
fn lowers_each_beam_user_state_family_and_timer_domain() {
    let mut payload = ParDoPayload::default();
    let mut requirements = vec!["beam:requirement:pardo:splittable_dofn:v1".into()];
    attach_stateful_contract(&mut payload, &mut requirements, &contract()).unwrap();
    assert_eq!(
        requirements,
        [
            "beam:requirement:pardo:splittable_dofn:v1",
            "beam:requirement:pardo:stateful:v1",
            "beam:requirement:pardo:on_window_expiration:v1",
        ]
    );
    assert_eq!(payload.state_specs.len(), 7);
    assert!(matches!(
        payload.state_specs["value"].spec,
        Some(state_spec::Spec::ReadModifyWriteSpec(_))
    ));
    assert!(matches!(
        payload.state_specs["bag"].spec,
        Some(state_spec::Spec::BagSpec(_))
    ));
    assert!(matches!(
        payload.state_specs["set"].spec,
        Some(state_spec::Spec::SetSpec(_))
    ));
    assert!(matches!(
        payload.state_specs["ordered"].spec,
        Some(state_spec::Spec::OrderedListSpec(_))
    ));
    assert!(matches!(
        payload.state_specs["map"].spec,
        Some(state_spec::Spec::MapSpec(_))
    ));
    assert!(matches!(
        payload.state_specs["multimap"].spec,
        Some(state_spec::Spec::MultimapSpec(_))
    ));
    let Some(state_spec::Spec::CombiningSpec(sum)) = &payload.state_specs["sum"].spec else {
        panic!("expected combining state");
    };
    assert_eq!(sum.accumulator_coder_id, "acc");
    assert_eq!(sum.combine_fn.as_ref().unwrap().urn, "acme/sum");
    assert_eq!(
        serde_json::from_slice::<Version>(&sum.combine_fn.as_ref().unwrap().payload).unwrap(),
        Version { major: 2, minor: 1 }
    );
    assert_eq!(
        payload.state_specs["map"].protocol.as_ref().unwrap().urn,
        "beam:user_state:multimap:v1"
    );
    assert_eq!(
        payload.state_specs["set"].protocol.as_ref().unwrap().urn,
        "beam:user_state:multimap:v1"
    );
    assert_eq!(
        payload.state_specs["ordered"]
            .protocol
            .as_ref()
            .unwrap()
            .urn,
        "beam:user_state:ordered_list:v1"
    );
    assert_eq!(
        payload.state_specs["value"].protocol.as_ref().unwrap().urn,
        "beam:user_state:bag:v1"
    );
    assert_eq!(
        payload.timer_family_specs["event"].time_domain,
        time_domain::Enum::EventTime as i32
    );
    assert_eq!(
        payload.timer_family_specs["process"].time_domain,
        time_domain::Enum::ProcessingTime as i32
    );
    assert_eq!(payload.on_window_expiration_timer_family_spec, "event");
}

#[test]
fn rejects_invalid_or_prepopulated_payload_without_mutating_it() {
    let mut payload = ParDoPayload::default();
    let mut requirements = Vec::new();
    let mut invalid = contract();
    invalid.states.insert(
        "bad".into(),
        UserStateKind::Bag {
            element_coder_id: String::new(),
        },
    );
    assert!(attach_stateful_contract(&mut payload, &mut requirements, &invalid).is_err());
    assert!(payload.state_specs.is_empty());
    assert!(requirements.is_empty());
    attach_stateful_contract(&mut payload, &mut requirements, &contract()).unwrap();
    let previous = payload.clone();
    let previous_requirements = requirements.clone();
    assert!(attach_stateful_contract(&mut payload, &mut requirements, &contract()).is_err());
    assert_eq!(payload, previous);
    assert_eq!(requirements, previous_requirements);
}

#[test]
fn stateful_requirement_is_deduplicated_without_expiration_timer() {
    let mut contract = contract();
    contract.on_window_expiration_timer = None;
    let mut payload = ParDoPayload::default();
    let mut requirements = vec!["beam:requirement:pardo:stateful:v1".into()];
    attach_stateful_contract(&mut payload, &mut requirements, &contract).unwrap();
    assert_eq!(requirements, ["beam:requirement:pardo:stateful:v1"]);
    assert!(payload.on_window_expiration_timer_family_spec.is_empty());
}
