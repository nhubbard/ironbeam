use beam_proto::pipeline_v1::{state_spec, time_domain, ParDoPayload};
use ironbeam_core::{
    Pipeline, StatefulParDoContract, TestStreamEvent, TimedStateResult, UserStateKind,
    UserTimerDomain, UserTimerFamily, UserTimerRequest, Version,
};
use ironbeam_graphx::v5::{attach_stateful_contract, translate_v5, validate_beam_contract};
use prost::Message;
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
fn public_value_state_api_lowers_to_valid_beam_state_contract() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32)])
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "running",
            "example/running-total",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let total = prior.unwrap_or(0) + value;
                Ok((Some(total), total))
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, "example/worker:v5", "value-state-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let transform = components
        .transforms
        .values()
        .find(|t| t.unique_name == "running")
        .unwrap();
    let payload =
        ParDoPayload::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    assert!(payload.state_specs.contains_key("value"));
    assert!(translated
        .pipeline
        .requirements
        .iter()
        .any(|urn| urn == "beam:requirement:pardo:stateful:v1"));
}

#[test]
fn public_combining_state_api_lowers_versioned_merge_to_beam() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32)])
        .unwrap()
        .stateful_combining_map_named::<i32, i32>(
            "running-sum",
            "example/running-sum",
            Version { major: 1, minor: 0 },
            ironbeam_core::CombiningFunction {
                semantic_id: "example/checked-sum",
                version: Version { major: 2, minor: 1 },
            },
            |left, right| Ok(left + right),
            |_, value, prior| {
                Ok((
                    ironbeam_core::CombiningStateMutation::Add(value),
                    prior.copied().unwrap_or_default(),
                ))
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, "example/worker:v5", "combining-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find(|transform| transform.unique_name == "running-sum")
        .unwrap();
    let payload =
        ParDoPayload::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    let Some(state_spec::Spec::CombiningSpec(cell)) = &payload.state_specs["combining"].spec else {
        panic!("expected Beam combining-state spec");
    };
    let merge = cell.combine_fn.as_ref().unwrap();
    assert_eq!(merge.urn, "example/checked-sum");
    assert_eq!(
        serde_json::from_slice::<Version>(&merge.payload).unwrap(),
        Version { major: 2, minor: 1 }
    );
}

#[test]
fn public_timed_state_api_declares_a_standard_timer_coder() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "pairs",
            vec![TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)])],
        )
        .unwrap()
        .stateful_timer_map_named::<i32, i32>(
            "running",
            "example/timed-total",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: String::new(),
                        fire_timestamp_ns: 1_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, _| {
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
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, "example/worker:v5", "timed-state-build").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let transform = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "running")
        .unwrap();
    let payload =
        ParDoPayload::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    let family = &payload.timer_family_specs["event"];
    let coder = &components.coders[&family.timer_family_coder_id];
    assert_eq!(coder.spec.as_ref().unwrap().urn, "beam:coder:timer:v1");
    assert_eq!(coder.component_coder_ids, ["v5_lp_bytes", "c_gw"]);
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
