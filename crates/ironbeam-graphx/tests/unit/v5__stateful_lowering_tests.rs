use super::*;
use ironbeam_core::{Element, StatefulParDoContract, UserStateKind, Version};
use std::collections::BTreeMap;

fn stateful_graph() -> (Graph, Registry) {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32)])
        .unwrap()
        .map_named("running", |pair| pair)
        .unwrap()
        .output("result")
        .unwrap();
    let (mut graph, registry) = pipeline.snapshot().unwrap();
    let contract = StatefulParDoContract {
        semantic_id: "example/running-total".into(),
        version: Version { major: 1, minor: 0 },
        key_coder_id: CODER_V5_LP_BYTES.into(),
        window_coder_id: ids::CODER_GLOBAL_WINDOW.into(),
        states: BTreeMap::from([(
            "total".into(),
            UserStateKind::Value {
                coder_id: CODER_V5_LP_BYTES.into(),
            },
        )]),
        timers: BTreeMap::new(),
        on_window_expiration_timer: None,
    };
    let node = graph.nodes.iter_mut().find(|node| node.key == "running").unwrap();
    node.semantic_id = contract.semantic_id.clone();
    node.semantic_version = contract.version;
    node.operation = Operation::StatefulParDo {
        contract: Box::new(contract),
        state_element: ElementDescriptor {
            type_id: i32::type_identity(),
            coder_id: i32::coder_identity(),
            coder_payload: i32::coder_payload(),
            coder_version: i32::CODER_VERSION,
        },
    };
    (graph, registry)
}

#[test]
fn stateful_pardo_lowers_with_kv_input_state_spec_and_requirement() {
    let (graph, registry) = stateful_graph();
    graph.validate().unwrap();
    let identity = execution_identity(&graph, &registry, "stateful-build").unwrap();
    let (beam, _) = lower(
        &graph,
        &["pairs".into(), "running".into(), "result".into()],
        &identity,
        "example/worker:v5",
        &HashSet::new(),
        &HashMap::new(),
    )
    .unwrap();
    let mut translated = TranslatedV5 {
        pipeline: beam,
        identity,
        chain: vec!["pairs".into(), "running".into(), "result".into()],
        finalization_sinks: HashSet::new(),
        test_stream_payloads: HashMap::new(),
    };
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    assert_eq!(components.pcollections[&collection_id("pairs")].coder_id, CODER_V5_KV);
    let transform = &components.transforms[&id("v5_t", "running")];
    let spec = transform.spec.as_ref().unwrap();
    let payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    assert!(payload.state_specs.contains_key("total"));
    assert!(translated.pipeline.requirements.iter().any(|urn| urn == urns::REQUIREMENT_STATEFUL));
    let transform = translated.pipeline.components.as_mut().unwrap()
        .transforms.get_mut(&id("v5_t", "running")).unwrap();
    let spec = transform.spec.as_mut().unwrap();
    let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    payload.state_specs.clear();
    spec.payload = payload.encode_to_vec();
    assert!(validate_beam_contract(&graph, &translated).unwrap_err()
        .to_string().contains("stateful DoFn contract differs"));
}

#[test]
fn stateful_pardo_rejects_mismatched_key_coder() {
    let (mut graph, registry) = stateful_graph();
    let node = graph.nodes.iter_mut().find(|node| node.key == "running").unwrap();
    let Operation::StatefulParDo { contract, .. } = &mut node.operation else { unreachable!() };
    contract.key_coder_id = ids::CODER_BYTES.into();
    let identity = execution_identity(&graph, &registry, "stateful-build").unwrap();
    let error = lower(
        &graph,
        &["pairs".into(), "running".into(), "result".into()],
        &identity,
        "example/worker:v5",
        &HashSet::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("coder IDs differ"));
}
