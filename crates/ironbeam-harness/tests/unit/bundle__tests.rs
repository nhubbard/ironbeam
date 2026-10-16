// Unit tests for src/bundle.rs.
use super::*;
use beam_proto::fn_execution_v1::RemoteGrpcPort;
use beam_proto::pipeline_v1::{
    state_spec, ApiServiceDescriptor, Coder, FunctionSpec, PCollection, PTransform, ParDoPayload,
    ReadModifyWriteStateSpec, StateSpec, TimerFamilySpec,
};
use prost::Message;

fn leaf_coder(urn: &str) -> Coder {
    Coder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    }
}

fn composite_coder(urn: &str, components: &[&str]) -> Coder {
    Coder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: components.iter().map(|s| s.to_string()).collect(),
    }
}

fn test_node_ref(key: &str) -> V5NodeRef {
    V5NodeRef {
        key: key.into(),
        identity: ironbeam_core::ExecutionIdentity {
            graph_fingerprint: "sha256-v1:abc".into(),
            binary_build_id: "test-build".into(),
            registry_manifest: "registry".into(),
            coder_manifest: "coders".into(),
            source_config_digest: "source".into(),
            sink_config_digest: "sink".into(),
        },
        co_group_input: None,
    }
}

fn map_windows_pbd(mapping_urn: &str, output_window_urn: &str) -> ProcessBundleDescriptor {
    let mut pbd = ProcessBundleDescriptor {
        id: "map-windows-pbd".into(),
        ..Default::default()
    };
    for (id, urn, components) in [
        ("key", urns::CODER_BYTES, vec![]),
        ("interval", urns::CODER_INTERVAL_WINDOW, vec![]),
        ("global", urns::CODER_GLOBAL_WINDOW, vec![]),
        ("input-kv", urns::CODER_KV, vec!["key", "interval"]),
        (
            "output-kv",
            urns::CODER_KV,
            vec![
                "key",
                if output_window_urn == urns::CODER_GLOBAL_WINDOW {
                    "global"
                } else {
                    "interval"
                },
            ],
        ),
        (
            "input-wv",
            urns::CODER_WINDOWED_VALUE,
            vec!["input-kv", "interval"],
        ),
        (
            "output-wv",
            urns::CODER_WINDOWED_VALUE,
            vec!["output-kv", "interval"],
        ),
    ] {
        pbd.coders
            .insert(id.into(), composite_coder(urn, &components));
    }
    for (id, coder_id) in [("before", "input-wv"), ("after", "output-wv")] {
        pbd.pcollections.insert(
            id.into(),
            PCollection {
                coder_id: coder_id.into(),
                ..Default::default()
            },
        );
    }
    pbd.transforms.insert(
        "source".into(),
        mk_port_ptransform(
            URN_RUNNER_SOURCE,
            "runner:1",
            "input-wv",
            "before",
            "source",
        ),
    );
    pbd.transforms.insert(
        "mapping".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: urns::TRANSFORM_MAP_WINDOWS.into(),
                payload: FunctionSpec {
                    urn: mapping_urn.into(),
                    payload: Vec::new(),
                }
                .encode_to_vec(),
            }),
            inputs: HashMap::from([("in".into(), "before".into())]),
            outputs: HashMap::from([("out".into(), "after".into())]),
            ..Default::default()
        },
    );
    pbd.transforms.insert(
        "sink".into(),
        mk_port_ptransform(URN_RUNNER_SINK, "runner:2", "output-wv", "after", "sink"),
    );
    pbd
}

fn merge_windows_pbd() -> ProcessBundleDescriptor {
    let mut pbd = map_windows_pbd(urns::WINDOW_MAPPING_IDENTITY, urns::CODER_INTERVAL_WINDOW);
    for (id, urn, children) in [
        ("input-iter", urns::CODER_ITERABLE, vec!["interval"]),
        ("unmerged-iter", urns::CODER_ITERABLE, vec!["interval"]),
        ("consumed-iter", urns::CODER_ITERABLE, vec!["interval"]),
        (
            "merged-kv",
            urns::CODER_KV,
            vec!["interval", "consumed-iter"],
        ),
        ("merged-iter", urns::CODER_ITERABLE, vec!["merged-kv"]),
        (
            "output-value",
            urns::CODER_KV,
            vec!["unmerged-iter", "merged-iter"],
        ),
    ] {
        pbd.coders
            .insert(id.into(), composite_coder(urn, &children));
    }
    pbd.coders.get_mut("input-kv").unwrap().component_coder_ids[1] = "input-iter".into();
    pbd.coders.get_mut("output-kv").unwrap().component_coder_ids[1] = "output-value".into();
    let merge = pbd.transforms.get_mut("mapping").unwrap();
    merge.spec = Some(FunctionSpec {
        urn: urns::TRANSFORM_MERGE_WINDOWS.into(),
        payload: ironbeam_graphx::windowing::session_window_fn(5_000_000)
            .unwrap()
            .encode_to_vec(),
    });
    pbd
}

#[test]
fn plans_runner_requested_session_merge_with_exact_nested_coders() {
    let mut pbd = merge_windows_pbd();
    assert!(matches!(
        &plan(&pbd).unwrap().steps[1],
        Step::MergeWindows {
            outer_window: WindowCoder::Interval,
            ..
        }
    ));

    pbd.coders
        .get_mut("output-value")
        .unwrap()
        .component_coder_ids[0] = "interval".into();
    assert!(matches!(plan(&pbd), Err(PlanError::BadPayload { .. })));

    let mut wrong_fn = merge_windows_pbd();
    wrong_fn
        .transforms
        .get_mut("mapping")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .payload = ironbeam_graphx::windowing::fixed_window_fn(5_000_000, 0)
        .unwrap()
        .encode_to_vec();
    assert!(matches!(plan(&wrong_fn), Err(PlanError::BadPayload { .. })));
}

#[test]
fn plans_global_and_identity_map_windows_with_exact_kv_coders() {
    let global = map_windows_pbd(urns::WINDOW_MAPPING_GLOBAL, urns::CODER_GLOBAL_WINDOW);
    let parsed = plan(&global).unwrap();
    assert!(matches!(
        &parsed.steps[1],
        Step::MapWindows {
            mapping: WindowMapping::Global,
            outer_window: WindowCoder::Interval,
            input_window: WindowCoder::Interval,
            ..
        }
    ));

    let identity = map_windows_pbd(urns::WINDOW_MAPPING_IDENTITY, urns::CODER_INTERVAL_WINDOW);
    assert!(matches!(
        &plan(&identity).unwrap().steps[1],
        Step::MapWindows {
            mapping: WindowMapping::Identity,
            ..
        }
    ));
    assert!(plan(&map_windows_pbd(
        urns::WINDOW_MAPPING_IDENTITY,
        urns::CODER_GLOBAL_WINDOW
    ))
    .is_err());
    assert!(plan(&map_windows_pbd(
        "unknown:window-mapping",
        urns::CODER_GLOBAL_WINDOW
    ))
    .is_err());
    let mut wrong_port = global;
    let mut port = RemoteGrpcPort::decode(
        wrong_port.transforms["source"]
            .spec
            .as_ref()
            .unwrap()
            .payload
            .as_slice(),
    )
    .unwrap();
    port.coder_id = "output-wv".into();
    wrong_port
        .transforms
        .get_mut("source")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .payload = port.encode_to_vec();
    assert!(plan(&wrong_port).is_err());
}

#[test]
fn map_windows_accepts_equivalent_coder_trees_under_runner_local_ids() {
    let mut pbd = map_windows_pbd(urns::WINDOW_MAPPING_IDENTITY, urns::CODER_INTERVAL_WINDOW);
    pbd.coders
        .insert("key-copy".into(), leaf_coder(urns::CODER_BYTES));
    pbd.coders.get_mut("output-kv").unwrap().component_coder_ids[0] = "key-copy".into();
    pbd.coders.insert(
        "input-wv-copy".into(),
        composite_coder(urns::CODER_WINDOWED_VALUE, &["input-kv", "interval"]),
    );
    pbd.coders.insert(
        "output-wv-copy".into(),
        composite_coder(urns::CODER_WINDOWED_VALUE, &["output-kv", "interval"]),
    );
    for (transform, port_coder) in [("source", "input-wv-copy"), ("sink", "output-wv-copy")] {
        let spec = pbd
            .transforms
            .get_mut(transform)
            .unwrap()
            .spec
            .as_mut()
            .unwrap();
        let mut port = RemoteGrpcPort::decode(spec.payload.as_slice()).unwrap();
        port.coder_id = port_coder.into();
        spec.payload = port.encode_to_vec();
    }
    assert!(plan(&pbd).is_ok());

    pbd.coders
        .insert("key-copy".into(), leaf_coder(urns::CODER_STRING_UTF8));
    assert!(plan(&pbd).is_err());

    pbd.coders.insert(
        "key-copy".into(),
        composite_coder(urns::CODER_LENGTH_PREFIX, &["key-copy"]),
    );
    assert!(!coders_equivalent(&pbd, "key-copy", "key-copy"));
    assert!(plan(&pbd).is_err());
}

#[test]
fn plans_fixed_and_sliding_side_window_mappings_from_beam_payloads() {
    for (mut spec, expected) in [
        (
            ironbeam_graphx::windowing::fixed_window_fn(100_000_000, 20_000_000).unwrap(),
            WindowMapping::Fixed {
                size_millis: 100,
                offset_millis: 20,
            },
        ),
        (
            ironbeam_graphx::windowing::sliding_window_fn(150_000_000, 100_000_000, 20_000_000)
                .unwrap(),
            WindowMapping::Sliding {
                size_millis: 150,
                period_millis: 100,
                offset_millis: 20,
            },
        ),
    ] {
        spec.urn = match expected {
            WindowMapping::Fixed { .. } => urns::WINDOW_MAPPING_FIXED,
            _ => urns::WINDOW_MAPPING_SLIDING,
        }
        .into();
        let mut pbd = map_windows_pbd(&spec.urn, urns::CODER_INTERVAL_WINDOW);
        let mapping = pbd
            .transforms
            .get_mut("mapping")
            .unwrap()
            .spec
            .as_mut()
            .unwrap();
        mapping.payload = spec.encode_to_vec();
        assert!(
            matches!(&plan(&pbd).unwrap().steps[1], Step::MapWindows { mapping, .. } if *mapping == expected)
        );

        let mut wrong_output = map_windows_pbd(&spec.urn, urns::CODER_GLOBAL_WINDOW);
        wrong_output
            .transforms
            .get_mut("mapping")
            .unwrap()
            .spec
            .as_mut()
            .unwrap()
            .payload = spec.encode_to_vec();
        assert!(plan(&wrong_output).is_err());

        let mut bad_payload = pbd;
        bad_payload
            .transforms
            .get_mut("mapping")
            .unwrap()
            .spec
            .as_mut()
            .unwrap()
            .payload = FunctionSpec {
            urn: spec.urn,
            payload: Vec::new(),
        }
        .encode_to_vec();
        assert!(plan(&bad_payload).is_err());
    }
}

fn mk_pardo_ptransform(key: &str, input_pc: &str, output_pc: &str) -> PTransform {
    let pardo = ParDoPayload {
        do_fn: Some(FunctionSpec {
            urn: urns::DOFN_IRONBEAM_V5.to_string(),
            payload: test_node_ref(key).encode().unwrap(),
        }),
        ..Default::default()
    };
    PTransform {
        unique_name: format!("pardo_{key}"),
        spec: Some(FunctionSpec {
            urn: urns::TRANSFORM_PARDO.to_string(),
            payload: pardo.encode_to_vec(),
        }),
        inputs: std::iter::once(("in".to_string(), input_pc.to_string())).collect(),
        outputs: std::iter::once(("out".to_string(), output_pc.to_string())).collect(),
        ..Default::default()
    }
}

#[test]
fn plans_v5_node_references_without_reinterpreting_them_as_v4_ids() {
    let node_ref = test_node_ref("source");
    let pardo = ParDoPayload {
        do_fn: Some(FunctionSpec {
            urn: urns::DOFN_IRONBEAM_V5.into(),
            payload: node_ref.encode().unwrap(),
        }),
        ..Default::default()
    };
    let mut pbd = ProcessBundleDescriptor {
        id: "v5-pbd".into(),
        state_api_service_descriptor: Some(ApiServiceDescriptor {
            url: "runner-state:1234".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    pbd.transforms.insert(
        "source".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "runner:1", "coder", "pc0", "source"),
    );
    pbd.transforms.insert(
        "pardo".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: urns::TRANSFORM_PARDO.into(),
                payload: pardo.encode_to_vec(),
            }),
            inputs: std::iter::once(("in".into(), "pc0".into())).collect(),
            outputs: std::iter::once(("out".into(), "pc1".into())).collect(),
            ..Default::default()
        },
    );
    let side_ref = V5NodeRef {
        key: "add-side".into(),
        ..node_ref.clone()
    };
    let side_pardo = ParDoPayload {
        do_fn: Some(FunctionSpec {
            urn: urns::DOFN_IRONBEAM_V5.into(),
            payload: side_ref.encode().unwrap(),
        }),
        side_inputs: HashMap::from([("side".into(), ironbeam_graphx::v5::side_input_spec())]),
        ..Default::default()
    };
    pbd.transforms.insert(
        "side-pardo".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: urns::TRANSFORM_PARDO.into(),
                payload: side_pardo.encode_to_vec(),
            }),
            inputs: HashMap::from([("in".into(), "pc1".into()), ("side".into(), "pc2".into())]),
            outputs: HashMap::from([("out".into(), "pc3".into())]),
            ..Default::default()
        },
    );
    let wait_ref = V5NodeRef {
        key: "wait-many".into(),
        ..node_ref.clone()
    };
    let wait_pardo = ParDoPayload {
        do_fn: Some(FunctionSpec {
            urn: urns::DOFN_IRONBEAM_V5.into(),
            payload: wait_ref.encode().unwrap(),
        }),
        side_inputs: HashMap::from([
            (
                "side0".into(),
                ironbeam_graphx::v5::windowed_wait_side_input_spec(),
            ),
            (
                "side1".into(),
                ironbeam_graphx::v5::windowed_wait_side_input_spec(),
            ),
        ]),
        ..Default::default()
    };
    pbd.transforms.insert(
        "wait-pardo".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: urns::TRANSFORM_PARDO.into(),
                payload: wait_pardo.encode_to_vec(),
            }),
            inputs: HashMap::from([
                ("in".into(), "pc3".into()),
                ("side0".into(), "pc4".into()),
                ("side1".into(), "pc5".into()),
            ]),
            outputs: HashMap::from([("out".into(), "pc6".into())]),
            ..Default::default()
        },
    );
    let parsed = plan(&pbd).unwrap();
    assert_eq!(parsed.state_endpoint.as_deref(), Some("runner-state:1234"));
    assert_eq!(
        parsed
            .side_inputs
            .get("add-side")
            .and_then(|sides| sides.get("side"))
            .map(String::as_str),
        Some("pc2")
    );
    assert_eq!(
        parsed.side_inputs.get("wait-many"),
        Some(&HashMap::from([
            ("side0".into(), "pc4".into()),
            ("side1".into(), "pc5".into())
        ]))
    );
    assert!(
        matches!(&parsed.steps[1], Step::V5ParDo { node_ref: actual, .. } if actual == &node_ref)
    );
    let mut missing_signal = pbd.clone();
    missing_signal
        .transforms
        .get_mut("wait-pardo")
        .unwrap()
        .inputs
        .remove("side1");
    assert!(matches!(
        plan(&missing_signal),
        Err(PlanError::BadPayload { .. })
    ));
    let mut invalid = pbd;
    let transform = invalid.transforms.get_mut("side-pardo").unwrap();
    let spec = transform.spec.as_mut().unwrap();
    let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    payload.side_inputs.clear();
    spec.payload = payload.encode_to_vec();
    assert!(matches!(plan(&invalid), Err(PlanError::BadPayload { .. })));
}

#[test]
fn rejects_unimplemented_user_state_and_timer_specs_before_execution() {
    for fault in ["state", "timer", "window-expiration"] {
        let mut pbd = ProcessBundleDescriptor {
            id: "stateful-contract".into(),
            ..Default::default()
        };
        pbd.transforms.insert(
            "source".into(),
            mk_port_ptransform(URN_RUNNER_SOURCE, "runner:1", "coder", "pc0", "source"),
        );
        let mut transform = mk_pardo_ptransform("stateful", "pc0", "pc1");
        let spec = transform.spec.as_mut().unwrap();
        let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
        match fault {
            "state" => {
                payload
                    .state_specs
                    .insert("cell".into(), StateSpec::default());
            }
            "timer" => {
                payload
                    .timer_family_specs
                    .insert("fire".into(), TimerFamilySpec::default());
            }
            "window-expiration" => payload.on_window_expiration_timer_family_spec = "fire".into(),
            _ => unreachable!(),
        }
        spec.payload = payload.encode_to_vec();
        pbd.transforms.insert("pardo".into(), transform);
        assert!(
            matches!(plan(&pbd), Err(PlanError::BadPayload { .. })),
            "{fault}"
        );
    }
}

#[test]
fn plans_only_a_value_cell_with_length_prefixed_bytes() {
    let mut pbd = ProcessBundleDescriptor {
        id: "value-state-contract".into(),
        state_api_service_descriptor: Some(ApiServiceDescriptor {
            url: "runner-state:1234".into(),
            ..Default::default()
        }),
        ..Default::default()
    };
    pbd.coders
        .insert("cell-bytes".into(), leaf_coder(urns::CODER_BYTES));
    pbd.coders.insert(
        "cell-lp".into(),
        composite_coder(urns::CODER_LENGTH_PREFIX, &["cell-bytes"]),
    );
    pbd.transforms.insert(
        "source".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "runner:1", "coder", "pc0", "source"),
    );
    let mut transform = mk_pardo_ptransform("stateful", "pc0", "pc1");
    let spec = transform.spec.as_mut().unwrap();
    let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    payload.state_specs.insert(
        "value".into(),
        StateSpec {
            spec: Some(state_spec::Spec::ReadModifyWriteSpec(
                ReadModifyWriteStateSpec {
                    coder_id: "cell-lp".into(),
                },
            )),
            protocol: Some(FunctionSpec {
                urn: "beam:user_state:bag:v1".into(),
                payload: Vec::new(),
            }),
        },
    );
    spec.payload = payload.encode_to_vec();
    pbd.transforms.insert("stateful".into(), transform);
    pbd.transforms.insert(
        "sink".into(),
        mk_port_ptransform(URN_RUNNER_SINK, "runner:1", "coder", "pc1", "sink"),
    );
    let planned = plan(&pbd).unwrap();
    assert!(matches!(
        planned.steps[1],
        Step::V5ParDo {
            stateful_value: true,
            ..
        }
    ));
    pbd.coders
        .insert("timer-window".into(), leaf_coder(urns::CODER_GLOBAL_WINDOW));
    pbd.coders.insert(
        "timer-coder".into(),
        composite_coder(urns::CODER_TIMER, &["cell-lp", "timer-window"]),
    );
    payload.timer_family_specs.insert(
        "event".into(),
        TimerFamilySpec {
            time_domain: time_domain::Enum::EventTime as i32,
            timer_family_coder_id: "timer-coder".into(),
        },
    );
    pbd.transforms
        .get_mut("stateful")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .payload = payload.encode_to_vec();
    assert!(matches!(
        plan(&pbd).unwrap().steps[1],
        Step::V5ParDo {
            stateful_value: true,
            ..
        }
    ));
    payload.on_window_expiration_timer_family_spec = "event".into();
    pbd.transforms
        .get_mut("stateful")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .payload = payload.encode_to_vec();
    assert!(matches!(
        plan(&pbd).unwrap().steps[1],
        Step::V5ParDo {
            stateful_value: true,
            ..
        }
    ));
    payload.on_window_expiration_timer_family_spec = "missing".into();
    pbd.transforms
        .get_mut("stateful")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .payload = payload.encode_to_vec();
    assert!(matches!(plan(&pbd), Err(PlanError::BadPayload { .. })));
    payload.on_window_expiration_timer_family_spec = "event".into();
    pbd.transforms
        .get_mut("stateful")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .payload = payload.encode_to_vec();
    pbd.coders
        .insert("timer-coder".into(), leaf_coder(urns::CODER_BYTES));
    assert!(matches!(plan(&pbd), Err(PlanError::BadPayload { .. })));
    pbd.coders.insert(
        "timer-coder".into(),
        composite_coder(urns::CODER_TIMER, &["cell-lp", "timer-window"]),
    );
    pbd.coders
        .insert("cell-bytes".into(), leaf_coder(urns::CODER_STRING_UTF8));
    assert!(matches!(plan(&pbd), Err(PlanError::BadPayload { .. })));
}

fn mk_port_ptransform(
    urn: &str,
    endpoint: &str,
    coder_id: &str,
    pc: &str,
    side: &str,
) -> PTransform {
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: endpoint.to_string(),
            ..Default::default()
        }),
        coder_id: coder_id.to_string(),
    };
    let inputs = if side == "sink" {
        std::iter::once(("in".to_string(), pc.to_string())).collect()
    } else {
        Default::default()
    };
    let outputs = if side == "source" {
        std::iter::once(("out".to_string(), pc.to_string())).collect()
    } else {
        Default::default()
    };
    PTransform {
        unique_name: format!("{side}_{endpoint}"),
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: port.encode_to_vec(),
        }),
        inputs,
        outputs,
        ..Default::default()
    }
}

#[test]
fn plans_source_pardo_sink() {
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-1".into(),
        ..Default::default()
    };
    pbd.transforms.insert(
        "src".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "runner:1", "c_wv", "pc0", "source"),
    );
    pbd.transforms
        .insert("p1".into(), mk_pardo_ptransform("p1", "pc0", "pc1"));
    pbd.transforms.insert(
        "snk".into(),
        mk_port_ptransform(URN_RUNNER_SINK, "runner:2", "c_wv", "pc1", "sink"),
    );

    let plan = plan(&pbd).expect("plan ok");
    assert_eq!(plan.steps.len(), 3);
    match &plan.steps[0] {
        Step::Source {
            transform_id,
            output_pc,
            ..
        } => {
            assert_eq!(transform_id, "src");
            assert_eq!(output_pc, "pc0");
        }
        other => panic!("expected Source, got {other:?}"),
    }
    match &plan.steps[1] {
        Step::V5ParDo {
            node_ref,
            input_pc,
            output_pc,
            ..
        } => {
            assert_eq!(node_ref.key, "p1");
            assert_eq!(input_pc, "pc0");
            assert_eq!(output_pc, "pc1");
        }
        other => panic!("expected V5ParDo, got {other:?}"),
    }
    match &plan.steps[2] {
        Step::Sink { input_pc, wire, .. } => {
            assert_eq!(input_pc, "pc1");
            // No coders table populated → classifier falls back to Opaque.
            assert_eq!(*wire, SinkWire::Opaque);
        }
        other => panic!("expected Sink, got {other:?}"),
    }
}

#[test]
fn plans_three_v5_sdf_stages_with_their_portable_identity() {
    let mut pbd = ProcessBundleDescriptor {
        id: "sdf-pbd".into(),
        ..Default::default()
    };
    pbd.transforms.insert(
        "src".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "runner:1", "c_wv", "impulse", "source"),
    );
    for (id, urn, input_pc, output_pc) in [
        (
            "pair",
            urns::TRANSFORM_SDF_PAIR_WITH_RESTRICTION,
            "impulse",
            "paired",
        ),
        (
            "split",
            urns::TRANSFORM_SDF_SPLIT_AND_SIZE,
            "paired",
            "sized",
        ),
        (
            "process",
            urns::TRANSFORM_SDF_PROCESS_SIZED,
            "sized",
            "events",
        ),
    ] {
        let payload = ParDoPayload {
            do_fn: Some(FunctionSpec {
                urn: urns::DOFN_IRONBEAM_V5.into(),
                payload: test_node_ref("events").encode().unwrap(),
            }),
            restriction_coder_id: "restriction-bytes".into(),
            ..Default::default()
        };
        pbd.transforms.insert(
            id.into(),
            PTransform {
                unique_name: id.into(),
                spec: Some(FunctionSpec {
                    urn: urn.into(),
                    payload: payload.encode_to_vec(),
                }),
                inputs: HashMap::from([("in".into(), input_pc.into())]),
                outputs: HashMap::from([("out".into(), output_pc.into())]),
                ..Default::default()
            },
        );
    }
    pbd.transforms.insert(
        "snk".into(),
        mk_port_ptransform(URN_RUNNER_SINK, "runner:2", "c_wv", "events", "sink"),
    );
    let parsed = plan(&pbd).unwrap();
    let stages: Vec<_> = parsed
        .steps
        .iter()
        .filter_map(|step| match step {
            Step::Sdf {
                stage, node_ref, ..
            } => {
                assert_eq!(node_ref, &test_node_ref("events"));
                Some(*stage)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        stages,
        [
            SdfStage::PairWithRestriction,
            SdfStage::SplitAndSizeRestrictions,
            SdfStage::ProcessSizedElements,
        ]
    );

    let mut draining = pbd.clone();
    draining
        .transforms
        .get_mut("process")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .urn = urns::TRANSFORM_SDF_TRUNCATE_SIZED.into();
    assert!(plan(&draining).unwrap().steps.iter().any(|step| matches!(
        step,
        Step::Sdf {
            stage: SdfStage::TruncateSizedRestrictions,
            ..
        }
    )));

    let mut stateful = pbd.clone();
    let spec = stateful
        .transforms
        .get_mut("process")
        .unwrap()
        .spec
        .as_mut()
        .unwrap();
    let mut stateful_payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    stateful_payload
        .state_specs
        .insert("cell".into(), StateSpec::default());
    spec.payload = stateful_payload.encode_to_vec();
    assert!(matches!(plan(&stateful), Err(PlanError::BadPayload { .. })));

    let bad = pbd.transforms.get_mut("split").unwrap();
    let spec = bad.spec.as_mut().unwrap();
    let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    payload.restriction_coder_id.clear();
    spec.payload = payload.encode_to_vec();
    assert!(matches!(plan(&pbd), Err(PlanError::BadPayload { .. })));
}

#[test]
fn rejects_v4_dofn_payload_before_bundle_execution() {
    let mut pbd = ProcessBundleDescriptor {
        id: "old-pbd".into(),
        ..Default::default()
    };
    let mut transform = mk_pardo_ptransform("p1", "pc0", "pc1");
    let spec = transform.spec.as_mut().unwrap();
    let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    payload.do_fn.as_mut().unwrap().urn = "beam:dofn:ironbeam:v1".into();
    spec.payload = payload.encode_to_vec();
    pbd.transforms.insert("p1".into(), transform);
    assert!(matches!(
        plan(&pbd),
        Err(PlanError::UnsupportedUrn { urn, .. }) if urn == "beam:dofn:ironbeam:v1"
    ));
}

#[test]
fn classifies_kv_lp_sink_from_coder_tree() {
    // Full windowed_value<kv<lp<bytes>, lp<bytes>>, global_window> tree.
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-kv".into(),
        ..Default::default()
    };
    pbd.coders
        .insert("c_bytes".into(), leaf_coder(urns::CODER_BYTES));
    pbd.coders
        .insert("c_gw".into(), leaf_coder(urns::CODER_GLOBAL_WINDOW));
    pbd.coders.insert(
        "c_lp".into(),
        composite_coder(urns::CODER_LENGTH_PREFIX, &["c_bytes"]),
    );
    pbd.coders.insert(
        "c_kv".into(),
        composite_coder(urns::CODER_KV, &["c_lp", "c_lp"]),
    );
    pbd.coders.insert(
        "c_wv".into(),
        composite_coder(urns::CODER_WINDOWED_VALUE, &["c_kv", "c_gw"]),
    );
    pbd.transforms
        .insert("p1".into(), mk_pardo_ptransform("p1", "pc0", "pc1"));
    pbd.transforms.insert(
        "snk".into(),
        mk_port_ptransform(URN_RUNNER_SINK, "a:1", "c_wv", "pc1", "sink"),
    );
    let plan = plan(&pbd).expect("plan ok");
    let sink = plan
        .steps
        .iter()
        .find(|s| matches!(s, Step::Sink { .. }))
        .expect("sink step");
    match sink {
        Step::Sink { wire, .. } => assert_eq!(*wire, SinkWire::KvLp),
        _ => unreachable!(),
    }
}

#[test]
fn classifies_kv_iter_source_as_kv_iter_lp() {
    // windowed_value<kv<lp<bytes>, iterable<lp<bytes>>>, global_window>
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-postgbk".into(),
        ..Default::default()
    };
    pbd.coders
        .insert("c_bytes".into(), leaf_coder(urns::CODER_BYTES));
    pbd.coders
        .insert("c_gw".into(), leaf_coder(urns::CODER_GLOBAL_WINDOW));
    pbd.coders.insert(
        "c_lp".into(),
        composite_coder(urns::CODER_LENGTH_PREFIX, &["c_bytes"]),
    );
    pbd.coders.insert(
        "c_iter".into(),
        composite_coder(urns::CODER_ITERABLE, &["c_lp"]),
    );
    pbd.coders.insert(
        "c_kv".into(),
        composite_coder(urns::CODER_KV, &["c_lp", "c_iter"]),
    );
    pbd.coders.insert(
        "c_wv".into(),
        composite_coder(urns::CODER_WINDOWED_VALUE, &["c_kv", "c_gw"]),
    );
    pbd.transforms.insert(
        "src".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "a:1", "c_wv", "pc0", "source"),
    );
    pbd.transforms
        .insert("p1".into(), mk_pardo_ptransform("p1", "pc0", "pc1"));
    let plan = plan(&pbd).expect("plan ok");
    match plan.steps.iter().find(|s| matches!(s, Step::Source { .. })) {
        Some(Step::Source { wire, .. }) => assert_eq!(*wire, SourceWire::KvIterLp),
        _ => panic!("no source step"),
    }
}

#[test]
fn classifies_pre_shuffle_keyed_source_as_kv_lp() {
    let mut pbd = ProcessBundleDescriptor::default();
    pbd.coders
        .insert("bytes".into(), leaf_coder(urns::CODER_BYTES));
    pbd.coders
        .insert("window".into(), leaf_coder(urns::CODER_GLOBAL_WINDOW));
    pbd.coders.insert(
        "lp".into(),
        composite_coder(urns::CODER_LENGTH_PREFIX, &["bytes"]),
    );
    pbd.coders
        .insert("kv".into(), composite_coder(urns::CODER_KV, &["lp", "lp"]));
    pbd.coders.insert(
        "wv".into(),
        composite_coder(urns::CODER_WINDOWED_VALUE, &["kv", "window"]),
    );
    assert_eq!(super::classify_source_wire(&pbd, "wv"), SourceWire::KvLp);
}

#[test]
fn classifies_state_backed_iterable_as_state_backed() {
    // windowed_value<kv<lp<bytes>, state_backed_iterable<bytes>>, gw>
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-sb".into(),
        ..Default::default()
    };
    pbd.coders
        .insert("c_bytes".into(), leaf_coder(urns::CODER_BYTES));
    pbd.coders
        .insert("c_gw".into(), leaf_coder(urns::CODER_GLOBAL_WINDOW));
    pbd.coders.insert(
        "c_lp".into(),
        composite_coder(urns::CODER_LENGTH_PREFIX, &["c_bytes"]),
    );
    pbd.coders.insert(
        "c_sb_iter".into(),
        composite_coder(urns::CODER_STATE_BACKED_ITERABLE, &["c_lp"]),
    );
    pbd.coders.insert(
        "c_kv".into(),
        composite_coder(urns::CODER_KV, &["c_lp", "c_sb_iter"]),
    );
    pbd.coders.insert(
        "c_wv".into(),
        composite_coder(urns::CODER_WINDOWED_VALUE, &["c_kv", "c_gw"]),
    );
    pbd.transforms.insert(
        "src".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "a:1", "c_wv", "pc0", "source"),
    );
    pbd.transforms
        .insert("p1".into(), mk_pardo_ptransform("p1", "pc0", "pc1"));
    let plan = plan(&pbd).expect("plan ok");
    match plan.steps.iter().find(|s| matches!(s, Step::Source { .. })) {
        Some(Step::Source { wire, .. }) => {
            assert_eq!(*wire, SourceWire::KvStateBackedIter)
        }
        _ => panic!("no source step"),
    }
}

#[test]
fn classifies_opaque_source_as_opaque() {
    // windowed_value<lp<bytes>, global_window>
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-op".into(),
        ..Default::default()
    };
    pbd.coders
        .insert("c_bytes".into(), leaf_coder(urns::CODER_BYTES));
    pbd.coders
        .insert("c_gw".into(), leaf_coder(urns::CODER_GLOBAL_WINDOW));
    pbd.coders.insert(
        "c_lp".into(),
        composite_coder(urns::CODER_LENGTH_PREFIX, &["c_bytes"]),
    );
    pbd.coders.insert(
        "c_wv".into(),
        composite_coder(urns::CODER_WINDOWED_VALUE, &["c_lp", "c_gw"]),
    );
    pbd.transforms.insert(
        "src".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "a:1", "c_wv", "pc0", "source"),
    );
    pbd.transforms
        .insert("p1".into(), mk_pardo_ptransform("p1", "pc0", "pc1"));
    let plan = plan(&pbd).expect("plan ok");
    match plan.steps.iter().find(|s| matches!(s, Step::Source { .. })) {
        Some(Step::Source { wire, .. }) => assert_eq!(*wire, SourceWire::Opaque),
        _ => panic!("no source step"),
    }
}

#[test]
fn classifies_opaque_sink_as_opaque() {
    // windowed_value<lp<bytes>, global_window> — no KV wrapper.
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-op".into(),
        ..Default::default()
    };
    pbd.coders
        .insert("c_bytes".into(), leaf_coder(urns::CODER_BYTES));
    pbd.coders
        .insert("c_gw".into(), leaf_coder(urns::CODER_GLOBAL_WINDOW));
    pbd.coders.insert(
        "c_lp".into(),
        composite_coder(urns::CODER_LENGTH_PREFIX, &["c_bytes"]),
    );
    pbd.coders.insert(
        "c_wv".into(),
        composite_coder(urns::CODER_WINDOWED_VALUE, &["c_lp", "c_gw"]),
    );
    pbd.transforms
        .insert("p1".into(), mk_pardo_ptransform("p1", "pc0", "pc1"));
    pbd.transforms.insert(
        "snk".into(),
        mk_port_ptransform(URN_RUNNER_SINK, "a:1", "c_wv", "pc1", "sink"),
    );
    let plan = plan(&pbd).expect("plan ok");
    match plan.steps.iter().find(|s| matches!(s, Step::Sink { .. })) {
        Some(Step::Sink { wire, .. }) => assert_eq!(*wire, SinkWire::Opaque),
        _ => panic!("no sink step"),
    }
}

#[test]
fn rejects_unknown_urn() {
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-2".into(),
        ..Default::default()
    };
    pbd.transforms.insert(
        "weird".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: "beam:transform:nonsense:v1".into(),
                payload: vec![],
            }),
            ..Default::default()
        },
    );
    let err = plan(&pbd).unwrap_err();
    assert!(
        matches!(err, PlanError::UnsupportedUrn { .. }),
        "got {err:?}"
    );
}

#[test]
fn rejects_multiple_sources() {
    let mut pbd = ProcessBundleDescriptor {
        id: "x".into(),
        ..Default::default()
    };
    pbd.transforms.insert(
        "s1".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "a:1", "c", "p", "source"),
    );
    pbd.transforms.insert(
        "s2".into(),
        mk_port_ptransform(URN_RUNNER_SOURCE, "b:1", "c", "q", "source"),
    );
    let err = plan(&pbd).unwrap_err();
    assert!(matches!(err, PlanError::TooManySources(2)), "got {err:?}");
}

#[test]
fn skips_impulse_and_gbk() {
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-3".into(),
        ..Default::default()
    };
    pbd.transforms.insert(
        "imp".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: urns::TRANSFORM_IMPULSE.to_string(),
                payload: vec![],
            }),
            outputs: std::iter::once(("out".to_string(), "pc0".to_string())).collect(),
            ..Default::default()
        },
    );
    pbd.transforms
        .insert("p1".into(), mk_pardo_ptransform("p1", "pc0", "pc1"));
    let plan = plan(&pbd).expect("plan ok");
    // Impulse is skipped; only the ParDo is planned.
    assert_eq!(plan.steps.len(), 1);
    assert!(matches!(plan.steps[0], Step::V5ParDo { .. }));
}
