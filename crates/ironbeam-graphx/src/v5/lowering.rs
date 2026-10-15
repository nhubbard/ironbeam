fn needs_side_global(graph: &Graph, node: &Node) -> bool {
    matches!(node.operation, Operation::SideInputMap | Operation::WaitOn)
        && graph
            .node(&node.additional_inputs[0].key)
            .is_some_and(|side| side.time.window != Window::Global)
}

fn beam_inputs(graph: &Graph, node: &Node, impulse_pc: &str) -> HashMap<String, String> {
    let primary = node
        .input
        .as_ref()
        .map(|parent| id("v5_pc", parent))
        .unwrap_or_else(|| impulse_pc.to_string());
    if matches!(
        node.operation,
        Operation::CoGroupByKey
            | Operation::JoinInner
            | Operation::JoinLeft
            | Operation::JoinRight
            | Operation::JoinFull
    ) {
        HashMap::from([("in".into(), cogroup_flatten_collection_id(&node.key))])
    } else if matches!(
        node.operation,
        Operation::FoldPerKey | Operation::FoldGlobal | Operation::GlobalCombine
    ) {
        HashMap::from([("in".into(), fold_group_collection_id(&node.key))])
    } else if matches!(node.operation, Operation::SideInputMap | Operation::WaitOn | Operation::WaitOnWindowed) {
        HashMap::from([
            ("in".into(), primary),
            (
                SIDE_INPUT_LOCAL_ID.into(),
                if !needs_side_global(graph, node) {
                    collection_id(&node.additional_inputs[0].key)
                } else {
                    side_global_collection_id(&node.key)
                },
            ),
        ])
    } else if matches!(node.operation, Operation::Flatten) {
        let mut inputs = HashMap::from([("in0".into(), primary)]);
        for (index, edge) in node.additional_inputs.iter().enumerate() {
            inputs.insert(format!("in{}", index + 1), id("v5_pc", &edge.key));
        }
        inputs
    } else {
        HashMap::from([("in".into(), primary)])
    }
}

fn lower(
    graph: &Graph,
    chain: &[String],
    identity: &ExecutionIdentity,
    container_image: &str,
    finalization_sinks: &HashSet<String>,
    test_streams: &HashMap<String, Vec<EncodedTestStreamEvent>>,
) -> Result<(BeamPipeline, HashMap<String, TestStreamPayload>), V5Error> {
    let mut transforms = HashMap::new();
    let mut pcollections = HashMap::new();
    let mut coders = HashMap::new();
    let mut root_ids = Vec::new();
    let mut test_stream_payloads = HashMap::new();
    let kv_inputs = keyed_inputs(graph);
    let mut windowing_strategies =
        HashMap::from([(ids::WINDOWING_ID.into(), global_windowing_strategy())]);
    for node in &graph.nodes {
        let mut strategy = match &node.time.window {
            Window::Global => global_windowing_strategy(),
            Window::Fixed { size_ns } => fixed_windowing_strategy(*size_ns, 0).map_err(V5Error::Wire)?,
            Window::FixedOffset { size_ns, offset_ns } => fixed_windowing_strategy(*size_ns, *offset_ns).map_err(V5Error::Wire)?,
            Window::Sliding { size_ns, period_ns, offset_ns } => sliding_windowing_strategy(*size_ns, *period_ns, *offset_ns).map_err(V5Error::Wire)?,
            Window::Session { gap_ns } => session_windowing_strategy(*gap_ns).map_err(V5Error::Wire)?,
        };
        apply_trigger(&mut strategy, node.time.trigger);
        strategy.output_time = match node.time.output_time {
            WindowOutputTime::EarliestInPane => output_time::Enum::EarliestInPane as i32,
            WindowOutputTime::EndOfWindow => output_time::Enum::EndOfWindow as i32,
            WindowOutputTime::LatestInPane => output_time::Enum::LatestInPane as i32,
        };
        strategy.allowed_lateness = node.time.allowed_lateness_ns / 1_000_000;
        windowing_strategies.insert(windowing_id(&node.time), strategy);
    }
    coders.insert(
        ids::CODER_BYTES.into(),
        Coder {
            spec: Some(FunctionSpec {
                urn: urns::CODER_BYTES.into(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );
    coders.insert(
        ids::CODER_GLOBAL_WINDOW.into(),
        Coder {
            spec: Some(FunctionSpec {
                urn: urns::CODER_GLOBAL_WINDOW.into(),
                payload: Vec::new(),
            }),
            component_coder_ids: Vec::new(),
        },
    );
    if windowing_strategies.len() > 1 {
        coders.insert(
            ids::CODER_INTERVAL_WINDOW.into(),
            Coder {
                spec: Some(FunctionSpec {
                    urn: urns::CODER_INTERVAL_WINDOW.into(),
                    payload: Vec::new(),
                }),
                component_coder_ids: Vec::new(),
            },
        );
    }
    if graph.nodes.iter().any(|node| {
        matches!(
            node.operation,
            Operation::GroupByKey
                | Operation::FoldPerKey
                | Operation::FoldGlobal
                | Operation::GlobalCombine
                | Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
        )
    }) {
        for (key, urn, components) in [
            (
                CODER_V5_LP_BYTES,
                urns::CODER_LENGTH_PREFIX,
                vec![ids::CODER_BYTES],
            ),
            (
                CODER_V5_KV,
                urns::CODER_KV,
                vec![CODER_V5_LP_BYTES, CODER_V5_LP_BYTES],
            ),
            (CODER_V5_ITER, urns::CODER_ITERABLE, vec![CODER_V5_LP_BYTES]),
            (
                CODER_V5_KV_ITER,
                urns::CODER_KV,
                vec![CODER_V5_LP_BYTES, CODER_V5_ITER],
            ),
        ] {
            coders.insert(
                key.into(),
                Coder {
                    spec: Some(FunctionSpec {
                        urn: urn.into(),
                        payload: Vec::new(),
                    }),
                    component_coder_ids: components.into_iter().map(str::to_owned).collect(),
                },
            );
        }
    }
    let impulse_pc = "v5_impulse_pc".to_string();
    pcollections.insert(
        impulse_pc.clone(),
        PCollection {
            unique_name: impulse_pc.clone(),
            coder_id: ids::CODER_BYTES.into(),
            is_bounded: is_bounded::Enum::Bounded as i32,
            windowing_strategy_id: ids::WINDOWING_ID.into(),
            display_data: Vec::new(),
        },
    );
    let impulse_id = "v5_impulse".to_string();
    transforms.insert(
        impulse_id.clone(),
        PTransform {
            unique_name: impulse_id.clone(),
            spec: Some(FunctionSpec {
                urn: urns::TRANSFORM_IMPULSE.into(),
                payload: Vec::new(),
            }),
            outputs: HashMap::from([("out".into(), impulse_pc.clone())]),
            ..Default::default()
        },
    );
    root_ids.push(impulse_id);

    for key in chain {
        let node = graph
            .node(key)
            .ok_or_else(|| V5Error::Graph(format!("missing node {key}")))?;
        if matches!(node.operation, Operation::Output) {
            continue;
        }
        if needs_side_global(graph, node) {
            let side_parent = &node.additional_inputs[0].key;
            let side_pc = collection_id(side_parent);
            let global_pc = side_global_collection_id(key);
            let global_transform = side_global_transform_id(key);
            let coder_id = pcollections
                .get(&side_pc)
                .ok_or_else(|| {
                    V5Error::Graph(format!("side collection {side_parent} is not lowered"))
                })?
                .coder_id
                .clone();
            pcollections.insert(
                global_pc.clone(),
                PCollection {
                    unique_name: format!("{key}:side-global"),
                    coder_id,
                    is_bounded: is_bounded::Enum::Bounded as i32,
                    windowing_strategy_id: ids::WINDOWING_ID.into(),
                    display_data: Vec::new(),
                },
            );
            transforms.insert(
                global_transform.clone(),
                PTransform {
                    unique_name: format!("{key}:side-global"),
                    spec: Some(FunctionSpec {
                        urn: urns::TRANSFORM_WINDOW_INTO.into(),
                        payload: WindowIntoPayload {
                            window_fn: Some(window_fn(&Window::Global)?),
                        }
                        .encode_to_vec(),
                    }),
                    inputs: HashMap::from([("in".into(), side_pc)]),
                    outputs: HashMap::from([("out".into(), global_pc)]),
                    environment_id: ids::ENV_ID.into(),
                    ..Default::default()
                },
            );
            root_ids.push(global_transform);
        }
        if matches!(
            node.operation,
            Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
        ) {
            let mut flattened_inputs = HashMap::new();
            for (input_index, parent) in cogroup_inputs(node).into_iter().enumerate() {
                let parent_node = graph
                    .node(parent)
                    .ok_or_else(|| V5Error::Graph(format!("missing co-group input {parent}")))?;
                let tag_pc = cogroup_tag_collection_id(key, input_index);
                let tag_transform = cogroup_tag_transform_id(key, input_index);
                pcollections.insert(
                    tag_pc.clone(),
                    PCollection {
                        unique_name: format!("{key}:tag{input_index}"),
                        coder_id: CODER_V5_KV.into(),
                        is_bounded: beam_boundedness(parent_node),
                        windowing_strategy_id: windowing_id(&node.time),
                        display_data: Vec::new(),
                    },
                );
                let node_ref = V5NodeRef {
                    key: key.clone(),
                    identity: identity.clone(),
                    co_group_input: Some(input_index),
                };
                let payload = ParDoPayload {
                    do_fn: Some(FunctionSpec {
                        urn: urns::DOFN_IRONBEAM_V5.into(),
                        payload: node_ref.encode()?,
                    }),
                    ..Default::default()
                }
                .encode_to_vec();
                transforms.insert(
                    tag_transform.clone(),
                    PTransform {
                        unique_name: format!("{key}:tag{input_index}"),
                        spec: Some(FunctionSpec {
                            urn: urns::TRANSFORM_PARDO.into(),
                            payload,
                        }),
                        inputs: HashMap::from([("in".into(), collection_id(parent))]),
                        outputs: HashMap::from([("out".into(), tag_pc.clone())]),
                        environment_id: ids::ENV_ID.into(),
                        ..Default::default()
                    },
                );
                flattened_inputs.insert(format!("in{input_index}"), tag_pc);
                root_ids.push(tag_transform);
            }
            let flatten_pc = cogroup_flatten_collection_id(key);
            let flatten_transform = cogroup_flatten_transform_id(key);
            pcollections.insert(
                flatten_pc.clone(),
                PCollection {
                    unique_name: format!("{key}:flatten"),
                    coder_id: CODER_V5_KV.into(),
                    is_bounded: beam_boundedness(node),
                    windowing_strategy_id: windowing_id(&node.time),
                    display_data: Vec::new(),
                },
            );
            transforms.insert(
                flatten_transform.clone(),
                PTransform {
                    unique_name: format!("{key}:flatten"),
                    spec: Some(FunctionSpec {
                        urn: urns::TRANSFORM_FLATTEN.into(),
                        payload: Vec::new(),
                    }),
                    inputs: flattened_inputs,
                    outputs: HashMap::from([("out".into(), flatten_pc)]),
                    ..Default::default()
                },
            );
            root_ids.push(flatten_transform);
        }
        if matches!(
            node.operation,
            Operation::FoldPerKey | Operation::FoldGlobal | Operation::GlobalCombine
        ) {
            let parent = node
                .input
                .as_deref()
                .ok_or_else(|| V5Error::Graph(format!("keyed fold {key} has no input")))?;
            let tag_pc = fold_tag_collection_id(key);
            let tag_transform = fold_tag_transform_id(key);
            pcollections.insert(
                tag_pc.clone(),
                PCollection {
                    unique_name: format!("{key}:tag"),
                    coder_id: CODER_V5_KV.into(),
                    is_bounded: beam_boundedness(node),
                    windowing_strategy_id: windowing_id(&node.time),
                    display_data: Vec::new(),
                },
            );
            let node_ref = V5NodeRef {
                key: key.clone(),
                identity: identity.clone(),
                co_group_input: Some(0),
            };
            let payload = ParDoPayload {
                do_fn: Some(FunctionSpec {
                    urn: urns::DOFN_IRONBEAM_V5.into(),
                    payload: node_ref.encode()?,
                }),
                ..Default::default()
            }
            .encode_to_vec();
            transforms.insert(
                tag_transform.clone(),
                PTransform {
                    unique_name: format!("{key}:tag"),
                    spec: Some(FunctionSpec {
                        urn: urns::TRANSFORM_PARDO.into(),
                        payload,
                    }),
                    inputs: HashMap::from([("in".into(), collection_id(parent))]),
                    outputs: HashMap::from([("out".into(), tag_pc.clone())]),
                    environment_id: ids::ENV_ID.into(),
                    ..Default::default()
                },
            );
            root_ids.push(tag_transform);
            let group_input = if matches!(
                node.operation,
                Operation::FoldGlobal | Operation::GlobalCombine
            ) && node.time.window == Window::Global
            {
                let seed_pc = fold_seed_collection_id(key);
                let seed_transform = fold_seed_transform_id(key);
                pcollections.insert(
                    seed_pc.clone(),
                    PCollection {
                        unique_name: format!("{key}:seed"),
                        coder_id: CODER_V5_KV.into(),
                        is_bounded: is_bounded::Enum::Bounded as i32,
                        windowing_strategy_id: ids::WINDOWING_ID.into(),
                        display_data: Vec::new(),
                    },
                );
                let seed_ref = V5NodeRef {
                    key: key.clone(),
                    identity: identity.clone(),
                    co_group_input: Some(1),
                };
                transforms.insert(
                    seed_transform.clone(),
                    PTransform {
                        unique_name: format!("{key}:seed"),
                        spec: Some(FunctionSpec {
                            urn: urns::TRANSFORM_PARDO.into(),
                            payload: ParDoPayload {
                                do_fn: Some(FunctionSpec {
                                    urn: urns::DOFN_IRONBEAM_V5.into(),
                                    payload: seed_ref.encode()?,
                                }),
                                ..Default::default()
                            }
                            .encode_to_vec(),
                        }),
                        inputs: HashMap::from([("in".into(), impulse_pc.clone())]),
                        outputs: HashMap::from([("out".into(), seed_pc.clone())]),
                        environment_id: ids::ENV_ID.into(),
                        ..Default::default()
                    },
                );
                root_ids.push(seed_transform);
                let flat_pc = fold_flatten_collection_id(key);
                let flat_transform = fold_flatten_transform_id(key);
                pcollections.insert(
                    flat_pc.clone(),
                    PCollection {
                        unique_name: format!("{key}:flatten"),
                        coder_id: CODER_V5_KV.into(),
                        is_bounded: beam_boundedness(node),
                        windowing_strategy_id: ids::WINDOWING_ID.into(),
                        display_data: Vec::new(),
                    },
                );
                transforms.insert(
                    flat_transform.clone(),
                    PTransform {
                        unique_name: format!("{key}:flatten"),
                        spec: Some(FunctionSpec {
                            urn: urns::TRANSFORM_FLATTEN.into(),
                            payload: Vec::new(),
                        }),
                        inputs: HashMap::from([
                            ("in0".into(), tag_pc.clone()),
                            ("in1".into(), seed_pc),
                        ]),
                        outputs: HashMap::from([("out".into(), flat_pc.clone())]),
                        ..Default::default()
                    },
                );
                root_ids.push(flat_transform);
                flat_pc
            } else {
                tag_pc
            };
            let group_pc = fold_group_collection_id(key);
            let group_transform = fold_group_transform_id(key);
            pcollections.insert(
                group_pc.clone(),
                PCollection {
                    unique_name: format!("{key}:group"),
                    coder_id: CODER_V5_KV_ITER.into(),
                    is_bounded: beam_boundedness(node),
                    windowing_strategy_id: windowing_id(&node.time),
                    display_data: Vec::new(),
                },
            );
            transforms.insert(
                group_transform.clone(),
                PTransform {
                    unique_name: format!("{key}:group"),
                    spec: Some(FunctionSpec {
                        urn: urns::TRANSFORM_GROUP_BY_KEY.into(),
                        payload: Vec::new(),
                    }),
                    inputs: HashMap::from([("in".into(), group_input)]),
                    outputs: HashMap::from([("out".into(), group_pc)]),
                    ..Default::default()
                },
            );
            root_ids.push(group_transform);
        }
        let spec = descriptor(node);
        let descriptor_payload =
            serde_json::to_vec(&spec).map_err(|e| V5Error::Wire(e.to_string()))?;
        let coder_key = id("v5_c", &hex::encode(&descriptor_payload));
        let lp_key = format!("{coder_key}_lp");
        let (urn, payload) = if node.coder_id == urns::CODER_ROW {
            Schema::decode(node.coder_payload.as_slice())
                .map_err(|error| V5Error::Wire(format!("invalid RowCoder schema at {key}: {error}")))?;
            (urns::CODER_ROW, node.coder_payload.clone())
        } else {
            (urns::CODER_IRONBEAM_V5, descriptor_payload)
        };
        coders.entry(coder_key.clone()).or_insert_with(|| Coder {
            spec: Some(FunctionSpec {
                urn: urn.into(),
                payload,
            }),
            component_coder_ids: Vec::new(),
        });
        coders.entry(lp_key.clone()).or_insert_with(|| Coder {
            spec: Some(FunctionSpec {
                urn: urns::CODER_LENGTH_PREFIX.into(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec![coder_key],
        });
        let output_pc = id("v5_pc", key);
        if !matches!(node.operation, Operation::Sink { .. }) {
            pcollections.insert(
                output_pc.clone(),
                PCollection {
                    unique_name: key.clone(),
                    coder_id: if matches!(
                        node.operation,
                        Operation::GroupByKey
                            | Operation::CoGroupByKey
                            | Operation::JoinInner
                            | Operation::JoinLeft
                            | Operation::JoinRight
                            | Operation::JoinFull
                    ) {
                        CODER_V5_KV_ITER.into()
                    } else if kv_inputs.contains(key.as_str()) {
                        CODER_V5_KV.into()
                    } else {
                        lp_key
                    },
                    is_bounded: beam_boundedness(node),
                    windowing_strategy_id: windowing_id(&node.time),
                    display_data: Vec::new(),
                },
            );
        }
        let transform_spec = if node.semantic_id == "ironbeam/test-stream" {
            let events = test_streams
                .get(key)
                .ok_or_else(|| V5Error::Graph(format!("missing test stream script for {key}")))?;
            let payload =
                build_test_stream_payload(key, pcollections[&output_pc].coder_id.clone(), events)?;
            test_stream_payloads.insert(key.clone(), payload.clone());
            FunctionSpec {
                urn: urns::TRANSFORM_TEST_STREAM.into(),
                payload: payload.encode_to_vec(),
            }
        } else if matches!(node.operation, Operation::FixedWindow | Operation::SlidingWindow | Operation::SessionWindow) {
            FunctionSpec {
                urn: urns::TRANSFORM_WINDOW_INTO.into(),
                payload: WindowIntoPayload {
                    window_fn: Some(window_fn(&node.time.window)?),
                }
                .encode_to_vec(),
            }
        } else if matches!(
            node.operation,
            Operation::Flatten
                | Operation::GroupByKey
                | Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
                | Operation::Reshuffle
        ) {
            FunctionSpec {
                urn: if matches!(node.operation, Operation::Flatten) {
                    urns::TRANSFORM_FLATTEN.into()
                } else if matches!(node.operation, Operation::Reshuffle) {
                    urns::TRANSFORM_RESHUFFLE.into()
                } else {
                    urns::TRANSFORM_GROUP_BY_KEY.into()
                },
                payload: Vec::new(),
            }
        } else {
            let node_ref = V5NodeRef {
                key: key.clone(),
                identity: identity.clone(),
                co_group_input: None,
            };
            let payload = ParDoPayload {
                do_fn: Some(FunctionSpec {
                    urn: urns::DOFN_IRONBEAM_V5.into(),
                    payload: node_ref.encode()?,
                }),
                side_inputs: if matches!(
                    node.operation,
                    Operation::SideInputMap | Operation::WaitOn | Operation::WaitOnWindowed
                ) {
                    HashMap::from([(
                        SIDE_INPUT_LOCAL_ID.into(),
                        if matches!(node.operation, Operation::WaitOnWindowed) {
                            windowed_wait_side_input_spec()
                        } else {
                            side_input_spec()
                        },
                    )])
                } else {
                    HashMap::new()
                },
                requests_finalization: finalization_sinks.contains(key),
                restriction_coder_id: if is_production_source(node) {
                    ids::CODER_BYTES.into()
                } else {
                    String::new()
                },
                ..Default::default()
            }
            .encode_to_vec();
            FunctionSpec {
                urn: urns::TRANSFORM_PARDO.into(),
                payload,
            }
        };
        let transform_id = id("v5_t", key);
        transforms.insert(
            transform_id.clone(),
            PTransform {
                unique_name: key.clone(),
                spec: Some(transform_spec),
                inputs: if node.semantic_id == "ironbeam/test-stream" {
                    HashMap::new()
                } else {
                    beam_inputs(graph, node, &impulse_pc)
                },
                outputs: if matches!(node.operation, Operation::Sink { .. }) {
                    HashMap::new()
                } else {
                    HashMap::from([("out".into(), output_pc.clone())])
                },
                environment_id: if node.semantic_id == "ironbeam/test-stream"
                    || matches!(
                        node.operation,
                        Operation::Flatten
                            | Operation::GroupByKey
                            | Operation::CoGroupByKey
                            | Operation::JoinInner
                            | Operation::JoinLeft
                            | Operation::JoinRight
                            | Operation::JoinFull
                            | Operation::Reshuffle
                    ) {
                    String::new()
                } else {
                    ids::ENV_ID.into()
                },
                ..Default::default()
            },
        );
        root_ids.push(transform_id);
    }
    combine_test_stream_payloads(
        chain,
        &mut transforms,
        &mut root_ids,
        &mut test_stream_payloads,
    )?;
    let mut env = docker_environment(container_image);
    env.capabilities.push(urns::DOFN_IRONBEAM_V5.into());
    env.capabilities.push(urns::CODER_IRONBEAM_V5.into());
    if graph.nodes.iter().any(|node| node.coder_id == urns::CODER_ROW) {
        env.capabilities.push(urns::CODER_ROW.into());
    }
    let identity_payload =
        serde_json::to_vec(identity).map_err(|e| V5Error::Wire(e.to_string()))?;
    Ok((
        BeamPipeline {
            components: Some(Components {
                transforms,
                pcollections,
                coders,
                windowing_strategies,
                environments: HashMap::from([(ids::ENV_ID.into(), env)]),
            }),
            root_transform_ids: root_ids,
            display_data: vec![DisplayData {
                urn: IDENTITY_URN.into(),
                payload: identity_payload,
            }],
            requirements: if graph.nodes.iter().any(|node| {
                matches!(node.operation, Operation::Source { .. })
                    && node.boundedness == Boundedness::Unbounded
                    && node.semantic_id != "ironbeam/test-stream"
            }) {
                vec![urns::REQUIREMENT_SDF.into()]
            } else {
                Vec::new()
            },
        },
        test_stream_payloads,
    ))
}

#[cfg(test)]
#[path = "../../tests/unit/v5__sdf_lowering_tests.rs"]
mod sdf_lowering_tests;
