/// Build the identity the driver and a separately rebuilt worker must agree on.
/// The binary build ID is supplied by the application; compiled closure bytes
/// cannot be recovered from a portable graph.
pub fn execution_identity(
    graph: &Graph,
    registry: &Registry,
    binary_build_id: &str,
) -> Result<ExecutionIdentity, V5Error> {
    if binary_build_id.is_empty() {
        return Err(V5Error::Graph("binary build ID must not be empty".into()));
    }
    let mut semantic = Vec::new();
    let mut coders = Vec::new();
    let mut sources = Vec::new();
    let mut sinks = Vec::new();
    for node in &graph.nodes {
        semantic.push((&node.key, &node.semantic_id, node.semantic_version));
        coders.push((&node.key, &node.type_id, &node.coder_id, &node.coder_payload, node.coder_version));
        if matches!(node.operation, Operation::Source { .. }) {
            let config =
                registry
                    .source_config_digest(&node.key)
                    .ok_or_else(|| V5Error::Unsupported {
                        key: node.key.clone(),
                        reason: "source has no reproducible configuration digest".into(),
                    })?;
            sources.push((&node.key, config));
        }
        if matches!(node.operation, Operation::Sink { .. }) {
            let config =
                registry
                    .sink_config_digest(&node.key)
                    .ok_or_else(|| V5Error::Unsupported {
                        key: node.key.clone(),
                        reason: "sink has no reproducible configuration digest".into(),
                    })?;
            sinks.push((
                &node.key,
                config,
                registry.sink_requires_bundle_finalization(&node.key),
            ));
        }
    }
    semantic.sort_by(|a, b| a.0.cmp(b.0));
    coders.sort_by(|a, b| a.0.cmp(b.0));
    sources.sort_by(|a, b| a.0.cmp(b.0));
    sinks.sort_by(|a, b| a.0.cmp(b.0));
    Ok(ExecutionIdentity {
        graph_fingerprint: graph.fingerprint()?,
        binary_build_id: binary_build_id.into(),
        registry_manifest: digest(&semantic)?,
        coder_manifest: digest(&coders)?,
        source_config_digest: digest(&sources)?,
        sink_config_digest: digest(&sinks)?,
    })
}

/// Read Ironbeam's execution identity from a portable Beam pipeline.
///
/// Returns a wire error when the identity is missing or malformed.
pub fn identity_from_beam(beam: &BeamPipeline) -> Result<ExecutionIdentity, V5Error> {
    let entry = beam
        .display_data
        .iter()
        .find(|entry| entry.urn == IDENTITY_URN)
        .ok_or_else(|| V5Error::Wire("v5 execution identity is missing".into()))?;
    serde_json::from_slice(&entry.payload).map_err(|error| V5Error::Wire(error.to_string()))
}

/// Validate and translate a v5 graph without opening its sources or sinks.
pub fn translate_v5(
    pipeline: &Pipeline,
    container_image: &str,
    binary_build_id: &str,
) -> Result<TranslatedV5, V5Error> {
    let (graph, registry) = pipeline.snapshot()?;
    for node in graph.nodes.iter().filter(|node| is_production_source(node)) {
        let Operation::Source { progress } = &node.operation else {
            unreachable!()
        };
        if !progress.stable_split_id
            || progress.replay == Replay::None
            || !registry.has_unbounded_source(&node.key)
            || !registry.has_coder(&node.key)
        {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "production source needs a resumable registered reader and coder".into(),
            });
        }
    }
    for node in &graph.nodes {
        if matches!(node.operation, Operation::Sink { .. })
            && registry.sink_requires_bundle_finalization(&node.key)
        {
            let parent = node
                .input
                .as_deref()
                .and_then(|key| graph.node(key))
                .ok_or_else(|| {
                    V5Error::Graph(format!("finalized sink {} has no input", node.key))
                })?;
            if !matches!(parent.operation, Operation::Reshuffle) {
                return Err(V5Error::Unsupported {
                    key: node.key.clone(),
                    reason: "bundle-finalized sink needs an immediate Reshuffle stage boundary"
                        .into(),
                });
            }
        }
    }
    let chain = portable_order(&graph)?;
    for node in &graph.nodes {
        if matches!(
            node.operation,
            Operation::GroupByKey
                | Operation::FoldPerKey
                | Operation::FoldGlobal
                | Operation::GlobalCombine
        ) && !registry.has_group_coder(&node.key)
        {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "grouping has no registered typed key/value coder".into(),
            });
        }
        if matches!(
            node.operation,
            Operation::FoldPerKey | Operation::FoldGlobal
        ) && !registry.has_fold(&node.key)
        {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "keyed fold has no registered combine factory".into(),
            });
        }
        if matches!(node.operation, Operation::GlobalCombine) && !registry.has_aggregate(&node.key)
        {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "global combine has no registered aggregate".into(),
            });
        }
        if matches!(
            node.operation,
            Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
        ) && !(registry.has_cogroup_coder(&node.key) || registry.has_join_coder(&node.key))
        {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "co-grouping has no registered tagged-value coder".into(),
            });
        }
        if matches!(node.operation, Operation::SideInputMap) && !registry.has_side_input(&node.key)
        {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "side input has no materialization contract".into(),
            });
        }
        if matches!(node.operation, Operation::StatefulParDo { .. })
            && (!registry.has_stateful_value(&node.key) || !registry.has_group_coder(&node.key))
        {
            return Err(V5Error::Unsupported {
                key: node.key.clone(),
                reason: "stateful ParDo needs a registered callback and keyed coder".into(),
            });
        }
    }
    for key in chain.iter().filter(|key| {
        graph
            .node(key)
            .is_some_and(|node| !matches!(node.operation, Operation::Output))
    }) {
        if !registry.has_coder(key) {
            return Err(V5Error::Unsupported {
                key: key.clone(),
                reason: "no worker coder registered for this element type".into(),
            });
        }
    }
    let identity = execution_identity(&graph, &registry, binary_build_id)?;
    let finalization_sinks = graph
        .nodes
        .iter()
        .filter(|node| {
            matches!(node.operation, Operation::Sink { .. })
                && registry.sink_requires_bundle_finalization(&node.key)
        })
        .map(|node| node.key.clone())
        .collect();
    let test_stream_payloads = graph
        .nodes
        .iter()
        .filter(|node| node.semantic_id == "ironbeam/test-stream")
        .map(|node| {
            let events = registry
                .test_stream(&node.key)
                .ok_or_else(|| V5Error::Unsupported {
                    key: node.key.clone(),
                    reason: "test stream has no registered events".into(),
                })?;
            Ok((node.key.clone(), events.to_vec()))
        })
        .collect::<Result<HashMap<_, _>, V5Error>>()?;
    let (beam, payloads) = lower(
        &graph,
        &registry,
        &chain,
        &identity,
        container_image,
        &finalization_sinks,
        &test_stream_payloads,
    )?;
    Ok(TranslatedV5 {
        pipeline: beam,
        identity,
        chain,
        finalization_sinks,
        test_stream_payloads: payloads,
    })
}

/// Check the portable contract before a worker opens a source. A worker must
/// still compare execution identity against its own rebuilt registry.
pub fn validate_beam_contract(graph: &Graph, translated: &TranslatedV5) -> Result<(), V5Error> {
    let expected_chain = portable_order(graph)?;
    let kv_inputs = keyed_inputs(graph);
    if graph.fingerprint()? != translated.identity.graph_fingerprint {
        return Err(V5Error::Wire(
            "v5 graph fingerprint differs from the driver".into(),
        ));
    }
    if expected_chain != translated.chain {
        return Err(V5Error::Wire(
            "v5 parent-before-child order differs from the graph".into(),
        ));
    }
    let embedded = identity_from_beam(&translated.pipeline)?;
    translated.identity.verify(&embedded)?;
    let components = translated
        .pipeline
        .components
        .as_ref()
        .ok_or_else(|| V5Error::Wire("Beam components are missing".into()))?;
    let test_stream_keys = expected_chain
        .iter()
        .filter(|key| {
            graph
                .node(key)
                .is_some_and(|node| node.semantic_id == "ironbeam/test-stream")
        })
        .collect::<Vec<_>>();
    let primary_test_stream = test_stream_keys.first().copied();
    let extra_test_stream_outputs = test_stream_keys.len().saturating_sub(1);
    let mut expected_roots = vec!["v5_impulse".to_string()];
    for key in &expected_chain {
        let node = graph
            .node(key)
            .ok_or_else(|| V5Error::Graph(format!("missing node {key}")))?;
        if matches!(node.operation, Operation::Output) {
            continue;
        }
        if node.semantic_id == "ironbeam/test-stream" && Some(key) != primary_test_stream {
            continue;
        }
        if needs_side_global(graph, node) {
            expected_roots.push(side_global_transform_id(key));
        }
        if matches!(
            node.operation,
            Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
        ) {
            for input_index in 0..cogroup_inputs(node).len() {
                expected_roots.push(cogroup_tag_transform_id(key, input_index));
            }
            expected_roots.push(cogroup_flatten_transform_id(key));
        }
        if matches!(
            node.operation,
            Operation::FoldPerKey | Operation::FoldGlobal | Operation::GlobalCombine
        ) {
            expected_roots.push(fold_tag_transform_id(key));
            if matches!(
                node.operation,
                Operation::FoldGlobal | Operation::GlobalCombine
            ) && node.time.window == Window::Global
            {
                expected_roots.push(fold_seed_transform_id(key));
                expected_roots.push(fold_flatten_transform_id(key));
            }
            expected_roots.push(fold_group_transform_id(key));
        }
        expected_roots.push(id("v5_t", key));
    }
    let sink_count = graph
        .nodes
        .iter()
        .filter(|node| matches!(node.operation, Operation::Sink { .. }))
        .count();
    if translated.pipeline.root_transform_ids != expected_roots
        || components.transforms.len() != expected_roots.len()
        || components.pcollections.len()
            != expected_roots.len() - sink_count + extra_test_stream_outputs
    {
        return Err(V5Error::Wire(
            "Beam transform or collection IDs differ".into(),
        ));
    }
    let impulse = components
        .transforms
        .get("v5_impulse")
        .ok_or_else(|| V5Error::Wire("missing impulse transform".into()))?;
    if impulse.spec.as_ref().map(|s| s.urn.as_str()) != Some(urns::TRANSFORM_IMPULSE)
        || impulse.outputs.get("out").map(String::as_str) != Some("v5_impulse_pc")
    {
        return Err(V5Error::Wire("wrong impulse transform".into()));
    }
    for key in &expected_chain {
        let node = graph
            .node(key)
            .ok_or_else(|| V5Error::Graph(format!("missing node {key}")))?;
        if matches!(node.operation, Operation::Output) {
            continue;
        }
        if node.semantic_id == "ironbeam/test-stream" && Some(key) != primary_test_stream {
            let primary = primary_test_stream
                .ok_or_else(|| V5Error::Wire("missing primary TestStream".into()))?;
            let transform = components
                .transforms
                .get(&id("v5_t", primary))
                .ok_or_else(|| V5Error::Wire("missing primary TestStream transform".into()))?;
            let pc_id = id("v5_pc", key);
            let pc = components
                .pcollections
                .get(&pc_id)
                .ok_or_else(|| V5Error::Wire(format!("missing output collection for {key}")))?;
            let primary_pc = components
                .pcollections
                .get(&id("v5_pc", primary))
                .ok_or_else(|| V5Error::Wire("missing primary TestStream output".into()))?;
            if transform.outputs.get(key.as_str()) != Some(&pc_id)
                || pc.unique_name != *key
                || pc.is_bounded != beam_boundedness(node)
                || pc.windowing_strategy_id != windowing_id(&node.time)
                || pc.coder_id != primary_pc.coder_id
            {
                return Err(V5Error::Wire(format!(
                    "tagged TestStream output differs for {key}"
                )));
            }
            continue;
        }
        if needs_side_global(graph, node) {
            let side_pc = collection_id(&node.additional_inputs[0].key);
            let global_pc = side_global_collection_id(key);
            let transform = components
                .transforms
                .get(&side_global_transform_id(key))
                .ok_or_else(|| V5Error::Wire(format!("missing side global transform {key}")))?;
            if transform.unique_name != format!("{key}:side-global")
                || transform.inputs != HashMap::from([("in".into(), side_pc.clone())])
                || transform.outputs.get("out") != Some(&global_pc)
                || transform.environment_id != ids::ENV_ID
                || transform.spec.as_ref()
                    != Some(&FunctionSpec {
                        urn: urns::TRANSFORM_WINDOW_INTO.into(),
                        payload: WindowIntoPayload {
                            window_fn: Some(window_fn(&Window::Global)?),
                        }
                        .encode_to_vec(),
                    })
            {
                return Err(V5Error::Wire(format!(
                    "side global transform differs for {key}"
                )));
            }
            let side = components
                .pcollections
                .get(&side_pc)
                .ok_or_else(|| V5Error::Wire(format!("missing side collection for {key}")))?;
            let global = components.pcollections.get(&global_pc).ok_or_else(|| {
                V5Error::Wire(format!("missing side global collection for {key}"))
            })?;
            if global.unique_name != format!("{key}:side-global")
                || global.coder_id != side.coder_id
                || global.windowing_strategy_id != ids::WINDOWING_ID
                || global.is_bounded != is_bounded::Enum::Bounded as i32
            {
                return Err(V5Error::Wire(format!(
                    "side global collection differs for {key}"
                )));
            }
        }
        if matches!(
            node.operation,
            Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
        ) {
            let mut expected_flatten_inputs = HashMap::new();
            for (input_index, parent) in cogroup_inputs(node).into_iter().enumerate() {
                let parent_node = graph
                    .node(parent)
                    .ok_or_else(|| V5Error::Graph(format!("missing co-group input {parent}")))?;
                let tag_pc = cogroup_tag_collection_id(key, input_index);
                let tag_transform = components
                    .transforms
                    .get(&cogroup_tag_transform_id(key, input_index))
                    .ok_or_else(|| {
                        V5Error::Wire(format!(
                            "missing co-group tag transform {key}:{input_index}"
                        ))
                    })?;
                if tag_transform.unique_name != format!("{key}:tag{input_index}")
                    || tag_transform.inputs != HashMap::from([("in".into(), collection_id(parent))])
                    || tag_transform.outputs.get("out") != Some(&tag_pc)
                    || tag_transform.environment_id != ids::ENV_ID
                {
                    return Err(V5Error::Wire(format!(
                        "wrong co-group tag wiring {key}:{input_index}"
                    )));
                }
                let tag_spec = tag_transform
                    .spec
                    .as_ref()
                    .ok_or_else(|| V5Error::Wire("missing co-group tag spec".into()))?;
                if tag_spec.urn != urns::TRANSFORM_PARDO {
                    return Err(V5Error::Wire("wrong co-group tag URN".into()));
                }
                let pardo = ParDoPayload::decode(tag_spec.payload.as_slice())
                    .map_err(|error| V5Error::Wire(error.to_string()))?;
                if pardo.requests_finalization {
                    return Err(V5Error::Wire(format!(
                        "unexpected bundle finalization request for {key}:tag{input_index}"
                    )));
                }
                let do_fn = pardo
                    .do_fn
                    .ok_or_else(|| V5Error::Wire("missing co-group tag DoFn".into()))?;
                if do_fn.urn != urns::DOFN_IRONBEAM_V5
                    || V5NodeRef::decode(&do_fn.payload)?
                        != (V5NodeRef {
                            key: key.clone(),
                            identity: translated.identity.clone(),
                            co_group_input: Some(input_index),
                        })
                {
                    return Err(V5Error::Wire(format!(
                        "wrong co-group tag reference {key}:{input_index}"
                    )));
                }
                let tag_collection = components
                    .pcollections
                    .get(&tag_pc)
                    .ok_or_else(|| V5Error::Wire("missing co-group tag collection".into()))?;
                if tag_collection.unique_name != format!("{key}:tag{input_index}")
                    || tag_collection.coder_id != CODER_V5_KV
                    || tag_collection.windowing_strategy_id != windowing_id(&node.time)
                    || tag_collection.is_bounded != beam_boundedness(parent_node)
                {
                    return Err(V5Error::Wire("wrong co-group tag collection".into()));
                }
                expected_flatten_inputs.insert(format!("in{input_index}"), tag_pc);
            }
            let flatten_pc = cogroup_flatten_collection_id(key);
            let flatten_transform = components
                .transforms
                .get(&cogroup_flatten_transform_id(key))
                .ok_or_else(|| V5Error::Wire("missing co-group Flatten".into()))?;
            if flatten_transform.unique_name != format!("{key}:flatten")
                || flatten_transform
                    .spec
                    .as_ref()
                    .map(|spec| spec.urn.as_str())
                    != Some(urns::TRANSFORM_FLATTEN)
                || flatten_transform
                    .spec
                    .as_ref()
                    .is_some_and(|spec| !spec.payload.is_empty())
                || flatten_transform.inputs != expected_flatten_inputs
                || flatten_transform.outputs.get("out") != Some(&flatten_pc)
                || !flatten_transform.environment_id.is_empty()
            {
                return Err(V5Error::Wire("wrong co-group Flatten transform".into()));
            }
            let flatten_collection = components
                .pcollections
                .get(&flatten_pc)
                .ok_or_else(|| V5Error::Wire("missing co-group Flatten collection".into()))?;
            if flatten_collection.unique_name != format!("{key}:flatten")
                || flatten_collection.coder_id != CODER_V5_KV
                || flatten_collection.windowing_strategy_id != windowing_id(&node.time)
                || flatten_collection.is_bounded != beam_boundedness(node)
            {
                return Err(V5Error::Wire("wrong co-group Flatten collection".into()));
            }
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
            let tag_transform = components
                .transforms
                .get(&fold_tag_transform_id(key))
                .ok_or_else(|| V5Error::Wire(format!("missing keyed-fold tag for {key}")))?;
            if tag_transform.unique_name != format!("{key}:tag")
                || tag_transform.inputs != HashMap::from([("in".into(), collection_id(parent))])
                || tag_transform.outputs != HashMap::from([("out".into(), tag_pc.clone())])
                || tag_transform.environment_id != ids::ENV_ID
            {
                return Err(V5Error::Wire(format!(
                    "keyed-fold tag wiring differs for {key}"
                )));
            }
            let tag_spec = tag_transform
                .spec
                .as_ref()
                .ok_or_else(|| V5Error::Wire(format!("missing keyed-fold tag spec for {key}")))?;
            if tag_spec.urn != urns::TRANSFORM_PARDO {
                return Err(V5Error::Wire(format!("wrong keyed-fold tag URN for {key}")));
            }
            let tag_pardo = ParDoPayload::decode(tag_spec.payload.as_slice())
                .map_err(|error| V5Error::Wire(error.to_string()))?;
            if tag_pardo.requests_finalization
                || tag_pardo.do_fn.as_ref().is_none_or(|do_fn| {
                    do_fn.urn != urns::DOFN_IRONBEAM_V5
                        || V5NodeRef::decode(&do_fn.payload).ok()
                            != Some(V5NodeRef {
                                key: key.clone(),
                                identity: translated.identity.clone(),
                                co_group_input: Some(0),
                            })
                })
            {
                return Err(V5Error::Wire(format!(
                    "keyed-fold tag contract differs for {key}"
                )));
            }
            let tagged = components.pcollections.get(&tag_pc).ok_or_else(|| {
                V5Error::Wire(format!("missing keyed-fold tag collection for {key}"))
            })?;
            if tagged.unique_name != format!("{key}:tag")
                || tagged.coder_id != CODER_V5_KV
                || tagged.windowing_strategy_id != windowing_id(&node.time)
                || tagged.is_bounded != beam_boundedness(node)
            {
                return Err(V5Error::Wire(format!(
                    "keyed-fold tag collection differs for {key}"
                )));
            }
            if matches!(
                node.operation,
                Operation::FoldGlobal | Operation::GlobalCombine
            ) && node.time.window == Window::Global
            {
                let seed_pc = fold_seed_collection_id(key);
                let seed = components.pcollections.get(&seed_pc).ok_or_else(|| {
                    V5Error::Wire(format!("missing global-fold seed collection for {key}"))
                })?;
                if seed.unique_name != format!("{key}:seed")
                    || seed.coder_id != CODER_V5_KV
                    || seed.windowing_strategy_id != ids::WINDOWING_ID
                    || seed.is_bounded != is_bounded::Enum::Bounded as i32
                {
                    return Err(V5Error::Wire(format!(
                        "global-fold seed collection differs for {key}"
                    )));
                }
                let seed_transform = components
                    .transforms
                    .get(&fold_seed_transform_id(key))
                    .ok_or_else(|| {
                        V5Error::Wire(format!("missing global-fold seed transform for {key}"))
                    })?;
                if seed_transform.unique_name != format!("{key}:seed")
                    || seed_transform.inputs
                        != HashMap::from([("in".into(), "v5_impulse_pc".into())])
                    || seed_transform.outputs != HashMap::from([("out".into(), seed_pc.clone())])
                    || seed_transform.environment_id != ids::ENV_ID
                {
                    return Err(V5Error::Wire(format!(
                        "global-fold seed wiring differs for {key}"
                    )));
                }
                let seed_spec = seed_transform.spec.as_ref().ok_or_else(|| {
                    V5Error::Wire(format!("missing global-fold seed spec for {key}"))
                })?;
                let seed_pardo = ParDoPayload::decode(seed_spec.payload.as_slice())
                    .map_err(|error| V5Error::Wire(error.to_string()))?;
                if seed_spec.urn != urns::TRANSFORM_PARDO
                    || seed_pardo.requests_finalization
                    || seed_pardo.do_fn.as_ref().is_none_or(|do_fn| {
                        do_fn.urn != urns::DOFN_IRONBEAM_V5
                            || V5NodeRef::decode(&do_fn.payload).ok()
                                != Some(V5NodeRef {
                                    key: key.clone(),
                                    identity: translated.identity.clone(),
                                    co_group_input: Some(1),
                                })
                    })
                {
                    return Err(V5Error::Wire(format!(
                        "global-fold seed contract differs for {key}"
                    )));
                }
                let flat_pc = fold_flatten_collection_id(key);
                let flat = components.pcollections.get(&flat_pc).ok_or_else(|| {
                    V5Error::Wire(format!("missing global-fold Flatten collection for {key}"))
                })?;
                if flat.unique_name != format!("{key}:flatten")
                    || flat.coder_id != CODER_V5_KV
                    || flat.windowing_strategy_id != ids::WINDOWING_ID
                    || flat.is_bounded != beam_boundedness(node)
                {
                    return Err(V5Error::Wire(format!(
                        "global-fold Flatten collection differs for {key}"
                    )));
                }
                let flat_transform = components
                    .transforms
                    .get(&fold_flatten_transform_id(key))
                    .ok_or_else(|| {
                        V5Error::Wire(format!("missing global-fold Flatten for {key}"))
                    })?;
                if flat_transform.unique_name != format!("{key}:flatten")
                    || flat_transform.spec.as_ref()
                        != Some(&FunctionSpec {
                            urn: urns::TRANSFORM_FLATTEN.into(),
                            payload: Vec::new(),
                        })
                    || flat_transform.inputs
                        != HashMap::from([("in0".into(), tag_pc.clone()), ("in1".into(), seed_pc)])
                    || flat_transform.outputs != HashMap::from([("out".into(), flat_pc)])
                    || !flat_transform.environment_id.is_empty()
                {
                    return Err(V5Error::Wire(format!(
                        "global-fold Flatten wiring differs for {key}"
                    )));
                }
            }
            let group_pc = fold_group_collection_id(key);
            let group_transform = components
                .transforms
                .get(&fold_group_transform_id(key))
                .ok_or_else(|| V5Error::Wire(format!("missing keyed-fold GBK for {key}")))?;
            if group_transform.unique_name != format!("{key}:group")
                || group_transform.spec.as_ref()
                    != Some(&FunctionSpec {
                        urn: urns::TRANSFORM_GROUP_BY_KEY.into(),
                        payload: Vec::new(),
                    })
                || group_transform.inputs
                    != HashMap::from([(
                        "in".into(),
                        if matches!(
                            node.operation,
                            Operation::FoldGlobal | Operation::GlobalCombine
                        ) && node.time.window == Window::Global
                        {
                            fold_flatten_collection_id(key)
                        } else {
                            tag_pc
                        },
                    )])
                || group_transform.outputs != HashMap::from([("out".into(), group_pc.clone())])
                || !group_transform.environment_id.is_empty()
            {
                return Err(V5Error::Wire(format!("keyed-fold GBK differs for {key}")));
            }
            let grouped = components.pcollections.get(&group_pc).ok_or_else(|| {
                V5Error::Wire(format!("missing keyed-fold grouped collection for {key}"))
            })?;
            if grouped.unique_name != format!("{key}:group")
                || grouped.coder_id != CODER_V5_KV_ITER
                || grouped.windowing_strategy_id != windowing_id(&node.time)
                || grouped.is_bounded != beam_boundedness(node)
            {
                return Err(V5Error::Wire(format!(
                    "keyed-fold grouped collection differs for {key}"
                )));
            }
        }
        let expected_inputs = if node.semantic_id == "ironbeam/test-stream" {
            HashMap::new()
        } else {
            beam_inputs(graph, node, "v5_impulse_pc")
        };
        let transform = components
            .transforms
            .get(&id("v5_t", key))
            .ok_or_else(|| V5Error::Wire(format!("missing transform for {key}")))?;
        if transform.unique_name != *key
            || transform.inputs != expected_inputs
            || transform.environment_id
                != if node.semantic_id == "ironbeam/test-stream"
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
                    )
                {
                    ""
                } else {
                    ids::ENV_ID
                }
        {
            return Err(V5Error::Wire(format!("transform wiring differs for {key}")));
        }
        let spec = transform
            .spec
            .as_ref()
            .ok_or_else(|| V5Error::Wire(format!("missing transform spec for {key}")))?;
        if node.semantic_id == "ironbeam/test-stream" {
            if extra_test_stream_outputs > 0 {
                let expected_outputs = test_stream_keys
                    .iter()
                    .map(|key| ((*key).clone(), id("v5_pc", key)))
                    .collect::<HashMap<_, _>>();
                if transform.outputs != expected_outputs {
                    return Err(V5Error::Wire("tagged TestStream outputs differ".into()));
                }
            }
            let expected = translated
                .test_stream_payloads
                .get(key)
                .ok_or_else(|| V5Error::Wire(format!("missing test stream payload for {key}")))?;
            if spec.urn != urns::TRANSFORM_TEST_STREAM
                || TestStreamPayload::decode(spec.payload.as_slice())
                    .map_err(|error| V5Error::Wire(error.to_string()))?
                    != *expected
            {
                return Err(V5Error::Wire(format!(
                    "test stream payload differs for {key}"
                )));
            }
        } else if matches!(node.operation, Operation::FixedWindow | Operation::SlidingWindow | Operation::SessionWindow) {
            if spec.urn != urns::TRANSFORM_WINDOW_INTO {
                return Err(V5Error::Wire(format!("wrong WindowInto URN for {key}")));
            }
            let payload = WindowIntoPayload::decode(spec.payload.as_slice())
                .map_err(|error| V5Error::Wire(error.to_string()))?;
            if payload.window_fn != Some(window_fn(&node.time.window)?) {
                return Err(V5Error::Wire(format!(
                    "wrong WindowInto function for {key}"
                )));
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
            let expected = if matches!(node.operation, Operation::Flatten) {
                urns::TRANSFORM_FLATTEN
            } else if matches!(node.operation, Operation::Reshuffle) {
                urns::TRANSFORM_RESHUFFLE
            } else {
                urns::TRANSFORM_GROUP_BY_KEY
            };
            if spec.urn != expected || !spec.payload.is_empty() {
                return Err(V5Error::Wire(format!(
                    "wrong native transform spec for {key}"
                )));
            }
        } else {
            if spec.urn != urns::TRANSFORM_PARDO {
                return Err(V5Error::Wire(format!("wrong transform URN for {key}")));
            }
            let pardo = ParDoPayload::decode(spec.payload.as_slice())
                .map_err(|error| V5Error::Wire(error.to_string()))?;
            let mut expected_state = ParDoPayload::default();
            if let Operation::StatefulParDo { contract, .. } = &node.operation {
                let mut requirements = Vec::new();
                attach_stateful_contract(&mut expected_state, &mut requirements, contract)?;
                if requirements.iter().any(|urn| !translated.pipeline.requirements.contains(urn)) {
                    return Err(V5Error::Wire(format!("stateful requirement missing for {key}")));
                }
                for family in contract.timers.values() {
                    let expected_coder = Coder {
                        spec: Some(FunctionSpec {
                            urn: urns::CODER_TIMER.into(),
                            payload: Vec::new(),
                        }),
                        component_coder_ids: vec![
                            contract.key_coder_id.clone(),
                            contract.window_coder_id.clone(),
                        ],
                    };
                    if components.coders.get(&family.coder_id) != Some(&expected_coder) {
                        return Err(V5Error::Wire(format!(
                            "timer family coder differs for {key}"
                        )));
                    }
                }
            }
            if pardo.state_specs != expected_state.state_specs
                || pardo.timer_family_specs != expected_state.timer_family_specs
                || pardo.on_window_expiration_timer_family_spec
                    != expected_state.on_window_expiration_timer_family_spec
            {
                return Err(V5Error::Wire(format!("stateful DoFn contract differs for {key}")));
            }
            let expected_restriction = if is_production_source(node) {
                ids::CODER_BYTES
            } else {
                ""
            };
            if pardo.restriction_coder_id != expected_restriction {
                return Err(V5Error::Wire(format!(
                    "SDF restriction coder differs for {key}"
                )));
            }
            if pardo.requests_finalization != translated.finalization_sinks.contains(key) {
                return Err(V5Error::Wire(format!(
                    "bundle finalization contract differs for {key}"
                )));
            }
            let expected_side = side_input_specs(node);
            if pardo.side_inputs != expected_side {
                return Err(V5Error::Wire(format!(
                    "side input contract differs for {key}"
                )));
            }
            let do_fn = pardo
                .do_fn
                .ok_or_else(|| V5Error::Wire(format!("missing DoFn for {key}")))?;
            if do_fn.urn != urns::DOFN_IRONBEAM_V5
                || V5NodeRef::decode(&do_fn.payload)?
                    != (V5NodeRef {
                        key: key.clone(),
                        identity: translated.identity.clone(),
                        co_group_input: None,
                    })
            {
                return Err(V5Error::Wire(format!("wrong v5 node reference for {key}")));
            }
        }
        let pc_id = id("v5_pc", key);
        if matches!(node.operation, Operation::Sink { .. }) {
            if !transform.outputs.is_empty() || components.pcollections.contains_key(&pc_id) {
                return Err(V5Error::Wire(format!(
                    "sink {key} must have no Beam output"
                )));
            }
            continue;
        }
        let output_tag =
            if node.semantic_id == "ironbeam/test-stream" && extra_test_stream_outputs > 0 {
                key.as_str()
            } else {
                "out"
            };
        if transform.outputs.get(output_tag) != Some(&pc_id) {
            return Err(V5Error::Wire(format!("wrong output collection for {key}")));
        }
        let pc = components
            .pcollections
            .get(&pc_id)
            .ok_or_else(|| V5Error::Wire(format!("missing output collection for {key}")))?;
        if pc.unique_name != *key
            || pc.is_bounded != beam_boundedness(node)
            || pc.windowing_strategy_id != windowing_id(&node.time)
        {
            return Err(V5Error::Wire(format!(
                "output collection metadata differs for {key}"
            )));
        }
        let mut expected_strategy = match &node.time.window {
            Window::Global => global_windowing_strategy(),
            Window::Fixed { size_ns } => {
                fixed_windowing_strategy(*size_ns, 0).map_err(V5Error::Wire)?
            }
            Window::FixedOffset { size_ns, offset_ns } => {
                fixed_windowing_strategy(*size_ns, *offset_ns).map_err(V5Error::Wire)?
            }
            Window::Sliding { size_ns, period_ns, offset_ns } => {
                sliding_windowing_strategy(*size_ns, *period_ns, *offset_ns).map_err(V5Error::Wire)?
            }
            Window::Session { gap_ns } => {
                session_windowing_strategy(*gap_ns).map_err(V5Error::Wire)?
            }
        };
        apply_trigger(&mut expected_strategy, node.time.trigger);
        expected_strategy.output_time = match node.time.output_time {
            WindowOutputTime::EarliestInPane => output_time::Enum::EarliestInPane as i32,
            WindowOutputTime::EndOfWindow => output_time::Enum::EndOfWindow as i32,
            WindowOutputTime::LatestInPane => output_time::Enum::LatestInPane as i32,
        };
        expected_strategy.allowed_lateness = node.time.allowed_lateness_ns / 1_000_000;
        if components
            .windowing_strategies
            .get(&pc.windowing_strategy_id)
            != Some(&expected_strategy)
        {
            return Err(V5Error::Wire(format!(
                "windowing strategy differs for {key}"
            )));
        }
        if matches!(
            node.operation,
            Operation::GroupByKey
                | Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
        ) || kv_inputs.contains(key.as_str())
        {
            let expected = if matches!(
                node.operation,
                Operation::GroupByKey
                    | Operation::CoGroupByKey
                    | Operation::JoinInner
                    | Operation::JoinLeft
                    | Operation::JoinRight
                    | Operation::JoinFull
            ) {
                CODER_V5_KV_ITER
            } else {
                CODER_V5_KV
            };
            if pc.coder_id != expected {
                return Err(V5Error::Wire(format!("wrong keyed coder for {key}")));
            }
            continue;
        }
        let outer = components
            .coders
            .get(&pc.coder_id)
            .ok_or_else(|| V5Error::Wire(format!("missing length-prefix coder for {key}")))?;
        if outer.spec.as_ref().map(|s| s.urn.as_str()) != Some(urns::CODER_LENGTH_PREFIX)
            || outer.component_coder_ids.len() != 1
        {
            return Err(V5Error::Wire(format!(
                "wrong length-prefix coder for {key}"
            )));
        }
        let inner = components
            .coders
            .get(&outer.component_coder_ids[0])
            .ok_or_else(|| V5Error::Wire(format!("missing element coder for {key}")))?;
        let inner_spec = inner
            .spec
            .as_ref()
            .ok_or_else(|| V5Error::Wire(format!("missing element coder spec for {key}")))?;
        let matches_element = if node.coder_id == urns::CODER_ROW {
            inner_spec.urn == urns::CODER_ROW
                && inner_spec.payload == node.coder_payload
                && inner.component_coder_ids.is_empty()
        } else if node.coder_id == urns::CODER_SHARDED_KEY {
            inner_spec.urn == urns::CODER_SHARDED_KEY
                && inner_spec.payload.is_empty()
                && inner.component_coder_ids.len() == 1
                && components.coders.get(&inner.component_coder_ids[0]).is_some_and(|key_coder| {
                    key_coder.component_coder_ids.is_empty()
                        && key_coder.spec.as_ref().is_some_and(|spec| {
                            spec.urn.as_bytes() == node.coder_payload && spec.payload.is_empty()
                        })
                })
        } else {
            inner_spec.urn == urns::CODER_IRONBEAM_V5
                && inner.component_coder_ids.is_empty()
                && serde_json::from_slice::<ElementDescriptor>(&inner_spec.payload)
                    .map_err(|error| V5Error::Wire(error.to_string()))?
                    == descriptor(node)
        };
        if !matches_element {
            return Err(V5Error::Wire(format!("element coder differs for {key}")));
        }
    }
    for (coder_id, urn, parts) in [
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
                    | Operation::StatefulParDo { .. }
            )
        }) {
            let coder = components
                .coders
                .get(coder_id)
                .ok_or_else(|| V5Error::Wire(format!("missing keyed coder {coder_id}")))?;
            if coder.spec.as_ref().map(|spec| spec.urn.as_str()) != Some(urn)
                || coder.component_coder_ids != parts
            {
                return Err(V5Error::Wire(format!("wrong keyed coder {coder_id}")));
            }
        }
    }
    if graph
        .nodes
        .iter()
        .any(|node| node.time.window != Window::Global)
    {
        let coder = components
            .coders
            .get(ids::CODER_INTERVAL_WINDOW)
            .ok_or_else(|| V5Error::Wire("missing interval-window coder".into()))?;
        if coder.spec.as_ref().map(|spec| spec.urn.as_str()) != Some(urns::CODER_INTERVAL_WINDOW)
            || !coder.component_coder_ids.is_empty()
        {
            return Err(V5Error::Wire("wrong interval-window coder".into()));
        }
    }
    let mut expected_requirements = Vec::new();
    for key in &expected_chain {
        if let Some(Node { operation: Operation::StatefulParDo { contract, .. }, .. }) = graph.node(key) {
            attach_stateful_contract(
                &mut ParDoPayload::default(),
                &mut expected_requirements,
                contract,
            )?;
        }
    }
    if graph.nodes.iter().any(is_production_source) {
        expected_requirements.push(urns::REQUIREMENT_SDF.to_string());
    }
    if translated.pipeline.requirements != expected_requirements {
        return Err(V5Error::Wire("Beam pipeline requirements differ".into()));
    }
    Ok(())
}
