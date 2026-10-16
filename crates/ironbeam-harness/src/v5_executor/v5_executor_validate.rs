impl V5BundleExecutor {
    fn validate(&self, plan: &BundlePlan) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut collections: HashMap<String, Option<String>> = HashMap::new();
        let mut seen_nodes = HashSet::<String>::new();
        let mut runner_source_seen = false;
        let mut user_source_seen = false;
        let mut boundary_seen = false;
        let mut terminal_seen = false;
        for step in &plan.steps {
            match step {
                Step::MapWindows { .. } | Step::MergeWindows { .. } => {
                    bail!("window mapping or merging requires its runner-owned bundle")
                }
                Step::Sdf {
                    stage,
                    transform_id,
                    ..
                } => {
                    bail!("v5 SDF stage {stage:?} ({transform_id}) is not executable yet")
                }
                Step::Source {
                    output_pc, wire, ..
                } => {
                    if runner_source_seen || !collections.is_empty() {
                        bail!("v5 bundle has more than one runner source");
                    }
                    let expected = if self.group_at_output(output_pc).is_some()
                        || self.fold_at_output(output_pc).is_some()
                        || self.cogroup_at_output(output_pc).is_some()
                    {
                        if *wire == SourceWire::KvStateBackedIter {
                            if plan.state_endpoint.as_deref().is_none_or(str::is_empty) {
                                bail!("v5 state-backed grouped source has no State endpoint");
                            }
                            SourceWire::KvStateBackedIter
                        } else {
                            SourceWire::KvIterLp
                        }
                    } else if self.group_at_input(output_pc).is_some()
                        || self.fold_at_input(output_pc).is_some()
                        || self.cogroup_at_tagged_input(output_pc).is_some()
                    {
                        SourceWire::KvLp
                    } else {
                        SourceWire::Opaque
                    };
                    if *wire != expected {
                        bail!("v5 bundle source coder differs from graph boundary at {output_pc}");
                    }
                    runner_source_seen = true;
                    collections.insert(output_pc.clone(), None);
                }
                Step::V5ParDo {
                    node_ref,
                    input_pc,
                    output_pc,
                    stateful_value,
                    ..
                } => {
                    let parent = collections.get(input_pc.as_str()).ok_or_else(|| {
                        anyhow!(
                            "v5 bundle collection wiring is disconnected at {}",
                            node_ref.key
                        )
                    })?;
                    let parent = parent.clone();
                    if collections.contains_key(output_pc.as_str()) {
                        bail!("v5 bundle repeats collection {output_pc}");
                    }
                    node_ref.identity.verify(&self.identity)?;
                    if let Some(input_index) = node_ref.co_group_input {
                        if self.graph.node(&node_ref.key).is_some_and(|node| {
                            matches!(
                                node.operation,
                                Operation::FoldPerKey
                                    | Operation::FoldGlobal
                                    | Operation::GlobalCombine
                            )
                        }) {
                            let node = self.graph.node(&node_ref.key).expect("checked above");
                            let upstream = node.input.as_deref().ok_or_else(|| {
                                anyhow!("v5 keyed fold {} has no parent", node.key)
                            })?;
                            let seed = input_index == 1
                                && matches!(
                                    node.operation,
                                    Operation::FoldGlobal | Operation::GlobalCombine
                                )
                                && node.time.window == Window::Global;
                            if input_index != 0 && !seed
                                || input_pc
                                    != &if seed {
                                        "v5_impulse_pc".to_owned()
                                    } else {
                                        collection_id(upstream)
                                    }
                                || output_pc
                                    != &if seed {
                                        fold_seed_collection_id(&node.key)
                                    } else {
                                        fold_tag_collection_id(&node.key)
                                    }
                                || !self.registry.has_group_coder(&node.key)
                                || !has_group_application(&self.registry, node)
                                || !seen_nodes.insert(format!("{}:tag{input_index}", node.key))
                            {
                                bail!("v5 keyed fold tag differs from worker graph");
                            }
                            if parent.as_deref() != if seed { None } else { Some(upstream) } {
                                if parent.is_some() || user_source_seen || boundary_seen {
                                    bail!("v5 keyed fold tag has an unexpected runner boundary");
                                }
                                boundary_seen = true;
                            }
                            if seed {
                                boundary_seen = true;
                            }
                            collections.insert(
                                output_pc.clone(),
                                Some(format!("{}:tag{input_index}", node.key)),
                            );
                            continue;
                        }
                        let upstream =
                            self.cogroup_parent(&node_ref.key, input_index)
                                .ok_or_else(|| {
                                    anyhow!("v5 co-group tag input is absent from worker graph")
                                })?;
                        if !(self.registry.has_cogroup_coder(&node_ref.key)
                            || self.registry.has_join_coder(&node_ref.key))
                            || input_pc != &collection_id(upstream)
                            || output_pc != &cogroup_tag_collection_id(&node_ref.key, input_index)
                            || !self.registry.has_coder(upstream)
                            || !seen_nodes.insert(format!("{}:tag{input_index}", node_ref.key))
                        {
                            bail!("v5 co-group tag differs from worker graph");
                        }
                        if parent.as_deref() != Some(upstream) {
                            if parent.is_some() || user_source_seen || boundary_seen {
                                bail!("v5 co-group tag has an unexpected runner boundary");
                            }
                            boundary_seen = true;
                        }
                        collections.insert(
                            output_pc.clone(),
                            Some(format!("{}:tag{input_index}", node_ref.key)),
                        );
                        continue;
                    }
                    if !seen_nodes.insert(node_ref.key.clone()) {
                        bail!("v5 bundle repeats node {}", node_ref.key);
                    }
                    let node = self
                        .graph
                        .node(&node_ref.key)
                        .ok_or_else(|| anyhow!("v5 node {} is absent from worker", node_ref.key))?;
                    if *stateful_value != matches!(node.operation, Operation::StatefulParDo { .. }) {
                        bail!("v5 stateful declaration differs from worker graph at {}", node.key);
                    }
                    if *stateful_value
                        && (!self.registry.has_stateful_value(&node.key)
                            || !self.registry.has_group_coder(&node.key)
                            || plan.state_endpoint.as_deref().is_none_or(str::is_empty))
                    {
                        bail!("v5 stateful value node {} lacks callback, coder, or State endpoint", node.key);
                    }
                    if matches!(node.operation, Operation::Sink { .. }) {
                        if !output_pc.is_empty() {
                            bail!("v5 sink {} has an unexpected output", node.key);
                        }
                    } else if output_pc != &collection_id(&node.key) {
                        bail!("v5 node {} output differs from worker graph", node.key);
                    }
                    if matches!(node.operation, Operation::SideInputMap | Operation::WaitOn | Operation::WaitOnWindowed) {
                        let mut expected_sides = HashMap::new();
                        for (index, side) in node.additional_inputs.iter().enumerate() {
                            let local = if matches!(node.operation, Operation::WaitOnWindowed)
                                && node.additional_inputs.len() > 1
                            {
                                format!("side{index}")
                            } else {
                                "side".to_owned()
                            };
                            let collection = if self
                                .graph
                                .node(&side.key)
                                .is_some_and(|side| side.time.window == Window::Global)
                                || matches!(node.operation, Operation::WaitOnWindowed)
                            {
                                collection_id(&side.key)
                            } else {
                                side_global_collection_id(&node.key)
                            };
                            expected_sides.insert(local, collection);
                        }
                        if (matches!(node.operation, Operation::SideInputMap)
                            && !self.registry.has_side_input(&node.key))
                            || plan.side_inputs.get(&node.key)
                                != Some(&expected_sides)
                            || plan.state_endpoint.as_deref().is_none_or(str::is_empty)
                        {
                            bail!("v5 side input {} differs from worker graph or has no State endpoint", node.key);
                        }
                    } else if plan.side_inputs.contains_key(&node.key) {
                        bail!("v5 node {} has an unexpected side input", node.key);
                    }
                    if matches!(node.operation, Operation::Source { .. }) {
                        if boundary_seen || parent.is_some() {
                            bail!("v5 bundle source node is not rooted at Impulse");
                        }
                        user_source_seen = true;
                    } else if matches!(
                        node.operation,
                        Operation::FoldPerKey | Operation::FoldGlobal | Operation::GlobalCombine
                    ) {
                        if input_pc != &fold_group_collection_id(&node.key)
                            || !has_group_application(&self.registry, node)
                            || !self.registry.has_group_coder(&node.key)
                            || user_source_seen
                            || boundary_seen
                            || parent.is_some()
                        {
                            bail!(
                                "v5 keyed fold {} is not connected to its native GBK boundary",
                                node.key
                            );
                        }
                        boundary_seen = true;
                    } else if node.input.as_deref() != parent.as_deref() {
                        // Prism may collapse any number of adjacent native
                        // Reshuffles and give the consumer an earlier input PC.
                        // Walk only Reshuffle edges, preserving graph identity
                        // and rejecting aliases through user transforms.
                        let (connected, elided) = node
                            .input
                            .as_deref()
                            .and_then(|key| matching_reshuffle_ancestor(&self.graph, key, input_pc))
                            .ok_or_else(|| {
                                anyhow!(
                            "v5 bundle node {} input {} is not connected to graph parent {:?}",
                            node.key, input_pc, node.input)
                            })?;
                        if parent.as_deref() == Some(connected) {
                            // The alias is already produced by this bundle.
                        } else {
                            let boundary = self.graph.node(connected).is_some_and(|candidate| {
                                matches!(
                                    candidate.operation,
                                    Operation::Flatten
                                        | Operation::FixedWindow
                                        | Operation::SlidingWindow
                                        | Operation::Reshuffle
                                        | Operation::Source { .. }
                                ) || candidate.semantic_id == "ironbeam/test-stream"
                                    || matches!(candidate.operation, Operation::GroupByKey)
                                        && self.registry.has_group_coder(connected)
                                    || matches!(
                                        candidate.operation,
                                        Operation::CoGroupByKey
                                            | Operation::JoinInner
                                            | Operation::JoinLeft
                                            | Operation::JoinRight
                                            | Operation::JoinFull
                                    ) && (self.registry.has_cogroup_coder(connected)
                                        || self.registry.has_join_coder(connected))
                            });
                            if !elided && !boundary {
                                bail!("v5 bundle node {} has no native runner boundary at {connected}", node.key);
                            }
                            if user_source_seen || boundary_seen || parent.is_some() {
                                bail!("v5 bundle has an unexpected runner boundary at {connected}");
                            }
                            boundary_seen = true;
                        }
                    }
                    if let Some(Some(parent_key)) = collections.get(input_pc.as_str()) {
                        if self.graph.node(parent_key).is_some_and(|parent_node| {
                            matches!(parent_node.operation, Operation::Sink { .. })
                        }) {
                            bail!("v5 bundle continues after sink {parent_key}");
                        }
                    }
                    if !matches!(
                        node.operation,
                        Operation::Source { .. }
                            | Operation::Map
                            | Operation::Filter
                            | Operation::FlatMap
                            | Operation::AssignTimestamp
                            | Operation::StatefulParDo { .. }
                            | Operation::FoldPerKey
                            | Operation::FoldGlobal
                            | Operation::GlobalCombine
                            | Operation::SideInputMap
                            | Operation::WaitOn
                            | Operation::WaitOnWindowed
                            | Operation::Sink { .. }
                    ) {
                        bail!("v5 Fn API executor does not support node {}", node.key);
                    }
                    if !self.registry.has_coder(&node.key) {
                        bail!("v5 worker has no coder for node {}", node.key);
                    }
                    if let Some(input) = &node.input {
                        if !self.registry.has_coder(input) {
                            bail!("v5 worker has no input coder for node {}", node.key);
                        }
                    }
                    if matches!(node.operation, Operation::Sink { .. }) {
                        terminal_seen = true;
                    }
                    if !output_pc.is_empty() {
                        collections.insert(output_pc.clone(), Some(node.key.clone()));
                    }
                    keys.push(node.key.clone());
                }
                Step::WindowInto {
                    input_pc,
                    output_pc,
                    window_fn,
                    ..
                } => {
                    if !collections.contains_key(input_pc.as_str())
                        || collections.contains_key(output_pc.as_str())
                    {
                        bail!("v5 WindowInto collection wiring is disconnected");
                    }
                    if let Some(node) = self.graph.nodes.iter().find(|node| {
                        matches!(node.operation, Operation::SideInputMap | Operation::WaitOn)
                            && self
                                .graph
                                .node(&node.additional_inputs[0].key)
                                .is_some_and(|side| side.time.window != Window::Global)
                            && side_global_collection_id(&node.key) == *output_pc
                    }) {
                        let side = &node.additional_inputs[0].key;
                        if *input_pc != collection_id(side)
                            || *window_fn
                                != (FunctionSpec {
                                    urn: ironbeam_graphx::urns::WINDOW_FN_GLOBAL.into(),
                                    payload: Vec::new(),
                                })
                            || !seen_nodes.insert(format!("{}:side-global", node.key))
                        {
                            bail!("v5 side global window differs from worker graph");
                        }
                        collections.insert(output_pc.clone(), Some(side.clone()));
                        continue;
                    }
                    let node = self
                        .graph
                        .nodes
                        .iter()
                        .find(|node| collection_id(&node.key) == *output_pc)
                        .ok_or_else(|| {
                            anyhow!("v5 WindowInto output {output_pc} is absent from worker")
                        })?;
                    if !matches!(node.operation, Operation::FixedWindow | Operation::SlidingWindow | Operation::SessionWindow)
                        || node
                            .input
                            .as_ref()
                            .is_none_or(|input| collection_id(input) != *input_pc)
                        || !seen_nodes.insert(node.key.clone())
                    {
                        bail!(
                            "v5 WindowInto node {} differs from the worker graph",
                            node.key
                        );
                    }
                    let expected = match &node.time.window {
                        Window::Fixed { size_ns } => fixed_window_fn(*size_ns, 0),
                        Window::FixedOffset { size_ns, offset_ns } => {
                            fixed_window_fn(*size_ns, *offset_ns)
                        }
                        Window::Sliding { size_ns, period_ns, offset_ns } => {
                            sliding_window_fn(*size_ns, *period_ns, *offset_ns)
                        }
                        Window::Session { gap_ns } => session_window_fn(*gap_ns),
                        Window::Global => bail!("v5 WindowInto {} has no fixed window", node.key),
                    }
                    .map_err(|error| anyhow!(error))?;
                    if *window_fn != expected {
                        bail!("v5 WindowInto {} function differs from worker", node.key);
                    }
                    if collections.get(input_pc) == Some(&None)
                        && node.input.as_deref().is_some_and(|parent| {
                            self.graph.node(parent).is_some_and(|source| {
                                source.semantic_id == "ironbeam/test-stream"
                                    && collection_id(parent) == *input_pc
                            })
                        })
                    {
                        boundary_seen = true;
                    }
                    collections.insert(output_pc.clone(), Some(node.key.clone()));
                }
                Step::Sink { input_pc, wire, .. } => {
                    if !collections.contains_key(input_pc.as_str()) {
                        bail!("v5 bundle runner sink is disconnected at {input_pc}");
                    }
                    let expected = if self.group_at_input(input_pc).is_some()
                        || self.fold_at_input(input_pc).is_some()
                        || self.cogroup_at_tagged_input(input_pc).is_some()
                    {
                        SinkWire::KvLp
                    } else {
                        SinkWire::Opaque
                    };
                    if *wire != expected {
                        bail!("v5 bundle sink coder differs from graph boundary at {input_pc}");
                    }
                    terminal_seen = true;
                }
            }
        }
        if !runner_source_seen || !(user_source_seen || boundary_seen) || !terminal_seen {
            bail!("v5 bundle requires a runner source, v5 input, and terminal sink (source={runner_source_seen}, user_source={user_source_seen}, boundary={boundary_seen}, terminal={terminal_seen})");
        }
        Ok(keys)
    }
}
