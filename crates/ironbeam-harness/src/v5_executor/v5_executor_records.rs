struct BundleInputs<'a> {
    side_views: &'a mut HashMap<String, Box<dyn Any + Send>>,
    state: Option<&'a StateClient>,
}

impl V5BundleExecutor {
    async fn process_record(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
        source_key: Option<&str>,
        source_value: WireValue,
        inputs: BundleInputs<'_>,
        outputs: &mut BundleOutputs<'_>,
    ) -> Result<()> {
        let mut collections: HashMap<&str, Vec<WireValue>> = HashMap::new();
        for step in &plan.steps {
            match step {
                Step::MapWindows { .. } => {
                    bail!("MapWindows cannot run in a graph ParDo bundle")
                }
                Step::Sdf {
                    stage: SdfStage::ProcessSizedElements,
                    node_ref,
                    output_pc,
                    ..
                } => {
                    let values = if source_key == Some(node_ref.key.as_str()) {
                        vec![source_value.clone()]
                    } else {
                        Vec::new()
                    };
                    collections.insert(output_pc, values);
                }
                Step::Sdf {
                    stage,
                    transform_id,
                    ..
                } => {
                    bail!("v5 SDF stage {stage:?} ({transform_id}) cannot run in a process bundle")
                }
                Step::Source { output_pc, .. } => {
                    let values = if source_key.is_some() {
                        Vec::new()
                    } else {
                        vec![source_value.clone()]
                    };
                    collections.insert(output_pc, values);
                }
                Step::V5ParDo {
                    transform_id,
                    node_ref,
                    input_pc,
                    output_pc,
                    ..
                } => {
                    if let Some(input_index) = node_ref.co_group_input {
                        if self.graph.node(&node_ref.key).is_some_and(|node| {
                            matches!(
                                node.operation,
                                Operation::FoldPerKey
                                    | Operation::FoldGlobal
                                    | Operation::GlobalCombine
                            )
                        }) {
                            if input_index == 1 {
                                let node = self.graph.node(&node_ref.key).expect("checked above");
                                if !matches!(
                                    node.operation,
                                    Operation::FoldGlobal | Operation::GlobalCombine
                                ) || node.time.window != Window::Global
                                    || input_pc != "v5_impulse_pc"
                                {
                                    bail!("v5 global fold seed differs from graph");
                                }
                                let input = collections
                                    .get(input_pc.as_str())
                                    .ok_or_else(|| anyhow!("missing v5 global fold impulse"))?;
                                let produced = input.iter().map(|value| {
                                    if !value.bytes.is_empty() || value.window.is_some() {
                                        bail!("v5 global fold seed expects a global empty impulse");
                                    }
                                    let mut bytes = Vec::new();
                                    encode_length_prefix(&[], &mut bytes)?;
                                    encode_length_prefix(&[], &mut bytes)?;
                                    Ok(WireValue { bytes, timestamp_ns: 0, window: None, pane: value.pane, element_metadata: value.element_metadata.clone() })
                                }).collect::<Result<Vec<_>>>()?;
                                collections.insert(output_pc, produced);
                                continue;
                            }
                            if input_index != 0 {
                                bail!("v5 fold tag index is neither data nor seed");
                            }
                            let input = collections
                                .get(input_pc.as_str())
                                .ok_or_else(|| anyhow!("missing v5 keyed fold input {input_pc}"))?;
                            let produced = input
                                .iter()
                                .map(|value| {
                                    let (key, payload) = self
                                        .registry
                                        .split_group_input(&node_ref.key, &value.bytes)?;
                                    let mut timed = value.timestamp_ns.to_be_bytes().to_vec();
                                    timed.extend_from_slice(&payload);
                                    let mut bytes = Vec::new();
                                    encode_length_prefix(&key, &mut bytes)?;
                                    encode_length_prefix(&timed, &mut bytes)?;
                                    Ok(WireValue {
                                        bytes,
                                        timestamp_ns: value.timestamp_ns,
                                        window: value.window,
                                        pane: value.pane,
                                        element_metadata: value.element_metadata.clone(),
                                    })
                                })
                                .collect::<Result<Vec<_>>>()?;
                            collections.insert(output_pc, produced);
                            continue;
                        }
                        let input = collections
                            .get(input_pc.as_str())
                            .ok_or_else(|| anyhow!("missing v5 co-group input {input_pc}"))?;
                        let produced = input
                            .iter()
                            .map(|value| {
                                let (key, tagged) = if self.registry.has_join_coder(&node_ref.key) {
                                    self.registry.split_join_input(
                                        &node_ref.key,
                                        input_index,
                                        &value.bytes,
                                        value.timestamp_ns,
                                    )?
                                } else {
                                    self.registry.split_cogroup_input(
                                        &node_ref.key,
                                        input_index,
                                        &value.bytes,
                                    )?
                                };
                                let mut bytes = Vec::new();
                                encode_length_prefix(&key, &mut bytes)?;
                                encode_length_prefix(&tagged, &mut bytes)?;
                                Ok(WireValue {
                                    bytes,
                                    timestamp_ns: value.timestamp_ns,
                                    window: value.window,
                                    pane: value.pane,
                                    element_metadata: value.element_metadata.clone(),
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        collections.insert(output_pc, produced);
                        continue;
                    }
                    let node = self
                        .graph
                        .node(&node_ref.key)
                        .ok_or_else(|| anyhow!("missing v5 node {}", node_ref.key))?;
                    let produced = match node.operation {
                        Operation::Source { .. } if source_key == Some(node.key.as_str()) => {
                            vec![source_value.clone()]
                        }
                        Operation::Source { .. } => Vec::new(),
                        Operation::Map
                        | Operation::Filter
                        | Operation::FlatMap
                        | Operation::AssignTimestamp => {
                            let input = collections
                                .get(input_pc.as_str())
                                .ok_or_else(|| anyhow!("missing v5 collection {input_pc}"))?;
                            let mut produced = Vec::new();
                            for value in input.iter().cloned() {
                                produced.extend(self.apply(&node_ref.key, value)?);
                            }
                            produced
                        }
                        Operation::StatefulParDo { ref contract, .. } => {
                            let input = collections
                                .get(input_pc.as_str())
                                .ok_or_else(|| anyhow!("missing v5 collection {input_pc}"))?;
                            let state = inputs.state.ok_or_else(|| anyhow!("stateful ParDo {} has no State client", node.key))?;
                            let parent = node.input.as_deref().ok_or_else(|| anyhow!("stateful ParDo {} has no input", node.key))?;
                            let mut produced = Vec::with_capacity(input.len());
                            for wire in input {
                                let (key, _) = self.registry.split_group_input(&node.key, &wire.bytes)?;
                                let mut encoded_key = Vec::new();
                                // Fn State is scoped by the Beam key coder's bytes,
                                // which include this length prefix.
                                encode_length_prefix(&key, &mut encoded_key)?;
                                let window = match wire.window {
                                    None => Vec::new(),
                                    Some(interval) => {
                                        if interval.start_ns % 1_000_000 != 0 || interval.end_ns % 1_000_000 != 0 {
                                            bail!("stateful ParDo {} requires millisecond-aligned windows", node.key);
                                        }
                                        let mut encoded = Vec::new();
                                        encode_interval_window(interval.start_ns / 1_000_000, interval.end_ns / 1_000_000, &mut encoded)?;
                                        encoded
                                    }
                                };
                                let access = UserStateAccess::new(state, instruction_id, transform_id, contract, window, encoded_key)?;
                                let output = access.apply_value_state("value", |encoded| {
                                    let prior = if encoded.is_empty() {
                                        None
                                    } else {
                                        let (value, used) = decode_length_prefix(encoded)?;
                                        if used != encoded.len() {
                                            bail!("stateful value cell contains more than one encoded value");
                                        }
                                        Some(value)
                                    };
                                    let envelope = Envelope {
                                        value: self.registry.decode_element(parent, &wire.bytes)?,
                                        timestamp_ns: wire.timestamp_ns,
                                        window: node.time.window.clone(),
                                        assigned_window: wire.window,
                                        pane: wire.pane,
                                    };
                                    let (next, result) = self.registry.apply_stateful_value(&node.key, envelope, prior)?;
                                    let mutation = match next {
                                        Some(bytes) => {
                                            let mut encoded = Vec::new();
                                            encode_length_prefix(&bytes, &mut encoded)?;
                                            ValueStateMutation::Set(encoded)
                                        }
                                        None => ValueStateMutation::Clear,
                                    };
                                    Ok((WireValue {
                                        bytes: self.registry.encode_element(&node.key, result.value.as_ref())?,
                                        timestamp_ns: result.timestamp_ns,
                                        window: result.assigned_window,
                                        pane: result.pane,
                                        element_metadata: wire.element_metadata.clone(),
                                    }, mutation))
                                }).await?;
                                produced.push(output);
                            }
                            produced
                        }
                        Operation::WaitOn | Operation::WaitOnWindowed => collections
                            .get(input_pc.as_str())
                            .ok_or_else(|| anyhow!("missing v5 collection {input_pc}"))?
                            .clone(),
                        Operation::FoldPerKey
                        | Operation::FoldGlobal
                        | Operation::GlobalCombine => {
                            let input = collections.get(input_pc.as_str()).ok_or_else(|| {
                                anyhow!("missing v5 keyed fold collection {input_pc}")
                            })?;
                            let mut produced = Vec::new();
                            for group in input {
                                produced.extend(self.apply_fold_group(&node.key, group)?);
                            }
                            produced
                        }
                        Operation::SideInputMap => {
                            let parent = node.input.as_deref().ok_or_else(|| {
                                anyhow!("v5 side input {} has no main input", node.key)
                            })?;
                            let view = inputs.side_views.get(&node.key).ok_or_else(|| {
                                anyhow!("v5 side view {} was not materialized", node.key)
                            })?;
                            collections
                                .get(input_pc.as_str())
                                .ok_or_else(|| anyhow!("missing v5 collection {input_pc}"))?
                                .iter()
                                .map(|wire| {
                                    let event = Envelope {
                                        pane: wire.pane,
                                        value: self.registry.decode_element(parent, &wire.bytes)?,
                                        timestamp_ns: wire.timestamp_ns,
                                        window: node.time.window.clone(),
                                        assigned_window: wire.window,
                                    };
                                    let output = catch_unwind(AssertUnwindSafe(|| {
                                        self.registry.apply_side_input(
                                            &node.key,
                                            event,
                                            view.as_ref(),
                                        )
                                    }))
                                    .map_err(|panic| {
                                        user_panic_error(&node.key, "aggregate", panic)
                                    })??;
                                    Ok(WireValue {
                                        bytes: self
                                            .registry
                                            .encode_element(&node.key, output.value.as_ref())?,
                                        timestamp_ns: output.timestamp_ns,
                                        window: output.assigned_window,
                                        pane: output.pane,
                                        element_metadata: wire.element_metadata.clone(),
                                    })
                                })
                                .collect::<Result<Vec<_>>>()?
                        }
                        Operation::Sink { .. } => {
                            let input = collections
                                .get(input_pc.as_str())
                                .ok_or_else(|| anyhow!("missing v5 collection {input_pc}"))?;
                            let parent = node
                                .input
                                .as_deref()
                                .ok_or_else(|| anyhow!("v5 sink {} has no input", node.key))?;
                            let session = outputs
                                .user_sinks
                                .get_mut(node.key.as_str())
                                .ok_or_else(|| anyhow!("v5 sink {} was not opened", node.key))?;
                            for value in input {
                                session.write(Envelope {
                                    pane: value.pane,
                                    value: self.registry.decode_element(parent, &value.bytes)?,
                                    timestamp_ns: value.timestamp_ns,
                                    window: self
                                        .graph
                                        .node(parent)
                                        .ok_or_else(|| anyhow!("missing sink parent {parent}"))?
                                        .time
                                        .window
                                        .clone(),
                                    assigned_window: value.window,
                                })?;
                                *outputs
                                    .user_sink_counts
                                    .entry(node.key.clone())
                                    .or_default() += 1;
                            }
                            Vec::new()
                        }
                        _ => bail!("v5 Fn API executor does not support node {}", node.key),
                    };
                    if !output_pc.is_empty() {
                        collections.insert(output_pc, produced);
                    }
                }
                Step::WindowInto {
                    input_pc,
                    output_pc,
                    ..
                } => {
                    if self.window_for_pc(output_pc)? == Window::Global {
                        let input = collections
                            .get(input_pc.as_str())
                            .ok_or_else(|| anyhow!("missing v5 collection {input_pc}"))?;
                        let produced = input
                            .iter()
                            .cloned()
                            .map(|mut value| {
                                value.window = None;
                                value
                            })
                            .collect();
                        collections.insert(output_pc, produced);
                        continue;
                    }
                    let node = self
                        .graph
                        .nodes
                        .iter()
                        .find(|node| collection_id(&node.key) == *output_pc)
                        .ok_or_else(|| anyhow!("missing v5 WindowInto output {output_pc}"))?;
                    let input = collections
                        .get(input_pc.as_str())
                        .ok_or_else(|| anyhow!("missing v5 collection {input_pc}"))?;
                    let produced = input
                        .iter()
                        .map(|value| {
                            node.time.window.assign_all(value.timestamp_ns).map(|windows| {
                                windows.into_iter().map(|window| {
                                    let mut value = value.clone();
                                    value.window = window;
                                    value
                                }).collect::<Vec<_>>()
                            }).map_err(anyhow::Error::from)
                        })
                        .collect::<Result<Vec<Vec<_>>>>()?
                        .into_iter()
                        .flatten()
                        .collect();
                    collections.insert(output_pc, produced);
                }
                Step::Sink {
                    transform_id,
                    input_pc,
                    wire,
                    ..
                } => {
                    let input = collections
                        .get(input_pc.as_str())
                        .ok_or_else(|| anyhow!("missing v5 collection {input_pc}"))?;
                    let client = outputs
                        .sink_clients
                        .get(transform_id)
                        .ok_or_else(|| anyhow!("missing v5 runner sink {transform_id}"))?;
                    for value in input {
                        let timestamp_millis = encode_timestamp_millis(value.timestamp_ns)?;
                        let mut body = Vec::new();
                        match wire {
                            SinkWire::Opaque => encode_length_prefix(&value.bytes, &mut body)?,
                            SinkWire::KvLp => {
                                if let Some(group) = self.group_at_input(input_pc) {
                                    let (key, grouped_value) =
                                        self.registry.split_group_input(group, &value.bytes)?;
                                    encode_length_prefix(&key, &mut body)?;
                                    encode_length_prefix(&grouped_value, &mut body)?;
                                } else if self.fold_at_input(input_pc).is_some()
                                    || self.cogroup_at_tagged_input(input_pc).is_some()
                                {
                                    let (_, key_len) = decode_length_prefix(&value.bytes)?;
                                    let (_, value_len) =
                                        decode_length_prefix(&value.bytes[key_len..])?;
                                    if key_len + value_len != value.bytes.len() {
                                        bail!("v5 co-group tagged KV contains trailing bytes");
                                    }
                                    body.extend_from_slice(&value.bytes);
                                } else {
                                    bail!("missing grouped input for {input_pc}");
                                }
                            }
                        }
                        let mut output = Vec::new();
                        match self.window_for_pc(input_pc)? {
                            Window::Global => {
                                if value.window.is_some() {
                                    bail!("global v5 output has an interval window");
                                }
                                encode_windowed_value_with_metadata(
                                    timestamp_millis, value.pane, value.element_metadata.as_deref(), &body, &mut output,
                                )?;
                            }
                            Window::Fixed { .. } | Window::FixedOffset { .. } | Window::Sliding { .. } | Window::Session { .. } => {
                                let interval = value.window.ok_or_else(|| {
                                    anyhow!("fixed v5 output is missing its interval window")
                                })?;
                                let start_ms = encode_timestamp_millis(interval.start_ns)?;
                                let end_ms = encode_timestamp_millis(interval.end_ns)?;
                                encode_interval_windowed_value_with_metadata(
                                    timestamp_millis,
                                    start_ms,
                                    end_ms,
                                    value.pane,
                                    value.element_metadata.as_deref(),
                                    &body,
                                    &mut output,
                                )?;
                            }
                        }
                        client.write_chunk(instruction_id, transform_id, output)?;
                        *outputs
                            .output_counts
                            .entry(transform_id.clone())
                            .or_default() += 1;
                    }
                }
            }
        }
        Ok(())
    }
}
