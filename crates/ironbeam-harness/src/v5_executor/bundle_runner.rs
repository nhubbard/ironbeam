#[async_trait]
impl BundleRunner for V5BundleExecutor {
    async fn split_bundle(
        &self,
        request: &ProcessBundleSplitRequest,
    ) -> Result<ProcessBundleSplitResponse> {
        let active = self
            .active_sdf
            .lock()
            .map_err(|_| anyhow!("v5 SDF active bundle registry poisoned"))?;
        if let Some(requested) = active.get(&request.instruction_id) {
            // A source checkpoint is emitted with ProcessBundleResponse after
            // this bundle relinquishes its current restriction. The split
            // instruction itself does not claim roots twice.
            requested.store(true, Ordering::Release);
        }
        Ok(ProcessBundleSplitResponse::default())
    }

    async fn run(&self, instruction_id: &str, plan: &BundlePlan) -> Result<ProcessBundleResponse> {
        if instruction_id.is_empty() {
            bail!("v5 bundle instruction id is empty");
        }
        if plan
            .steps
            .iter()
            .any(|step| matches!(step, Step::Sdf { .. }))
        {
            if plan.steps.iter().any(|step| {
                matches!(
                    step,
                    Step::Sdf {
                        stage: SdfStage::ProcessSizedElements,
                        ..
                    }
                )
            }) {
                return self.run_sdf_process(instruction_id, plan).await;
            }
            return self.run_sdf_preparation(instruction_id, plan).await;
        }
        let keys = self.validate(plan)?;
        let (source_id, source_port, source_pc, source_wire) = plan
            .steps
            .iter()
            .find_map(|step| match step {
                Step::Source {
                    transform_id,
                    port,
                    output_pc,
                    wire,
                } => Some((transform_id, port, output_pc, wire)),
                _ => None,
            })
            .ok_or_else(|| anyhow!("v5 bundle has no runner source"))?;
        let source_keys = keys
            .iter()
            .filter(|key| {
                self.graph
                    .node(key)
                    .is_some_and(|node| matches!(node.operation, Operation::Source { .. }))
            })
            .collect::<Vec<_>>();
        let source_client = self.data_client(source_port).await?;
        let mut reader = source_client.open_read(instruction_id, source_id);
        let mut sink_clients = HashMap::new();
        for step in &plan.steps {
            if let Step::Sink {
                transform_id, port, ..
            } = step
            {
                sink_clients.insert(transform_id.clone(), self.data_client(port).await?);
            }
        }
        let state = if !plan.side_inputs.is_empty() || *source_wire == SourceWire::KvStateBackedIter
        {
            let endpoint = plan
                .state_endpoint
                .as_deref()
                .ok_or_else(|| anyhow!("v5 bundle has no runner State endpoint"))?;
            Some(StateClient::connect(endpoint, self.worker_id.as_deref()).await?)
        } else {
            None
        };
        let mut side_views: HashMap<String, Box<dyn Any + Send>> = HashMap::new();
        if !plan.side_inputs.is_empty() {
            let state = state.as_ref().expect("connected for side inputs");
            for step in &plan.steps {
                let Step::V5ParDo {
                    transform_id,
                    node_ref,
                    ..
                } = step
                else {
                    continue;
                };
                let Some(node) = self.graph.node(&node_ref.key) else {
                    continue;
                };
                if !matches!(node.operation, Operation::SideInputMap | Operation::WaitOn) {
                    continue;
                }
                let side_parent = &node.additional_inputs[0].key;
                let state_key = StateKey {
                    r#type: Some(state_key::Type::IterableSideInput(
                        state_key::IterableSideInput {
                            transform_id: transform_id.clone(),
                            side_input_id: "side".into(),
                            window: Vec::new(),
                        },
                    )),
                };
                if matches!(node.operation, Operation::WaitOn) {
                    state.drain(instruction_id, state_key).await?;
                    continue;
                }
                let encoded = state.get_all(instruction_id, state_key).await?;
                let mut values = Vec::new();
                let mut consumed = 0;
                while consumed < encoded.len() {
                    let (value, len) = decode_length_prefix(&encoded[consumed..])
                        .context("decode v5 side input element")?;
                    values.push(self.registry.decode_element(side_parent, value)?);
                    consumed += len;
                }
                side_views.insert(
                    node.key.clone(),
                    catch_unwind(AssertUnwindSafe(|| {
                        self.registry.materialize_side_input(&node.key, values)
                    }))
                    .map_err(|panic| user_panic_error(&node.key, "aggregate", panic))??,
                );
            }
        }
        let mut encoded = Vec::new();
        if !source_keys.is_empty() {
            while let Some(chunk) = reader.recv().await {
                if encoded.len().saturating_add(chunk.len()) > 1024 * 1024 {
                    bail!("v5 Impulse trigger exceeds the 1 MiB control limit");
                }
                encoded.extend_from_slice(&chunk);
            }
        }
        // Validate runner input before opening any user endpoint.
        if !source_keys.is_empty() {
            if encoded.is_empty() {
                // Prism may schedule an empty source-rooted bundle. It has no
                // trigger, so opening the user source would duplicate its data.
                for step in &plan.steps {
                    if let Step::Sink { transform_id, .. } = step {
                        sink_clients
                            .get(transform_id)
                            .expect("opened above")
                            .close_write(instruction_id, transform_id)?;
                    }
                }
                for client in sink_clients.values() {
                    client.flush_outbound().await?;
                }
                return Ok(ProcessBundleResponse {
                    monitoring_infos: vec![element_count_info(source_pc, 0)?],
                    ..Default::default()
                });
            }
            let (windowed, header_len) =
                decode_windowed_value(&encoded).context("decode v5 Impulse windowed value")?;
            let (body, body_len) = decode_length_prefix(windowed.element)?;
            if header_len + body_len != encoded.len() {
                bail!("v5 source-rooted bundle requires exactly one Impulse element");
            }
            if !body.is_empty() {
                bail!("v5 Impulse trigger must have an empty payload");
            }
        }
        let mut user_sinks = HashMap::new();
        for key in &keys {
            if self
                .graph
                .node(key)
                .is_some_and(|node| matches!(node.operation, Operation::Sink { .. }))
            {
                let session = self.registry.open_sink(key)?;
                let mut guard = SinkGuard {
                    session,
                    committed: false,
                };
                guard.begin_bundle(instruction_id)?;
                if guard.requires_finalization()?
                    != self.registry.sink_requires_bundle_finalization(key)
                {
                    bail!("v5 sink finalization declaration differs from graph at {key}");
                }
                user_sinks.insert(key.clone(), guard);
            }
        }
        let mut output_counts: HashMap<String, u64> = HashMap::new();
        let mut user_sink_counts: HashMap<String, u64> = HashMap::new();
        let mut outputs = BundleOutputs {
            user_sinks: &mut user_sinks,
            user_sink_counts: &mut user_sink_counts,
            sink_clients: &sink_clients,
            output_counts: &mut output_counts,
        };
        let mut source_count = 0_u64;
        if !source_keys.is_empty() {
            // The Impulse is a trigger. Keep only one user source record's
            // intermediate values in memory, including fan-out to branches.
            for source_key in source_keys {
                for item in self.registry.open_source(source_key)? {
                    let value = item?;
                    if value.window != Window::Global || value.assigned_window.is_some() {
                        bail!("v5 Fn API executor received unsupported window metadata");
                    }
                    let wire = WireValue {
                        bytes: self
                            .registry
                            .encode_element(source_key, value.value.as_ref())?,
                        timestamp_ns: value.timestamp_ns,
                        window: None,
                        pane: PaneInfo::NO_FIRING,
                        element_metadata: None,
                    };
                    self.process_record(
                        instruction_id,
                        plan,
                        Some(source_key),
                        wire,
                        &side_views,
                        &mut outputs,
                    )?;
                }
            }
            source_count = 1;
        } else {
            // A native Beam transform such as Flatten creates a new bundle
            // boundary. Parse each complete record as chunks arrive; retain
            // only the incomplete suffix across chunk boundaries.
            while let Some(chunk) = reader.recv().await {
                encoded.extend_from_slice(&chunk);
                let mut consumed = 0;
                let interval_window = self.window_for_pc(source_pc)? != Window::Global;
                while let Some((records, record_len)) = match source_wire {
                    SourceWire::Opaque => (if source_pc == "v5_impulse_pc" {
                        decode_impulse_boundary_record(&encoded[consumed..])?
                    } else {
                        decode_boundary_record(&encoded[consumed..], interval_window)?
                    })
                    .map(|(record, length)| (vec![record], length)),
                    SourceWire::KvLp => {
                        if let Some(group) = self.group_at_input(source_pc) {
                            decode_keyed_boundary_record(
                                &encoded[consumed..],
                                &self.registry,
                                group,
                                interval_window,
                            )?
                            .map(|(record, length)| (vec![record], length))
                        } else if self.fold_at_input(source_pc).is_some()
                            || self.cogroup_at_tagged_input(source_pc).is_some()
                        {
                            decode_tagged_boundary_record(&encoded[consumed..], interval_window)?
                                .map(|(record, length)| (vec![record], length))
                        } else {
                            bail!("missing keyed input for {source_pc}");
                        }
                    }
                    SourceWire::KvIterLp => {
                        if self.fold_at_output(source_pc).is_some() {
                            decode_group_boundary_parts(
                                &encoded[consumed..],
                                self.window_for_pc(source_pc)?,
                            )?
                            .map(|(group, length)| {
                                if group.state_token.is_some() {
                                    bail!("v5 keyed fold iterable requires the Fn State API");
                                }
                                Ok((vec![assemble_fold_boundary_row(group)?], length))
                            })
                            .transpose()?
                        } else if let Some(group) = self.group_at_output(source_pc) {
                            decode_group_boundary_rows(
                                &encoded[consumed..],
                                &self.registry,
                                group,
                                self.window_for_pc(source_pc)?,
                                false,
                            )?
                        } else if let Some(group) = self.cogroup_at_output(source_pc) {
                            decode_group_boundary_rows(
                                &encoded[consumed..],
                                &self.registry,
                                group,
                                self.window_for_pc(source_pc)?,
                                true,
                            )?
                        } else {
                            bail!("missing grouping node for {source_pc}");
                        }
                    }
                    SourceWire::KvStateBackedIter => {
                        let fold_key = self.fold_at_output(source_pc);
                        let group_key = self
                            .group_at_output(source_pc)
                            .or_else(|| self.cogroup_at_output(source_pc))
                            .or(fold_key)
                            .ok_or_else(|| anyhow!("missing grouping node for {source_pc}"))?;
                        let window = self.window_for_pc(source_pc)?;
                        if let Some((mut group, record_len)) =
                            decode_group_boundary_parts(&encoded[consumed..], window.clone())?
                        {
                            if let Some(token) = group.state_token.take() {
                                let state =
                                    state.as_ref().expect("connected for state-backed iterable");
                                let encoded_values = state
                                    .get_all(
                                        instruction_id,
                                        StateKey {
                                            r#type: Some(state_key::Type::Runner(
                                                state_key::Runner { key: token },
                                            )),
                                        },
                                    )
                                    .await?;
                                append_state_backed_values(&mut group, &encoded_values)?;
                            }
                            let rows = if fold_key.is_some() {
                                vec![assemble_fold_boundary_row(group)?]
                            } else {
                                assemble_group_boundary_rows(
                                    group,
                                    &self.registry,
                                    group_key,
                                    window,
                                    self.cogroup_at_output(source_pc).is_some(),
                                )?
                            };
                            Some((rows, record_len))
                        } else {
                            None
                        }
                    }
                } {
                    for record in records {
                        self.process_record(
                            instruction_id,
                            plan,
                            None,
                            record,
                            &side_views,
                            &mut outputs,
                        )?;
                    }
                    consumed += record_len;
                    source_count += 1;
                }
                if consumed != 0 {
                    encoded.drain(..consumed);
                }
            }
            if !encoded.is_empty() {
                bail!("v5 runner boundary ended with a truncated record");
            }
        }
        for step in &plan.steps {
            if let Step::Sink { transform_id, .. } = step {
                sink_clients
                    .get(transform_id)
                    .expect("opened above")
                    .close_write(instruction_id, transform_id)?;
            }
        }
        for client in sink_clients.values() {
            client.flush_outbound().await?;
        }
        for sink in user_sinks.values_mut() {
            sink.commit()?;
        }
        let mut finalizing = Vec::new();
        for (key, sink) in user_sinks.drain() {
            if sink.requires_finalization()?
                != self.registry.sink_requires_bundle_finalization(&key)
            {
                bail!("v5 sink finalization declaration changed at {key}");
            }
            if self.registry.sink_requires_bundle_finalization(&key) {
                finalizing.push((key, sink));
            }
        }
        let requires_finalization = !finalizing.is_empty();
        if requires_finalization {
            self.finalizations.register(instruction_id, finalizing)?;
        }
        let mut monitoring_infos = vec![element_count_info(source_pc, source_count)?];
        for step in &plan.steps {
            if let Step::Sink {
                transform_id,
                input_pc,
                ..
            } = step
            {
                monitoring_infos.push(element_count_info(
                    input_pc,
                    *output_counts.get(transform_id.as_str()).unwrap_or(&0),
                )?);
            } else if let Step::V5ParDo {
                transform_id,
                node_ref,
                ..
            } = step
            {
                if self
                    .graph
                    .node(&node_ref.key)
                    .is_some_and(|node| matches!(node.operation, Operation::Sink { .. }))
                {
                    monitoring_infos.push(sink_input_count_info(
                        transform_id,
                        *user_sink_counts.get(&node_ref.key).unwrap_or(&0),
                    )?);
                }
            }
        }
        Ok(ProcessBundleResponse {
            monitoring_infos,
            requires_finalization,
            ..Default::default()
        })
    }

    async fn finalize_bundle(&self, instruction_id: &str) -> Result<()> {
        self.finalizations.finalize(instruction_id, &self.registry)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/v5_executor__boundary_tests.rs"]
mod boundary_tests;

#[cfg(test)]
#[path = "../../tests/unit/v5_executor__finalization_tests.rs"]
mod finalization_tests;
