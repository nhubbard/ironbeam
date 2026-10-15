impl V5BundleExecutor {
    async fn run_sdf_process(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
    ) -> Result<ProcessBundleResponse> {
        let [Step::Source {
            transform_id: source_id,
            port: source_port,
            output_pc: source_pc,
            ..
        }, Step::Sdf {
            stage: SdfStage::ProcessSizedElements,
            transform_id: process_id,
            node_ref,
            input_pc,
            output_pc,
        }, downstream @ ..] = plan.steps.as_slice()
        else {
            bail!("v5 SDF process bundle needs a runner source and process stage");
        };
        node_ref.identity.verify(&self.identity)?;
        if node_ref.co_group_input.is_some()
            || source_pc != input_pc
            || output_pc != &collection_id(&node_ref.key)
        {
            bail!("v5 SDF process collection wiring differs from worker graph");
        }
        let source_key = node_ref.key.as_str();
        let node = self
            .graph
            .node(source_key)
            .ok_or_else(|| anyhow!("v5 SDF source {source_key} is absent from worker"))?;
        let Operation::Source { progress } = &node.operation else {
            bail!("v5 SDF process does not name a source");
        };
        if node.boundedness != Boundedness::Unbounded
            || !progress.stable_split_id
            || progress.replay == Replay::None
            || !self.registry.has_unbounded_source(source_key)
            || !self.registry.has_coder(source_key)
        {
            bail!("v5 SDF process has no resumable coded source");
        }
        let mut collections = HashMap::from([(output_pc.clone(), source_key.to_owned())]);
        let mut consumed = HashSet::new();
        let mut seen_nodes = HashSet::new();
        let mut runner_sink_ids = HashSet::new();
        let mut user_sink_keys = Vec::new();
        let mut terminals = 0;
        for step in downstream {
            match step {
                Step::V5ParDo {
                    node_ref,
                    input_pc,
                    output_pc,
                    ..
                } => {
                    node_ref.identity.verify(&self.identity)?;
                    let next = self.graph.node(&node_ref.key).ok_or_else(|| {
                        anyhow!("v5 SDF downstream node {} is absent", node_ref.key)
                    })?;
                    if node_ref.co_group_input == Some(0)
                        && matches!(
                            next.operation,
                            Operation::FoldPerKey
                                | Operation::FoldGlobal
                                | Operation::GlobalCombine
                        )
                    {
                        let upstream = next
                            .input
                            .as_deref()
                            .ok_or_else(|| anyhow!("v5 keyed fold {} has no parent", next.key))?;
                        if collections.get(input_pc).map(String::as_str) != Some(upstream)
                            || input_pc != &collection_id(upstream)
                            || output_pc != &fold_tag_collection_id(&next.key)
                            || !self.registry.has_group_coder(&next.key)
                            || !has_group_application(&self.registry, next)
                            || !seen_nodes.insert(format!("{}:tag", next.key))
                            || collections
                                .insert(output_pc.clone(), format!("{}:tag", next.key))
                                .is_some()
                        {
                            bail!("v5 SDF keyed fold tag differs from worker graph");
                        }
                        consumed.insert(input_pc.clone());
                        continue;
                    }
                    if let Some(input_index) = node_ref.co_group_input {
                        let upstream =
                            self.cogroup_parent(&node_ref.key, input_index)
                                .ok_or_else(|| {
                                    anyhow!("v5 SDF co-group tag input is absent from worker graph")
                                })?;
                        if collections.get(input_pc).map(String::as_str) != Some(upstream)
                            || input_pc != &collection_id(upstream)
                            || output_pc != &cogroup_tag_collection_id(&node_ref.key, input_index)
                            || !(self.registry.has_cogroup_coder(&node_ref.key)
                                || self.registry.has_join_coder(&node_ref.key))
                            || !self.registry.has_coder(upstream)
                            || !seen_nodes.insert(format!("{}:tag{input_index}", node_ref.key))
                            || collections
                                .insert(
                                    output_pc.clone(),
                                    format!("{}:tag{input_index}", node_ref.key),
                                )
                                .is_some()
                        {
                            bail!("v5 SDF co-group tag differs from worker graph");
                        }
                        consumed.insert(input_pc.clone());
                        continue;
                    }
                    if node_ref.co_group_input.is_some()
                        || collections.get(input_pc).map(String::as_str) != next.input.as_deref()
                        || !seen_nodes.insert(node_ref.key.clone())
                    {
                        bail!("v5 SDF downstream node {} is disconnected", node_ref.key);
                    }
                    consumed.insert(input_pc.clone());
                    match &next.operation {
                        Operation::Map
                        | Operation::Filter
                        | Operation::FlatMap
                        | Operation::AssignTimestamp
                        | Operation::WaitOnWindowed => {
                            if output_pc != &collection_id(&node_ref.key)
                                || !self.registry.has_coder(&node_ref.key)
                                || collections
                                    .insert(output_pc.clone(), node_ref.key.clone())
                                    .is_some()
                            {
                                bail!(
                                    "v5 SDF downstream coder or output differs at {}",
                                    node_ref.key
                                );
                            }
                            if matches!(next.operation, Operation::WaitOnWindowed) {
                                let side = next.additional_inputs.first().ok_or_else(|| {
                                    anyhow!("v5 SDF windowed wait {} has no signal", next.key)
                                })?;
                                if plan.side_inputs.get(&next.key)
                                    != Some(&collection_id(&side.key))
                                    || plan.state_endpoint.as_deref().is_none_or(str::is_empty)
                                {
                                    bail!(
                                        "v5 SDF windowed wait {} differs from worker side input",
                                        next.key
                                    );
                                }
                            }
                        }
                        Operation::Sink { spec } => {
                            if !output_pc.is_empty()
                                || spec.mode != SinkMode::Append
                                || spec.replay != Replay::Idempotent
                                || !spec.commit
                                || !self.registry.has_sink(&node_ref.key)
                            {
                                bail!(
                                    "v5 SDF sink {} needs an idempotent committed append contract",
                                    node_ref.key
                                );
                            }
                            user_sink_keys.push(node_ref.key.clone());
                            terminals += 1;
                        }
                        _ => bail!(
                            "v5 SDF downstream node {} is not executable yet",
                            node_ref.key
                        ),
                    }
                }
                Step::WindowInto {
                    input_pc,
                    output_pc,
                    window_fn,
                    ..
                } => {
                    let node = self
                        .graph
                        .nodes
                        .iter()
                        .find(|node| collection_id(&node.key) == *output_pc)
                        .ok_or_else(|| anyhow!("v5 SDF WindowInto output {output_pc} is absent"))?;
                    if collections.get(input_pc).map(String::as_str) != node.input.as_deref()
                        || !matches!(node.operation, Operation::FixedWindow | Operation::SlidingWindow | Operation::SessionWindow)
                        || !self.registry.has_coder(&node.key)
                        || !seen_nodes.insert(node.key.clone())
                    {
                        bail!("v5 SDF WindowInto {} differs from worker graph", node.key);
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
                        Window::Global => {
                            bail!("v5 SDF WindowInto {} has no fixed window", node.key)
                        }
                    }
                    .map_err(|error| anyhow!(error))?;
                    if *window_fn != expected {
                        bail!(
                            "v5 SDF WindowInto {} function differs from worker graph",
                            node.key
                        );
                    }
                    if collections
                        .insert(output_pc.clone(), node.key.clone())
                        .is_some()
                    {
                        bail!("v5 SDF WindowInto {} duplicates a collection", node.key);
                    }
                    consumed.insert(input_pc.clone());
                }
                Step::Sink {
                    transform_id,
                    input_pc,
                    wire,
                    ..
                } if collections.contains_key(input_pc)
                    && runner_sink_ids.insert(transform_id.clone())
                    && *wire
                        == if self.group_at_input(input_pc).is_some()
                            || self.fold_at_input(input_pc).is_some()
                            || self.cogroup_at_tagged_input(input_pc).is_some()
                        {
                            SinkWire::KvLp
                        } else {
                            SinkWire::Opaque
                        } =>
                {
                    consumed.insert(input_pc.clone());
                    terminals += 1;
                }
                _ => bail!("v5 SDF process bundle has unsupported downstream wiring"),
            }
        }
        if terminals == 0 {
            bail!("v5 SDF process bundle has no terminal sink");
        }
        if collections.keys().any(|pc| !consumed.contains(pc)) {
            bail!("v5 SDF process bundle has a branch without a terminal sink");
        }

        let (split_requested, _active_guard) = self.register_sdf_bundle(instruction_id, source_id)?;

        let source_client = self.data_client(source_port).await?;
        let mut reader = source_client.open_read(instruction_id, source_id);
        let mut sink_clients = HashMap::new();
        for step in downstream {
            if let Step::Sink {
                transform_id, port, ..
            } = step
            {
                sink_clients.insert(transform_id.clone(), self.data_client(port).await?);
            }
        }
        let mut user_sinks = HashMap::new();
        for key in &user_sink_keys {
            let session = self.registry.open_sink(key)?;
            let mut guard = SinkGuard {
                session,
                committed: false,
            };
            guard.begin_bundle(instruction_id)?;
            if guard.requires_finalization()?
                != self.registry.sink_requires_bundle_finalization(key)
            {
                bail!("v5 SDF sink finalization declaration differs at {key}");
            }
            user_sinks.insert(key.clone(), guard);
        }
        let mut user_sink_counts = HashMap::new();
        let mut output_counts = HashMap::new();
        let mut outputs = BundleOutputs {
            user_sinks: &mut user_sinks,
            user_sink_counts: &mut user_sink_counts,
            sink_clients: &sink_clients,
            output_counts: &mut output_counts,
        };
        let mut input_count = 0_u64;
        let mut residual_roots = Vec::new();
        let mut pending = Vec::new();
        while let Some(chunk) = reader.recv().await {
            pending.extend_from_slice(&chunk);
            let mut consumed = 0;
            while let Some((windowed, header_len)) =
                try_decode_windowed_value(&pending[consumed..], false)?
            {
                let Some(body_len) = try_sized_body_len(windowed.element)? else {
                    break;
                };
                let sized = decode_sized(source_key, &windowed.element[..body_len])?;
                if !sized.pair.element.is_empty() {
                    bail!("v5 SDF process input is not an empty Impulse trigger");
                }
                let mut poller = SourcePoller::open(
                    &self.graph,
                    &self.registry,
                    source_key,
                    sized.pair.restriction.clone(),
                )?;
                let result = loop {
                    if split_requested.load(Ordering::Acquire) || crate::supervision::split_requested() {
                        break None;
                    }
                    match poller.poll(128)? {
                        SourcePoll::Pending => tokio::time::sleep(Duration::from_millis(10)).await,
                        ready => break Some(ready),
                    }
                };
                let (elements, residual, watermark_ns, delay) = match result {
                    None => (
                        Vec::new(),
                        Some(sized.pair.restriction.clone()),
                        None,
                        Some(prost_types::Duration {
                            seconds: 0,
                            nanos: 100_000_000,
                        }),
                    ),
                    Some(SourcePoll::Pending) => unreachable!("loop only exits for a ready poll"),
                    Some(SourcePoll::Ready {
                        elements,
                        residual,
                        watermark_ns,
                    }) => (elements, residual, watermark_ns, None),
                };
                for element in elements {
                    self.process_record(
                        instruction_id,
                        plan,
                        Some(source_key),
                        WireValue {
                            bytes: element.bytes,
                            timestamp_ns: element.timestamp_ns,
                            window: None,
                            pane: PaneInfo::NO_FIRING,
                            element_metadata: None,
                        },
                        BundleInputs {
                            side_views: &mut HashMap::new(),
                            state: None,
                            timer_data: &source_client,
                            timer_source_key: None,
                        },
                        &mut outputs,
                    ).await?;
                }
                if let Some(restriction) = residual {
                    let element = encode_sized(&SizedSourcePair {
                        pair: crate::sdf_wire::SourcePair {
                            element: Vec::new(),
                            restriction,
                        },
                        size: sized.size,
                    })?;
                    let mut encoded = Vec::new();
                    encode_windowed_value(windowed.timestamp_millis, &element, &mut encoded)?;
                    residual_roots.push(DelayedBundleApplication {
                        application: Some(BundleApplication {
                            transform_id: process_id.clone(),
                            input_id: "in".into(),
                            element: encoded,
                            output_watermarks: watermark_ns
                                .map(|watermark| {
                                    HashMap::from([("out".into(), protobuf_timestamp(watermark))])
                                })
                                .unwrap_or_default(),
                            is_bounded: is_bounded::Enum::Unbounded as i32,
                        }),
                        requested_time_delay: delay,
                    });
                }
                input_count += 1;
                consumed += header_len + body_len;
                if consumed == pending.len() {
                    break;
                }
            }
            if consumed != 0 {
                pending.drain(..consumed);
            }
            if pending.len() > 2 * 1024 * 1024 {
                bail!("v5 SDF process has an oversized incomplete runner record");
            }
        }
        if !pending.is_empty() {
            bail!("v5 SDF process ended with a truncated runner record");
        }
        for step in downstream {
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
            if self.registry.sink_requires_bundle_finalization(&key) {
                finalizing.push((key, sink));
            }
        }
        let requires_finalization = !finalizing.is_empty();
        if requires_finalization {
            self.finalizations.register(instruction_id, finalizing)?;
        }
        let mut monitoring_infos = vec![element_count_info(source_pc, input_count)?];
        for step in downstream {
            match step {
                Step::Sink {
                    transform_id,
                    input_pc,
                    ..
                } => monitoring_infos.push(element_count_info(
                    input_pc,
                    *output_counts.get(transform_id.as_str()).unwrap_or(&0),
                )?),
                Step::V5ParDo {
                    transform_id,
                    node_ref,
                    ..
                } if user_sink_keys.contains(&node_ref.key) => {
                    monitoring_infos.push(sink_input_count_info(
                        transform_id,
                        *user_sink_counts.get(&node_ref.key).unwrap_or(&0),
                    )?)
                }
                _ => {}
            }
        }
        Ok(ProcessBundleResponse {
            monitoring_infos,
            residual_roots,
            requires_finalization,
            ..Default::default()
        })
    }
}
