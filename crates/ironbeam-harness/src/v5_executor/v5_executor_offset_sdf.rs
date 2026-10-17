impl V5BundleExecutor {
    /// Execute a bounded user SDF's pair, split, and truncate stages. Prism
    /// may fuse a bounded source with the pair stage or place a Data boundary.
    async fn run_offset_sdf_preparation(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
    ) -> Result<ProcessBundleResponse> {
        let [Step::Source {
            transform_id: source_id,
            port: source_port,
            output_pc: source_pc,
            wire: SourceWire::Opaque,
        }, middle @ .., Step::Sink {
            transform_id: sink_id,
            port: sink_port,
            input_pc: sink_pc,
            wire: SinkWire::Opaque,
        }] = plan.steps.as_slice() else {
            bail!("offset SDF preparation requires one opaque runner source and sink");
        };
        let (fused_source, middle) = if let [Step::V5ParDo {
            node_ref,
            input_pc,
            output_pc,
            stateful_value: false,
            ..
        }, tail @ ..] = middle {
            node_ref.identity.verify(&self.identity)?;
            let node = self.graph.node(&node_ref.key)
                .ok_or_else(|| anyhow!("offset SDF upstream source is absent"))?;
            if !matches!(node.operation, Operation::Source { .. })
                || node.boundedness != Boundedness::Bounded
                || node_ref.co_group_input.is_some()
                || input_pc != source_pc
                || input_pc != "v5_impulse_pc"
                || output_pc != &collection_id(&node_ref.key)
                || !self.registry.has_coder(&node_ref.key)
            {
                bail!("offset SDF fused source differs from worker graph");
            }
            (Some(node_ref.key.as_str()), tail)
        } else {
            (None, middle)
        };
        if middle.is_empty() {
            bail!("offset SDF preparation has no stage");
        }
        let mut node_key = None;
        let fused_pc = fused_source.map(collection_id);
        let mut expected_pc = fused_pc.as_deref().unwrap_or(source_pc);
        let mut prior = None;
        for step in middle {
            let Step::Sdf { stage, node_ref, input_pc, output_pc, .. } = step else {
                bail!("offset SDF preparation contains a non-SDF transform: {step:?}");
            };
            if *stage == SdfStage::ProcessSizedElements {
                bail!("offset SDF process requires a separate runner stage");
            }
            node_ref.identity.verify(&self.identity)?;
            if node_ref.co_group_input.is_some()
                || node_key.is_some_and(|key| key != node_ref.key)
                || input_pc != expected_pc
                || output_pc.is_empty()
                || output_pc == input_pc
            {
                bail!("offset SDF preparation wiring differs from worker graph");
            }
            let node = self.graph.node(&node_ref.key)
                .ok_or_else(|| anyhow!("offset SDF node {} is absent", node_ref.key))?;
            if !matches!(node.operation, Operation::SplittableParDo)
                || !self.registry.has_splittable(&node_ref.key)
                || !self.registry.has_coder(&node_ref.key)
            {
                bail!("offset SDF node has no bounded coded callback");
            }
            if *stage == SdfStage::PairWithRestriction
                && node.input.as_deref().and_then(|parent| {
                    matching_reshuffle_ancestor(&self.graph, parent, input_pc)
                }).is_none()
            {
                bail!("offset SDF pair stage input differs from worker graph");
            }
            if !matches!(
                (prior, stage),
                (None, _)
                    | (Some(SdfStage::PairWithRestriction), SdfStage::SplitAndSizeRestrictions)
                    | (Some(SdfStage::SplitAndSizeRestrictions), SdfStage::TruncateSizedRestrictions)
            ) {
                bail!("offset SDF preparation stages are out of order");
            }
            node_key = Some(node_ref.key.as_str());
            expected_pc = output_pc;
            prior = Some(*stage);
        }
        if expected_pc != sink_pc {
            bail!("offset SDF preparation runner sink is disconnected");
        }
        let node_key = node_key.expect("nonempty stages verified");
        let interval_window = self.graph.node(node_key).is_some_and(|node| node.time.window != Window::Global);
        let first = match &middle[0] {
            Step::Sdf { stage, .. } => *stage,
            _ => unreachable!(),
        };
        let source_client = self.data_client(source_port).await?;
        let sink_client = self.data_client(sink_port).await?;
        let mut reader = source_client.open_read(instruction_id, source_id);
        let mut pending = Vec::new();
        let mut source_count = 0_u64;
        let mut output_count = 0_u64;
        if let Some(upstream_key) = fused_source {
            while let Some(chunk) = reader.recv().await {
                if pending.len().saturating_add(chunk.len()) > 1024 * 1024 {
                    bail!("offset SDF fused Impulse exceeds 1 MiB");
                }
                pending.extend_from_slice(&chunk);
            }
            if !pending.is_empty() {
                let (trigger, header_len) = decode_windowed_value(&pending)?;
                let (body, body_len) = decode_length_prefix(trigger.element)?;
                if header_len + body_len != pending.len() || !body.is_empty() {
                    bail!("offset SDF fused source requires one empty Impulse trigger");
                }
                source_count = 1;
                for item in self.registry.open_source(upstream_key)? {
                    let item = item?;
                    if item.window != Window::Global || item.assigned_window.is_some() {
                        bail!("offset SDF fused source has unsupported window metadata");
                    }
                    let mut coded = Vec::new();
                    encode_length_prefix(
                        &self.registry.encode_element(upstream_key, item.value.as_ref())?,
                        &mut coded,
                    )?;
                    for output in self.prepared_offset_values(node_key, middle, coded)? {
                        let mut encoded = Vec::new();
                        let timestamp_millis = item.timestamp_ns.div_euclid(1_000_000);
                        encode_windowed_value(timestamp_millis, &output, &mut encoded)?;
                        sink_client.write_chunk(instruction_id, sink_id, encoded)?;
                        output_count += 1;
                    }
                }
            }
            sink_client.close_write(instruction_id, sink_id)?;
            sink_client.flush_outbound().await?;
            return Ok(ProcessBundleResponse {
                monitoring_infos: vec![
                    element_count_info(source_pc, source_count)?,
                    element_count_info(sink_pc, output_count)?,
                ],
                ..Default::default()
            });
        }
        while let Some(chunk) = reader.recv().await {
            pending.extend_from_slice(&chunk);
            let mut consumed = 0;
            while let Some((windowed, header_len)) =
                try_decode_windowed_value(&pending[consumed..], interval_window)?
            {
                let body_len = match first {
                    SdfStage::PairWithRestriction => try_bytes_body_len(windowed.element)?,
                    SdfStage::SplitAndSizeRestrictions => try_pair_body_len(windowed.element)?,
                    SdfStage::TruncateSizedRestrictions => try_sized_body_len(windowed.element)?,
                    SdfStage::ProcessSizedElements => unreachable!(),
                };
                let Some(body_len) = body_len else { break };
                for output in self.prepared_offset_values(node_key, middle, windowed.element[..body_len].to_vec())? {
                    let mut encoded = Vec::new();
                    if let Some((start, end)) = windowed.interval_millis {
                        encode_interval_windowed_value_with_metadata(
                            windowed.timestamp_millis,
                            start,
                            end,
                            windowed.pane,
                            windowed.element_metadata,
                            &output,
                            &mut encoded,
                        )?;
                    } else {
                        encode_windowed_value_with_metadata(
                            windowed.timestamp_millis,
                            windowed.pane,
                            windowed.element_metadata,
                            &output,
                            &mut encoded,
                        )?;
                    }
                    sink_client.write_chunk(instruction_id, sink_id, encoded)?;
                    output_count += 1;
                }
                consumed += header_len + body_len;
                source_count += 1;
                if consumed == pending.len() { break; }
            }
            if consumed != 0 { pending.drain(..consumed); }
            if pending.len() > 2 * 1024 * 1024 {
                bail!("offset SDF preparation has an oversized incomplete runner record");
            }
        }
        if !pending.is_empty() {
            bail!("offset SDF preparation ended with a truncated runner record");
        }
        sink_client.close_write(instruction_id, sink_id)?;
        sink_client.flush_outbound().await?;
        Ok(ProcessBundleResponse {
            monitoring_infos: vec![
                element_count_info(source_pc, source_count)?,
                element_count_info(sink_pc, output_count)?,
            ],
            ..Default::default()
        })
    }

    fn prepared_offset_values(
        &self,
        node_key: &str,
        stages: &[Step],
        input: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>> {
        use crate::sdf_offset_wire as offset_wire;

        let mut outputs = vec![input];
        for step in stages {
            let Step::Sdf { stage, .. } = step else { unreachable!() };
            let mut next = Vec::new();
            for body in outputs {
                match stage {
                    SdfStage::PairWithRestriction => {
                        let (element, used) = decode_length_prefix(&body)?;
                        if used != body.len() {
                            bail!("offset SDF input has trailing bytes");
                        }
                        let restriction = self.registry.initial_splittable_restriction(node_key, element)?;
                        next.push(offset_wire::pair_with_restriction(element, restriction)?);
                    }
                    SdfStage::SplitAndSizeRestrictions => {
                        let range = offset_wire::decode_pair(&body)?.restriction;
                        // Bound preparation memory even for the full signed span.
                        let positions = u128::try_from(
                            i128::from(range.stop) - i128::from(range.start),
                        )?;
                        let chunk_size = u64::try_from(positions.div_ceil(1024).max(1024))?;
                        for chunk in offset_wire::split_and_size(&body, chunk_size)? {
                            next.push(chunk?);
                        }
                    }
                    SdfStage::TruncateSizedRestrictions => {
                        next.push(offset_wire::truncate_sized(&body)?);
                    }
                    SdfStage::ProcessSizedElements => unreachable!(),
                }
            }
            outputs = next;
        }
        Ok(outputs)
    }
}
