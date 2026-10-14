impl V5BundleExecutor {
    fn apply(&self, key: &str, input: WireValue) -> Result<Vec<WireValue>> {
        let node = self
            .graph
            .node(key)
            .ok_or_else(|| anyhow!("missing v5 node {key}"))?;
        let values = match node.operation {
            Operation::Map
            | Operation::Filter
            | Operation::FlatMap
            | Operation::AssignTimestamp => {
                let parent = node
                    .input
                    .as_deref()
                    .ok_or_else(|| anyhow!("missing input for v5 node {key}"))?;
                let envelope = Envelope {
                    value: self.registry.decode_element(parent, &input.bytes)?,
                    timestamp_ns: input.timestamp_ns,
                    window: node.time.window.clone(),
                    assigned_window: input.window,
                };
                catch_unwind(AssertUnwindSafe(|| self.registry.apply_many(key, envelope)))
                    .map_err(|panic| user_panic_error(key, "transform", panic))??
            }
            _ => bail!("v5 Fn API executor does not support node {key}"),
        };
        values
            .into_iter()
            .map(|value| {
                if matches!(node.operation, Operation::AssignTimestamp)
                    && value.timestamp_ns == i64::MAX
                {
                    bail!(
                        "v5 portable timestamp i64::MAX is reserved for Beam global-window output"
                    );
                }
                if value.window != node.time.window
                    || value.assigned_window.is_some() != (node.time.window != Window::Global)
                {
                    bail!("v5 Fn API executor received inconsistent window metadata");
                }
                Ok(WireValue {
                    bytes: self.registry.encode_element(key, value.value.as_ref())?,
                    timestamp_ns: value.timestamp_ns,
                    window: value.assigned_window,
                    pane: input.pane,
                    element_metadata: input.element_metadata.clone(),
                })
            })
            .collect()
    }

    fn apply_fold_group(&self, key: &str, input: &WireValue) -> Result<Vec<WireValue>> {
        let node = self
            .graph
            .node(key)
            .ok_or_else(|| anyhow!("missing v5 keyed fold {key}"))?;
        let parent = node
            .input
            .as_deref()
            .ok_or_else(|| anyhow!("v5 keyed fold {key} has no input"))?;
        let (encoded_key, used) = decode_length_prefix(&input.bytes)?;
        if matches!(
            node.operation,
            Operation::FoldGlobal | Operation::GlobalCombine
        ) && !encoded_key.is_empty()
        {
            bail!("v5 global fold {key} has a nonempty runner key");
        }
        let mut remaining = &input.bytes[used..];
        let mut fold = if matches!(node.operation, Operation::GlobalCombine) {
            None
        } else {
            Some(
                catch_unwind(AssertUnwindSafe(|| self.registry.open_fold(key)))
                    .map_err(|panic| user_panic_error(key, "fold factory", panic))??,
            )
        };
        let mut aggregate_input = Vec::new();
        let mut count = 0_u64;
        while !remaining.is_empty() {
            let (tagged, used) = decode_length_prefix(remaining)?;
            if matches!(
                node.operation,
                Operation::FoldGlobal | Operation::GlobalCombine
            ) && tagged.is_empty()
            {
                remaining = &remaining[used..];
                continue;
            }
            let timestamp = tagged
                .get(..8)
                .ok_or_else(|| anyhow!("v5 keyed fold value lacks timestamp"))?;
            let timestamp_ns = i64::from_be_bytes(timestamp.try_into().expect("eight bytes"));
            let typed = self
                .registry
                .assemble_group_input(key, encoded_key, &tagged[8..])?;
            let event = Envelope {
                value: self.registry.decode_element(parent, &typed)?,
                timestamp_ns,
                window: node.time.window.clone(),
                assigned_window: input.window,
            };
            if let Some(fold) = &mut fold {
                catch_unwind(AssertUnwindSafe(|| fold.add(event)))
                    .map_err(|panic| user_panic_error(key, "fold", panic))??;
            } else {
                aggregate_input.push(event);
            }
            remaining = &remaining[used..];
            count += 1;
        }
        if count == 0
            && (!matches!(
                node.operation,
                Operation::FoldGlobal | Operation::GlobalCombine
            ) || node.time.window != Window::Global)
        {
            bail!("v5 keyed fold {key} received an empty runner group");
        }
        let outputs = if let Some(fold) = fold {
            catch_unwind(AssertUnwindSafe(|| fold.finish()))
                .map_err(|panic| user_panic_error(key, "fold", panic))??
        } else {
            catch_unwind(AssertUnwindSafe(|| {
                self.registry.apply_aggregate(key, vec![aggregate_input])
            }))
            .map_err(|panic| user_panic_error(key, "aggregate", panic))??
        };
        if outputs.len() != 1 {
            bail!(
                "v5 keyed fold {key} produced {} outputs for one runner key",
                outputs.len()
            );
        }
        outputs
            .into_iter()
            .map(|event| {
                if event.window != node.time.window || event.assigned_window != input.window {
                    bail!("v5 keyed fold {key} returned inconsistent window metadata");
                }
                Ok(WireValue {
                    bytes: self.registry.encode_element(key, event.value.as_ref())?,
                    timestamp_ns: event.timestamp_ns,
                    window: event.assigned_window,
                    pane: input.pane,
                    element_metadata: input.element_metadata.clone(),
                })
            })
            .collect()
    }
}
