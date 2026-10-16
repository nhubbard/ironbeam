impl V5BundleExecutor {
    #[allow(clippy::too_many_arguments)] // The Beam bundle, node, and both service streams form one callback scope.
    async fn apply_timed_element_cell(
        &self,
        instruction_id: &str,
        transform_id: &str,
        node: &Node,
        parent: &str,
        wire: &WireValue,
        state: &StateClient,
        data: &DataClient,
    ) -> Result<Vec<WireValue>> {
        let contract = match &node.operation {
            Operation::StatefulParDo { contract, .. } => contract,
            _ => bail!("timed callback node {} is not stateful", node.key),
        };
        let (key, _) = self.registry.split_group_input(&node.key, &wire.bytes)?;
        let mut encoded_key = Vec::new();
        encode_length_prefix(&key, &mut encoded_key)?;
        let window = Self::timer_state_window(wire.window)?;
        let elapsed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| anyhow!("worker processing clock: {error}"))?;
        let processing_time_ns = i64::try_from(elapsed.as_nanos())
            .map_err(|_| anyhow!("worker processing clock exceeds i64 nanoseconds"))?;
        let access = UserStateAccess::new(
            state,
            instruction_id,
            transform_id,
            contract,
            window,
            encoded_key,
        )?;
        let (outputs, timers) = access
            .apply_value_state("value", |encoded| {
                let prior = Self::decode_single_value_cell(encoded)?;
                let input = Envelope {
                    value: self.registry.decode_element(parent, &wire.bytes)?,
                    timestamp_ns: wire.timestamp_ns,
                    window: node.time.window.clone(),
                    assigned_window: wire.window,
                    pane: wire.pane,
                };
                let change = self.registry.invoke_timed_element_at(
                    &node.key,
                    input,
                    prior,
                    Some(processing_time_ns),
                )?;
                let outputs = change
                    .outputs
                    .into_iter()
                    .map(|output| {
                        Ok(WireValue {
                            bytes: self.registry.encode_element(&node.key, output.value.as_ref())?,
                            timestamp_ns: output.timestamp_ns,
                            window: output.assigned_window,
                            pane: output.pane,
                            element_metadata: wire.element_metadata.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(((outputs, change.timers), Self::timed_state_mutation(change.state)?))
            })
            .await?;
        Self::update_processing_timer_ledger(&access, contract, None, &timers).await?;
        Self::send_timer_mutations(data, instruction_id, transform_id, contract, timers)?;
        Ok(outputs)
    }

    async fn apply_timed_firing_cell(
        &self,
        instruction_id: &str,
        transform_id: &str,
        node: &Node,
        firing: &TimerFiring,
        state: &StateClient,
        data: &DataClient,
    ) -> Result<Vec<WireValue>> {
        let contract = match &node.operation {
            Operation::StatefulParDo { contract, .. } => contract,
            _ => bail!("timed callback node {} is not stateful", node.key),
        };
        let access = UserStateAccess::new(
            state,
            instruction_id,
            transform_id,
            contract,
            Self::timer_state_window(firing.identity.window)?,
            firing.identity.encoded_key.clone(),
        )?;
        if firing.domain == UserTimerDomain::ProcessingTime
            && contract.states.contains_key(PROCESSING_TIMER_LEDGER_STATE_ID)
            && !Self::processing_timer_is_armed(&access, firing).await?
        {
            return Ok(Vec::new());
        }
        let (outputs, timers) = access
            .apply_value_state("value", |encoded| {
                let prior = Self::decode_single_value_cell(encoded)?;
                let change = self.registry.invoke_timed_firing(&node.key, firing, prior)?;
                let outputs = change
                    .outputs
                    .into_iter()
                    .map(|output| {
                        Ok(WireValue {
                            bytes: self.registry.encode_element(&node.key, output.value.as_ref())?,
                            timestamp_ns: output.timestamp_ns,
                            window: output.assigned_window,
                            pane: output.pane,
                            element_metadata: None,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(((outputs, change.timers), Self::timed_state_mutation(change.state)?))
            })
            .await?;
        Self::update_processing_timer_ledger(&access, contract, Some(firing), &timers).await?;
        Self::send_timer_mutations(data, instruction_id, transform_id, contract, timers)?;
        Ok(outputs)
    }

    fn decode_single_value_cell(encoded: &[u8]) -> Result<Option<&[u8]>> {
        if encoded.is_empty() {
            return Ok(None);
        }
        let (value, used) = decode_length_prefix(encoded)?;
        if used != encoded.len() {
            bail!("timed value cell contains more than one encoded value");
        }
        Ok(Some(value))
    }

    fn timed_state_mutation(state: Option<Vec<u8>>) -> Result<ValueStateMutation> {
        match state {
            Some(bytes) => {
                let mut encoded = Vec::new();
                encode_length_prefix(&bytes, &mut encoded)?;
                Ok(ValueStateMutation::Set(encoded))
            }
            None => Ok(ValueStateMutation::Clear),
        }
    }

    fn timer_state_window(window: Option<WindowInterval>) -> Result<Vec<u8>> {
        let Some(window) = window else {
            return Ok(Vec::new());
        };
        if window.start_ns % 1_000_000 != 0 || window.end_ns % 1_000_000 != 0 {
            bail!("timed state requires millisecond-aligned windows");
        }
        let mut encoded = Vec::new();
        encode_interval_window(
            window.start_ns / 1_000_000,
            window.end_ns / 1_000_000,
            &mut encoded,
        )?;
        Ok(encoded)
    }

    fn send_timer_mutations(
        data: &DataClient,
        instruction_id: &str,
        transform_id: &str,
        contract: &ironbeam_core::StatefulParDoContract,
        timers: Vec<TimerMutation>,
    ) -> Result<()> {
        for timer in timers {
            let (family_id, record) = match timer {
                TimerMutation::Set(identity, schedule) => {
                    let family = contract.timers.get(&identity.family_id)
                        .ok_or_else(|| anyhow!("callback used an undeclared timer family"))?;
                    let firing = TimerFiring {
                        identity,
                        schedule,
                        domain: family.domain,
                    };
                    (firing.identity.family_id.clone(), TimerRecord::from_firing(&firing)?)
                }
                TimerMutation::Clear(identity) => {
                    let family_id = identity.family_id.clone();
                    (family_id, TimerRecord::from_clear(&identity)?)
                }
            };
            data.write_timer_record(instruction_id, transform_id, &family_id, &record)?;
        }
        Ok(())
    }
}
