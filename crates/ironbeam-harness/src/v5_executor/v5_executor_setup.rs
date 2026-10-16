struct ActiveSdfBundle {
    source_id: String,
    requested: Arc<AtomicBool>,
    offset_work: Arc<Mutex<Option<ActiveOffsetWork>>>,
}

struct ActiveOffsetWork {
    tracker: ironbeam_core::OffsetRangeTracker,
    estimator: ironbeam_core::OffsetWatermarkEstimator,
    input_header: Vec<u8>,
    input_element: Vec<u8>,
    process_id: String,
    boundedness: Boundedness,
    channel_index: i64,
    original_stop: i64,
    reported_residual: Arc<AtomicBool>,
}

struct ActiveSdfGuard<'a> {
    entries: &'a Mutex<HashMap<String, ActiveSdfBundle>>,
    instruction_id: String,
}

struct ActiveSdfHandle<'a> {
    requested: Arc<AtomicBool>,
    offset_work: Arc<Mutex<Option<ActiveOffsetWork>>>,
    _guard: ActiveSdfGuard<'a>,
}

impl Drop for ActiveSdfGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(&self.instruction_id);
        }
    }
}

impl V5BundleExecutor {
    fn window_for_pc(&self, pc: &str) -> Result<Window> {
        if pc == "v5_impulse_pc" {
            return Ok(Window::Global);
        }
        if self.graph.nodes.iter().any(|node| {
            matches!(node.operation, Operation::SideInputMap | Operation::WaitOn)
                && side_global_collection_id(&node.key) == pc
        }) {
            return Ok(Window::Global);
        }
        if let Some(window) = self
            .graph
            .nodes
            .iter()
            .find(|node| collection_id(&node.key) == pc)
            .map(|node| node.time.window.clone())
        {
            return Ok(window);
        }
        if let Some(window) = self
            .graph
            .nodes
            .iter()
            .find(|node| {
                matches!(
                    node.operation,
                    Operation::FoldPerKey | Operation::FoldGlobal | Operation::GlobalCombine
                ) && (fold_group_collection_id(&node.key) == pc
                    || fold_tag_collection_id(&node.key) == pc
                    || fold_seed_collection_id(&node.key) == pc
                    || fold_flatten_collection_id(&node.key) == pc)
            })
            .map(|node| node.time.window.clone())
        {
            return Ok(window);
        }
        self.graph
            .nodes
            .iter()
            .find(|node| {
                matches!(
                    node.operation,
                    Operation::CoGroupByKey
                        | Operation::JoinInner
                        | Operation::JoinLeft
                        | Operation::JoinRight
                        | Operation::JoinFull
                ) && (cogroup_flatten_collection_id(&node.key) == pc
                    || (0..1 + node.additional_inputs.len())
                        .any(|index| cogroup_tag_collection_id(&node.key, index) == pc))
            })
            .map(|node| node.time.window.clone())
            .ok_or_else(|| anyhow!("unknown v5 window for collection {pc}"))
    }

    fn window_for_plan_pc(&self, plan: &BundlePlan, pc: &str) -> Result<Window> {
        if let Some(node_ref) = plan.steps.iter().find_map(|step| match step {
            Step::Sdf {
                node_ref,
                output_pc,
                ..
            } if output_pc == pc => Some(node_ref),
            _ => None,
        }) {
            node_ref.identity.verify(&self.identity)?;
            return self
                .graph
                .node(&node_ref.key)
                .map(|node| node.time.window.clone())
                .ok_or_else(|| anyhow!("unknown v5 SDF window for collection {pc}"));
        }
        self.window_for_pc(pc)
    }

    fn cogroup_parent(&self, key: &str, input_index: usize) -> Option<&str> {
        let node = self.graph.node(key)?;
        if !matches!(
            node.operation,
            Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
        ) {
            return None;
        }
        node.input
            .iter()
            .map(String::as_str)
            .chain(node.additional_inputs.iter().map(|edge| edge.key.as_str()))
            .nth(input_index)
    }

    fn cogroup_at_output(&self, pc: &str) -> Option<&str> {
        self.graph
            .nodes
            .iter()
            .find(|node| {
                matches!(
                    node.operation,
                    Operation::CoGroupByKey
                        | Operation::JoinInner
                        | Operation::JoinLeft
                        | Operation::JoinRight
                        | Operation::JoinFull
                ) && (self.registry.has_cogroup_coder(&node.key)
                    || self.registry.has_join_coder(&node.key))
                    && collection_id(&node.key) == pc
            })
            .map(|node| node.key.as_str())
    }

    fn cogroup_at_tagged_input(&self, pc: &str) -> Option<&str> {
        self.graph
            .nodes
            .iter()
            .find(|node| {
                matches!(
                    node.operation,
                    Operation::CoGroupByKey
                        | Operation::JoinInner
                        | Operation::JoinLeft
                        | Operation::JoinRight
                        | Operation::JoinFull
                ) && (self.registry.has_cogroup_coder(&node.key)
                    || self.registry.has_join_coder(&node.key))
                    && (cogroup_flatten_collection_id(&node.key) == pc
                        || (0..1 + node.additional_inputs.len())
                            .any(|index| cogroup_tag_collection_id(&node.key, index) == pc))
            })
            .map(|node| node.key.as_str())
    }

    fn group_at_output(&self, pc: &str) -> Option<&str> {
        self.graph
            .nodes
            .iter()
            .find(|node| {
                matches!(node.operation, Operation::GroupByKey)
                    && self.registry.has_group_coder(&node.key)
                    && collection_id(&node.key) == pc
            })
            .map(|node| node.key.as_str())
    }

    fn fold_at_output(&self, pc: &str) -> Option<&str> {
        self.graph
            .nodes
            .iter()
            .find(|node| {
                matches!(
                    node.operation,
                    Operation::FoldPerKey | Operation::FoldGlobal | Operation::GlobalCombine
                ) && self.registry.has_group_coder(&node.key)
                    && has_group_application(&self.registry, node)
                    && fold_group_collection_id(&node.key) == pc
            })
            .map(|node| node.key.as_str())
    }

    fn fold_at_input(&self, pc: &str) -> Option<&str> {
        self.graph
            .nodes
            .iter()
            .find(|node| {
                matches!(
                    node.operation,
                    Operation::FoldPerKey | Operation::FoldGlobal | Operation::GlobalCombine
                ) && self.registry.has_group_coder(&node.key)
                    && (fold_tag_collection_id(&node.key) == pc
                        || matches!(
                            node.operation,
                            Operation::FoldGlobal | Operation::GlobalCombine
                        ) && node.time.window == Window::Global
                            && (fold_seed_collection_id(&node.key) == pc
                                || fold_flatten_collection_id(&node.key) == pc))
            })
            .map(|node| node.key.as_str())
    }

    fn group_at_input(&self, pc: &str) -> Option<&str> {
        // Mirror graphx's keyed coder propagation through native forwarding
        // boundaries, including a Reshuffle that Prism may elide.
        let candidate = self
            .graph
            .nodes
            .iter()
            .find(|node| collection_id(&node.key) == pc)?
            .key
            .as_str();
        self.graph
            .nodes
            .iter()
            .find(|node| {
                matches!(node.operation, Operation::GroupByKey | Operation::StatefulParDo { .. })
                    && self.registry.has_group_coder(&node.key)
                    && node
                        .input
                        .as_deref()
                        .is_some_and(|input| self.forwarding_input_reaches(input, candidate))
            })
            .map(|node| node.key.as_str())
    }

    fn forwarding_input_reaches(&self, input: &str, candidate: &str) -> bool {
        if input == candidate {
            return true;
        }
        let Some(node) = self.graph.node(input) else {
            return false;
        };
        matches!(node.operation, Operation::Flatten | Operation::Reshuffle)
            && node
                .input
                .iter()
                .map(String::as_str)
                .chain(node.additional_inputs.iter().map(|edge| edge.key.as_str()))
                .any(|parent| self.forwarding_input_reaches(parent, candidate))
    }

    /// Construct an executor from the worker's rebuilt pipeline.
    ///
    /// `binary_build_id` must match the driver's declared execution identity.
    pub fn new(pipeline: &Pipeline, binary_build_id: &str) -> Result<Self> {
        Self::new_for_worker(pipeline, binary_build_id, None)
    }

    /// Construct an executor bound to an optional Beam worker ID.
    pub fn new_for_worker(
        pipeline: &Pipeline,
        binary_build_id: &str,
        worker_id: Option<&str>,
    ) -> Result<Self> {
        let (graph, registry) = pipeline.snapshot()?;
        let identity = execution_identity(&graph, &registry, binary_build_id)?;
        let finalizations = match std::env::var_os("IRONBEAM_V5_FINALIZATION_JOURNAL_DIR") {
            Some(directory) => BundleFinalizations::with_journal(Arc::new(
                FileFinalizationJournal::new(directory, identity.clone())?,
            )),
            None => BundleFinalizations::default(),
        };
        Ok(Self {
            graph,
            registry,
            identity,
            worker_id: worker_id.map(str::to_owned),
            data_clients: AsyncMutex::new(HashMap::new()),
            active_sdf: Mutex::new(HashMap::new()),
            finalizations,
        })
    }

    /// Build a worker with a shared object-backed bundle finalization journal.
    /// The application must supply the same provider and URI on every worker
    /// process, including replacement execution children.
    pub fn new_for_worker_with_object_journal(
        pipeline: &Pipeline,
        binary_build_id: &str,
        worker_id: Option<&str>,
        stores: Arc<ironbeam_cloud_objects::ObjectStores>,
        journal_uri: &str,
    ) -> Result<Self> {
        if std::env::var_os("IRONBEAM_V5_FINALIZATION_JOURNAL_DIR").is_some() {
            bail!("file and object finalization journals cannot both be configured");
        }
        let mut executor = Self::new_for_worker(pipeline, binary_build_id, worker_id)?;
        let journal = crate::finalization_journal::ObjectFinalizationJournal::new(
            stores,
            journal_uri,
            executor.identity.clone(),
        )?;
        executor.finalizations = BundleFinalizations::with_journal(Arc::new(journal));
        Ok(executor)
    }

    /// Supply a durable journal backed by the worker's staging provider.
    /// The implementation must bind records to this executor's identity.
    pub fn with_finalization_journal(mut self, journal: Arc<dyn FinalizationJournal>) -> Self {
        self.finalizations = BundleFinalizations::with_journal(journal);
        self
    }

    /// Route every port of a supervised attempt through an isolated replay
    /// client. Its outbound stream is published only after the attempt exits
    /// successfully.
    pub(crate) async fn use_replay_client(&self, plan: &BundlePlan, client: DataClient) -> Result<()> {
        let mut clients = self.data_clients.lock().await;
        for step in &plan.steps {
            let port = match step {
                Step::Source { port, .. } | Step::Sink { port, .. } => port,
                _ => continue,
            };
            let endpoint = port
                .api_service_descriptor
                .as_ref()
                .ok_or_else(|| anyhow!("missing Fn API data endpoint"))?
                .url
                .as_str();
            clients.insert(normalize_endpoint(endpoint)?, client.clone());
        }
        Ok(())
    }

    fn register_sdf_bundle(
        &self,
        instruction_id: &str,
        source_id: &str,
    ) -> Result<ActiveSdfHandle<'_>> {
        let requested = Arc::new(AtomicBool::new(false));
        let offset_work = Arc::new(Mutex::new(None));
        let mut active = self
            .active_sdf
            .lock()
            .map_err(|_| anyhow!("v5 SDF active bundle registry poisoned"))?;
        if active.contains_key(instruction_id) {
            bail!("v5 SDF bundle instruction id is already active");
        }
        active.insert(
            instruction_id.into(),
            ActiveSdfBundle {
                source_id: source_id.into(),
                requested: Arc::clone(&requested),
                offset_work: Arc::clone(&offset_work),
            },
        );
        Ok(ActiveSdfHandle {
            requested,
            offset_work,
            _guard: ActiveSdfGuard {
                entries: &self.active_sdf,
                instruction_id: instruction_id.into(),
            },
        })
    }

    async fn data_client(&self, port: &RemoteGrpcPort) -> Result<DataClient> {
        let endpoint = port
            .api_service_descriptor
            .as_ref()
            .ok_or_else(|| anyhow!("missing Fn API data endpoint"))?
            .url
            .clone();
        let url = normalize_endpoint(&endpoint)?;
        let mut clients = self.data_clients.lock().await;
        if let Some(existing) = clients.get(&url).cloned() {
            return Ok(existing);
        }
        let client = DataClient::connect_for_worker(&url, self.worker_id.as_deref()).await?;
        clients.insert(url, client.clone());
        Ok(client)
    }

    /// Execute the one-split source's preparatory SDF stages. Prism may fuse
    /// pair and split/size or place a Data boundary between them. ProcessSized
    /// is deliberately rejected here until its checkpoint contract is wired.
    async fn run_sdf_preparation(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
    ) -> Result<ProcessBundleResponse> {
        let [Step::Source {
            transform_id: source_id,
            port: source_port,
            output_pc: source_pc,
            ..
        }, middle @ .., Step::Sink {
            transform_id: sink_id,
            port: sink_port,
            input_pc: sink_pc,
            ..
        }] = plan.steps.as_slice()
        else {
            bail!("v5 SDF preparation requires one runner source and one runner sink");
        };
        if middle.is_empty() {
            bail!("v5 SDF preparation has no stage");
        }
        let mut source_key: Option<&str> = None;
        let mut expected_pc = source_pc.as_str();
        let mut prior_stage = None;
        for step in middle {
            let Step::Sdf {
                stage,
                node_ref,
                input_pc,
                output_pc,
                ..
            } = step
            else {
                bail!("v5 SDF preparation contains a non-SDF transform");
            };
            if *stage == SdfStage::ProcessSizedElements {
                bail!("v5 SDF process stage requires runner-owned checkpoint execution");
            }
            node_ref.identity.verify(&self.identity)?;
            if node_ref.co_group_input.is_some() {
                bail!("v5 SDF source has an unexpected co-group input");
            }
            let key = node_ref.key.as_str();
            if source_key.is_some_and(|previous| previous != key) {
                bail!("v5 SDF preparation mixes source identities");
            }
            source_key = Some(key);
            let node = self
                .graph
                .node(key)
                .ok_or_else(|| anyhow!("v5 SDF source {key} is absent from worker"))?;
            let Operation::Source { progress } = &node.operation else {
                bail!("v5 SDF {key} is not a source");
            };
            if node.boundedness != Boundedness::Unbounded
                || !progress.stable_split_id
                || progress.replay == Replay::None
                || !self.registry.has_unbounded_source(key)
                || !self.registry.has_coder(key)
            {
                bail!("v5 SDF {key} has no resumable coded source");
            }
            if matches!(stage, SdfStage::PairWithRestriction) && input_pc != "v5_impulse_pc" {
                bail!("v5 SDF pair stage is not rooted at Impulse");
            }
            if input_pc != expected_pc || output_pc.is_empty() || output_pc == input_pc {
                bail!("v5 SDF {stage:?} collection wiring differs from worker graph");
            }
            if !matches!(
                (prior_stage, stage),
                (None, _)
                    | (Some(SdfStage::PairWithRestriction), SdfStage::SplitAndSizeRestrictions)
                    | (Some(SdfStage::SplitAndSizeRestrictions), SdfStage::TruncateSizedRestrictions)
            ) {
                bail!("v5 SDF preparation stages are out of order");
            }
            expected_pc = output_pc;
            prior_stage = Some(*stage);
        }
        if sink_pc != expected_pc {
            bail!("v5 SDF preparation runner sink is disconnected");
        }
        let source_key = source_key.expect("nonempty middle verified");
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
        while let Some(chunk) = reader.recv().await {
            pending.extend_from_slice(&chunk);
            let mut consumed = 0;
            while let Some((windowed, header_len)) =
                try_decode_windowed_value(&pending[consumed..], false)?
            {
                let body_len = match first {
                    SdfStage::PairWithRestriction => try_bytes_body_len(windowed.element)?,
                    SdfStage::SplitAndSizeRestrictions => try_pair_body_len(windowed.element)?,
                    SdfStage::TruncateSizedRestrictions => try_sized_body_len(windowed.element)?,
                    SdfStage::ProcessSizedElements => unreachable!(),
                };
                let Some(body_len) = body_len else { break };
                let body = &windowed.element[..body_len];
                let mut output = Some(body.to_vec());
                for step in middle {
                    let Step::Sdf { stage, .. } = step else {
                        unreachable!()
                    };
                    let Some(value) = output.take() else { break };
                    output = match stage {
                        SdfStage::PairWithRestriction => {
                            let (impulse, used) = decode_length_prefix(&value)?;
                            if used != value.len() {
                                bail!("v5 SDF Impulse has trailing bytes");
                            }
                            Some(pair_with_restriction(source_key, impulse)?)
                        }
                        SdfStage::SplitAndSizeRestrictions => {
                            Some(split_and_size(source_key, &value)?)
                        }
                        SdfStage::TruncateSizedRestrictions => truncate_sized(source_key, &value)?,
                        SdfStage::ProcessSizedElements => unreachable!(),
                    };
                }
                if let Some(output) = output {
                    let mut encoded = Vec::new();
                    encode_windowed_value_with_metadata(
                        windowed.timestamp_millis,
                        windowed.pane,
                        windowed.element_metadata,
                        &output,
                        &mut encoded,
                    )?;
                    sink_client.write_chunk(instruction_id, sink_id, encoded)?;
                    output_count += 1;
                }
                consumed += header_len + body_len;
                source_count += 1;
                if consumed == pending.len() {
                    break;
                }
            }
            if consumed != 0 {
                pending.drain(..consumed);
            }
            if pending.len() > 2 * 1024 * 1024 {
                bail!("v5 SDF preparation has an oversized incomplete runner record");
            }
        }
        if !pending.is_empty() {
            bail!("v5 SDF preparation ended with a truncated runner record");
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
}
