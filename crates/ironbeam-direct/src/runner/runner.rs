impl DirectRunner {
    /// Validates every node before any source is opened.
    ///
    /// # Errors
    /// Returns a typed graph or unsupported-capability error.
    pub fn validate(&self, graph: &Graph) -> Result<(), Error> {
        Self::validate_with_unbounded(graph, false)
    }
    pub(crate) fn validate_with_unbounded(graph: &Graph, unbounded_source: bool) -> Result<(), Error> {
        graph.validate_for(
            "direct",
            &Capabilities {
                external_bounded_source: true,
                unbounded_source,
                flatten: true,
                reshuffle: true,
                wait_on: true,
                group_by_key: true,
                co_group_by_key: true,
                join_inner: true,
                join_outer: true,
                fixed_window: true,
                assign_timestamp: true,
                side_input_map: true,
                global_combine: true,
                incremental_combine: true,
                ordered_fold: true,
                batch_map_values: true,
                append_sink: true,
                final_result_sink: true,
                ..Capabilities::default()
            },
        )?;
        for node in &graph.nodes {
            if node.time.trigger != WindowTrigger::Default {
                return Err(Error::unsupported(&node.key, "direct", "early-count-trigger"));
            }
            let capability = match &node.operation {
                Operation::Source { .. }
                | Operation::Map
                | Operation::Filter
                | Operation::FlatMap
                | Operation::AssignTimestamp
                | Operation::FixedWindow | Operation::SlidingWindow
            | Operation::SessionWindow
                | Operation::Flatten
                | Operation::Reshuffle
                | Operation::WaitOn
                | Operation::GroupByKey
                | Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
                | Operation::SideInputMap
                | Operation::GlobalCombine
                | Operation::FoldGlobal
                | Operation::FoldPerKey
                | Operation::FoldOrderedGlobal
                | Operation::FoldOrderedPerKey
                | Operation::BatchMapValues { .. }
                | Operation::Output => None,
                Operation::Sink { spec }
                    if matches!(spec.mode, SinkMode::Append | SinkMode::FinalResult) =>
                {
                    None
                }
                Operation::Sink { .. } => Some("deferred-sink-execution"),
                Operation::Custom { semantic_id, .. } => Some(semantic_id.as_str()),
            };
            if let Some(capability) = capability {
                return Err(Error::unsupported(&node.key, "direct", capability));
            }
        }
        Ok(())
    }
    /// Lowers a requested output to the direct physical plan without opening its source.
    ///
    /// # Errors
    /// Returns a validation, capability, or resource configuration error.
    pub fn explain<T: Element>(
        &self,
        output: &Output<T>,
        config: &JobConfig,
    ) -> Result<PlanSummary, Error> {
        let (graph, registry) = output.snapshot()?;
        self.validate(&graph)?;
        validate_config(config)?;
        Ok(planner::lower(
            &graph,
            &registry,
            output.key(),
            config.buffer_budget_elements,
        )?
        .summary)
    }
    /// Runs one bounded output, using a job-owned pool when parallel work needs it.
    ///
    /// # Errors
    /// Returns validation, cancellation, or execution errors.
    ///
    /// # Panics
    /// Panics only if a validated plan violates the runner's internal pool
    /// ownership invariant.
    #[allow(clippy::too_many_lines)]
    pub fn run_collect<T: Element>(
        &self,
        output: &Output<T>,
        config: &JobConfig,
    ) -> Result<(Vec<T>, JobHandle), Error> {
        let (graph, registry) = output.snapshot()?;
        self.validate(&graph)?;
        validate_config(config)?;
        let plan = planner::lower(
            &graph,
            &registry,
            output.key(),
            config.buffer_budget_elements,
        )?;
        if plan.materialized
            && config.checkpoint.is_some()
            && (plan.summary.routes != 1
                || registry
                    .erased_vec_source_batches(&plan.summary.source_key)
                    .is_none())
        {
            return Err(Error::unsupported(
                output.key(),
                "direct",
                "aggregate-checkpoint",
            ));
        }
        if plan.materialized {
            ensure_aggregates(&graph, &registry, output.key())?;
        }
        if config.checkpoint.is_some() && plan.summary.routes != 1 {
            return Err(Error::unsupported(
                output.key(),
                "direct",
                "multi-source-checkpoint",
            ));
        }
        if (config.spill.is_some() || config.checkpoint.is_some()) && !T::supports_coder() {
            return Err(Error::unsupported(
                output.key(),
                "direct",
                "element-coder-for-durable-output",
            ));
        }
        let checkpoint_identity = if config.checkpoint.is_some() {
            Some(checkpoint::Identity::new::<T>(
                plan.summary.graph_fingerprint.clone(),
                registry
                    .source_config_digest(&plan.summary.source_key)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        Error::unsupported(
                            &plan.summary.source_key,
                            "direct",
                            "source-config-digest",
                        )
                    })?,
                plan.summary.source_key.clone(),
                output.key().to_owned(),
                config
                    .checkpoint
                    .as_ref()
                    .map_or_else(String::new, |c| c.binary_build_id.clone()),
            ))
        } else {
            None
        };
        let inline_vec_copy = !plan.materialized
            && config.parallelism == 1
            && config.checkpoint.is_none()
            && plan.routes.len() == 1
            && plan.routes[0].operations.is_empty()
            && registry
                .vec_source_batches::<T>(&plan.routes[0].source_key)
                .is_some();
        let inline_typed_route = !plan.materialized
            && config.parallelism == 1
            && config.checkpoint.is_none()
            && plan.routes.len() == 1
            && !plan.routes[0].operations.is_empty()
            && registry
                .vec_source_batches::<T>(&plan.routes[0].source_key)
                .is_some()
            && plan.routes[0]
                .operations
                .iter()
                .all(|key| registry.typed_value_transform::<T>(key).is_some());
        let pool = (!(inline_vec_copy || inline_typed_route))
            .then(|| {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(config.parallelism)
                    .build()
                    .map_err(|e| Error::execution("job", "direct", e.to_string()))
            })
            .transpose()?;
        #[allow(unused_mut)]
        let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        #[cfg(feature = "metrics")]
        {
            handle
                .metrics_collector
                .clone_from(&config.metrics_collector);
        }
        if config.cancellation.load(Ordering::Acquire) {
            handle.cancel();
            return Err(cancelled());
        }
        handle.set_status(JobStatus::Running);
        let result = if plan.materialized {
            materialized::execute(
                pool.as_ref().expect("materialized execution owns a pool"),
                &graph,
                &registry,
                output.key(),
                config,
                checkpoint_identity.zip(plan.summary.source_len_hint),
                &handle,
            )
        } else {
            execute(
                pool.as_ref(),
                &registry,
                &plan,
                output.key(),
                config,
                checkpoint_identity,
                &handle,
            )
        };
        match result {
            Ok(values) => {
                handle.set_status(JobStatus::Succeeded);
                Ok((values, handle))
            }
            Err(error) => {
                handle.set_status(if error.kind == ErrorKind::Cancelled {
                    JobStatus::Cancelled
                } else {
                    JobStatus::Failed
                });
                Err(error)
            }
        }
    }
    /// Collect two linear output branches in one bounded job, opening their
    /// shared source once. The split element needs a registered coder so each
    /// branch receives its own value and time metadata.
    ///
    /// # Errors
    /// Rejects different builders, fan-in or stateful routes, and uncoded
    /// split or durable output values before opening a source.
    pub fn run_collect_pair<T: Element, U: Element>(
        &self,
        left: &Output<T>,
        right: &Output<U>,
        config: &JobConfig,
    ) -> Result<PairResult<T, U>, Error> {
        if !left.shares_pipeline_with(right) {
            return Err(Error::graph("job", "outputs belong to different pipelines"));
        }
        let (graph, registry) = left.snapshot()?;
        self.validate(&graph)?;
        validate_config(config)?;
        let plan = multi_output::prepare(&graph, &registry, left.key(), right.key())?;
        if config.checkpoint.is_some() && (!T::supports_coder() || !U::supports_coder()) {
            return Err(Error::unsupported(
                "job",
                "direct",
                "element-coder-for-durable-output",
            ));
        }
        let checkpoint_identity = config
            .checkpoint
            .as_ref()
            .map(|checkpoint| {
                Ok(checkpoint::Identity::new_pair::<T, U>(
                    graph.fingerprint()?,
                    registry
                        .source_config_digest(&plan.source_key)
                        .ok_or_else(|| {
                            Error::unsupported(&plan.source_key, "direct", "source-config-digest")
                        })?
                        .to_owned(),
                    plan.source_key.clone(),
                    left.key().to_owned(),
                    right.key().to_owned(),
                    checkpoint.binary_build_id.clone(),
                ))
            })
            .transpose()?;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.parallelism)
            .build()
            .map_err(|error| Error::execution("job", "direct", error.to_string()))?;
        #[allow(unused_mut)]
        let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        #[cfg(feature = "metrics")]
        {
            handle
                .metrics_collector
                .clone_from(&config.metrics_collector);
        }
        if config.cancellation.load(Ordering::Acquire) {
            handle.cancel();
            return Err(cancelled());
        }
        handle.set_status(JobStatus::Running);
        match multi_output::execute(
            &pool,
            &registry,
            &plan,
            left.key(),
            right.key(),
            config,
            checkpoint_identity,
            &handle,
        ) {
            Ok((left_values, right_values)) => {
                handle.set_status(JobStatus::Succeeded);
                Ok(PairResult {
                    left: left_values,
                    right: right_values,
                    job: handle,
                })
            }
            Err(error) => {
                handle.set_status(if error.kind == ErrorKind::Cancelled {
                    JobStatus::Cancelled
                } else {
                    JobStatus::Failed
                });
                Err(error)
            }
        }
    }
    /// Execute two idempotent sink branches in one bounded job. Their shared
    /// source and common transforms run once. Commits are sequential; if the
    /// second commit fails, the first may already be visible and a retry must
    /// be safe for both sinks.
    #[allow(clippy::too_many_lines)] // Both sink lifecycles share one ordered commit path.
    pub fn run_sink_pair<T: Element, U: Element>(
        &self,
        left: &Sink<T>,
        right: &Sink<U>,
        config: &JobConfig,
    ) -> Result<JobHandle, Error> {
        if !left.shares_pipeline_with(right) {
            return Err(Error::graph("job", "sinks belong to different pipelines"));
        }
        if left.key() == right.key() {
            return Err(Error::graph("job", "sink branches must be distinct"));
        }
        let (graph, registry) = left.snapshot()?;
        self.validate(&graph)?;
        validate_config(config)?;
        if config.checkpoint.is_some() {
            return Err(Error::unsupported("job", "direct", "multi-sink-checkpoint"));
        }
        for key in [left.key(), right.key()] {
            let Some(Operation::Sink { spec }) = graph.node(key).map(|node| &node.operation) else {
                return Err(Error::graph(key, "requested terminal is not a sink"));
            };
            if !spec.commit || spec.replay != Replay::Idempotent {
                return Err(Error::unsupported(
                    key,
                    "direct",
                    "multi-sink-idempotent-commit",
                ));
            }
            if !registry.has_sink(key) {
                return Err(Error::unsupported(key, "direct", "sink-factory"));
            }
            if registry.sink_requires_bundle_finalization(key) {
                return Err(Error::unsupported(key, "direct", "bundle-finalization"));
            }
        }
        let plan = multi_output::prepare_sinks(&graph, &registry, left.key(), right.key())?;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.parallelism)
            .build()
            .map_err(|error| Error::execution("job", "direct", error.to_string()))?;
        #[allow(unused_mut)]
        let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        #[cfg(feature = "metrics")]
        {
            handle
                .metrics_collector
                .clone_from(&config.metrics_collector);
        }
        if config.cancellation.load(Ordering::Acquire) {
            handle.cancel();
            return Err(cancelled());
        }
        handle.set_status(JobStatus::Running);
        let result = (|| {
            let open = |key: &str| {
                catch_unwind(AssertUnwindSafe(|| registry.open_sink(key))).unwrap_or_else(|_| {
                    Err(Error::execution(key, "direct", "sink factory panicked"))
                })
            };
            let mut left_session = open(left.key())?;
            let mut right_session = match open(right.key()) {
                Ok(session) => session,
                Err(error) => {
                    let _ = catch_unwind(AssertUnwindSafe(|| left_session.abort()));
                    return Err(error);
                }
            };
            let probes = ensure_bounded_sink_session(left.key(), left_session.as_ref())
                .and_then(|()| ensure_bounded_sink_session(right.key(), right_session.as_ref()));
            if let Err(error) = probes {
                let _ = catch_unwind(AssertUnwindSafe(|| left_session.abort()));
                let _ = catch_unwind(AssertUnwindSafe(|| right_session.abort()));
                return Err(error);
            }
            let result = multi_output::execute_sinks(
                &pool,
                &registry,
                &plan,
                left.key(),
                right.key(),
                config,
                &handle,
                left_session.as_mut(),
                right_session.as_mut(),
            );
            if let Err(error) = result {
                let _ = catch_unwind(AssertUnwindSafe(|| left_session.abort()));
                let _ = catch_unwind(AssertUnwindSafe(|| right_session.abort()));
                return Err(error);
            }
            if config.cancellation.load(Ordering::Acquire) {
                let _ = catch_unwind(AssertUnwindSafe(|| left_session.abort()));
                let _ = catch_unwind(AssertUnwindSafe(|| right_session.abort()));
                return Err(cancelled());
            }
            if let Err(error) = catch_unwind(AssertUnwindSafe(|| left_session.commit()))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        left.key(),
                        "direct",
                        "sink commit panicked",
                    ))
                })
            {
                let _ = catch_unwind(AssertUnwindSafe(|| left_session.abort()));
                let _ = catch_unwind(AssertUnwindSafe(|| right_session.abort()));
                return Err(error);
            }
            if let Err(error) = catch_unwind(AssertUnwindSafe(|| right_session.commit()))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        right.key(),
                        "direct",
                        "sink commit panicked",
                    ))
                })
            {
                let _ = catch_unwind(AssertUnwindSafe(|| right_session.abort()));
                return Err(Error {
                    message: format!(
                        "{}; left sink {} is already committed; retry both idempotently",
                        error.message,
                        left.key()
                    ),
                    ..error
                });
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                handle.set_status(JobStatus::Succeeded);
                Ok(handle)
            }
            Err(error) => {
                handle.set_status(if error.kind == ErrorKind::Cancelled {
                    JobStatus::Cancelled
                } else {
                    JobStatus::Failed
                });
                Err(error)
            }
        }
    }
    /// Run two or more named idempotent sinks from bounded sources.
    /// Sources may join through `Flatten`. Each source and shared stateless
    /// stage executes once, and each branch receives its own coded value at a
    /// fan-out. Sinks commit in the supplied order.
    ///
    /// # Errors
    /// Rejects non-`Flatten` fan-in, stateful branches, uncoded splits,
    /// non-idempotent sinks, and checkpoints before opening a source or sink.
    pub fn run_sinks(
        &self,
        pipeline: &Pipeline,
        keys: &[&str],
        config: &JobConfig,
    ) -> Result<JobHandle, Error> {
        let (graph, registry) = pipeline.snapshot()?;
        self.validate(&graph)?;
        validate_config(config)?;
        if config.checkpoint.is_some() {
            return Err(Error::unsupported("job", "direct", "multi-sink-checkpoint"));
        }
        let plan = multi_sink::prepare(&graph, &registry, keys)?;
        #[allow(unused_mut)]
        let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        #[cfg(feature = "metrics")]
        {
            handle
                .metrics_collector
                .clone_from(&config.metrics_collector);
        }
        if config.cancellation.load(Ordering::Acquire) {
            handle.cancel();
            return Err(cancelled());
        }
        handle.set_status(JobStatus::Running);
        match multi_sink::execute(&registry, &plan, keys, config, &handle) {
            Ok(()) => {
                handle.set_status(JobStatus::Succeeded);
                Ok(handle)
            }
            Err(error) => {
                handle.set_status(if error.kind == ErrorKind::Cancelled {
                    JobStatus::Cancelled
                } else {
                    JobStatus::Failed
                });
                Err(error)
            }
        }
    }
    /// Executes a deferred sink and commits after every input has succeeded.
    ///
    /// # Errors
    /// Returns typed validation, source, transform, sink, or commit errors.
    #[allow(clippy::too_many_lines)]
    pub fn run_sink<T: Element>(
        &self,
        sink: &Sink<T>,
        config: &JobConfig,
    ) -> Result<JobHandle, Error> {
        let (graph, registry) = sink.snapshot()?;
        self.validate(&graph)?;
        validate_config(config)?;
        if !matches!(graph.node(sink.key()).map(|node| &node.operation), Some(Operation::Sink { spec }) if spec.commit)
        {
            return Err(Error::unsupported(sink.key(), "direct", "sink-commit"));
        }
        if config.checkpoint.is_some() {
            return Err(Error::unsupported(sink.key(), "direct", "sink-checkpoint"));
        }
        if !registry.has_sink(sink.key()) {
            return Err(Error::unsupported(sink.key(), "direct", "sink-factory"));
        }
        if registry.sink_requires_bundle_finalization(sink.key()) {
            return Err(Error::unsupported(
                sink.key(),
                "direct",
                "bundle-finalization",
            ));
        }
        let plan = planner::lower(&graph, &registry, sink.key(), config.buffer_budget_elements)?;
        if plan.materialized {
            ensure_aggregates(&graph, &registry, sink.key())?;
        }
        if config.spill.is_some() && !T::supports_coder() {
            return Err(Error::unsupported(
                sink.key(),
                "direct",
                "element-coder-for-durable-output",
            ));
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.parallelism)
            .build()
            .map_err(|e| Error::execution("job", "direct", e.to_string()))?;
        #[allow(unused_mut)]
        let mut handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        #[cfg(feature = "metrics")]
        {
            handle
                .metrics_collector
                .clone_from(&config.metrics_collector);
        }
        if config.cancellation.load(Ordering::Acquire) {
            handle.cancel();
            return Err(cancelled());
        }
        handle.set_status(JobStatus::Running);
        let result = (|| {
            let mut session = catch_unwind(AssertUnwindSafe(|| registry.open_sink(sink.key())))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        sink.key(),
                        "direct",
                        "sink factory panicked",
                    ))
                })?;
            if let Err(error) = ensure_bounded_sink_session(sink.key(), session.as_ref()) {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                return Err(error);
            }
            if let Err(error) = sink::execute::<T>(
                &pool,
                &graph,
                &registry,
                &plan,
                config,
                &handle,
                session.as_mut(),
            ) {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                return Err(error);
            }
            if config.cancellation.load(Ordering::Acquire) {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                return Err(cancelled());
            }
            let committed =
                catch_unwind(AssertUnwindSafe(|| session.commit())).unwrap_or_else(|_| {
                    Err(Error::execution(
                        sink.key(),
                        "direct",
                        "sink commit panicked",
                    ))
                });
            if let Err(error) = committed {
                let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                return Err(error);
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                handle.set_status(JobStatus::Succeeded);
                Ok(handle)
            }
            Err(error) => {
                handle.set_status(if error.kind == ErrorKind::Cancelled {
                    JobStatus::Cancelled
                } else {
                    JobStatus::Failed
                });
                Err(error)
            }
        }
    }
}
