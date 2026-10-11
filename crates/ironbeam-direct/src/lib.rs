//! Bounded direct execution of the Ironbeam 5 logical graph.
#![allow(clippy::redundant_pub_crate, clippy::missing_errors_doc)]
mod checkpoint;
mod helpers;
mod materialized;
mod metrics;
mod planner;
mod spill;

pub use checkpoint::CheckpointConfig;
pub use helpers::{
    DirectFallibleExt, DirectFlatPartitionExt, DirectKeySortedExt, DirectKeyedCollectExt,
    DirectPartitionExt, DirectSortedExt,
};
pub use metrics::JobMetrics;
pub use planner::PlanSummary;
pub use spill::SpillConfig;

use ironbeam_core::{
    Capabilities, Element, Envelope, Error, ErrorKind, Graph, Operation, Output, Registry,
    RetryClass, Sink, SinkMode, SourceIterator, Window,
};
use metrics::MetricsState;
use rayon::prelude::*;
use spill::OutputStore;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_JOB: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct JobConfig {
    pub parallelism: usize,
    pub memory_budget_bytes: usize,
    pub buffer_budget_elements: usize,
    pub cancellation: Arc<AtomicBool>,
    pub spill: Option<SpillConfig>,
    pub checkpoint: Option<CheckpointConfig>,
}
impl Default for JobConfig {
    fn default() -> Self {
        Self {
            parallelism: 1,
            memory_budget_bytes: 64 * 1024 * 1024,
            buffer_budget_elements: 4096,
            cancellation: Arc::new(AtomicBool::new(false)),
            spill: None,
            checkpoint: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStatus {
    Submitted,
    Running,
    Draining,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone)]
pub struct JobHandle {
    id: u64,
    status: Arc<Mutex<JobStatus>>,
    cancellation: Arc<AtomicBool>,
    processed: Arc<AtomicU64>,
    metrics: Arc<MetricsState>,
}
impl JobHandle {
    #[must_use]
    pub fn running_fixture() -> Self {
        Self::new(Arc::new(AtomicBool::new(false)), JobStatus::Running)
    }
    fn new(cancellation: Arc<AtomicBool>, status: JobStatus) -> Self {
        Self {
            id: NEXT_JOB.fetch_add(1, Ordering::Relaxed),
            status: Arc::new(Mutex::new(status)),
            cancellation,
            processed: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(MetricsState::default()),
        }
    }
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }
    pub fn status(&self) -> JobStatus {
        *self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    #[must_use]
    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn metrics(&self) -> JobMetrics {
        self.metrics.snapshot()
    }
    pub fn cancel(&self) {
        self.cancellation.store(true, Ordering::Release);
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*status, JobStatus::Succeeded | JobStatus::Failed) {
            *status = JobStatus::Cancelled;
        }
    }
    fn set_status(&self, status: JobStatus) {
        *self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = status;
    }
}

#[derive(Clone, Debug, Default)]
pub struct DirectRunner;

pub trait DirectCollectExt<T: Element> {
    /// Runs this output with explicit local resources.
    ///
    /// # Errors
    /// Returns a typed validation, execution, resource, or recovery error.
    fn collect_with(&self, config: &JobConfig) -> Result<(Vec<T>, JobHandle), Error>;
    /// Runs this output sequentially with default local resources.
    ///
    /// # Errors
    /// Returns a typed Direct runner error.
    fn collect_seq(&self) -> Result<Vec<T>, Error>;
    /// Runs this output with a job-owned parallel pool.
    ///
    /// # Errors
    /// Returns a typed Direct runner error.
    fn collect_par(&self, parallelism: usize) -> Result<Vec<T>, Error>;
}
impl<T: Element> DirectCollectExt<T> for Output<T> {
    fn collect_with(&self, config: &JobConfig) -> Result<(Vec<T>, JobHandle), Error> {
        DirectRunner.run_collect(self, config)
    }
    fn collect_seq(&self) -> Result<Vec<T>, Error> {
        Ok(self.collect_with(&JobConfig::default())?.0)
    }
    fn collect_par(&self, parallelism: usize) -> Result<Vec<T>, Error> {
        Ok(self
            .collect_with(&JobConfig {
                parallelism,
                ..JobConfig::default()
            })?
            .0)
    }
}
impl DirectRunner {
    /// Validates every node before any source is opened.
    ///
    /// # Errors
    /// Returns a typed graph or unsupported-capability error.
    pub fn validate(&self, graph: &Graph) -> Result<(), Error> {
        graph.validate_for(
            "direct",
            &Capabilities {
                external_bounded_source: true,
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
            let capability = match &node.operation {
                Operation::Source { .. }
                | Operation::Map
                | Operation::Filter
                | Operation::FlatMap
                | Operation::AssignTimestamp
                | Operation::FixedWindow
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
    /// Runs one bounded output with a job-owned thread pool.
    ///
    /// # Errors
    /// Returns validation, cancellation, or execution errors.
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
        if plan.materialized && config.checkpoint.is_some() {
            return Err(Error::unsupported(
                output.key(),
                "direct",
                "aggregate-checkpoint",
            ));
        }
        if plan.materialized {
            ensure_aggregates(&graph, &registry, output.key())?;
        }
        if config.checkpoint.is_some() && plan.routes.len() != 1 {
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
                plan.source_digest.clone().ok_or_else(|| {
                    Error::unsupported(&plan.summary.source_key, "direct", "source-config-digest")
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
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.parallelism)
            .build()
            .map_err(|e| Error::execution("job", "direct", e.to_string()))?;
        let handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        if config.cancellation.load(Ordering::Acquire) {
            handle.cancel();
            return Err(cancelled());
        }
        handle.set_status(JobStatus::Running);
        let result = if plan.materialized {
            materialized::execute(&pool, &graph, &registry, output.key(), config, &handle)
        } else {
            execute(
                &pool,
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
    /// Executes a deferred sink and commits after every input has succeeded.
    ///
    /// # Errors
    /// Returns typed validation, source, transform, sink, or commit errors.
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
        let handle = JobHandle::new(config.cancellation.clone(), JobStatus::Submitted);
        if config.cancellation.load(Ordering::Acquire) {
            handle.cancel();
            return Err(cancelled());
        }
        handle.set_status(JobStatus::Running);
        let result = (|| {
            let values: Vec<T> = if plan.materialized {
                materialized::execute(&pool, &graph, &registry, sink.key(), config, &handle)?
            } else {
                execute(&pool, &registry, &plan, sink.key(), config, None, &handle)?
            };
            let mut session = catch_unwind(AssertUnwindSafe(|| registry.open_sink(sink.key())))
                .unwrap_or_else(|_| {
                    Err(Error::execution(
                        sink.key(),
                        "direct",
                        "sink factory panicked",
                    ))
                })?;
            for value in values {
                let event = Envelope {
                    value: Box::new(value),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                };
                let written = catch_unwind(AssertUnwindSafe(|| session.write(event)))
                    .unwrap_or_else(|_| {
                        Err(Error::execution(
                            sink.key(),
                            "direct",
                            "sink writer panicked",
                        ))
                    });
                if let Err(error) = written {
                    let _ = catch_unwind(AssertUnwindSafe(|| session.abort()));
                    return Err(error);
                }
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
fn ensure_aggregates(
    graph: &Graph,
    registry: &ironbeam_core::Registry,
    output_key: &str,
) -> Result<(), Error> {
    let mut stack = vec![output_key];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(key) = stack.pop() {
        if !seen.insert(key) {
            continue;
        }
        let node = graph
            .node(key)
            .ok_or_else(|| Error::graph(key, "physical node missing"))?;
        if matches!(
            node.operation,
            Operation::GroupByKey
                | Operation::CoGroupByKey
                | Operation::JoinInner
                | Operation::JoinLeft
                | Operation::JoinRight
                | Operation::JoinFull
                | Operation::SideInputMap
                | Operation::GlobalCombine
                | Operation::BatchMapValues { .. }
        ) && !registry.has_aggregate(key)
        {
            return Err(Error::unsupported(key, "direct", "aggregate-function"));
        }
        if matches!(
            node.operation,
            Operation::FoldGlobal
                | Operation::FoldPerKey
                | Operation::FoldOrderedGlobal
                | Operation::FoldOrderedPerKey
        ) && !registry.has_fold(key)
        {
            return Err(Error::unsupported(key, "direct", "fold-function"));
        }
        if let Some(input) = node.input.as_deref() {
            stack.push(input);
        }
        stack.extend(node.additional_inputs.iter().map(|edge| edge.key.as_str()));
    }
    Ok(())
}
fn open_source_safe(registry: &Registry, key: &str) -> Result<SourceIterator, Error> {
    catch_unwind(AssertUnwindSafe(|| registry.open_source(key)))
        .unwrap_or_else(|_| Err(Error::execution(key, "direct", "source factory panicked")))
}
fn read_source_batch(
    source: &mut SourceIterator,
    limit: usize,
    key: &str,
) -> Result<Vec<Envelope>, Error> {
    catch_unwind(AssertUnwindSafe(|| {
        source.by_ref().take(limit).collect::<Result<_, _>>()
    }))
    .unwrap_or_else(|_| Err(Error::execution(key, "direct", "source iterator panicked")))
}
fn validate_config(config: &JobConfig) -> Result<(), Error> {
    if config.parallelism == 0
        || config.memory_budget_bytes == 0
        || config.buffer_budget_elements == 0
    {
        return Err(Error::graph(
            "job",
            "resource budgets and parallelism must be positive",
        ));
    }
    if config
        .checkpoint
        .as_ref()
        .is_some_and(|c| c.interval_batches == 0)
    {
        return Err(Error::graph(
            "checkpoint",
            "checkpoint interval must be positive",
        ));
    }
    if config
        .checkpoint
        .as_ref()
        .is_some_and(|c| c.binary_build_id.is_empty())
    {
        return Err(Error::graph(
            "checkpoint",
            "binary build identity must be supplied",
        ));
    }
    if config
        .spill
        .as_ref()
        .is_some_and(|s| s.threshold_bytes == 0)
    {
        return Err(Error::graph("spill", "spill threshold must be positive"));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn execute<T: Element>(
    pool: &rayon::ThreadPool,
    registry: &ironbeam_core::Registry,
    plan: &planner::PhysicalPlan,
    output_key: &str,
    config: &JobConfig,
    checkpoint_identity: Option<checkpoint::Identity>,
    handle: &JobHandle,
) -> Result<Vec<T>, Error> {
    let mut store = OutputStore::<T>::new(
        config.memory_budget_bytes,
        config.spill.as_ref(),
        handle.metrics.clone(),
    )?;
    let checkpoint = config
        .checkpoint
        .as_ref()
        .zip(checkpoint_identity)
        .map(|(cfg, identity)| {
            checkpoint::CheckpointManager::new(cfg, identity, handle.metrics.clone())
        })
        .transpose()?;
    let recovered_offset = if let Some(manager) = &checkpoint {
        manager.recover(&mut store)?
    } else {
        0
    };
    if plan
        .summary
        .source_len_hint
        .is_some_and(|len| recovered_offset > len)
    {
        return Err(Error::recovery(
            &plan.summary.source_key,
            "checkpoint offset exceeds source length",
        ));
    }
    handle.processed.store(
        handle.metrics.recovered_elements.load(Ordering::Relaxed),
        Ordering::Relaxed,
    );
    for route in &plan.routes {
        let single_route = route
            .operations
            .iter()
            .all(|key| registry.has_single_transform(key));
        let mut source_offset = recovered_offset;
        let mut source = open_source_safe(registry, &route.source_key)?;
        for _ in 0..source_offset {
            if read_source_batch(&mut source, 1, &route.source_key)?.is_empty() {
                return Err(Error::recovery(
                    &route.source_key,
                    "source ended before checkpoint position",
                ));
            }
        }
        let mut batches = 0_usize;
        loop {
            if config.cancellation.load(Ordering::Acquire) {
                return Err(cancelled());
            }
            let batch =
                read_source_batch(&mut source, plan.summary.batch_limit, &route.source_key)?;
            if batch.is_empty() {
                break;
            }
            let batch_len = batch.len();
            handle.metrics.source_elements.fetch_add(
                u64::try_from(batch_len).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            handle.metrics.peak_batch_elements.fetch_max(
                u64::try_from(batch_len).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            if single_route {
                let results: Vec<Result<Option<Envelope>, Error>> = pool.install(|| {
                    batch
                        .into_par_iter()
                        .map(|event| {
                            let mut current = Some(event);
                            for key in &route.operations {
                                let Some(event) = current else {
                                    break;
                                };
                                if config.cancellation.load(Ordering::Acquire) {
                                    return Err(cancelled());
                                }
                                current = match catch_unwind(AssertUnwindSafe(|| {
                                    registry.apply(key, event)
                                })) {
                                    Ok(result) => result?,
                                    Err(_) => {
                                        return Err(Error::execution(
                                            key,
                                            "direct",
                                            "user transform panicked",
                                        ));
                                    }
                                };
                                if current.is_none() {
                                    handle
                                        .metrics
                                        .filtered_elements
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Ok(current)
                        })
                        .collect()
                });
                for result in results {
                    if let Some(event) = result? {
                        push_output(event, &mut store, output_key, handle)?;
                    }
                }
            } else {
                let results: Vec<Result<Vec<Envelope>, Error>> = pool.install(|| {
                    batch
                        .into_par_iter()
                        .map(|event| {
                            let mut events = vec![event];
                            for key in &route.operations {
                                let mut next_events = Vec::new();
                                for event in events {
                                    if config.cancellation.load(Ordering::Acquire) {
                                        return Err(cancelled());
                                    }
                                    match catch_unwind(AssertUnwindSafe(|| {
                                        registry.apply_many(key, event)
                                    })) {
                                        Ok(Ok(produced)) => {
                                            if produced.is_empty() {
                                                handle
                                                    .metrics
                                                    .filtered_elements
                                                    .fetch_add(1, Ordering::Relaxed);
                                            }
                                            next_events.extend(produced);
                                        }
                                        Ok(Err(error)) => return Err(error),
                                        Err(_) => {
                                            return Err(Error::execution(
                                                key,
                                                "direct",
                                                "user transform panicked",
                                            ));
                                        }
                                    }
                                }
                                events = next_events;
                            }
                            Ok(events)
                        })
                        .collect()
                });
                for result in results {
                    for event in result? {
                        push_output(event, &mut store, output_key, handle)?;
                    }
                }
            }
            source_offset = source_offset
                .checked_add(batch_len)
                .ok_or_else(|| Error::resource("job", "source offset overflow"))?;
            batches += 1;
            handle.metrics.batches.fetch_add(1, Ordering::Relaxed);
            if let Some(manager) = &checkpoint
                && manager.should_save(batches)
            {
                manager.save(source_offset, &store)?;
            }
        }
        if let Some(manager) = &checkpoint
            && batches > 0
            && !manager.should_save(batches)
        {
            manager.save(source_offset, &store)?;
        }
    }
    store.into_vec()
}
fn push_output<T: Element>(
    event: Envelope,
    store: &mut OutputStore<T>,
    output_key: &str,
    handle: &JobHandle,
) -> Result<(), Error> {
    let value = event
        .value
        .downcast::<T>()
        .map_err(|_| Error::execution(output_key, "direct", "output type mismatch"))?;
    store.push(*value)?;
    let total = handle
        .metrics
        .output_elements
        .fetch_add(1, Ordering::Relaxed)
        + 1;
    handle.processed.store(total, Ordering::Relaxed);
    Ok(())
}
fn cancelled() -> Error {
    Error {
        kind: ErrorKind::Cancelled,
        node: None,
        backend: Some("direct".into()),
        retry: RetryClass::Permanent,
        message: "job cancelled".into(),
    }
}
