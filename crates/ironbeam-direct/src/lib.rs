//! Bounded direct execution of the Ironbeam 5 logical graph.
#![allow(clippy::redundant_pub_crate)]
mod checkpoint;
mod metrics;
mod planner;
mod spill;

pub use checkpoint::CheckpointConfig;
pub use metrics::JobMetrics;
pub use planner::PlanSummary;
pub use spill::SpillConfig;

use ironbeam_core::{
    Capabilities, Element, Envelope, Error, ErrorKind, Graph, Operation, Output, RetryClass,
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
impl DirectRunner {
    /// Validates every node before any source is opened.
    ///
    /// # Errors
    /// Returns a typed graph or unsupported-capability error.
    pub fn validate(&self, graph: &Graph) -> Result<(), Error> {
        graph.validate_for("direct", &Capabilities::default())?;
        for node in &graph.nodes {
            let capability = match &node.operation {
                Operation::Source { .. }
                | Operation::Map
                | Operation::Filter
                | Operation::Output => None,
                Operation::FixedWindow => Some("fixed-window-execution"),
                Operation::Sink { .. } => Some("deferred-sink-execution"),
                Operation::GlobalCombine => Some("global-combine"),
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
        let result = execute(
            &pool,
            &registry,
            &plan,
            output.key(),
            config,
            checkpoint_identity,
            &handle,
        );
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
    let mut source_offset = if let Some(manager) = &checkpoint {
        manager.recover(&mut store)?
    } else {
        0
    };
    if plan
        .summary
        .source_len_hint
        .is_some_and(|len| source_offset > len)
    {
        return Err(Error::recovery(
            &plan.summary.source_key,
            "checkpoint offset exceeds source length",
        ));
    }
    let mut source = registry.open_source(&plan.summary.source_key)?;
    for _ in 0..source_offset {
        if source.next().is_none() {
            return Err(Error::recovery(
                &plan.summary.source_key,
                "source ended before checkpoint position",
            ));
        }
    }
    handle.processed.store(
        handle.metrics.recovered_elements.load(Ordering::Relaxed),
        Ordering::Relaxed,
    );
    let mut batches = 0_usize;
    loop {
        if config.cancellation.load(Ordering::Acquire) {
            return Err(cancelled());
        }
        let batch: Vec<Envelope> = source.by_ref().take(plan.summary.batch_limit).collect();
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
        let results: Vec<Result<Option<Envelope>, Error>> = pool.install(|| {
            batch
                .into_par_iter()
                .map(|mut event| {
                    for key in &plan.operations {
                        if config.cancellation.load(Ordering::Acquire) {
                            return Err(cancelled());
                        }
                        event = match catch_unwind(AssertUnwindSafe(|| registry.apply(key, event)))
                        {
                            Ok(Ok(Some(next))) => next,
                            Ok(Ok(None)) => return Ok(None),
                            Ok(Err(error)) => return Err(error),
                            Err(_) => {
                                return Err(Error::execution(
                                    key,
                                    "direct",
                                    "user transform panicked",
                                ));
                            }
                        };
                    }
                    Ok(Some(event))
                })
                .collect()
        });
        for result in results {
            match result? {
                Some(event) => {
                    let value = event.value.downcast::<T>().map_err(|_| {
                        Error::execution(output_key, "direct", "output type mismatch")
                    })?;
                    store.push(*value)?;
                    let total = handle
                        .metrics
                        .output_elements
                        .fetch_add(1, Ordering::Relaxed)
                        + 1;
                    handle.processed.store(total, Ordering::Relaxed);
                }
                None => {
                    handle
                        .metrics
                        .filtered_elements
                        .fetch_add(1, Ordering::Relaxed);
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
    store.into_vec()
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
