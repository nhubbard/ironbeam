//! Bounded direct execution of Ironbeam 5's first logical graph slice.
use ironbeam_core::{
    Capabilities, Element, Envelope, Error, ErrorKind, Graph, Operation, Output, RetryClass,
};
use rayon::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_JOB: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct JobConfig {
    pub parallelism: usize,
    pub memory_budget_bytes: usize,
    pub buffer_budget_elements: usize,
    pub cancellation: Arc<AtomicBool>,
}
impl Default for JobConfig {
    fn default() -> Self {
        Self {
            parallelism: 1,
            memory_budget_bytes: 64 * 1024 * 1024,
            buffer_budget_elements: 4096,
            cancellation: Arc::new(AtomicBool::new(false)),
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
        if config.parallelism == 0
            || config.memory_budget_bytes == 0
            || config.buffer_budget_elements == 0
        {
            return Err(Error::graph(
                "job",
                "resource budgets and parallelism must be positive",
            ));
        }
        let mut path = Vec::new();
        let mut key = output.key().to_owned();
        loop {
            let node = graph
                .node(&key)
                .ok_or_else(|| Error::graph(&key, "output path missing"))?;
            path.push(node.key.clone());
            match &node.input {
                Some(parent) => key = parent.clone(),
                None => break,
            }
        }
        path.reverse();
        let output_key = output.key().to_owned();
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
        let result = pool.install(|| -> Result<Vec<T>, Error> {
            let mut values: Vec<Envelope> = registry.open_source(&path[0])?.collect();
            for key in path.iter().skip(1) {
                if config.cancellation.load(Ordering::Acquire) {
                    return Err(cancelled());
                }
                if matches!(
                    graph.node(key).map(|node| &node.operation),
                    Some(Operation::Output)
                ) {
                    continue;
                }
                let next: Vec<Result<Option<Envelope>, Error>> = values
                    .into_par_iter()
                    .map(|value| {
                        if config.cancellation.load(Ordering::Acquire) {
                            return Err(cancelled());
                        }
                        registry.apply(key, value)
                    })
                    .collect();
                values = next
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect();
                handle
                    .processed
                    .store(values.len() as u64, Ordering::Relaxed);
            }
            values
                .into_iter()
                .map(|e| {
                    e.value.downcast::<T>().map(|value| *value).map_err(|_| {
                        Error::execution(&output_key, "direct", "output type mismatch")
                    })
                })
                .collect()
        });
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
fn cancelled() -> Error {
    Error {
        kind: ErrorKind::Cancelled,
        node: None,
        backend: Some("direct".into()),
        retry: RetryClass::Permanent,
        message: "job cancelled".into(),
    }
}
