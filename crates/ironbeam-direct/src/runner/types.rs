/// Parallelism, memory, cancellation, spilling, and checkpoint settings for Direct.
#[derive(Clone, Debug)]
pub struct JobConfig {
    pub parallelism: usize,
    pub memory_budget_bytes: usize,
    pub buffer_budget_elements: usize,
    pub cancellation: Arc<AtomicBool>,
    pub spill: Option<SpillConfig>,
    pub checkpoint: Option<CheckpointConfig>,
    #[cfg(feature = "metrics")]
    pub metrics_collector: Option<ironbeam_metrics::MetricsCollector>,
}

/// Values from two output branches completed by one Direct job.
pub struct PairResult<T, U> {
    pub left: Vec<T>,
    pub right: Vec<U>,
    pub job: JobHandle,
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
            #[cfg(feature = "metrics")]
            metrics_collector: None,
        }
    }
}

/// Lifecycle state of one Direct job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStatus {
    Submitted,
    Running,
    Draining,
    Succeeded,
    Failed,
    Cancelled,
}

/// Shared handle for status, cancellation, and metrics of one Direct job.
#[derive(Clone)]
pub struct JobHandle {
    pub(crate) id: u64,
    pub(crate) status: Arc<Mutex<JobStatus>>,
    pub(crate) cancellation: Arc<AtomicBool>,
    pub(crate) processed: Arc<AtomicU64>,
    pub(crate) metrics: Arc<MetricsState>,
    #[cfg(feature = "metrics")]
    pub(crate) metrics_collector: Option<ironbeam_metrics::MetricsCollector>,
}
impl JobHandle {
    /// Create a running handle for tests and runner contract fixtures.
    #[must_use]
    pub fn running_fixture() -> Self {
        Self::new(Arc::new(AtomicBool::new(false)), JobStatus::Running)
    }
    pub(crate) fn new(cancellation: Arc<AtomicBool>, status: JobStatus) -> Self {
        Self {
            id: NEXT_JOB.fetch_add(1, Ordering::Relaxed),
            status: Arc::new(Mutex::new(status)),
            cancellation,
            processed: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(MetricsState::default()),
            #[cfg(feature = "metrics")]
            metrics_collector: None,
        }
    }
    #[must_use]
    /// Return this process's unique job identifier.
    pub const fn id(&self) -> u64 {
        self.id
    }
    /// Return the most recently recorded job lifecycle status.
    pub fn status(&self) -> JobStatus {
        *self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    /// Return the number of elements committed by the job.
    #[must_use]
    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }
    /// Snapshot the job's built-in metrics.
    #[must_use]
    pub fn metrics(&self) -> JobMetrics {
        self.metrics.snapshot()
    }
    /// Borrow the optional custom metrics collector for this job.
    #[cfg(feature = "metrics")]
    #[must_use]
    pub const fn custom_metrics(&self) -> Option<&ironbeam_metrics::MetricsCollector> {
        self.metrics_collector.as_ref()
    }
    /// Request cancellation; terminal statuses remain unchanged.
    pub fn cancel(&self) {
        self.cancellation.store(true, Ordering::Release);
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            *status,
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled
        ) {
            *status = JobStatus::Cancelled;
            drop(status);
            #[cfg(feature = "metrics")]
            if let Some(collector) = &self.metrics_collector {
                collector.record_end();
            }
        }
    }
    pub(crate) fn set_status(&self, status: JobStatus) {
        let mut current = self
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *current == status {
            return;
        }
        *current = status;
        drop(current);
        #[cfg(feature = "metrics")]
        if let Some(collector) = &self.metrics_collector {
            match status {
                JobStatus::Running => collector.record_start(),
                JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled => {
                    collector.record_end();
                }
                JobStatus::Submitted | JobStatus::Draining => {}
            }
        }
    }
}

/// Runner that executes a logical graph in the local process.
#[derive(Clone, Debug, Default)]
pub struct DirectRunner;

/// Convenience terminals for collecting bounded outputs with Direct.
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
impl<T: Element> DirectCollectExt<T> for PCollection<T> {
    fn collect_with(&self, config: &JobConfig) -> Result<(Vec<T>, JobHandle), Error> {
        let key = format!("{}/direct-collect", self.key());
        DirectRunner.run_collect(&self.detached_output(&key)?, config)
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
