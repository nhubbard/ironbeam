//! Per-job counters; no process-global metrics collector.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JobMetrics {
    pub source_elements: u64,
    pub output_elements: u64,
    pub filtered_elements: u64,
    pub batches: u64,
    pub peak_batch_elements: u64,
    pub spill_files: u64,
    pub spilled_bytes: u64,
    pub checkpoints: u64,
    pub recovered_elements: u64,
}

#[derive(Default)]
pub(crate) struct MetricsState {
    pub(crate) source_elements: AtomicU64,
    pub(crate) output_elements: AtomicU64,
    pub(crate) filtered_elements: AtomicU64,
    pub(crate) batches: AtomicU64,
    pub(crate) peak_batch_elements: AtomicU64,
    pub(crate) spill_files: AtomicU64,
    pub(crate) spilled_bytes: AtomicU64,
    pub(crate) checkpoints: AtomicU64,
    pub(crate) recovered_elements: AtomicU64,
}
impl MetricsState {
    pub(crate) fn snapshot(&self) -> JobMetrics {
        let read = |value: &AtomicU64| value.load(Ordering::Relaxed);
        JobMetrics {
            source_elements: read(&self.source_elements),
            output_elements: read(&self.output_elements),
            filtered_elements: read(&self.filtered_elements),
            batches: read(&self.batches),
            peak_batch_elements: read(&self.peak_batch_elements),
            spill_files: read(&self.spill_files),
            spilled_bytes: read(&self.spilled_bytes),
            checkpoints: read(&self.checkpoints),
            recovered_elements: read(&self.recovered_elements),
        }
    }
}
