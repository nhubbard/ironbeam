//! Bounded direct execution of the Ironbeam 5 logical graph.
#![allow(clippy::redundant_pub_crate, clippy::missing_errors_doc)]
mod checkpoint;
mod helpers;
mod materialized;
mod metrics;
mod multi_output;
mod multi_sink;
mod planner;
mod sink;
mod spill;
mod stream;
mod stream_checkpoint;

pub use checkpoint::CheckpointConfig;
pub use helpers::{
    DirectFallibleExt, DirectFlatPartitionExt, DirectKeySortedExt, DirectKeyedCollectExt,
    DirectPartitionByExt, DirectPartitionExt, DirectSortedExt, Partition3, Partition4,
    Partitioned3, Partitioned4,
};
pub use metrics::JobMetrics;
pub use planner::{PlanDecision, PlanSummary};
pub use spill::SpillConfig;
pub use stream::StreamJob;

mod runner;
pub use runner::{DirectCollectExt, DirectRunner, JobConfig, JobHandle, JobStatus, PairResult};
pub(crate) use runner::{
    cancelled, ensure_bounded_sink_session, open_source_safe, read_source_batch, validate_config,
};
