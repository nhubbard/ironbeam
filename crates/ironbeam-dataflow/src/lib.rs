//! Top-level entry point. The same binary runs in **driver** mode (on a user's
//! laptop / CI) and **harness** mode (inside a Dataflow worker) — `run_v5`
//! dispatches based on argv.
//!
//! The crate root exports the driver, worker entry points, and portable helpers.

pub mod artifact;
pub mod auth;
pub mod driver;
pub mod job_metrics;
pub mod mode;
pub mod options;
pub mod poll;
pub mod portable_metrics {
    pub use ironbeam_harness::portable_metrics::*;
}
pub mod portable_publication {
    pub use ironbeam_harness::portable_publication::*;
}
mod runtime;
pub mod stage;
pub mod submit;

pub use artifact::{attach_worker_binary, StagedArtifact};
pub use auth::{GcloudCliTokenSource, GcpAuthTokenSource, StaticTokenSource, TokenSource};
pub use driver::{Driver, JobOutcome};
pub use ironbeam_harness::{enable_log_buffering, init_tracing, BeamFnLoggingLayer};
pub use job_metrics::{get_job_metrics, DataflowJobMetrics, DataflowMetric, DataflowMetricName};
pub use mode::Mode;
pub use options::{DataflowOptions, DEFAULT_SDK_CONTAINER_IMAGE};
pub use poll::{is_terminal, list_messages, PollConfig, TERMINAL_STATES};
#[cfg(any(
    feature = "gcp-objects",
    feature = "aws-objects",
    feature = "azure-objects"
))]
pub use runtime::run_v5_from_env_with_hosted_objects;
pub use runtime::{
    prepare_v5, run_v5, run_v5_from_env, run_v5_from_env_with_object_journal,
    run_v5_from_env_with_object_stores, RunOutcome,
};
pub use stage::{parse_gs_url, GcsStager, MemoryStager, Stager};
pub use submit::{build_job_request, submit_job, SubmittedJob, REQUIRED_EXPERIMENTS};
