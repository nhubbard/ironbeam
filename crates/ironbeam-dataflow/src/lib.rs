//! Top-level entry point. The same binary runs in **driver** mode (on a user's
//! laptop / CI) and **harness** mode (inside a Dataflow worker) — `run_v5`
//! dispatches based on argv.
//!
//! See the workspace `README.md` and `V5_ALIGNMENT.md` for status and architecture.

pub mod artifact;
pub mod auth;
pub mod driver;
pub mod mode;
pub mod options;
pub mod poll;
pub mod portable_metrics {
    pub use ironbeam_harness::portable_metrics::*;
}
pub mod portable_publication {
    pub use ironbeam_harness::portable_publication::*;
}
pub mod stage;
pub mod submit;

use ironbeam_cloud_objects::ObjectStores;
use std::sync::Arc;

pub use artifact::{attach_worker_binary, StagedArtifact};
pub use auth::{GcloudCliTokenSource, GcpAuthTokenSource, StaticTokenSource, TokenSource};
pub use driver::{Driver, JobOutcome};
pub use mode::Mode;
pub use options::{DataflowOptions, DEFAULT_SDK_CONTAINER_IMAGE};
pub use poll::{is_terminal, list_messages, PollConfig, TERMINAL_STATES};
pub use stage::{parse_gs_url, GcsStager, MemoryStager, Stager};
pub use submit::{build_job_request, submit_job, SubmittedJob, REQUIRED_EXPERIMENTS};

/// Prepare and validate the bounded v5 contract before local or remote use.
pub fn prepare_v5(
    pipeline: &ironbeam_core::Pipeline,
    container_image: &str,
    binary_build_id: &str,
) -> Result<ironbeam_graphx::v5::TranslatedV5, ironbeam_graphx::v5::V5Error> {
    let translated = ironbeam_graphx::v5::translate_v5(pipeline, container_image, binary_build_id)?;
    let (graph, _) = pipeline.snapshot()?;
    ironbeam_graphx::v5::validate_beam_contract(&graph, &translated)?;
    Ok(translated)
}

/// Run a v5 core pipeline on Dataflow or inside a portable SDK worker.
/// The same application `build` function and `binary_build_id` must be used
/// in both processes. The current translator accepts its supported bounded
/// graph and window operations, including branches and bounded `wait_on`
/// signals; unsupported graphs fail before staging or submission.
pub async fn run_v5<F>(
    build: F,
    opts: DataflowOptions,
    binary_build_id: &str,
) -> anyhow::Result<RunOutcome>
where
    F: Fn(&ironbeam_core::Pipeline) -> anyhow::Result<()>,
{
    run_v5_inner(build, Some(opts), binary_build_id, None).await
}

async fn run_v5_inner<F>(
    build: F,
    opts: Option<DataflowOptions>,
    binary_build_id: &str,
    object_journal: Option<(Arc<ObjectStores>, String)>,
) -> anyhow::Result<RunOutcome>
where
    F: Fn(&ironbeam_core::Pipeline) -> anyhow::Result<()>,
{
    init_tracing();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match Mode::detect_portable(&args) {
        Mode::Driver => {
            let opts = opts.ok_or_else(|| anyhow::anyhow!("v5 driver options are missing"))?;
            let pipeline = ironbeam_core::Pipeline::default();
            build(&pipeline)?;
            let translated = prepare_v5(&pipeline, &opts.sdk_container_image, binary_build_id)?;
            let worker_path = opts
                .worker_binary_path
                .clone()
                .map(Ok)
                .unwrap_or_else(std::env::current_exe)?;
            validate_v5_worker_binary(&worker_path)?;
            let token_source = GcpAuthTokenSource::new().await?;
            let stager = GcsStager::new(&opts.staging_location, token_source.clone())?;
            let driver = Driver::new(stager, token_source);
            let outcome = driver.submit_pipeline(translated.pipeline, &opts).await?;
            require_v5_success(outcome).map(RunOutcome::Submitted)
        }
        Mode::Harness => {
            let pipeline = ironbeam_core::Pipeline::default();
            build(&pipeline)?;
            let endpoints = if Mode::detect(&args) == Mode::Harness {
                ironbeam_harness::Endpoints::from_argv(args)?
            } else {
                ironbeam_harness::Endpoints::from_process_argv(args).await?
            };
            if let Some((stores, uri)) = object_journal {
                ironbeam_harness::serve_v5_with_object_journal(
                    endpoints,
                    &pipeline,
                    binary_build_id,
                    stores,
                    uri,
                )
                .await?;
            } else {
                ironbeam_harness::serve_v5(endpoints, &pipeline, binary_build_id).await?;
            }
            Ok(RunOutcome::HarnessExited)
        }
    }
}

fn require_v5_success(outcome: JobOutcome) -> anyhow::Result<JobOutcome> {
    if outcome.final_state != "JOB_STATE_DONE" {
        anyhow::bail!(
            "v5 Dataflow job {} ended in {}",
            outcome.job_id,
            outcome.final_state
        );
    }
    Ok(outcome)
}

fn validate_v5_worker_binary(path: &std::path::Path) -> anyhow::Result<()> {
    use std::io::Read;

    let mut file = std::fs::File::open(path).map_err(|error| {
        anyhow::anyhow!("cannot read v5 worker binary {}: {error}", path.display())
    })?;
    let mut header = [0_u8; 20];
    file.read_exact(&mut header).map_err(|error| {
        anyhow::anyhow!(
            "cannot read v5 worker binary header {}: {error}",
            path.display()
        )
    })?;
    // Dataflow's Linux SDK workers require a 64-bit little-endian x86-64 ELF.
    if &header[..4] != b"\x7fELF"
        || header[4] != 2
        || header[5] != 1
        || u16::from_le_bytes([header[18], header[19]]) != 62
    {
        anyhow::bail!(
            "v5 worker binary {} is not a Linux x86-64 ELF; set IRONBEAM_WORKER_BINARY to a cross-compiled worker",
            path.display()
        );
    }
    Ok(())
}

/// Environment-configured variant of [`run_v5`]. Dataflow options are only
/// loaded in driver mode; worker mode receives endpoints from Beam instead.
pub async fn run_v5_from_env<F>(build: F, binary_build_id: &str) -> anyhow::Result<RunOutcome>
where
    F: Fn(&ironbeam_core::Pipeline) -> anyhow::Result<()>,
{
    if Mode::detect_portable(std::env::args().skip(1)) == Mode::Driver {
        run_v5_inner(
            build,
            Some(DataflowOptions::from_env()?),
            binary_build_id,
            None,
        )
        .await
    } else {
        // Worker mode never reads driver credentials or staging settings.
        run_v5_inner(build, None, binary_build_id, None).await
    }
}

/// Run the same application in driver or worker mode with a shared object
/// finalization journal. Construct the provider registry and pass this URI on
/// every invocation of the binary, including supervised execution children.
pub async fn run_v5_from_env_with_object_journal<F>(
    build: F,
    binary_build_id: &str,
    stores: Arc<ObjectStores>,
    journal_uri: impl Into<String>,
) -> anyhow::Result<RunOutcome>
where
    F: Fn(&ironbeam_core::Pipeline) -> anyhow::Result<()>,
{
    let options = if Mode::detect_portable(std::env::args().skip(1)) == Mode::Driver {
        Some(DataflowOptions::from_env()?)
    } else {
        None
    };
    run_v5_inner(
        build,
        options,
        binary_build_id,
        Some((stores, journal_uri.into())),
    )
    .await
}

pub use ironbeam_harness::{enable_log_buffering, init_tracing, BeamFnLoggingLayer};

/// Outcome of `run_v5`. `Submitted` is the driver-path result; `HarnessExited`
/// is returned after harness mode terminates (the Control stream has closed).
#[derive(Debug)]
pub enum RunOutcome {
    Submitted(JobOutcome),
    HarnessExited,
}

#[cfg(test)]
#[path = "../tests/unit/lib__v5_run_tests.rs"]
mod v5_run_tests;
