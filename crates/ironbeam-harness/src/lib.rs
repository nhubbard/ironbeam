//! Embedded Beam Fn API harness for ironbeam.
//!
//! The user's binary, when running inside a Dataflow worker, hands control
//! to [`serve`] after rebuilding the ironbeam pipeline locally. `serve` opens
//! the Control / Logging client streams and pumps bundle instructions
//! through [`dispatch::Dispatcher`].
//!
//! ### What's wired
//! - argv / endpoint parsing
//! - Control bidi stream
//! - Logging client
//! - Provision client for `GetProvisionInfo` on startup
//! - PBD → `BundlePlan` structural analysis
//! - Fingerprint cross-check helper
//!
//! The v5 executor runs bounded bundles through Beam Fn Data and Fn State,
//! rebuilding and verifying the local graph before processing. The control
//! loop fetches uncached bundle descriptors. Whole-job final-result commits
//! and unbounded execution remain separate release gates.

pub mod bundle;
pub mod coders;
pub mod control;
pub mod data;
pub mod dispatch;
pub mod endpoints;
pub mod executor;
pub mod finalization_journal;
pub mod fingerprint_check;
pub mod logging;
pub mod portable_metrics;
pub mod portable_publication;
pub mod provision;
pub mod sdf_wire;
pub mod state;
pub mod timer_coder;
mod supervision;
pub mod unbounded;
pub mod v5;
pub mod v5_executor;

pub use bundle::{BundlePlan, Step};
pub use dispatch::{Dispatcher, PbdCache};
pub use endpoints::Endpoints;
pub use logging::{
    enable_log_buffering, init_tracing, install_tracing_sink, BeamFnLoggingLayer, LoggingClient,
};

use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;

/// Top-level entry point for harness mode. Connects Logging and Control to
/// the runner's endpoints and blocks until the Control stream terminates.
///
/// `local_fingerprint` is the structural fingerprint of the ironbeam pipeline
/// rebuilt inside this process; the Dispatcher (M2b) will cross-check it
/// against the driver's fingerprint embedded in pipeline display data.
pub async fn serve(endpoints: Endpoints, local_fingerprint: u64) -> Result<()> {
    serve_inner(endpoints, local_fingerprint, None, false, PbdCache::new()).await
}

/// Harness entry that wires an element-level [`executor::BundleRunner`] into the
/// control loop. The no-runner [`serve`] variant only runs the PBD analyzer
/// and is meant for Dispatcher-level tests.
pub async fn serve_with_runner(
    endpoints: Endpoints,
    local_fingerprint: u64,
    runner: Arc<dyn executor::BundleRunner>,
) -> Result<()> {
    serve_inner(
        endpoints,
        local_fingerprint,
        Some(runner),
        false,
        PbdCache::new(),
    )
    .await
}

/// Start a v5 worker built from the same application pipeline as the driver.
/// The build ID must match the driver's portable execution identity.
pub async fn serve_v5(
    endpoints: Endpoints,
    pipeline: &ironbeam_core::Pipeline,
    binary_build_id: &str,
) -> Result<()> {
    if let Some((request, response)) = supervision::child_paths() {
        return supervision::run_child(&endpoints, pipeline, binary_build_id, &request, &response)
            .await;
    }
    let worker_id = endpoints.id.clone();
    let cache = PbdCache::new();
    if std::env::var_os("IRONBEAM_V5_SUPERVISE_EXECUTION").is_some() {
        supervision::validate_pipeline(pipeline)?;
        let runner: Arc<dyn executor::BundleRunner> = Arc::new(
            supervision::SupervisedV5Runner::new(cache.clone(), worker_id)?,
        );
        return serve_inner(endpoints, 0, Some(runner), true, cache).await;
    }
    let runner: Arc<dyn executor::BundleRunner> = Arc::new(
        v5_executor::V5BundleExecutor::new_for_worker(pipeline, binary_build_id, Some(&worker_id))?,
    );
    serve_inner(endpoints, 0, Some(runner), true, cache).await
}

async fn serve_inner(
    endpoints: Endpoints,
    local_fingerprint: u64,
    runner: Option<Arc<dyn executor::BundleRunner>>,
    with_worker_id: bool,
    cache: PbdCache,
) -> Result<()> {
    logging::init_tracing();
    logging::enable_log_buffering();

    info!(worker_id = %endpoints.id, "harness starting");

    // Logging is best-effort. Missing or broken logging endpoint shouldn't
    // block the harness from processing bundles. When it succeeds we install
    // the client as the sink for `BeamFnLoggingLayer`, flushing any buffered
    // startup `tracing::*!` events into the runner's log view.
    let worker_id = with_worker_id.then_some(endpoints.id.as_str());
    let logging =
        match logging::LoggingClient::connect_for_worker(&endpoints.logging_endpoint, worker_id)
            .await
        {
            Ok(l) => {
                logging::install_tracing_sink(l.clone());
                Some(l)
            }
            Err(e) => {
                tracing::warn!(error=?e, "failed to connect logging; continuing without");
                None
            }
        };
    let _ = logging; // held for Drop / future use

    // Provision is optional on some runners; log and continue if absent/broken.
    if let Some(pe) = endpoints.provision_endpoint.as_deref() {
        let info = match worker_id {
            Some(id) => provision::get_provision_info_for_worker(pe, id).await,
            None => provision::get_provision_info(pe).await,
        };
        match info {
            Ok(info) => info!(
                retrieval_token = %info.retrieval_token,
                deps = info.dependencies.len(),
                "provision info fetched"
            ),
            Err(e) => tracing::warn!(error=?e, "GetProvisionInfo failed; continuing"),
        }
    }

    let dispatcher = Arc::new(Dispatcher::new(cache, local_fingerprint));

    let session = control::start_for_worker(
        &endpoints.control_endpoint,
        Arc::clone(&dispatcher),
        runner,
        worker_id,
    )
    .await
    .context("open control session")?;

    session.join().await
}
