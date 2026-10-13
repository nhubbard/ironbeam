//! `BeamFnControl` client: opens the bidirectional `Control` stream, pumps
//! `InstructionRequest`s through the `Dispatcher`, and writes
//! `InstructionResponse`s back.
//!
//! Also handles `GetProcessBundleDescriptor` lookups via the same client
//! connection — when the `Dispatcher` (M2b) encounters a pbd_id it hasn't seen,
//! it asks this module to fetch the PBD from the runner.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use beam_proto::fn_execution_v1::beam_fn_control_client::BeamFnControlClient;
use beam_proto::fn_execution_v1::{
    instruction_request::Request as InstrReq, instruction_response::Response as InstrResp,
    FinalizeBundleResponse, GetProcessBundleDescriptorRequest, InstructionResponse,
    ProcessBundleDescriptor, ProcessBundleSplitRequest,
};
use futures_util::FutureExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use crate::bundle;
use crate::dispatch::{Dispatcher, PbdCache};
use crate::endpoints::{normalize_endpoint, worker_request};
use crate::executor::BundleRunner;

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload")
}

async fn finalize_response(
    id: String,
    bundle_id: &str,
    runner: &dyn BundleRunner,
) -> InstructionResponse {
    let result = if bundle_id.is_empty() {
        Err(anyhow!("FinalizeBundle has no bundle instruction id"))
    } else {
        AssertUnwindSafe(runner.finalize_bundle(bundle_id))
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| Err(anyhow!("executor panicked: {}", panic_message(&*panic))))
    };
    match result {
        Ok(()) => InstructionResponse {
            instruction_id: id,
            error: String::new(),
            response: Some(InstrResp::FinalizeBundle(FinalizeBundleResponse::default())),
        },
        Err(error) => InstructionResponse {
            instruction_id: id,
            error: format!("bundle finalization failed: {error:#}"),
            response: None,
        },
    }
}

async fn split_response(
    id: String,
    request: ProcessBundleSplitRequest,
    runner: &dyn BundleRunner,
) -> InstructionResponse {
    let result = if request.instruction_id.is_empty() {
        Err(anyhow!("ProcessBundleSplit has no bundle instruction id"))
    } else {
        AssertUnwindSafe(runner.split_bundle(&request))
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| Err(anyhow!("executor panicked: {}", panic_message(&*panic))))
    };
    match result {
        Ok(response) => InstructionResponse {
            instruction_id: id,
            error: String::new(),
            response: Some(InstrResp::ProcessBundleSplit(response)),
        },
        Err(error) => InstructionResponse {
            instruction_id: id,
            error: format!("bundle split failed: {error:#}"),
            response: None,
        },
    }
}

async fn process_bundle_response(
    instruction_id: String,
    plan: bundle::BundlePlan,
    runner: Arc<dyn BundleRunner>,
    dispatcher: Arc<Dispatcher>,
) -> InstructionResponse {
    // User transforms can panic inside a bundle future. Keep the control
    // stream alive long enough to return an error for this instruction.
    let result = AssertUnwindSafe(runner.run(&instruction_id, &plan))
        .catch_unwind()
        .await;
    match result {
        Ok(Ok(mut response)) => match dispatcher.register_bundle_metrics(&mut response) {
            Ok(()) => InstructionResponse {
                instruction_id,
                error: String::new(),
                response: Some(InstrResp::ProcessBundle(response)),
            },
            Err(error) => InstructionResponse {
                instruction_id,
                error: format!("bundle monitoring metadata failed: {error}"),
                response: None,
            },
        },
        Ok(Err(error)) => InstructionResponse {
            instruction_id,
            error: format!("executor failed: {error:#}"),
            response: None,
        },
        Err(panic) => InstructionResponse {
            instruction_id,
            error: format!("executor panicked: {}", panic_message(&*panic)),
            response: None,
        },
    }
}

/// Handle returned to the user — they can await `.await` to block until the
/// stream terminates.
pub struct ControlSession {
    join: tokio::task::JoinHandle<Result<()>>,
}

impl ControlSession {
    pub async fn join(self) -> Result<()> {
        match self.join.await {
            Ok(r) => r,
            Err(e) if e.is_cancelled() => Ok(()),
            Err(e) => Err(anyhow!("control task panicked: {e}")),
        }
    }
}

/// Open the Control bidi stream and run the dispatch loop.
///
/// Returns a handle once the stream has been established (or errored during
/// setup). The actual I/O runs in a spawned task that lives as long as the
/// stream.
pub async fn start(
    endpoint: &str,
    dispatcher: Arc<Dispatcher>,
    runner: Option<Arc<dyn BundleRunner>>,
) -> Result<ControlSession> {
    start_for_worker(endpoint, dispatcher, runner, None).await
}

pub async fn start_for_worker(
    endpoint: &str,
    dispatcher: Arc<Dispatcher>,
    runner: Option<Arc<dyn BundleRunner>>,
    worker_id: Option<&str>,
) -> Result<ControlSession> {
    let url = normalize_endpoint(endpoint).context("control endpoint")?;
    let channel = Channel::from_shared(url.clone())
        .context("control channel")?
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .with_context(|| format!("control connect to {url}"))?;

    let (tx, rx) = mpsc::unbounded_channel::<InstructionResponse>();
    let request_stream = UnboundedReceiverStream::new(rx);

    let client = BeamFnControlClient::new(channel);
    // One clone drives the Control bidi stream; another survives for unary
    // GetProcessBundleDescriptor calls inside the loop. tonic clients are
    // cheaply cloneable — they share the channel.
    let mut stream_client = client.clone();
    let mut pbd_client = client;
    let mut response_stream = stream_client
        .control(worker_request(request_stream, worker_id)?)
        .await
        .context("open control stream")?
        .into_inner();

    info!(endpoint = %url, "control stream opened");

    let worker_id = worker_id.map(str::to_owned);
    let cache = dispatcher.pbd_cache.clone();
    let join = tokio::spawn(async move {
        let mut running = JoinSet::new();
        loop {
            let next = tokio::select! {
                next = response_stream.message() => next,
                completed = running.join_next(), if !running.is_empty() => {
                    if let Some(Err(error)) = completed {
                        warn!(?error, "bundle response task failed");
                    }
                    continue;
                }
            };
            match next {
                Ok(Some(req)) => {
                    debug!(instruction_id = %req.instruction_id, "instruction received");

                    // Fetch-on-miss: if this is a ProcessBundle for an uncached
                    // PBD, pull it before dispatching so the Dispatcher can plan.
                    if let Some(InstrReq::ProcessBundle(pbr)) = req.request.as_ref() {
                        let pbd_id = &pbr.process_bundle_descriptor_id;
                        if cache.get(pbd_id).is_none() {
                            match pbd_client
                                .get_process_bundle_descriptor(worker_request(
                                    GetProcessBundleDescriptorRequest {
                                        process_bundle_descriptor_id: pbd_id.clone(),
                                    },
                                    worker_id.as_deref(),
                                )?)
                                .await
                            {
                                Ok(resp) => {
                                    let pbd = resp.into_inner();
                                    debug!(
                                        %pbd_id,
                                        transforms = pbd.transforms.len(),
                                        "PBD fetched"
                                    );
                                    cache.insert(pbd);
                                }
                                Err(status) => warn!(
                                    ?status,
                                    %pbd_id,
                                    "GetProcessBundleDescriptor failed; \
                                     Dispatcher will surface 'unknown pbd_id' to runner"
                                ),
                            }
                        }
                    }

                    // Bundle finalization is an executor callback, not a batch
                    // no-op. ProcessBundle uses the cached PBD; other requests
                    // fall through to the Dispatcher.
                    let mut handled = false;
                    if let (Some(runner), Some(InstrReq::FinalizeBundle(finalize))) =
                        (runner.as_ref(), req.request.as_ref())
                    {
                        let resp = finalize_response(
                            req.instruction_id.clone(),
                            &finalize.instruction_id,
                            runner.as_ref(),
                        )
                        .await;
                        if tx.send(resp).is_err() {
                            error!("response channel closed mid-finalization");
                            break;
                        }
                        handled = true;
                    }
                    if let (Some(runner), Some(InstrReq::ProcessBundle(pbr))) =
                        (runner.as_ref(), req.request.as_ref())
                    {
                        let pbd_id = pbr.process_bundle_descriptor_id.clone();
                        if let Some(pbd) = cache.get(&pbd_id) {
                            match bundle::plan(&pbd) {
                                Ok(plan) => {
                                    let runner = Arc::clone(runner);
                                    let dispatcher = Arc::clone(&dispatcher);
                                    let tx = tx.clone();
                                    let instruction_id = req.instruction_id.clone();
                                    running.spawn(async move {
                                        let resp = process_bundle_response(
                                            instruction_id,
                                            plan,
                                            runner,
                                            dispatcher,
                                        )
                                        .await;
                                        if tx.send(resp).is_err() {
                                            error!("response channel closed mid-bundle");
                                        }
                                    });
                                    handled = true;
                                }
                                Err(e) => warn!(?e, "plan failed; falling back to Dispatcher"),
                            }
                        }
                    }
                    if let (Some(runner), Some(InstrReq::ProcessBundleSplit(split))) =
                        (runner.as_ref(), req.request.as_ref())
                    {
                        let runner = Arc::clone(runner);
                        let split = split.clone();
                        let tx = tx.clone();
                        let instruction_id = req.instruction_id.clone();
                        running.spawn(async move {
                            let resp = split_response(instruction_id, split, runner.as_ref()).await;
                            if tx.send(resp).is_err() {
                                error!("response channel closed mid-split");
                            }
                        });
                        handled = true;
                    }

                    if !handled {
                        let resp = dispatcher.handle(req);
                        if tx.send(resp).is_err() {
                            error!("response channel closed; runner disconnected");
                            break;
                        }
                    }
                }
                Ok(None) => {
                    info!("control stream closed by runner");
                    running.abort_all();
                    break;
                }
                Err(status) => {
                    warn!(?status, "control stream error");
                    running.abort_all();
                    return Err(anyhow!("control stream error: {status}"));
                }
            }
        }
        Ok(())
    });

    Ok(ControlSession { join })
}

/// Convenience: unary `GetProcessBundleDescriptor`. Used by the Dispatcher
/// (M2b) to fetch PBDs lazily. Exposed as a free function so callers can
/// construct a short-lived client or reuse the channel.
pub async fn fetch_pbd(
    endpoint: &str,
    pbd_id: &str,
    cache: &PbdCache,
) -> Result<ProcessBundleDescriptor> {
    if let Some(hit) = cache.get(pbd_id) {
        return Ok(hit);
    }

    let url = normalize_endpoint(endpoint).context("control endpoint")?;
    let channel = Channel::from_shared(url)?.connect().await?;
    let mut client = BeamFnControlClient::new(channel);
    let pbd = client
        .get_process_bundle_descriptor(GetProcessBundleDescriptorRequest {
            process_bundle_descriptor_id: pbd_id.to_string(),
        })
        .await?
        .into_inner();
    cache.insert(pbd.clone());
    Ok(pbd)
}

#[cfg(test)]
#[path = "../tests/unit/control__tests.rs"]
mod tests;
