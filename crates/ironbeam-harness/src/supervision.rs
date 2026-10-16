//! Stable Fn Control ownership with isolated, replayable bundle attempts.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use beam_proto::fn_execution_v1::{
    elements::Timers as ElemTimers, ProcessBundleDescriptor, ProcessBundleResponse,
    ProcessBundleSplitRequest, ProcessBundleSplitResponse,
};
use beam_proto::pipeline_v1::ParDoPayload;
use ironbeam_core::{Operation, Pipeline, Replay};
use prost::Message;
use tokio::process::Command;
use tokio::sync::{oneshot, watch, Mutex as AsyncMutex};

use crate::bundle::{self, BundlePlan, SdfStage, Step};
use crate::data::DataClient;
use crate::dispatch::PbdCache;
use crate::endpoints::normalize_endpoint;
use crate::endpoints::Endpoints;
use crate::executor::BundleRunner;
use crate::v5_executor::V5BundleExecutor;

const MAX_REPLAY_INPUT_BYTES: usize = 64 * 1024 * 1024;
const MAX_ATTEMPTS: usize = 2;
const CHILD_REQUEST: &str = "IRONBEAM_V5_CHILD_REQUEST";
const CHILD_RESPONSE: &str = "IRONBEAM_V5_CHILD_RESPONSE";
const SPLIT_SIGNAL: &str = "IRONBEAM_V5_SPLIT_SIGNAL";

#[derive(Clone)]
enum SplitOutcome {
    Pending,
    Complete(Option<ProcessBundleSplitResponse>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SplitMode {
    Unset,
    Checkpoint,
    Fractional,
}

struct ActiveBundle {
    source_id: String,
    signal: PathBuf,
    request: PathBuf,
    fractional_eligible: bool,
    split_mode: Mutex<SplitMode>,
    outcome: watch::Sender<SplitOutcome>,
    // The parent holds its bundle response until the split caller accepts the roots.
    acknowledgment: Mutex<Option<oneshot::Receiver<()>>>,
}

fn route_split_roots(
    response: &mut ProcessBundleResponse,
    split: Option<ProcessBundleSplitResponse>,
    waiting_for_split_response: bool,
) -> Option<ProcessBundleSplitResponse> {
    if waiting_for_split_response {
        return split;
    }
    if let Some(split) = split {
        // A canceled split request must not strand the residual in the child.
        response.residual_roots.extend(split.residual_roots);
    }
    None
}

#[derive(Clone, PartialEq, Message)]
struct ReplayRequest {
    #[prost(string, tag = "1")]
    instruction_id: String,
    #[prost(bytes, tag = "2")]
    pbd: Vec<u8>,
    #[prost(bytes, repeated, tag = "3")]
    input_chunks: Vec<Vec<u8>>,
    #[prost(bool, tag = "4")]
    finalize: bool,
    #[prost(message, repeated, tag = "5")]
    timer_inputs: Vec<ElemTimers>,
}

#[derive(Clone, PartialEq, Message)]
struct ReplayOutput {
    #[prost(string, tag = "1")]
    transform_id: String,
    #[prost(bytes, tag = "2")]
    data: Vec<u8>,
    #[prost(bool, tag = "3")]
    is_last: bool,
    #[prost(string, tag = "4")]
    timer_family_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct ReplayResult {
    #[prost(bytes, tag = "1")]
    response: Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    outputs: Vec<ReplayOutput>,
    #[prost(string, tag = "3")]
    error: String,
    #[prost(message, optional, tag = "4")]
    split_response: Option<ProcessBundleSplitResponse>,
}

fn declared_timer_families(
    plan: &BundlePlan,
    pbd: &ProcessBundleDescriptor,
) -> Result<Vec<(String, String)>> {
    let mut families = Vec::new();
    for step in &plan.steps {
        let Step::V5ParDo {
            transform_id,
            stateful_value: true,
            ..
        } = step
        else {
            continue;
        };
        let transform = pbd
            .transforms
            .get(transform_id)
            .ok_or_else(|| anyhow!("missing timed transform {transform_id}"))?;
        let spec = transform
            .spec
            .as_ref()
            .ok_or_else(|| anyhow!("timed transform {transform_id} has no spec"))?;
        let payload = ParDoPayload::decode(spec.payload.as_slice())
            .with_context(|| format!("decode timed transform {transform_id}"))?;
        for family_id in payload.timer_family_specs.keys() {
            families.push((transform_id.clone(), family_id.clone()));
        }
    }
    families.sort();
    families.dedup();
    Ok(families)
}

/// The supervisor owns the Prism Control stream. Its child executes one
/// bundle, and a failed child can be replaced before Prism sees a response.
pub(crate) struct SupervisedV5Runner {
    cache: PbdCache,
    worker_id: String,
    executable: PathBuf,
    args: Vec<OsString>,
    user_sdf_keys: HashSet<String>,
    active: Mutex<HashMap<String, Arc<ActiveBundle>>>,
    data_clients: AsyncMutex<HashMap<String, DataClient>>,
}

impl SupervisedV5Runner {
    pub(crate) fn new(cache: PbdCache, worker_id: String, pipeline: &Pipeline) -> Result<Self> {
        let (graph, _) = pipeline.snapshot()?;
        let user_sdf_keys = graph
            .nodes
            .iter()
            .filter(|node| matches!(node.operation, Operation::SplittableParDo))
            .map(|node| node.key.clone())
            .collect();
        Ok(Self {
            cache,
            worker_id,
            executable: std::env::current_exe().context("supervised worker executable")?,
            args: std::env::args_os().skip(1).collect(),
            user_sdf_keys,
            active: Mutex::new(HashMap::new()),
            data_clients: AsyncMutex::new(HashMap::new()),
        })
    }

    async fn data_client(&self, endpoint: &str) -> Result<DataClient> {
        let url = normalize_endpoint(endpoint)?;
        let mut clients = self.data_clients.lock().await;
        if let Some(client) = clients.get(&url) {
            return Ok(client.clone());
        }
        let client = DataClient::connect_for_worker(&url, Some(&self.worker_id)).await?;
        clients.insert(url, client.clone());
        Ok(client)
    }

    async fn execute_child(
        &self,
        request: &ReplayRequest,
        split_signal: Option<&Path>,
    ) -> Result<ReplayResult> {
        let directory = tempfile::tempdir().context("create bundle attempt directory")?;
        let request_path = directory.path().join("request.pb");
        let response_path = directory.path().join("response.pb");
        std::fs::write(&request_path, request.encode_to_vec())?;
        let mut command = Command::new(&self.executable);
        command
            .args(&self.args)
            .env(CHILD_REQUEST, &request_path)
            .env(CHILD_RESPONSE, &response_path)
            .kill_on_drop(true);
        if let Some(split_signal) = split_signal {
            command.env(SPLIT_SIGNAL, split_signal);
        }
        let mut child = command.spawn().context("spawn bundle execution child")?;
        let kill_paths = if request.finalize {
            None
        } else {
            std::env::var_os("IRONBEAM_V5_SUPERVISOR_TRIGGER")
                .zip(std::env::var_os("IRONBEAM_V5_SUPERVISOR_KILLED"))
                .map(|(trigger, killed)| (PathBuf::from(trigger), PathBuf::from(killed)))
        };
        let status = if let Some((trigger, killed)) = kill_paths {
            loop {
                tokio::select! {
                    status = child.wait() => break status?,
                    () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                        if trigger.exists()
                            && !killed.exists()
                            && std::fs::read_to_string(&trigger)
                                .ok()
                                .and_then(|value| value.parse::<u32>().ok())
                                == child.id()
                        {
                            let pid = child.id().ok_or_else(|| anyhow!("bundle child has no PID"))?;
                            child.kill().await.context("kill selected bundle child")?;
                            let temporary = killed.with_extension("pending");
                            std::fs::write(&temporary, pid.to_string())?;
                            std::fs::rename(temporary, killed)?;
                            bail!("bundle execution child {pid} was deliberately killed");
                        }
                    }
                }
            }
        } else {
            child.wait().await?
        };
        if !status.success() {
            bail!("bundle execution child exited with {status}");
        }
        let bytes = std::fs::read(&response_path).context("read bundle child result")?;
        ReplayResult::decode(bytes.as_slice()).context("decode bundle child result")
    }

    async fn run_replay(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
    ) -> Result<ProcessBundleResponse> {
        let pbd = self
            .cache
            .get(&plan.pbd_id)
            .ok_or_else(|| anyhow!("supervisor has no PBD {}", plan.pbd_id))?;
        let (source_id, source_port) = plan
            .steps
            .iter()
            .find_map(|step| match step {
                Step::Source {
                    transform_id, port, ..
                } => Some((transform_id, port)),
                _ => None,
            })
            .ok_or_else(|| anyhow!("supervised bundle has no runner source"))?;
        let source_endpoint = source_port
            .api_service_descriptor
            .as_ref()
            .ok_or_else(|| anyhow!("source has no Fn Data endpoint"))?
            .url
            .as_str();
        let source_client = self.data_client(source_endpoint).await?;
        let directory = tempfile::tempdir().context("create split signal directory")?;
        let split_signal = directory.path().join("split");
        let (outcome, _) = watch::channel(SplitOutcome::Pending);
        let active_bundle = Arc::new(ActiveBundle {
            source_id: source_id.clone(),
            request: directory.path().join("split-request.pb"),
            signal: split_signal.clone(),
            fractional_eligible: plan.steps.iter().any(|step| {
                matches!(step,
                    Step::Sdf { stage: SdfStage::ProcessSizedElements, node_ref, .. }
                    if self.user_sdf_keys.contains(&node_ref.key)
                )
            }),
            split_mode: Mutex::new(SplitMode::Unset),
            outcome,
            acknowledgment: Mutex::new(None),
        });
        self.active
            .lock()
            .map_err(|_| anyhow!("supervised active bundle registry poisoned"))?
            .insert(instruction_id.into(), Arc::clone(&active_bundle));
        let result = self
            .run_registered_bundle(
                instruction_id,
                plan,
                &pbd,
                &source_client,
                source_id,
                &split_signal,
            )
            .await;
        let result = match result {
            Ok((mut response, split)) => {
                let acknowledgment = active_bundle
                    .acknowledgment
                    .lock()
                    .ok()
                    .and_then(|mut acknowledgment| acknowledgment.take());
                let split = match (split, acknowledgment) {
                    (Some(split), Some(acknowledgment)) => {
                        // Only a successful child can release split roots. Wait for
                        // the Control reply path to take them before returning the
                        // ProcessBundle response.
                        active_bundle
                            .outcome
                            .send_replace(SplitOutcome::Complete(Some(split.clone())));
                        let delivered = acknowledgment.await.is_ok();
                        route_split_roots(&mut response, Some(split), delivered)
                    }
                    (split, _) => {
                        active_bundle
                            .outcome
                            .send_replace(SplitOutcome::Complete(None));
                        route_split_roots(&mut response, split, false)
                    }
                };
                Ok((response, split))
            }
            Err(error) => {
                active_bundle
                    .outcome
                    .send_replace(SplitOutcome::Complete(None));
                Err(error)
            }
        };
        self.active
            .lock()
            .ok()
            .map(|mut active| active.remove(instruction_id));
        result.map(|(response, _)| response)
    }

    async fn run_registered_bundle(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
        pbd: &ProcessBundleDescriptor,
        source_client: &DataClient,
        source_id: &str,
        split_signal: &Path,
    ) -> Result<(ProcessBundleResponse, Option<ProcessBundleSplitResponse>)> {
        let mut reader = source_client.open_read(instruction_id, source_id);
        let mut input_chunks = Vec::new();
        let mut input_bytes = 0_usize;
        while let Some(chunk) = reader.recv().await {
            input_bytes = input_bytes
                .checked_add(chunk.len())
                .ok_or_else(|| anyhow!("supervised bundle input length overflow"))?;
            if input_bytes > MAX_REPLAY_INPUT_BYTES {
                bail!("supervised bundle input exceeds 64 MiB replay limit");
            }
            input_chunks.push(chunk);
        }
        let timer_families = declared_timer_families(plan, pbd)?;
        let mut timer_inputs = Vec::new();
        for (transform_id, family_id) in &timer_families {
            let mut reader = source_client.open_timer_read(instruction_id, transform_id, family_id);
            while let Some(chunk) = reader.recv().await {
                input_bytes = input_bytes
                    .checked_add(chunk.len())
                    .ok_or_else(|| anyhow!("supervised bundle input length overflow"))?;
                if input_bytes > MAX_REPLAY_INPUT_BYTES {
                    bail!("supervised bundle input exceeds 64 MiB replay limit");
                }
                timer_inputs.push(ElemTimers {
                    instruction_id: instruction_id.into(),
                    transform_id: transform_id.clone(),
                    timer_family_id: family_id.clone(),
                    timers: chunk,
                    is_last: false,
                });
            }
            timer_inputs.push(ElemTimers {
                instruction_id: instruction_id.into(),
                transform_id: transform_id.clone(),
                timer_family_id: family_id.clone(),
                timers: Vec::new(),
                is_last: true,
            });
        }

        let result = self
            .run_attempts(
                &ReplayRequest {
                    instruction_id: instruction_id.into(),
                    pbd: pbd.encode_to_vec(),
                    input_chunks,
                    finalize: false,
                    timer_inputs,
                },
                split_signal,
            )
            .await;
        let result = result?;
        if !result.error.is_empty() {
            bail!("bundle child failed: {}", result.error);
        }
        let response = ProcessBundleResponse::decode(result.response.as_slice())
            .context("decode child ProcessBundleResponse")?;
        if result
            .split_response
            .as_ref()
            .is_some_and(|split| !split.residual_roots.is_empty())
            && !response.residual_roots.is_empty()
        {
            bail!("supervised child reported the same residual in split and bundle responses");
        }
        let mut clients = HashMap::new();
        for step in &plan.steps {
            if let Step::Sink {
                transform_id, port, ..
            } = step
            {
                let endpoint = port
                    .api_service_descriptor
                    .as_ref()
                    .ok_or_else(|| anyhow!("sink has no Fn Data endpoint"))?
                    .url
                    .as_str();
                clients.insert(transform_id.as_str(), self.data_client(endpoint).await?);
            }
        }
        let mut closed = HashSet::new();
        let allowed_timers = timer_families.into_iter().collect::<HashSet<_>>();
        let mut closed_timers = HashSet::new();
        for output in &result.outputs {
            if !output.timer_family_id.is_empty() {
                let timer = (output.transform_id.clone(), output.timer_family_id.clone());
                if !allowed_timers.contains(&timer) {
                    bail!(
                        "child emitted undeclared timer family {}",
                        output.timer_family_id
                    );
                }
                if closed_timers.contains(&timer) {
                    bail!("child emitted timer data after closing a family");
                }
                if output.is_last {
                    if !output.data.is_empty() {
                        bail!("child closed timer family with a nonempty payload");
                    }
                    closed_timers.insert(timer);
                }
                continue;
            }
            if !clients.contains_key(output.transform_id.as_str()) {
                bail!("child emitted an unknown sink {}", output.transform_id);
            }
            if closed.contains(output.transform_id.as_str()) {
                bail!(
                    "child emitted data after closing sink {}",
                    output.transform_id
                );
            }
            if output.is_last {
                if !output.data.is_empty() {
                    bail!(
                        "child closed sink {} with a nonempty payload",
                        output.transform_id
                    );
                }
                closed.insert(output.transform_id.as_str());
            }
        }
        if closed.len() != clients.len() {
            bail!("child left a runner sink open");
        }
        if closed_timers != allowed_timers {
            bail!("child left a timer family open");
        }
        for output in result.outputs {
            if !output.timer_family_id.is_empty() {
                if output.is_last {
                    source_client.close_timer_write(
                        instruction_id,
                        &output.transform_id,
                        &output.timer_family_id,
                    )?;
                } else {
                    source_client.write_timer_chunk(
                        instruction_id,
                        &output.transform_id,
                        &output.timer_family_id,
                        output.data,
                    )?;
                }
                continue;
            }
            let client = clients
                .get(output.transform_id.as_str())
                .expect("validated runner sink");
            if output.is_last {
                client.close_write(instruction_id, &output.transform_id)?;
            } else {
                client.write_chunk(instruction_id, &output.transform_id, output.data)?;
            }
        }
        for client in clients.values() {
            client.flush_outbound().await?;
        }
        source_client.flush_outbound().await?;
        Ok((response, result.split_response))
    }

    async fn run_attempts(
        &self,
        request: &ReplayRequest,
        split_signal: &Path,
    ) -> Result<ReplayResult> {
        let mut last_error = None;
        for _ in 0..MAX_ATTEMPTS {
            match self.execute_child(request, Some(split_signal)).await {
                Ok(result) => return Ok(result),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("at least one attempt"))
    }
}

pub(crate) fn validate_pipeline(pipeline: &Pipeline) -> Result<()> {
    validate_pipeline_with_object_journal(pipeline, false)
}

pub(crate) fn validate_pipeline_with_object_journal(
    pipeline: &Pipeline,
    has_object_journal: bool,
) -> Result<()> {
    let (graph, registry) = pipeline.snapshot()?;
    let has_journal =
        has_object_journal || std::env::var_os("IRONBEAM_V5_FINALIZATION_JOURNAL_DIR").is_some();
    for node in &graph.nodes {
        match &node.operation {
            Operation::Source { progress } if progress.replay == Replay::None => {
                bail!("supervised source {} has no replay contract", node.key);
            }
            Operation::Sink { spec } => {
                let finalizing = registry.sink_requires_bundle_finalization(&node.key);
                if spec.replay != Replay::Idempotent && !finalizing {
                    bail!("supervised sink {} is not replay-safe", node.key);
                }
                if finalizing && !has_journal {
                    bail!(
                        "supervised sink {} needs a durable finalization journal",
                        node.key
                    );
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[async_trait]
impl BundleRunner for SupervisedV5Runner {
    async fn run(&self, instruction_id: &str, plan: &BundlePlan) -> Result<ProcessBundleResponse> {
        self.run_replay(instruction_id, plan).await
    }

    async fn split_bundle(
        &self,
        request: &ProcessBundleSplitRequest,
    ) -> Result<ProcessBundleSplitResponse> {
        let active = self
            .active
            .lock()
            .map_err(|_| anyhow!("supervised active bundle registry poisoned"))?
            .get(&request.instruction_id)
            .cloned();
        let Some(active) = active else {
            return Ok(ProcessBundleSplitResponse::default());
        };
        let Some(desired) = request.desired_splits.get(&active.source_id) else {
            return Ok(ProcessBundleSplitResponse::default());
        };
        if desired.fraction_of_remainder == 0.0 && desired.allowed_split_points.is_empty() {
            let mut mode = active
                .split_mode
                .lock()
                .map_err(|_| anyhow!("supervised split mode poisoned"))?;
            if *mode != SplitMode::Unset {
                return Ok(ProcessBundleSplitResponse::default());
            }
            std::fs::write(&active.signal, b"split")?;
            *mode = SplitMode::Checkpoint;
            return Ok(ProcessBundleSplitResponse::default());
        }
        if !active.fractional_eligible || !(0.0..1.0).contains(&desired.fraction_of_remainder) {
            return Ok(ProcessBundleSplitResponse::default());
        }
        {
            let mut mode = active
                .split_mode
                .lock()
                .map_err(|_| anyhow!("supervised split mode poisoned"))?;
            if *mode != SplitMode::Unset {
                return Ok(ProcessBundleSplitResponse::default());
            }
            *mode = SplitMode::Fractional;
        }
        let mut outcome = active.outcome.subscribe();
        if matches!(&*outcome.borrow(), SplitOutcome::Complete(_)) {
            return Ok(ProcessBundleSplitResponse::default());
        }
        let (acknowledgment_sender, acknowledgment_receiver) = oneshot::channel();
        *active
            .acknowledgment
            .lock()
            .map_err(|_| anyhow!("supervised split acknowledgment registry poisoned"))? =
            Some(acknowledgment_receiver);
        let mut acknowledgment_sender = Some(acknowledgment_sender);
        let pending = active.request.with_extension("pending");
        if let Err(error) = std::fs::write(&pending, request.encode_to_vec())
            .and_then(|()| std::fs::rename(&pending, &active.request))
        {
            if matches!(&*outcome.borrow(), SplitOutcome::Complete(_)) {
                return Ok(ProcessBundleSplitResponse::default());
            }
            return Err(error.into());
        }
        loop {
            match outcome.borrow_and_update().clone() {
                SplitOutcome::Complete(response) => {
                    if let Some(sender) = acknowledgment_sender.take() {
                        let _ = sender.send(());
                    }
                    return Ok(response.unwrap_or_default());
                }
                SplitOutcome::Pending => {}
            }
            outcome.changed().await?;
        }
    }

    async fn finalize_bundle(&self, instruction_id: &str) -> Result<()> {
        let result = self
            .execute_child(
                &ReplayRequest {
                    instruction_id: instruction_id.into(),
                    pbd: Vec::new(),
                    input_chunks: Vec::new(),
                    finalize: true,
                    timer_inputs: Vec::new(),
                },
                None,
            )
            .await?;
        if result.error.is_empty() {
            Ok(())
        } else {
            bail!("bundle finalization child failed: {}", result.error)
        }
    }
}

async fn monitor_fractional_request(
    runner: Arc<V5BundleExecutor>,
    instruction_id: String,
    request_path: PathBuf,
    stop: Arc<AtomicBool>,
) -> Result<Option<ProcessBundleSplitResponse>> {
    // Atomic file rename is the request boundary between the Control owner
    // and its isolated execution child.
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(None);
        }
        match std::fs::read(&request_path) {
            Ok(bytes) => {
                let request = ProcessBundleSplitRequest::decode(bytes.as_slice())
                    .context("decode supervised split request")?;
                if request.instruction_id != instruction_id {
                    bail!("supervised split request targets another bundle");
                }
                if runner.offset_split_ready(&instruction_id)? {
                    return runner.split_bundle(&request).await.map(Some);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Worker binary entry point for a single isolated execution attempt.
pub(crate) async fn run_child(
    endpoints: &Endpoints,
    pipeline: &ironbeam_core::Pipeline,
    binary_build_id: &str,
    request_path: &Path,
    response_path: &Path,
    object_journal: Option<&(Arc<ironbeam_cloud_objects::ObjectStores>, String)>,
) -> Result<()> {
    let bytes = std::fs::read(request_path).context("read bundle replay request")?;
    let request =
        ReplayRequest::decode(bytes.as_slice()).context("decode bundle replay request")?;
    let runner = Arc::new(if let Some((stores, uri)) = object_journal {
        V5BundleExecutor::new_for_worker_with_object_journal(
            pipeline,
            binary_build_id,
            Some(&endpoints.id),
            Arc::clone(stores),
            uri,
        )?
    } else {
        V5BundleExecutor::new_for_worker(pipeline, binary_build_id, Some(&endpoints.id))?
    });
    let result = if request.finalize {
        match runner.finalize_bundle(&request.instruction_id).await {
            Ok(()) => ReplayResult::default(),
            Err(error) => ReplayResult {
                error: format!("{error:#}"),
                ..Default::default()
            },
        }
    } else {
        let pbd = ProcessBundleDescriptor::decode(request.pbd.as_slice())?;
        let plan = bundle::plan(&pbd)?;
        let source_id = plan
            .steps
            .iter()
            .find_map(|step| match step {
                Step::Source { transform_id, .. } => Some(transform_id.as_str()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("replayed bundle has no source"))?;
        let (client, mut outbound) = DataClient::for_replay_with_timers(
            &request.instruction_id,
            source_id,
            request.input_chunks,
            request.timer_inputs,
        );
        runner.use_replay_client(&plan, client).await?;
        let stop = Arc::new(AtomicBool::new(false));
        let monitor = plan
            .steps
            .iter()
            .any(|step| {
                matches!(
                    step,
                    Step::Sdf {
                        stage: SdfStage::ProcessSizedElements,
                        ..
                    }
                )
            })
            .then(|| std::env::var_os(SPLIT_SIGNAL))
            .flatten()
            .map(|signal| {
                tokio::spawn(monitor_fractional_request(
                    Arc::clone(&runner),
                    request.instruction_id.clone(),
                    PathBuf::from(signal).with_file_name("split-request.pb"),
                    Arc::clone(&stop),
                ))
            });
        let execution = runner.run(&request.instruction_id, &plan).await;
        stop.store(true, Ordering::Release);
        let split_response = match monitor {
            Some(task) => task.await.context("join supervised split monitor")??,
            None => None,
        };
        match execution {
            Ok(response) => {
                let mut outputs = Vec::new();
                while let Ok(elements) = outbound.try_recv() {
                    outputs.extend(elements.data.into_iter().map(|data| ReplayOutput {
                        transform_id: data.transform_id,
                        data: data.data,
                        is_last: data.is_last,
                        timer_family_id: String::new(),
                    }));
                    outputs.extend(elements.timers.into_iter().map(|timer| ReplayOutput {
                        transform_id: timer.transform_id,
                        data: timer.timers,
                        is_last: timer.is_last,
                        timer_family_id: timer.timer_family_id,
                    }));
                }
                ReplayResult {
                    response: response.encode_to_vec(),
                    outputs,
                    error: String::new(),
                    split_response,
                }
            }
            Err(error) => ReplayResult {
                error: format!("{error:#}"),
                ..Default::default()
            },
        }
    };
    let temporary = response_path.with_extension("pending");
    std::fs::write(&temporary, result.encode_to_vec())?;
    std::fs::rename(temporary, response_path)?;
    Ok(())
}

pub(crate) fn child_paths() -> Option<(PathBuf, PathBuf)> {
    Some((
        std::env::var_os(CHILD_REQUEST)?.into(),
        std::env::var_os(CHILD_RESPONSE)?.into(),
    ))
}

/// Read by the SDF poll loop without taking the Control connection down.
pub(crate) fn split_requested() -> bool {
    std::env::var_os(SPLIT_SIGNAL).is_some_and(|path| Path::new(&path).exists())
}

#[cfg(test)]
#[path = "../tests/unit/supervision__tests.rs"]
mod tests;
