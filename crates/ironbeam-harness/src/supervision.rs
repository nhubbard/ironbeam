//! Stable Fn Control ownership with isolated, replayable bundle attempts.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use beam_proto::fn_execution_v1::{
    ProcessBundleDescriptor, ProcessBundleResponse, ProcessBundleSplitRequest,
    ProcessBundleSplitResponse,
};
use ironbeam_core::{Operation, Pipeline, Replay};
use prost::Message;
use tokio::process::Command;
use tokio::sync::Mutex as AsyncMutex;

use crate::bundle::{self, BundlePlan, Step};
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
}

#[derive(Clone, PartialEq, Message)]
struct ReplayOutput {
    #[prost(string, tag = "1")]
    transform_id: String,
    #[prost(bytes, tag = "2")]
    data: Vec<u8>,
    #[prost(bool, tag = "3")]
    is_last: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ReplayResult {
    #[prost(bytes, tag = "1")]
    response: Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    outputs: Vec<ReplayOutput>,
    #[prost(string, tag = "3")]
    error: String,
}

/// The supervisor owns the Prism Control stream. Its child executes one
/// bundle, and a failed child can be replaced before Prism sees a response.
pub(crate) struct SupervisedV5Runner {
    cache: PbdCache,
    worker_id: String,
    executable: PathBuf,
    args: Vec<OsString>,
    active: Mutex<HashMap<String, PathBuf>>,
    data_clients: AsyncMutex<HashMap<String, DataClient>>,
}

impl SupervisedV5Runner {
    pub(crate) fn new(cache: PbdCache, worker_id: String) -> Result<Self> {
        Ok(Self {
            cache,
            worker_id,
            executable: std::env::current_exe().context("supervised worker executable")?,
            args: std::env::args_os().skip(1).collect(),
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
        self.active
            .lock()
            .map_err(|_| anyhow!("supervised active bundle registry poisoned"))?
            .insert(instruction_id.into(), split_signal.clone());
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
        self.active
            .lock()
            .ok()
            .map(|mut active| active.remove(instruction_id));
        result
    }

    async fn run_registered_bundle(
        &self,
        instruction_id: &str,
        plan: &BundlePlan,
        pbd: &ProcessBundleDescriptor,
        source_client: &DataClient,
        source_id: &str,
        split_signal: &Path,
    ) -> Result<ProcessBundleResponse> {
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

        let result = self
            .run_attempts(
                &ReplayRequest {
                    instruction_id: instruction_id.into(),
                    pbd: pbd.encode_to_vec(),
                    input_chunks,
                    finalize: false,
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
        for output in &result.outputs {
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
        for output in result.outputs {
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
        Ok(response)
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
    let (graph, registry) = pipeline.snapshot()?;
    let has_journal = std::env::var_os("IRONBEAM_V5_FINALIZATION_JOURNAL_DIR").is_some();
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
        let path = self
            .active
            .lock()
            .map_err(|_| anyhow!("supervised active bundle registry poisoned"))?
            .get(&request.instruction_id)
            .cloned();
        if let Some(path) = path {
            std::fs::write(path, b"split")?;
        }
        Ok(ProcessBundleSplitResponse::default())
    }

    async fn finalize_bundle(&self, instruction_id: &str) -> Result<()> {
        let result = self
            .execute_child(
                &ReplayRequest {
                    instruction_id: instruction_id.into(),
                    pbd: Vec::new(),
                    input_chunks: Vec::new(),
                    finalize: true,
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

/// Worker binary entry point for a single isolated execution attempt.
pub(crate) async fn run_child(
    endpoints: &Endpoints,
    pipeline: &ironbeam_core::Pipeline,
    binary_build_id: &str,
    request_path: &Path,
    response_path: &Path,
) -> Result<()> {
    let bytes = std::fs::read(request_path).context("read bundle replay request")?;
    let request =
        ReplayRequest::decode(bytes.as_slice()).context("decode bundle replay request")?;
    let runner = V5BundleExecutor::new_for_worker(pipeline, binary_build_id, Some(&endpoints.id))?;
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
        let (client, mut outbound) =
            DataClient::for_replay(&request.instruction_id, source_id, request.input_chunks);
        runner.use_replay_client(&plan, client).await?;
        match runner.run(&request.instruction_id, &plan).await {
            Ok(response) => {
                let mut outputs = Vec::new();
                while let Ok(elements) = outbound.try_recv() {
                    if !elements.timers.is_empty() {
                        bail!("supervised bundle emitted unsupported Fn Data timers");
                    }
                    outputs.extend(elements.data.into_iter().map(|data| ReplayOutput {
                        transform_id: data.transform_id,
                        data: data.data,
                        is_last: data.is_last,
                    }));
                }
                ReplayResult {
                    response: response.encode_to_vec(),
                    outputs,
                    error: String::new(),
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
