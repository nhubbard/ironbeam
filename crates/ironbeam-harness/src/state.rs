//! Beam Fn State client shared by portable side inputs and runner iterables.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use beam_proto::fn_execution_v1::{
    beam_fn_state_client::BeamFnStateClient, state_request, state_response, StateAppendRequest,
    StateClearRequest, StateGetRequest, StateKey, StateRequest, StateResponse,
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;

use crate::endpoints::{normalize_endpoint, worker_request};

mod user;
pub use user::{UserStateAccess, UserStateScope};

#[derive(Clone)]
pub struct StateClient {
    outbound: mpsc::UnboundedSender<StateRequest>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<StateResponse>>>>,
    next_id: Arc<AtomicU64>,
}

// A cancelled bundle must not leave its outstanding State RPC registered until
// the runner responds. This also covers the timeout and closed-stream paths.
struct PendingRequest {
    id: String,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<StateResponse>>>>,
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

impl StateClient {
    pub async fn connect(endpoint: &str, worker_id: Option<&str>) -> Result<Self> {
        let url = normalize_endpoint(endpoint).context("state endpoint")?;
        let channel = Channel::from_shared(url.clone())?
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .with_context(|| format!("state connect to {url}"))?;
        let (outbound, receiver) = mpsc::unbounded_channel();
        let mut client = BeamFnStateClient::new(channel);
        let request = worker_request(UnboundedReceiverStream::new(receiver), worker_id)?;
        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<StateResponse>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let responses = Arc::clone(&pending);
        tokio::spawn(async move {
            // Some runners wait for the first StateRequest before sending RPC
            // headers. Open the bidi RPC concurrently with the first get.
            let Ok(opened) = client.state(request).await else {
                responses.lock().unwrap().clear();
                return;
            };
            let mut inbound = opened.into_inner();
            while let Ok(Some(response)) = inbound.message().await {
                if let Some(sender) = responses.lock().unwrap().remove(&response.id) {
                    let _ = sender.send(response);
                }
            }
            responses.lock().unwrap().clear();
        });
        Ok(Self {
            outbound,
            pending,
            next_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn get_all(&self, instruction_id: &str, state_key: StateKey) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        self.get_pages(instruction_id, state_key, |page| {
            data.extend_from_slice(page);
            Ok(())
        })
        .await?;
        Ok(data)
    }

    /// Read a complete state entry without retaining its value bytes.
    pub async fn drain(&self, instruction_id: &str, state_key: StateKey) -> Result<()> {
        self.get_pages(instruction_id, state_key, |_| Ok(())).await
    }

    /// Append encoded values to a runner-owned state cell in the active bundle.
    pub async fn append(
        &self,
        instruction_id: &str,
        state_key: StateKey,
        data: Vec<u8>,
    ) -> Result<()> {
        let response = self
            .request(
                instruction_id,
                state_key,
                state_request::Request::Append(StateAppendRequest { data }),
            )
            .await?;
        if !matches!(response.response, Some(state_response::Response::Append(_))) {
            bail!("Beam state append returned a non-Append response");
        }
        Ok(())
    }

    /// Clear a runner-owned state cell in the active bundle.
    pub async fn clear(&self, instruction_id: &str, state_key: StateKey) -> Result<()> {
        let response = self
            .request(
                instruction_id,
                state_key,
                state_request::Request::Clear(StateClearRequest {}),
            )
            .await?;
        if !matches!(response.response, Some(state_response::Response::Clear(_))) {
            bail!("Beam state clear returned a non-Clear response");
        }
        Ok(())
    }

    async fn request(
        &self,
        instruction_id: &str,
        state_key: StateKey,
        request: state_request::Request,
    ) -> Result<StateResponse> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().unwrap().insert(id.clone(), sender);
        let _pending_request = PendingRequest {
            id: id.clone(),
            pending: Arc::clone(&self.pending),
        };
        let request = StateRequest {
            id: id.clone(),
            instruction_id: instruction_id.into(),
            state_key: Some(state_key),
            request: Some(request),
        };
        if self.outbound.send(request).is_err() {
            bail!("Beam state stream closed before request {id}");
        }
        let response = match tokio::time::timeout(Duration::from_secs(60), receiver).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => bail!("Beam state stream closed before response {id}"),
            Err(_) => bail!("Beam state request {id} timed out"),
        };
        if !response.error.is_empty() {
            bail!("Beam state request {id} failed: {}", response.error);
        }
        Ok(response)
    }

    async fn get_pages(
        &self,
        instruction_id: &str,
        state_key: StateKey,
        mut visit: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        let mut continuation_token = Vec::new();
        let mut seen = HashSet::new();
        loop {
            let response = self
                .request(
                    instruction_id,
                    state_key.clone(),
                    state_request::Request::Get(StateGetRequest { continuation_token }),
                )
                .await?;
            let Some(state_response::Response::Get(get)) = response.response else {
                bail!("Beam state request returned a non-Get response");
            };
            visit(&get.data)?;
            continuation_token = get.continuation_token;
            if continuation_token.is_empty() {
                return Ok(());
            }
            if !seen.insert(continuation_token.clone()) {
                bail!("Beam state request repeated a continuation token");
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/state__tests.rs"]
mod tests;
