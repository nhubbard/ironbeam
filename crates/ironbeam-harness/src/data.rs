//! `BeamFnData` bidi stream client.
//!
//! The Data service ferries bundle *element* bytes between runner and SDK,
//! independently of the Control service (which carries bundle *instructions*).
//! A single `Data` bidi stream multiplexes every in-flight bundle's input and
//! output by `(instruction_id, transform_id)`. The runner sends `Elements`
//! messages for DataSource transforms; the SDK sends `Elements` messages for
//! DataSink transforms. `is_last=true` closes a given logical stream.
//!
//! ### MVP model (this module)
//! - One open Data stream, created per `DataClient::connect`.
//! - `open_read(instruction_id, transform_id)` returns an unbounded receiver
//!   that yields each `data` byte-slice chunk the runner sends for that key;
//!   when a message with `is_last=true` arrives, the receiver is closed.
//! - `write_chunk(...)` and `close_write(...)` send `Elements.Data` messages
//!   out, batched one-per-call.
//!
//! A single bundle typically opens one read (DataSource) and one write
//! (DataSink), both on the same `DataClient`. Reader registration is atomic
//! with inbound routing because Prism can run source bundles concurrently.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use beam_proto::fn_execution_v1::beam_fn_data_client::BeamFnDataClient;
use beam_proto::fn_execution_v1::{elements::Data as ElemData, Elements};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::endpoints::{normalize_endpoint, worker_request};

type Key = (String, String); // (instruction_id, transform_id)

/// Per-key reader state. `sender` is dropped on `is_last=true`, which closes
/// the corresponding receiver the executor is draining.
struct ReadSlot {
    sender: mpsc::UnboundedSender<Vec<u8>>,
}

/// Chunks that arrived from the runner before `open_read` registered a
/// reader for the key. Dataflow's runner-v2 pushes source data as soon as
/// the Data RPC opens, which may precede the SDK's Control-loop dispatching
/// the matching `ProcessBundle` instruction. `open_read` drains this into
/// the fresh reader's channel.
#[derive(Default)]
struct PendingBuffer {
    chunks: Vec<Vec<u8>>,
    is_last: bool,
}

#[derive(Default)]
struct InboundState {
    readers: HashMap<Key, ReadSlot>,
    pending: HashMap<Key, PendingBuffer>,
}

impl InboundState {
    fn route(&mut self, data: ElemData) {
        let key = (data.instruction_id, data.transform_id);
        if let Some(reader) = self.readers.get(&key) {
            if !data.data.is_empty() {
                let _ = reader.sender.send(data.data);
            }
            if data.is_last {
                self.readers.remove(&key);
            }
        } else {
            let pending = self.pending.entry(key).or_default();
            if !data.data.is_empty() {
                pending.chunks.push(data.data);
            }
            if data.is_last {
                pending.is_last = true;
            }
        }
    }
}

#[derive(Clone)]
pub struct DataClient {
    /// Tx into the outbound gRPC request stream (SDK → runner).
    outbound_tx: mpsc::UnboundedSender<Elements>,
    /// Readers and early chunks share one lock so registration cannot race
    /// with routing a runner chunk into the pending buffer.
    inbound: Arc<Mutex<InboundState>>,
}

impl DataClient {
    /// Dial the Data endpoint and open a bidi stream.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        Self::connect_for_worker(endpoint, None).await
    }

    pub async fn connect_for_worker(endpoint: &str, worker_id: Option<&str>) -> Result<Self> {
        let url = normalize_endpoint(endpoint).context("data endpoint")?;
        let channel = Channel::from_shared(url.clone())
            .context("data channel")?
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .with_context(|| format!("data connect to {url}"))?;

        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<Elements>();
        let request_stream = UnboundedReceiverStream::new(outbound_rx);

        let mut client = BeamFnDataClient::new(channel);
        let inbound = client
            .data(worker_request(request_stream, worker_id)?)
            .await
            .context("open data stream")?
            .into_inner();

        let state = Arc::new(Mutex::new(InboundState::default()));
        tokio::spawn(run_inbound_loop(inbound, Arc::clone(&state)));

        debug!(endpoint = %url, "data stream opened");
        Ok(Self {
            outbound_tx,
            inbound: state,
        })
    }

    /// Register a reader for `(instruction_id, transform_id)`. The returned
    /// `UnboundedReceiver` yields each raw `data` byte-chunk as it arrives
    /// and closes when `is_last=true`. Each chunk is the runner's encoded
    /// concatenation of one-or-more windowed elements — the caller is
    /// responsible for element-boundary parsing (see `coders::decode_windowed_value`).
    pub fn open_read(
        &self,
        instruction_id: impl Into<String>,
        transform_id: impl Into<String>,
    ) -> mpsc::UnboundedReceiver<Vec<u8>> {
        let key = (instruction_id.into(), transform_id.into());
        let (tx, rx) = mpsc::unbounded_channel();

        // Drain anything the inbound loop buffered before the reader was
        // registered. If `is_last` already arrived, skip registering a
        // forwarding reader — `tx` drops and the caller's `rx` sees EOF
        // after the buffered chunks.
        let mut inbound = self.inbound.lock().unwrap();
        let pre_buffered = inbound.pending.remove(&key);
        let already_closed = match pre_buffered {
            Some(buf) => {
                for chunk in buf.chunks {
                    let _ = tx.send(chunk);
                }
                buf.is_last
            }
            None => false,
        };

        if !already_closed {
            inbound.readers.insert(key, ReadSlot { sender: tx });
        }
        rx
    }

    /// Send one outbound chunk for `(instruction_id, transform_id)`.
    pub fn write_chunk(
        &self,
        instruction_id: &str,
        transform_id: &str,
        data: Vec<u8>,
    ) -> Result<()> {
        self.send_data(ElemData {
            instruction_id: instruction_id.to_string(),
            transform_id: transform_id.to_string(),
            data,
            is_last: false,
        })
    }

    /// Send the terminal `is_last=true` message for `(instruction_id, transform_id)`.
    pub fn close_write(&self, instruction_id: &str, transform_id: &str) -> Result<()> {
        self.send_data(ElemData {
            instruction_id: instruction_id.to_string(),
            transform_id: transform_id.to_string(),
            data: Vec::new(),
            is_last: true,
        })
    }

    fn send_data(&self, d: ElemData) -> Result<()> {
        self.outbound_tx
            .send(Elements {
                data: vec![d],
                timers: Vec::new(),
            })
            .map_err(|_| anyhow!("data outbound channel closed"))
    }
}

async fn run_inbound_loop(
    mut inbound: tonic::Streaming<Elements>,
    state: Arc<Mutex<InboundState>>,
) {
    loop {
        match inbound.message().await {
            Ok(Some(elems)) => {
                for d in elems.data {
                    // A runner may send before Control dispatch registers
                    // the reader. Registration and routing must be atomic.
                    state.lock().unwrap().route(d);
                }
                // elems.timers: ignored for MVP (no user timers).
            }
            Ok(None) => {
                debug!("data stream closed by runner");
                break;
            }
            Err(status) => {
                warn!(?status, "data stream error");
                break;
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/data__tests.rs"]
mod tests;
