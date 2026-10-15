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
//! - `open_timer_read(...)` routes encoded timer-family chunks independently
//!   of element chunks, including those received before reader registration.
//! - `write_chunk(...)` and `write_timer_chunk(...)` send element or timer
//!   messages out, batched one-per-call. Declared-family callbacks consume
//!   these streams, including timer-only bundles.
//!
//! A single bundle typically opens one read (DataSource) and one write
//! (DataSink), both on the same `DataClient`. Reader registration is atomic
//! with inbound routing because Prism can run source bundles concurrently.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::timer_coder::TimerRecord;
use anyhow::{anyhow, Context, Result};
use beam_proto::fn_execution_v1::beam_fn_data_client::BeamFnDataClient;
use beam_proto::fn_execution_v1::{
    elements::{Data as ElemData, Timers as ElemTimers},
    Elements,
};
use futures_util::StreamExt;
use tokio::sync::{mpsc, Notify};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;
use tracing::{debug, warn};

use crate::endpoints::{normalize_endpoint, worker_request};

mod timer_dispatch;
pub use timer_dispatch::TimerFamilyScope;

type Key = (String, String); // (instruction_id, transform_id)
type TimerKey = (String, String, String); // (instruction_id, transform_id, timer_family_id)
const MAX_REPLAY_OUTPUT_BYTES: usize = 64 * 1024 * 1024;

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
    timer_readers: HashMap<TimerKey, ReadSlot>,
    timer_pending: HashMap<TimerKey, PendingBuffer>,
}

/// Count messages handed to the Fn Data transport before acknowledging a
/// ProcessBundle on the separate Control stream.
struct OutboundProgress {
    enqueued: Mutex<u64>,
    consumed: AtomicU64,
    changed: Notify,
}

impl Default for OutboundProgress {
    fn default() -> Self {
        Self {
            enqueued: Mutex::new(0),
            consumed: AtomicU64::new(0),
            changed: Notify::new(),
        }
    }
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

    fn route_timer(&mut self, timer: ElemTimers) {
        let key = (
            timer.instruction_id,
            timer.transform_id,
            timer.timer_family_id,
        );
        if let Some(reader) = self.timer_readers.get(&key) {
            if !timer.timers.is_empty() {
                let _ = reader.sender.send(timer.timers);
            }
            if timer.is_last {
                self.timer_readers.remove(&key);
            }
        } else {
            let pending = self.timer_pending.entry(key).or_default();
            if !timer.timers.is_empty() {
                pending.chunks.push(timer.timers);
            }
            if timer.is_last {
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
    replay: bool,
    replay_output_budget: Option<(Arc<AtomicUsize>, usize)>,
    outbound_progress: Option<Arc<OutboundProgress>>,
}

impl DataClient {
    /// Construct an isolated Fn Data client for one replayable bundle attempt.
    /// Outbound chunks remain local until the supervisor accepts the attempt.
    #[cfg(test)]
    pub(crate) fn for_replay(
        instruction_id: &str,
        source_id: &str,
        chunks: Vec<Vec<u8>>,
    ) -> (Self, mpsc::UnboundedReceiver<Elements>) {
        Self::for_replay_with_timers(instruction_id, source_id, chunks, Vec::new())
    }

    pub(crate) fn for_replay_with_timers(
        instruction_id: &str,
        source_id: &str,
        chunks: Vec<Vec<u8>>,
        timers: Vec<ElemTimers>,
    ) -> (Self, mpsc::UnboundedReceiver<Elements>) {
        Self::for_replay_with_limit(
            instruction_id,
            source_id,
            chunks,
            timers,
            MAX_REPLAY_OUTPUT_BYTES,
        )
    }

    fn for_replay_with_limit(
        instruction_id: &str,
        source_id: &str,
        chunks: Vec<Vec<u8>>,
        timers: Vec<ElemTimers>,
        output_limit: usize,
    ) -> (Self, mpsc::UnboundedReceiver<Elements>) {
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        let mut inbound = InboundState::default();
        inbound.pending.insert(
            (instruction_id.to_owned(), source_id.to_owned()),
            PendingBuffer {
                chunks,
                is_last: true,
            },
        );
        for timer in timers {
            inbound.route_timer(timer);
        }
        (
            Self {
                outbound_tx,
                inbound: Arc::new(Mutex::new(inbound)),
                replay: true,
                replay_output_budget: Some((Arc::new(AtomicUsize::new(0)), output_limit)),
                outbound_progress: None,
            },
            outbound_rx,
        )
    }

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
        let progress = Arc::new(OutboundProgress::default());
        let transport_progress = Arc::clone(&progress);
        let request_stream = UnboundedReceiverStream::new(outbound_rx).inspect(move |_| {
            transport_progress.consumed.fetch_add(1, Ordering::Release);
            transport_progress.changed.notify_waiters();
        });

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
            replay: false,
            replay_output_budget: None,
            outbound_progress: Some(progress),
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

    /// Register a byte-stream reader for one timer family in one bundle.
    /// Timers received before registration are replayed in arrival order.
    pub fn open_timer_read(
        &self,
        instruction_id: impl Into<String>,
        transform_id: impl Into<String>,
        timer_family_id: impl Into<String>,
    ) -> mpsc::UnboundedReceiver<Vec<u8>> {
        let key = (
            instruction_id.into(),
            transform_id.into(),
            timer_family_id.into(),
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let mut inbound = self.inbound.lock().unwrap();
        let pre_buffered = inbound.timer_pending.remove(&key);
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
            inbound.timer_readers.insert(key, ReadSlot { sender: tx });
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
        })?;
        if self.replay && cfg!(debug_assertions) {
            if let Some(path) = std::env::var_os("IRONBEAM_TEST_KILL_AFTER_REPLAY_OUTPUT") {
                use std::io::Write;
                if let Ok(mut witness) = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                {
                    let _ = writeln!(witness, "{}", std::process::id());
                    let _ = witness.sync_all();
                    std::process::exit(87);
                }
            }
        }
        Ok(())
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

    /// Send encoded timer-family bytes to the runner for one bundle.
    pub fn write_timer_chunk(
        &self,
        instruction_id: &str,
        transform_id: &str,
        timer_family_id: &str,
        timers: Vec<u8>,
    ) -> Result<()> {
        self.send_timer(ElemTimers {
            instruction_id: instruction_id.into(),
            transform_id: transform_id.into(),
            timer_family_id: timer_family_id.into(),
            timers,
            is_last: false,
        })
    }

    /// Encode and send one Beam timer record to its declared family stream.
    pub fn write_timer_record(
        &self,
        instruction_id: &str,
        transform_id: &str,
        timer_family_id: &str,
        record: &TimerRecord,
    ) -> Result<()> {
        let mut bytes = Vec::new();
        record.encode(&mut bytes)?;
        self.write_timer_chunk(instruction_id, transform_id, timer_family_id, bytes)
    }

    /// Close an outbound timer-family stream after all timer bytes are sent.
    pub fn close_timer_write(
        &self,
        instruction_id: &str,
        transform_id: &str,
        timer_family_id: &str,
    ) -> Result<()> {
        self.send_timer(ElemTimers {
            instruction_id: instruction_id.into(),
            transform_id: transform_id.into(),
            timer_family_id: timer_family_id.into(),
            timers: Vec::new(),
            is_last: true,
        })
    }

    /// Wait until the outbound transport has consumed every message queued
    /// before this call. Replay clients are drained by their supervisor.
    pub async fn flush_outbound(&self) -> Result<()> {
        let Some(progress) = &self.outbound_progress else {
            return Ok(());
        };
        let target = *progress.enqueued.lock().unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while progress.consumed.load(Ordering::Acquire) < target {
                // A notification can race with subscription. The short
                // timeout rechecks the count without relying on that wakeup.
                let _ =
                    tokio::time::timeout(Duration::from_millis(10), progress.changed.notified())
                        .await;
            }
        })
        .await
        .context("Fn Data outbound stream did not drain before bundle completion")
    }

    fn send_data(&self, d: ElemData) -> Result<()> {
        let payload_len = d.data.len();
        self.send_elements(
            Elements {
                data: vec![d],
                timers: Vec::new(),
            },
            payload_len,
        )
    }

    fn send_timer(&self, timer: ElemTimers) -> Result<()> {
        let payload_len = timer.timers.len();
        self.send_elements(
            Elements {
                data: Vec::new(),
                timers: vec![timer],
            },
            payload_len,
        )
    }

    fn send_elements(&self, elements: Elements, payload_len: usize) -> Result<()> {
        if let Some((used, limit)) = &self.replay_output_budget {
            let mut prior = used.load(Ordering::Relaxed);
            loop {
                let next = prior
                    .checked_add(payload_len)
                    .filter(|next| next <= limit)
                    .ok_or_else(|| anyhow!("supervised bundle output exceeds replay limit"))?;
                match used.compare_exchange_weak(prior, next, Ordering::Relaxed, Ordering::Relaxed)
                {
                    Ok(_) => break,
                    Err(current) => prior = current,
                }
            }
        }
        let mut enqueued = self
            .outbound_progress
            .as_ref()
            .map(|progress| progress.enqueued.lock().unwrap());
        self.outbound_tx
            .send(elements)
            .map_err(|_| anyhow!("data outbound channel closed"))?;
        if let Some(count) = &mut enqueued {
            **count += 1;
        }
        Ok(())
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
                for timer in elems.timers {
                    state.lock().unwrap().route_timer(timer);
                }
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
