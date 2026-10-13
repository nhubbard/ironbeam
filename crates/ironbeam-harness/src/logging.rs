//! Best-effort logger that forwards messages to the runner's `BeamFnLogging`
//! service plus a `tracing_subscriber::Layer` that tees every `tracing`
//! event into the same stream. On Dataflow, both paths land in the worker's
//! Cloud Logging view so operators see user `tracing::info!` output next to
//! harness-emitted messages — no separate stdout log to hunt down.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result};
use beam_proto::fn_execution_v1::beam_fn_logging_client::BeamFnLoggingClient;
use beam_proto::fn_execution_v1::{log_entry, LogEntry};
use prost_types::Timestamp;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::{debug, warn, Event, Level, Subscriber};
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

use crate::endpoints::{normalize_endpoint, worker_request};

/// Global BeamFnLogging handle used by [`BeamFnLoggingLayer`]. Installed by
/// the harness once it connects to the runner's logging endpoint.
static LOGGING_SINK: OnceLock<LoggingClient> = OnceLock::new();

/// Pre-connect events emitted while buffering is enabled (harness startup).
static LOG_BUFFER: Mutex<Vec<LogEntry>> = Mutex::new(Vec::new());
static BUFFER_ENABLED: AtomicBool = AtomicBool::new(false);

/// Install the global tracing subscriber. Idempotent — safe to call from the
/// user's `main`, [`crate::serve`], or `ironbeam_dataflow::run_v5_from_env`.
///
/// Both the fmt layer and [`BeamFnLoggingLayer`] share the same `RUST_LOG`
/// filter (default `info`). Without that filter on the remote layer, tonic /
/// hyper / h2 `debug!` callsites stay enabled and flood BeamFnLogging — the
/// Dataflow worker throttles and eventually SIGTERM's the SDK harness.
pub fn init_tracing() {
    let env_filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());

    let _ = tracing_subscriber::registry()
        .with(BeamFnLoggingLayer::new().with_filter(env_filter.clone()))
        .with(tracing_subscriber::fmt::layer().with_filter(env_filter))
        .try_init();
}

/// Buffer `tracing` events for BeamFnLogging until [`install_tracing_sink`]
/// connects. Call at harness entry (before the first lifecycle `info!`) so
/// startup messages are not lost. No-op in driver mode if never enabled.
pub fn enable_log_buffering() {
    BUFFER_ENABLED.store(true, Ordering::Relaxed);
}

/// Install a connected [`LoggingClient`] as the sink for all tracing events
/// routed through [`BeamFnLoggingLayer`], flushing any buffered startup
/// events. Idempotent: subsequent calls are silently ignored (one harness
/// process, one sink).
pub fn install_tracing_sink(client: LoggingClient) {
    if LOGGING_SINK.set(client.clone()).is_ok() {
        let buffered = LOG_BUFFER
            .lock()
            .map(|mut buf| std::mem::take(&mut *buf))
            .unwrap_or_default();
        for entry in buffered {
            client.send(entry);
        }
    }
}

#[derive(Clone)]
pub struct LoggingClient {
    tx: mpsc::UnboundedSender<LogEntry>,
}

impl LoggingClient {
    /// Spawn a background task that dials the logging endpoint and forwards
    /// every `LogEntry` sent on the returned handle's channel. On connection
    /// failure, logs are dropped locally — the harness continues.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        Self::connect_for_worker(endpoint, None).await
    }

    pub async fn connect_for_worker(endpoint: &str, worker_id: Option<&str>) -> Result<Self> {
        let url = normalize_endpoint(endpoint).context("logging endpoint")?;
        let channel = Channel::from_shared(url.clone())
            .context("logging channel")?
            .connect()
            .await
            .context("logging connect")?;

        let (tx, rx) = mpsc::unbounded_channel::<LogEntry>();

        let worker_id = worker_id.map(str::to_owned);
        tokio::spawn(async move {
            let stream = batched_log_stream(rx);
            let mut client = BeamFnLoggingClient::new(channel);
            let request = match worker_request(stream, worker_id.as_deref()) {
                Ok(request) => request,
                Err(error) => {
                    warn!(?error, "invalid logging worker metadata");
                    return;
                }
            };
            match client.logging(request).await {
                Ok(mut resp) => {
                    while let Ok(Some(_control)) = resp.get_mut().message().await {
                        // Runner may send LogControl to throttle; we ignore.
                    }
                }
                Err(e) => warn!(error=?e, "logging stream broke"),
            }
            debug!("logging client exited");
        });

        Ok(Self { tx })
    }

    pub fn send(&self, entry: LogEntry) {
        let _ = self.tx.send(entry);
    }

    pub fn info(&self, message: impl Into<String>) {
        self.send(LogEntry {
            severity: log_entry::severity::Enum::Info as i32,
            message: message.into(),
            ..Default::default()
        });
    }

    pub fn error(&self, message: impl Into<String>) {
        self.send(LogEntry {
            severity: log_entry::severity::Enum::Error as i32,
            message: message.into(),
            ..Default::default()
        });
    }
}

/// Batch outbound log entries so we emit one `LogEntry.List` per gRPC message
/// instead of one message per line — the proto expects batching and the
/// Dataflow worker rate-limits per stream message.
fn batched_log_stream(
    mut rx: mpsc::UnboundedReceiver<LogEntry>,
) -> impl tokio_stream::Stream<Item = beam_proto::fn_execution_v1::log_entry::List> {
    async_stream::stream! {
        const MAX_BATCH: usize = 32;
        let flush_every = Duration::from_millis(100);
        let mut batch = Vec::with_capacity(MAX_BATCH);
        let mut flush_at = tokio::time::Instant::now() + flush_every;

        loop {
            let sleep = tokio::time::sleep_until(flush_at);
            tokio::pin!(sleep);
            tokio::select! {
                entry = rx.recv() => {
                    match entry {
                        Some(entry) => {
                            batch.push(entry);
                            if batch.len() >= MAX_BATCH {
                                yield beam_proto::fn_execution_v1::log_entry::List {
                                    log_entries: std::mem::take(&mut batch),
                                };
                                flush_at = tokio::time::Instant::now() + flush_every;
                            }
                        }
                        None => {
                            if !batch.is_empty() {
                                yield beam_proto::fn_execution_v1::log_entry::List {
                                    log_entries: batch,
                                };
                            }
                            break;
                        }
                    }
                }
                _ = &mut sleep => {
                    if !batch.is_empty() {
                        yield beam_proto::fn_execution_v1::log_entry::List {
                            log_entries: std::mem::take(&mut batch),
                        };
                    }
                    flush_at = tokio::time::Instant::now() + flush_every;
                }
            }
        }
    }
}

// ---- tracing bridge ------------------------------------------------------

/// `tracing_subscriber::Layer` that forwards every `tracing::Event` into
/// the BeamFnLogging stream. Events emitted before [`install_tracing_sink`]
/// are buffered when [`enable_log_buffering`] has been called (harness
/// startup); otherwise the layer is a no-op for the remote sink. The fmt
/// layer still prints to stderr for local runs.
#[derive(Debug, Default, Clone, Copy)]
pub struct BeamFnLoggingLayer;

impl BeamFnLoggingLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S: Subscriber> Layer<S> for BeamFnLoggingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        forward_event(event);
    }
}

fn forward_event(event: &Event<'_>) {
    let Some(entry) = event_to_log_entry(event) else {
        return;
    };
    if let Some(client) = LOGGING_SINK.get() {
        client.send(entry);
    } else if BUFFER_ENABLED.load(Ordering::Relaxed) {
        if let Ok(mut buf) = LOG_BUFFER.lock() {
            buf.push(entry);
        }
    }
}

fn event_to_log_entry(event: &Event<'_>) -> Option<LogEntry> {
    let meta = event.metadata();
    let mut msg = String::new();
    let mut visitor = MessageVisitor { buf: &mut msg };
    event.record(&mut visitor);
    if msg.is_empty() {
        return None;
    }
    Some(LogEntry {
        severity: level_to_severity(meta.level()) as i32,
        timestamp: Some(now_timestamp()),
        message: msg,
        log_location: format!("{}:{}", meta.target(), meta.line().unwrap_or(0)),
        ..Default::default()
    })
}

fn level_to_severity(level: &Level) -> log_entry::severity::Enum {
    match *level {
        Level::ERROR => log_entry::severity::Enum::Error,
        Level::WARN => log_entry::severity::Enum::Warn,
        Level::INFO => log_entry::severity::Enum::Info,
        Level::DEBUG => log_entry::severity::Enum::Debug,
        Level::TRACE => log_entry::severity::Enum::Trace,
    }
}

fn now_timestamp() -> Timestamp {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        },
        Err(_) => Timestamp::default(),
    }
}

/// Stringifies a tracing event by concatenating its `message` field and any
/// structured fields as `name=value` pairs. Keeps the output compact —
/// BeamFnLogging already carries level and timestamp separately.
struct MessageVisitor<'a> {
    buf: &'a mut String,
}

impl<'a> tracing::field::Visit for MessageVisitor<'a> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        if field.name() == "message" {
            let _ = write!(self.buf, "{value:?}");
        } else {
            if !self.buf.is_empty() {
                self.buf.push(' ');
            }
            let _ = write!(self.buf, "{}={:?}", field.name(), value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        use std::fmt::Write;
        if field.name() == "message" {
            self.buf.push_str(value);
        } else {
            if !self.buf.is_empty() {
                self.buf.push(' ');
            }
            let _ = write!(self.buf, "{}={:?}", field.name(), value);
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/logging__tracing_bridge_tests.rs"]
mod tracing_bridge_tests;
