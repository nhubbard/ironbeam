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
//! The v5 executor runs supported bounded and unbounded bundles through Beam
//! Fn Data and Fn State. The control loop fetches bundle descriptors and
//! checks the worker graph against the driver graph.

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
pub mod sdf_offset_wire;
pub mod sdf_wire;
mod server;
pub mod state;
mod supervision;
pub mod timer_coder;
pub mod unbounded;
pub mod v5;
pub mod v5_executor;

pub use bundle::{BundlePlan, Step};
pub use dispatch::{Dispatcher, PbdCache};
pub use endpoints::Endpoints;
pub use logging::{
    enable_log_buffering, init_tracing, install_tracing_sink, BeamFnLoggingLayer, LoggingClient,
};
pub use server::{serve, serve_v5, serve_v5_with_object_journal, serve_with_runner};
