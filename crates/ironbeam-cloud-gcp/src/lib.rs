//! Google Cloud clients for Ironbeam 5 applications.
//!
//! Clients use Google Cloud Application Default Credentials. Each service is
//! gated by a Cargo feature so applications compile only the clients they use.

mod project;
pub use project::{GcpError, GcpProject};

#[cfg(feature = "storage")]
mod storage;
#[cfg(feature = "storage")]
pub use storage::GcpStorage;

#[cfg(feature = "pubsub")]
mod pubsub;
#[cfg(feature = "pubsub")]
pub use pubsub::GcpPubSub;

#[cfg(feature = "bigquery")]
mod bigquery;
#[cfg(feature = "bigquery")]
pub use bigquery::GcpBigQuery;

#[cfg(feature = "secrets")]
mod secrets;
#[cfg(feature = "secrets")]
pub use secrets::GcpSecrets;
