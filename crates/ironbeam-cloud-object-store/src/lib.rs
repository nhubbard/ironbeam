//! Opt-in hosted object storage implementations for Ironbeam's `ObjectIO`.
//!
//! The adapter uses the same provider-neutral API for S3, GCS, and Azure Blob.
//! Cloud clients are built lazily per bucket/container from their environment
//! credentials. The underlying async client runs on a dedicated runtime so
//! synchronous Ironbeam sources can call it from a runner thread.

mod implementation;
pub use implementation::*;
