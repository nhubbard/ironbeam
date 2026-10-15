//! Bounded Parquet file sources and deferred sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

mod implementation;
pub use implementation::*;

#[cfg(feature = "cloud")]
mod cloud_io;
#[cfg(feature = "cloud")]
pub use cloud_io::cloud_parquet_uri_source;
