#![cfg(any(feature = "file", feature = "cloud"))]
#![allow(clippy::missing_errors_doc)]
//! Bounded file adapters for the v5 logical graph.
//! Sources bind their input bytes at graph construction. Final-result sinks write
//! to a sibling temporary file and publish only after the whole job succeeds.
#[cfg(feature = "file")]
pub mod final_result;
#[cfg(feature = "cloud")]
pub mod object_final;
#[cfg(feature = "file")]
pub mod sequence_final;

mod implementation;
pub use implementation::*;
pub(crate) use implementation::{create_dir_all_durable, sync_directory};
