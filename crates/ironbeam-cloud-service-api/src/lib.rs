//! Core traits for cloud IO operations.
//!
//! These traits provide synchronous interfaces for various cloud services,
//! with internal async handling where necessary.

mod api;
pub use api::*;
