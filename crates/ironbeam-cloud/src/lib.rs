#![allow(clippy::missing_errors_doc)]
//! Provider-neutral cloud contracts, fake services, and operation utilities.
pub mod compression;
pub mod fake;
pub mod helpers;
pub mod objects;
pub mod readers;
pub mod traits;
pub mod utils;
pub use fake::*;
pub use objects::{LocalObjectIO, ObjectLocation, ObjectStores};
pub use traits::*;
