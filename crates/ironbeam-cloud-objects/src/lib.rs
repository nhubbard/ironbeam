//! Provider-neutral object storage contract and URI routing.
mod implementation;
pub use implementation::*;
pub mod objects;
pub use objects::{LocalObjectIO, ObjectLocation, ObjectStores};
