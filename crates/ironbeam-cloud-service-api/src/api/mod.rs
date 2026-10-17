//!
//! These traits provide synchronous interfaces for various cloud services,
//! with internal async handling where necessary.

use std::collections::HashMap;
use std::fmt;

pub use ironbeam_cloud_objects::{CloudIOError, CloudResult, ErrorKind, ObjectIO, ObjectMetadata};

mod credentials;
pub use credentials::*;
mod resource;
pub use resource::*;
mod warehouse;
pub use warehouse::*;
mod pubsub;
pub use pubsub::*;
mod database;
pub use database::*;
mod key_value;
pub use key_value::*;
mod search;
pub use search::*;
mod metrics;
pub use metrics::*;
mod config;
pub use config::*;
mod queue;
pub use queue::*;
mod cache;
pub use cache::*;
mod graph;
pub use graph::*;
mod compute;
pub use compute::*;
mod notifications;
pub use notifications::*;
mod inference;
pub use inference::*;

#[cfg(test)]
#[path = "../../tests/unit/lib__tests.rs"]
mod tests;
