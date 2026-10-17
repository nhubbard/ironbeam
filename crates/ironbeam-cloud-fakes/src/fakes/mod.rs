#![allow(clippy::wildcard_imports)] // Split fake implementations share the parent test types.
//!
//! These implementations use in-memory data structures to simulate cloud services,
//! making them ideal for unit testing without external dependencies.

use ironbeam_cloud_service_api::{
    CacheIO, CloudConfig, CloudCredentials, CloudIOError, CloudResult, ComputeIO, ComputeResult,
    ConfigIO, ConfigValue, DatabaseIO, Document, EdgeDirection, ErrorKind, GraphEdge, GraphIO,
    GraphNode, InferenceInput, InferenceOutput, IntelligenceIO, InvocationStatus, KeyValueIO,
    Message, MetricIO, MetricPoint, MetricQuery, Notification, NotificationIO, NotificationResult,
    NotificationStatus, ObjectIO, ObjectMetadata, PubSubIO, QueryResult, QueueIO, QueueMessage,
    Row, SearchHit, SearchIO, SearchQuery, Transaction, WarehouseIO,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Type aliases for complex nested types
type BucketStorage = Arc<Mutex<HashMap<String, HashMap<String, Vec<u8>>>>>;
type CollectionStorage = Arc<Mutex<HashMap<String, HashMap<String, Document>>>>;
type IndexStorage = Arc<Mutex<HashMap<String, HashMap<String, HashMap<String, String>>>>>;
type SchemaMap = Arc<Mutex<HashMap<String, Vec<(String, String)>>>>;
type FunctionMap = Arc<Mutex<HashMap<String, Box<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>>>>;

mod credentials;
pub use credentials::*;
mod warehouse;
pub use warehouse::*;
mod objects;
pub use objects::*;
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
#[path = "../../tests/unit/lib__contract_tests.rs"]
mod contract_tests;
