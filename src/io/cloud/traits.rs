//! Core traits for cloud IO operations.
//!
//! These traits provide synchronous interfaces for various cloud services,
//! with internal async handling where necessary.

use std::collections::HashMap;
use std::error::Error;
use std::fmt;

// ============================================================================
// Core Error Type
// ============================================================================

/// Generic error type for cloud IO operations
#[derive(Debug, Clone)]
pub struct CloudIOError {
    pub message: String,
    pub kind: ErrorKind,
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    Authentication,
    Authorization,
    NotFound,
    AlreadyExists,
    InvalidInput,
    Network,
    Timeout,
    ServiceUnavailable,
    RateLimited,
    InternalError,
    Other,
}

impl fmt::Display for CloudIOError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl Error for CloudIOError {}

impl CloudIOError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind,
            source: None,
        }
    }

    #[must_use]
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }
}

pub type CloudResult<T> = Result<T, CloudIOError>;

// ============================================================================
// Credential and Configuration Traits
// ============================================================================

/// Trait for cloud service credentials
pub trait CloudCredentials: Send + Sync {
    /// Returns a unique identifier for this credential (e.g., access key ID, service account email)
    fn identifier(&self) -> &str;

    /// Returns the credential type (e.g., `"api_key"`, `"oauth2"`, `"iam_role"`)
    fn credential_type(&self) -> &str;

    /// Validates the credentials (returns Ok if valid)
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials are invalid, expired, or cannot be validated
    fn validate(&self) -> CloudResult<()>;

    /// Returns any additional metadata about the credentials
    fn metadata(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

/// Trait for cloud service configuration
pub trait CloudConfig: Send + Sync {
    /// Returns the region or location for the service
    fn region(&self) -> Option<&str> {
        None
    }

    /// Returns the endpoint URL (if a custom endpoint is used)
    fn endpoint(&self) -> Option<&str> {
        None
    }

    /// Returns the timeout in seconds for operations
    fn timeout_secs(&self) -> u64 {
        30
    }

    /// Returns the maximum number of retry attempts
    fn max_retries(&self) -> u32 {
        3
    }

    /// Returns additional provider-specific configuration
    fn extra(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

// ============================================================================
// Resource Identifier Types
// ============================================================================

/// Represents a resource in the cloud (bucket, table, queue, etc.)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResourceId {
    pub provider: String,
    pub resource_type: String,
    pub name: String,
    pub namespace: Option<String>, // project, account, etc.
}

impl ResourceId {
    pub fn new(
        provider: impl Into<String>,
        resource_type: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            resource_type: resource_type.into(),
            name: name.into(),
            namespace: None,
        }
    }

    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ns) = &self.namespace {
            write!(
                f,
                "{}:{}:{}/{}",
                self.provider, self.resource_type, ns, self.name
            )
        } else {
            write!(f, "{}:{}:{}", self.provider, self.resource_type, self.name)
        }
    }
}

// ============================================================================
// WarehouseIO - Analytical Databases
// ============================================================================

/// Query result from a warehouse
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
}

/// Trait for analytical data warehouse operations
pub trait WarehouseIO: Send + Sync {
    /// Execute a query and return results
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or there's a connection issue
    fn query(&self, sql: &str) -> CloudResult<QueryResult>;

    /// Execute a query without returning results (for DDL/DML)
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or there's a connection issue
    fn execute(&self, sql: &str) -> CloudResult<()>;

    /// Load data from a source (e.g., cloud storage) into a table
    ///
    /// # Errors
    ///
    /// Returns an error if the source is inaccessible, the table doesn't exist, or the data format is invalid
    fn load_data(
        &self,
        table: &str,
        source_uri: &str,
        options: HashMap<String, String>,
    ) -> CloudResult<()>;

    /// Export data from a query to a destination
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails, the destination is inaccessible, or permissions are not enough
    fn export_data(
        &self,
        sql: &str,
        destination_uri: &str,
        options: HashMap<String, String>,
    ) -> CloudResult<()>;

    /// Check if a table exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or insufficient permissions
    fn table_exists(&self, table: &str) -> CloudResult<bool>;

    /// Get table schema
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist, there's a connection issue, or permissions are not enough
    fn get_schema(&self, table: &str) -> CloudResult<Vec<(String, String)>>;
}

// ============================================================================
// ObjectIO - Object Storage
// ============================================================================

/// Metadata for an object in storage
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub last_modified: Option<i64>, // Unix timestamp
    pub etag: Option<String>,
    pub custom_metadata: HashMap<String, String>,
}

/// Trait for object storage operations
pub trait ObjectIO: Send + Sync {
    /// Upload data to object storage
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket doesn't exist, permissions are not enough, or the upload fails
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()>;

    /// Download data from object storage
    ///
    /// # Errors
    ///
    /// Returns an error if the object doesn't exist, permissions are not enough, or the download fails
    fn get_object(&self, bucket: &str, key: &str) -> CloudResult<Vec<u8>>;

    /// Delete an object
    ///
    /// # Errors
    ///
    /// Returns an error if the object doesn't exist, permissions are not enough, or the deletion fails
    fn delete_object(&self, bucket: &str, key: &str) -> CloudResult<()>;

    /// List objects with a prefix
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket doesn't exist, permissions are not enough, or the listing fails
    fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> CloudResult<Vec<ObjectMetadata>>;

    /// Check if an object exists
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket doesn't exist, permissions are not enough, or the check fails
    fn object_exists(&self, bucket: &str, key: &str) -> CloudResult<bool>;

    /// Get object metadata without downloading content
    ///
    /// # Errors
    ///
    /// Returns an error if the object doesn't exist, permissions are not enough, or the operation fails
    fn get_metadata(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata>;

    /// Copy an object within or between buckets
    ///
    /// # Errors
    ///
    /// Returns an error if the source doesn't exist, permissions are not enough, or the copy fails
    fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> CloudResult<()>;
}

// ============================================================================
// PubSubIO - Message Streaming
// ============================================================================

/// A message in a pub/sub system
#[derive(Debug, Clone)]
pub struct Message {
    pub id: String,
    pub data: Vec<u8>,
    pub attributes: HashMap<String, String>,
    pub publish_time: Option<i64>, // Unix timestamp
}

/// Trait for pub/sub streaming operations
pub trait PubSubIO: Send + Sync {
    /// Publish a message to a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, permissions are not enough, or publishing fails
    fn publish(
        &self,
        topic: &str,
        data: &[u8],
        attributes: HashMap<String, String>,
    ) -> CloudResult<String>;

    /// Publish multiple messages to a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, permissions are not enough, or publishing fails
    fn publish_batch(
        &self,
        topic: &str,
        messages: Vec<(Vec<u8>, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>>;

    /// Subscribe to a topic (creates subscription if it doesn't exist)
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, permissions are not enough, or subscription creation fails
    fn subscribe(&self, topic: &str, subscription_name: &str) -> CloudResult<()>;

    /// Pull messages from a subscription
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription doesn't exist, permissions are not enough, or pulling fails
    fn pull(&self, subscription: &str, max_messages: u32) -> CloudResult<Vec<Message>>;

    /// Acknowledge messages (mark as processed)
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription doesn't exist, acknowledgment IDs are invalid, or the operation fails
    fn acknowledge(&self, subscription: &str, ack_ids: Vec<String>) -> CloudResult<()>;

    /// Check if a topic exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or permissions are not enough
    fn topic_exists(&self, topic: &str) -> CloudResult<bool>;
}

// ============================================================================
// DatabaseIO - Relational Databases
// ============================================================================

/// A row from a database query
pub type Row = HashMap<String, String>;

/// Trait for relational database operations
pub trait DatabaseIO: Send + Sync {
    /// Execute a query and return results
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or there's a connection issue
    fn query(&self, sql: &str, params: Vec<String>) -> CloudResult<Vec<Row>>;

    /// Execute a statement without returning results (INSERT, UPDATE, DELETE)
    ///
    /// # Errors
    ///
    /// Returns an error if the statement is invalid, execution fails, or there's a connection issue
    fn execute(&self, sql: &str, params: Vec<String>) -> CloudResult<u64>;

    /// Begin a transaction
    ///
    /// # Errors
    ///
    /// Returns an error if transaction creation fails or there's a connection issue
    fn begin_transaction(&self) -> CloudResult<Box<dyn Transaction>>;

    /// Check if a table exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or permissions are not enough
    fn table_exists(&self, table: &str) -> CloudResult<bool>;

    /// Get table schema
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist, there's a connection issue, or permissions are not enough
    fn get_schema(&self, table: &str) -> CloudResult<Vec<(String, String)>>;
}

/// Trait for database transactions
pub trait Transaction: Send + Sync {
    /// Execute a query within the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or the transaction has been rolled back
    fn query(&mut self, sql: &str, params: Vec<String>) -> CloudResult<Vec<Row>>;

    /// Execute a statement within the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the statement is invalid, execution fails, or the transaction has been rolled back
    fn execute(&mut self, sql: &str, params: Vec<String>) -> CloudResult<u64>;

    /// Commit the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the commit fails or there's a connection issue
    fn commit(self: Box<Self>) -> CloudResult<()>;

    /// Roll back the transaction
    ///
    /// # Errors
    ///
    /// Returns an error if the rollback fails or there's a connection issue
    fn rollback(self: Box<Self>) -> CloudResult<()>;
}

// ============================================================================
// KeyValueIO - Document/NoSQL Databases
// ============================================================================

/// A document in a key-value store
#[derive(Debug, Clone)]
pub struct Document {
    pub key: String,
    pub data: HashMap<String, String>,
    pub version: Option<String>, // For optimistic locking
}

/// Trait for key-value/document store operations
pub trait KeyValueIO: Send + Sync {
    /// Put a document
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the operation fails
    fn put(&self, collection: &str, key: &str, data: HashMap<String, String>) -> CloudResult<()>;

    /// Get a document
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the operation fails
    fn get(&self, collection: &str, key: &str) -> CloudResult<Option<Document>>;

    /// Delete a document
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the deletion fails
    fn delete(&self, collection: &str, key: &str) -> CloudResult<()>;

    /// Query documents (simple key-value filtering)
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the query fails
    fn query(
        &self,
        collection: &str,
        filters: HashMap<String, String>,
    ) -> CloudResult<Vec<Document>>;

    /// Get multiple documents in a batch
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the operation fails
    fn batch_get(&self, collection: &str, keys: Vec<String>) -> CloudResult<Vec<Option<Document>>>;

    /// Batch put multiple documents
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the operation fails
    fn batch_put(
        &self,
        collection: &str,
        documents: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<()>;

    /// Check if a document exists
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist, permissions are not enough, or the check fails
    fn exists(&self, collection: &str, key: &str) -> CloudResult<bool>;
}

// ============================================================================
// SearchIO - Search and Log Services
// ============================================================================

/// A search result hit
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: String,
    pub score: f64,
    pub fields: HashMap<String, String>,
}

/// Search query parameters
#[derive(Debug, Clone)]
pub struct SearchQuery {
    pub query: String,
    pub filters: HashMap<String, String>,
    pub limit: u32,
    pub offset: u32,
}

/// Trait for search and log operations
pub trait SearchIO: Send + Sync {
    /// Index a document
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, permissions are not enough, or indexing fails
    fn index(&self, index: &str, id: &str, document: HashMap<String, String>) -> CloudResult<()>;

    /// Batch index documents
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, permissions are not enough, or indexing fails
    fn batch_index(
        &self,
        index: &str,
        documents: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<()>;

    /// Search documents
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, the query is invalid, or search fails
    fn search(&self, index: &str, query: SearchQuery) -> CloudResult<Vec<SearchHit>>;

    /// Delete a document from the index
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, permissions are not enough, or deletion fails
    fn delete(&self, index: &str, id: &str) -> CloudResult<()>;

    /// Get a document by ID
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist, permissions are not enough, or the operation fails
    fn get(&self, index: &str, id: &str) -> CloudResult<Option<HashMap<String, String>>>;

    /// Check if an index exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or permissions are not enough
    fn index_exists(&self, index: &str) -> CloudResult<bool>;
}

// ============================================================================
// MetricIO - Metrics and Monitoring
// ============================================================================

/// A metric data point
#[derive(Debug, Clone)]
pub struct MetricPoint {
    pub name: String,
    pub value: f64,
    pub timestamp: i64, // Unix timestamp
    pub tags: HashMap<String, String>,
}

/// Metric query parameters
#[derive(Debug, Clone)]
pub struct MetricQuery {
    pub metric_name: String,
    pub start_time: i64,
    pub end_time: i64,
    pub aggregation: Option<String>, // avg, sum, min, max, etc.
    pub tags: HashMap<String, String>,
}

/// Trait for metrics operations
pub trait MetricIO: Send + Sync {
    /// Put a single metric
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist, permissions are not enough, or the operation fails
    fn put_metric(&self, namespace: &str, metric: MetricPoint) -> CloudResult<()>;

    /// Put multiple metrics
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist, permissions are not enough, or the operation fails
    fn put_metrics(&self, namespace: &str, metrics: Vec<MetricPoint>) -> CloudResult<()>;

    /// Query metrics
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist, the query is invalid, or the operation fails
    fn query_metrics(&self, namespace: &str, query: MetricQuery) -> CloudResult<Vec<MetricPoint>>;

    /// List available metrics in a namespace
    ///
    /// # Errors
    ///
    /// Returns an error if the namespace doesn't exist, permissions are not enough, or the listing fails
    fn list_metrics(&self, namespace: &str) -> CloudResult<Vec<String>>;
}

// ============================================================================
// ConfigIO - Configuration Services
// ============================================================================

/// A configuration value
#[derive(Debug, Clone)]
pub struct ConfigValue {
    pub key: String,
    pub value: String,
    pub version: Option<String>,
    pub is_secret: bool,
}

/// Trait for configuration service operations
pub trait ConfigIO: Send + Sync {
    /// Get a configuration value
    ///
    /// # Errors
    ///
    /// Returns an error if the key doesn't exist, permissions are not enough, or the operation fails
    fn get(&self, key: &str) -> CloudResult<ConfigValue>;

    /// Set a configuration value
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough, the value is invalid, or the operation fails
    fn set(&self, key: &str, value: &str, is_secret: bool) -> CloudResult<()>;

    /// Delete a configuration value
    ///
    /// # Errors
    ///
    /// Returns an error if the key doesn't exist, permissions are not enough, or the deletion fails
    fn delete(&self, key: &str) -> CloudResult<()>;

    /// List all configuration keys with a prefix
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the listing fails
    fn list(&self, prefix: Option<&str>) -> CloudResult<Vec<String>>;

    /// Get multiple configuration values
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the operation fails
    fn batch_get(&self, keys: Vec<String>) -> CloudResult<Vec<Option<ConfigValue>>>;
}

// ============================================================================
// QueueIO - Message Queues
// ============================================================================

/// A queue message
#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub id: String,
    pub receipt_handle: String, // For acknowledgment
    pub body: String,
    pub attributes: HashMap<String, String>,
    pub receive_count: u32,
}

/// Trait for message queue operations
pub trait QueueIO: Send + Sync {
    /// Send a message to a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are not enough, or sending fails
    fn send(
        &self,
        queue: &str,
        body: &str,
        attributes: HashMap<String, String>,
    ) -> CloudResult<String>;

    /// Send multiple messages to a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are not enough, or sending fails
    fn send_batch(
        &self,
        queue: &str,
        messages: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>>;

    /// Receive messages from a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are insufficient, or receiving fails
    fn receive(
        &self,
        queue: &str,
        max_messages: u32,
        visibility_timeout_secs: u32,
    ) -> CloudResult<Vec<QueueMessage>>;

    /// Delete a message from the queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, the receipt handle is invalid, or deletion fails
    fn delete(&self, queue: &str, receipt_handle: &str) -> CloudResult<()>;

    /// Delete multiple messages from the queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, receipt handles are invalid, or deletion fails
    fn delete_batch(&self, queue: &str, receipt_handles: Vec<String>) -> CloudResult<()>;

    /// Get approximate queue size
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are not enough, or the operation fails
    fn queue_size(&self, queue: &str) -> CloudResult<u64>;

    /// Purge all messages from a queue
    ///
    /// # Errors
    ///
    /// Returns an error if the queue doesn't exist, permissions are not enough, or purging fails
    fn purge(&self, queue: &str) -> CloudResult<()>;
}

// ============================================================================
// CacheIO - In-Memory Caching
// ============================================================================

/// Trait for cache operations
pub trait CacheIO: Send + Sync {
    /// Get a value from the cache
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the operation fails
    fn get(&self, key: &str) -> CloudResult<Option<Vec<u8>>>;

    /// Set a value in the cache with optional TTL in seconds
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the operation fails
    fn set(&self, key: &str, value: &[u8], ttl_secs: Option<u64>) -> CloudResult<()>;

    /// Delete a value from the cache
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the deletion fails
    fn delete(&self, key: &str) -> CloudResult<()>;

    /// Check if a key exists
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the check fails
    fn exists(&self, key: &str) -> CloudResult<bool>;

    /// Get multiple values
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the operation fails
    fn get_batch(&self, keys: Vec<String>) -> CloudResult<Vec<Option<Vec<u8>>>>;

    /// Set multiple values
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or the operation fails
    fn set_batch(&self, items: Vec<(String, Vec<u8>, Option<u64>)>) -> CloudResult<()>;

    /// Increment a counter
    ///
    /// # Errors
    ///
    /// Returns an error if the key doesn't exist, isn't a numeric value, or there's a connection issue
    fn increment(&self, key: &str, delta: i64) -> CloudResult<i64>;

    /// Flush all data (use with caution!)
    ///
    /// # Errors
    ///
    /// Returns an error if there's a connection issue or permissions are not enough
    fn flush(&self) -> CloudResult<()>;
}

// ============================================================================
// GraphIO - Graph Databases
// ============================================================================

/// A node in a graph
#[derive(Debug, Clone)]
pub struct GraphNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: HashMap<String, String>,
}

/// An edge in a graph
#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub id: String,
    pub label: String,
    pub from_node: String,
    pub to_node: String,
    pub properties: HashMap<String, String>,
}

/// Trait for graph database operations
pub trait GraphIO: Send + Sync {
    /// Add a node to the graph
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the operation fails
    fn add_node(
        &self,
        labels: Vec<String>,
        properties: HashMap<String, String>,
    ) -> CloudResult<String>;

    /// Get a node by ID
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the operation fails
    fn get_node(&self, id: &str) -> CloudResult<Option<GraphNode>>;

    /// Update node properties
    ///
    /// # Errors
    ///
    /// Returns an error if the node doesn't exist, permissions are not enough, or the update fails
    fn update_node(&self, id: &str, properties: HashMap<String, String>) -> CloudResult<()>;

    /// Delete a node
    ///
    /// # Errors
    ///
    /// Returns an error if the node doesn't exist, permissions are not enough, or the deletion fails
    fn delete_node(&self, id: &str) -> CloudResult<()>;

    /// Add an edge between nodes
    ///
    /// # Errors
    ///
    /// Returns an error if either node doesn't exist, permissions are not enough, or the operation fails
    fn add_edge(
        &self,
        from: &str,
        to: &str,
        label: &str,
        properties: HashMap<String, String>,
    ) -> CloudResult<String>;

    /// Get an edge by ID
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the operation fails
    fn get_edge(&self, id: &str) -> CloudResult<Option<GraphEdge>>;

    /// Delete an edge
    ///
    /// # Errors
    ///
    /// Returns an error if the edge doesn't exist, permissions are not enough, or the deletion fails
    fn delete_edge(&self, id: &str) -> CloudResult<()>;

    /// Query the graph (using provider-specific query language)
    ///
    /// # Errors
    ///
    /// Returns an error if the query is invalid, execution fails, or there's a connection issue
    fn query(
        &self,
        query: &str,
        params: HashMap<String, String>,
    ) -> CloudResult<Vec<HashMap<String, String>>>;

    /// Find neighbors of a node
    ///
    /// # Errors
    ///
    /// Returns an error if the node doesn't exist, permissions are not enough, or the operation fails
    fn get_neighbors(&self, node_id: &str, direction: EdgeDirection)
    -> CloudResult<Vec<GraphNode>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
    Both,
}

// ============================================================================
// ComputeIO - Serverless Compute
// ============================================================================

/// Result of a compute invocation
#[derive(Debug, Clone)]
pub struct ComputeResult {
    pub status_code: u16,
    pub output: Vec<u8>,
    pub logs: Option<String>,
    pub execution_time_ms: u64,
}

/// Trait for serverless compute operations
pub trait ComputeIO: Send + Sync {
    /// Invoke a function synchronously
    ///
    /// # Errors
    ///
    /// Returns an error if the function doesn't exist, permissions are not enough, or invocation fails
    fn invoke(&self, function_name: &str, payload: &[u8]) -> CloudResult<ComputeResult>;

    /// Invoke a function asynchronously (fire and forget)
    ///
    /// # Errors
    ///
    /// Returns an error if the function doesn't exist, permissions are not enough, or invocation fails
    fn invoke_async(&self, function_name: &str, payload: &[u8]) -> CloudResult<String>;

    /// Get the status of an async invocation
    ///
    /// # Errors
    ///
    /// Returns an error if the invocation ID is invalid, permissions are not enough, or the operation fails
    fn get_invocation_status(&self, invocation_id: &str) -> CloudResult<InvocationStatus>;

    /// List the available functions
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the listing fails
    fn list_functions(&self) -> CloudResult<Vec<String>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    TimedOut,
}

// ============================================================================
// NotificationIO - Push Notifications
// ============================================================================

/// A notification to be sent
#[derive(Debug, Clone)]
pub struct Notification {
    pub target: String, // Topic ARN, device token, email, etc.
    pub subject: Option<String>,
    pub message: String,
    pub attributes: HashMap<String, String>,
}

/// Result of sending a notification
#[derive(Debug, Clone)]
pub struct NotificationResult {
    pub message_id: String,
    pub status: NotificationStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotificationStatus {
    Sent,
    Failed,
    Pending,
}

/// Trait for notification operations
pub trait NotificationIO: Send + Sync {
    /// Send a notification
    ///
    /// # Errors
    ///
    /// Returns an error if the target is invalid, permissions are not enough, or sending fails
    fn send(&self, notification: Notification) -> CloudResult<NotificationResult>;

    /// Send multiple notifications
    ///
    /// # Errors
    ///
    /// Returns an error if any target is invalid, permissions are not enough, or sending fails
    fn send_batch(&self, notifications: Vec<Notification>) -> CloudResult<Vec<NotificationResult>>;

    /// Subscribe an endpoint to a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, the endpoint is invalid, or subscription fails
    fn subscribe(&self, topic: &str, endpoint: &str, protocol: &str) -> CloudResult<String>;

    /// Unsubscribe from a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription doesn't exist, permissions are not enough, or unsubscription fails
    fn unsubscribe(&self, subscription_id: &str) -> CloudResult<()>;

    /// Create a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic already exists, permissions are not enough, or creation fails
    fn create_topic(&self, name: &str) -> CloudResult<String>;

    /// Delete a topic
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist, permissions are not enough, or deletion fails
    fn delete_topic(&self, topic: &str) -> CloudResult<()>;
}

// ============================================================================
// IntelligenceIO - ML Model Inference
// ============================================================================

/// Input for model inference
#[derive(Debug, Clone)]
pub struct InferenceInput {
    pub data: Vec<u8>,
    pub content_type: String, // e.g., "application/json", "text/plain"
}

/// Output from model inference
#[derive(Debug, Clone)]
pub struct InferenceOutput {
    pub data: Vec<u8>,
    pub content_type: String,
    pub model_version: Option<String>,
    pub inference_time_ms: u64,
}

/// Trait for ML model inference operations
pub trait IntelligenceIO: Send + Sync {
    /// Invoke a model for inference
    ///
    /// # Errors
    ///
    /// Returns an error if the model doesn't exist, the input is invalid, or inference fails
    fn predict(&self, model_name: &str, input: InferenceInput) -> CloudResult<InferenceOutput>;

    /// Batch prediction
    ///
    /// # Errors
    ///
    /// Returns an error if the model doesn't exist, any input is invalid, or inference fails
    fn predict_batch(
        &self,
        model_name: &str,
        inputs: Vec<InferenceInput>,
    ) -> CloudResult<Vec<InferenceOutput>>;

    /// List available models
    ///
    /// # Errors
    ///
    /// Returns an error if permissions are not enough or the listing fails
    fn list_models(&self) -> CloudResult<Vec<String>>;

    /// Get model metadata
    ///
    /// # Errors
    ///
    /// Returns an error if the model doesn't exist, permissions are not enough, or the operation fails
    fn get_model_info(&self, model_name: &str) -> CloudResult<HashMap<String, String>>;
}
