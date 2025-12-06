//! Fake implementations for testing.
//!
//! These implementations use in-memory data structures to simulate cloud services,
//! making them ideal for unit testing without external dependencies.

use crate::io::cloud::traits::{
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

// ============================================================================
// Fake Credentials and Config
// ============================================================================

#[derive(Debug, Clone)]
pub struct FakeCredentials {
    pub identifier: String,
    pub credential_type: String,
}

impl CloudCredentials for FakeCredentials {
    fn identifier(&self) -> &str {
        &self.identifier
    }

    fn credential_type(&self) -> &str {
        &self.credential_type
    }

    fn validate(&self) -> CloudResult<()> {
        if self.identifier.is_empty() {
            return Err(CloudIOError::new(
                ErrorKind::Authentication,
                "Empty identifier",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FakeConfig {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub timeout_secs: u64,
    pub max_retries: u32,
}

impl Default for FakeConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint: None,
            timeout_secs: 30,
            max_retries: 3,
        }
    }
}

impl CloudConfig for FakeConfig {
    fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    fn timeout_secs(&self) -> u64 {
        self.timeout_secs
    }

    fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

// ============================================================================
// FakeWarehouseIO
// ============================================================================

#[derive(Clone)]
pub struct FakeWarehouseIO {
    tables: Arc<Mutex<HashMap<String, Vec<Vec<String>>>>>,
    schemas: SchemaMap,
}

impl FakeWarehouseIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
            schemas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Adds a table with schema and data to the fake warehouse.
    ///
    /// # Panics
    ///
    /// Panics if the mutex protecting the tables or schemas is poisoned.
    pub fn add_table(&self, name: &str, schema: Vec<(String, String)>, data: Vec<Vec<String>>) {
        self.tables
            .lock()
            .expect("tables mutex poisoned")
            .insert(name.to_string(), data);
        self.schemas
            .lock()
            .expect("schemas mutex poisoned")
            .insert(name.to_string(), schema);
    }
}

impl Default for FakeWarehouseIO {
    fn default() -> Self {
        Self::new()
    }
}

impl WarehouseIO for FakeWarehouseIO {
    fn query(&self, sql: &str) -> CloudResult<QueryResult> {
        // Simple fake: assume the query is "SELECT * FROM table_name"
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() >= 4 && parts[0].eq_ignore_ascii_case("SELECT") {
            let table_name = parts[3];
            let tables = self.tables.lock().expect("tables mutex poisoned");
            let schemas = self.schemas.lock().expect("schemas mutex poisoned");

            if let (Some(data), Some(schema)) = (tables.get(table_name), schemas.get(table_name)) {
                let columns: Vec<String> = schema.iter().map(|(name, _)| name.clone()).collect();
                let row_count = data.len();
                let rows = data.clone();
                drop(tables);
                drop(schemas);
                Ok(QueryResult {
                    columns,
                    rows,
                    row_count,
                })
            } else {
                Err(CloudIOError::new(
                    ErrorKind::NotFound,
                    format!("Table {table_name} not found"),
                ))
            }
        } else {
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                row_count: 0,
            })
        }
    }

    fn execute(&self, _sql: &str) -> CloudResult<()> {
        Ok(())
    }

    fn load_data(
        &self,
        table: &str,
        _source_uri: &str,
        _options: HashMap<String, String>,
    ) -> CloudResult<()> {
        self.tables
            .lock()
            .expect("tables mutex poisoned")
            .entry(table.to_string())
            .or_default();
        Ok(())
    }

    fn export_data(
        &self,
        _sql: &str,
        _destination_uri: &str,
        _options: HashMap<String, String>,
    ) -> CloudResult<()> {
        Ok(())
    }

    fn table_exists(&self, table: &str) -> CloudResult<bool> {
        Ok(self
            .tables
            .lock()
            .expect("tables mutex poisoned")
            .contains_key(table))
    }

    fn get_schema(&self, table: &str) -> CloudResult<Vec<(String, String)>> {
        self.schemas
            .lock()
            .expect("schemas mutex poisoned")
            .get(table)
            .cloned()
            .ok_or_else(|| {
                CloudIOError::new(ErrorKind::NotFound, format!("Table {table} not found"))
            })
    }
}

// ============================================================================
// FakeObjectIO
// ============================================================================

#[derive(Clone)]
pub struct FakeObjectIO {
    storage: BucketStorage,
}

impl FakeObjectIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeObjectIO {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectIO for FakeObjectIO {
    fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> {
        self.storage
            .lock()
            .expect("storage mutex poisoned")
            .entry(bucket.to_string())
            .or_default()
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn get_object(&self, bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
        let storage = self.storage.lock().expect("storage mutex poisoned");
        storage
            .get(bucket)
            .and_then(|b| b.get(key))
            .cloned()
            .ok_or_else(|| {
                CloudIOError::new(
                    ErrorKind::NotFound,
                    format!("Object {bucket}/{key} not found"),
                )
            })
    }

    fn delete_object(&self, bucket: &str, key: &str) -> CloudResult<()> {
        if let Some(bucket_map) = self
            .storage
            .lock()
            .expect("storage mutex poisoned")
            .get_mut(bucket)
        {
            bucket_map.remove(key);
        }
        Ok(())
    }

    fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> CloudResult<Vec<ObjectMetadata>> {
        let storage = self.storage.lock().expect("storage mutex poisoned");
        let bucket_map = storage.get(bucket).ok_or_else(|| {
            CloudIOError::new(ErrorKind::NotFound, format!("Bucket {bucket} not found"))
        })?;

        let mut objects: Vec<ObjectMetadata> = bucket_map
            .iter()
            .filter(|(key, _)| prefix.is_none_or(|p| key.starts_with(p)))
            .map(|(key, data)| ObjectMetadata {
                key: key.clone(),
                size: data.len() as u64,
                content_type: Some("application/octet-stream".to_string()),
                last_modified: Some(0),
                etag: Some(format!("etag-{key}")),
                custom_metadata: HashMap::new(),
            })
            .collect();

        drop(storage);
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(objects)
    }

    fn object_exists(&self, bucket: &str, key: &str) -> CloudResult<bool> {
        let storage = self.storage.lock().expect("storage mutex poisoned");
        Ok(storage.get(bucket).is_some_and(|b| b.contains_key(key)))
    }

    fn get_metadata(&self, bucket: &str, key: &str) -> CloudResult<ObjectMetadata> {
        let storage = self.storage.lock().expect("storage mutex poisoned");
        storage
            .get(bucket)
            .and_then(|b| b.get(key))
            .map(|data| ObjectMetadata {
                key: key.to_string(),
                size: data.len() as u64,
                content_type: Some("application/octet-stream".to_string()),
                last_modified: Some(0),
                etag: Some(format!("etag-{key}")),
                custom_metadata: HashMap::new(),
            })
            .ok_or_else(|| {
                CloudIOError::new(
                    ErrorKind::NotFound,
                    format!("Object {bucket}/{key} not found"),
                )
            })
    }

    fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> CloudResult<()> {
        let data = self.get_object(src_bucket, src_key)?;
        self.put_object(dst_bucket, dst_key, &data)
    }
}

// ============================================================================
// FakePubSubIO
// ============================================================================

#[derive(Clone)]
pub struct FakePubSubIO {
    topics: Arc<Mutex<HashMap<String, Vec<Message>>>>,
    subscriptions: Arc<Mutex<HashMap<String, Vec<Message>>>>,
    message_counter: Arc<Mutex<u64>>,
}

impl FakePubSubIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            topics: Arc::new(Mutex::new(HashMap::new())),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            message_counter: Arc::new(Mutex::new(0)),
        }
    }

    fn next_id(&self) -> String {
        let mut counter = self
            .message_counter
            .lock()
            .expect("message_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("msg-{id}")
    }
}

impl Default for FakePubSubIO {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubIO for FakePubSubIO {
    fn publish(
        &self,
        topic: &str,
        data: &[u8],
        attributes: HashMap<String, String>,
    ) -> CloudResult<String> {
        let msg_id = self.next_id();
        let message = Message {
            id: msg_id.clone(),
            data: data.to_vec(),
            attributes,
            publish_time: Some(0),
        };

        self.topics
            .lock()
            .expect("topics mutex poisoned")
            .entry(topic.to_string())
            .or_default()
            .push(message);

        Ok(msg_id)
    }

    fn publish_batch(
        &self,
        topic: &str,
        messages: Vec<(Vec<u8>, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>> {
        messages
            .into_iter()
            .map(|(data, attrs)| self.publish(topic, &data, attrs))
            .collect()
    }

    fn subscribe(&self, topic: &str, subscription_name: &str) -> CloudResult<()> {
        self.subscriptions
            .lock()
            .expect("subscriptions mutex poisoned")
            .insert(format!("{topic}/{subscription_name}"), Vec::new());
        Ok(())
    }

    fn pull(&self, subscription: &str, max_messages: u32) -> CloudResult<Vec<Message>> {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .expect("subscriptions mutex poisoned");
        let messages = subscriptions.entry(subscription.to_string()).or_default();

        let count = std::cmp::min(max_messages as usize, messages.len());
        let pulled = messages.drain(0..count).collect();
        drop(subscriptions);
        Ok(pulled)
    }

    fn acknowledge(&self, _subscription: &str, _ack_ids: Vec<String>) -> CloudResult<()> {
        Ok(())
    }

    fn topic_exists(&self, topic: &str) -> CloudResult<bool> {
        Ok(self
            .topics
            .lock()
            .expect("topics mutex poisoned")
            .contains_key(topic))
    }
}

// ============================================================================
// FakeDatabaseIO
// ============================================================================

#[derive(Clone)]
pub struct FakeDatabaseIO {
    tables: Arc<Mutex<HashMap<String, Vec<Row>>>>,
    schemas: SchemaMap,
}

impl FakeDatabaseIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
            schemas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Adds a table with schema to the fake database.
    ///
    /// # Panics
    ///
    /// Panics if the mutex protecting the tables or schemas is poisoned.
    pub fn add_table(&self, name: &str, schema: Vec<(String, String)>) {
        self.tables
            .lock()
            .expect("tables mutex poisoned")
            .insert(name.to_string(), Vec::new());
        self.schemas
            .lock()
            .expect("schemas mutex poisoned")
            .insert(name.to_string(), schema);
    }
}

impl Default for FakeDatabaseIO {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseIO for FakeDatabaseIO {
    fn query(&self, sql: &str, _params: Vec<String>) -> CloudResult<Vec<Row>> {
        // Simple fake: assume the query is "SELECT * FROM table_name"
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() >= 4 && parts[0].eq_ignore_ascii_case("SELECT") {
            let table_name = parts[3];
            let tables = self.tables.lock().expect("tables mutex poisoned");

            tables.get(table_name).cloned().ok_or_else(|| {
                CloudIOError::new(ErrorKind::NotFound, format!("Table {table_name} not found"))
            })
        } else {
            Ok(Vec::new())
        }
    }

    fn execute(&self, sql: &str, _params: Vec<String>) -> CloudResult<u64> {
        // Simple fake: count INSERT/UPDATE/DELETE
        if sql.to_uppercase().contains("INSERT") {
            Ok(1)
        } else {
            Ok(0)
        }
    }

    fn begin_transaction(&self) -> CloudResult<Box<dyn Transaction>> {
        Ok(Box::new(FakeTransaction {
            db: self.clone(),
            committed: false,
        }))
    }

    fn table_exists(&self, table: &str) -> CloudResult<bool> {
        Ok(self
            .tables
            .lock()
            .expect("tables mutex poisoned")
            .contains_key(table))
    }

    fn get_schema(&self, table: &str) -> CloudResult<Vec<(String, String)>> {
        self.schemas
            .lock()
            .expect("schemas mutex poisoned")
            .get(table)
            .cloned()
            .ok_or_else(|| {
                CloudIOError::new(ErrorKind::NotFound, format!("Table {table} not found"))
            })
    }
}

struct FakeTransaction {
    db: FakeDatabaseIO,
    committed: bool,
}

impl Transaction for FakeTransaction {
    fn query(&mut self, sql: &str, params: Vec<String>) -> CloudResult<Vec<Row>> {
        self.db.query(sql, params)
    }

    fn execute(&mut self, sql: &str, params: Vec<String>) -> CloudResult<u64> {
        self.db.execute(sql, params)
    }

    fn commit(mut self: Box<Self>) -> CloudResult<()> {
        self.committed = true;
        Ok(())
    }

    fn rollback(self: Box<Self>) -> CloudResult<()> {
        Ok(())
    }
}

// ============================================================================
// FakeKeyValueIO
// ============================================================================

#[derive(Clone)]
pub struct FakeKeyValueIO {
    collections: CollectionStorage,
}

impl FakeKeyValueIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            collections: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeKeyValueIO {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueIO for FakeKeyValueIO {
    fn put(&self, collection: &str, key: &str, data: HashMap<String, String>) -> CloudResult<()> {
        let mut collections = self.collections.lock().expect("collections mutex poisoned");
        let coll = collections.entry(collection.to_string()).or_default();
        coll.insert(
            key.to_string(),
            Document {
                key: key.to_string(),
                data,
                version: Some("v1".to_string()),
            },
        );
        drop(collections);
        Ok(())
    }

    fn get(&self, collection: &str, key: &str) -> CloudResult<Option<Document>> {
        let collections = self.collections.lock().expect("collections mutex poisoned");
        Ok(collections
            .get(collection)
            .and_then(|c| c.get(key))
            .cloned())
    }

    fn delete(&self, collection: &str, key: &str) -> CloudResult<()> {
        if let Some(coll) = self
            .collections
            .lock()
            .expect("collections mutex poisoned")
            .get_mut(collection)
        {
            coll.remove(key);
        }
        Ok(())
    }

    fn query(
        &self,
        collection: &str,
        filters: HashMap<String, String>,
    ) -> CloudResult<Vec<Document>> {
        let collections = self.collections.lock().expect("collections mutex poisoned");
        let coll = collections.get(collection).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::NotFound,
                format!("Collection {collection} not found"),
            )
        })?;

        let results: Vec<Document> = coll
            .values()
            .filter(|doc| filters.iter().all(|(k, v)| doc.data.get(k) == Some(v)))
            .cloned()
            .collect();

        drop(collections);
        Ok(results)
    }

    fn batch_get(&self, collection: &str, keys: Vec<String>) -> CloudResult<Vec<Option<Document>>> {
        keys.into_iter().map(|k| self.get(collection, &k)).collect()
    }

    fn batch_put(
        &self,
        collection: &str,
        documents: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<()> {
        for (key, data) in documents {
            self.put(collection, &key, data)?;
        }
        Ok(())
    }

    fn exists(&self, collection: &str, key: &str) -> CloudResult<bool> {
        let collections = self.collections.lock().expect("collections mutex poisoned");
        Ok(collections
            .get(collection)
            .is_some_and(|c| c.contains_key(key)))
    }
}

// ============================================================================
// FakeSearchIO
// ============================================================================

#[derive(Clone)]
pub struct FakeSearchIO {
    indices: IndexStorage,
}

impl FakeSearchIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            indices: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeSearchIO {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchIO for FakeSearchIO {
    fn index(&self, index: &str, id: &str, document: HashMap<String, String>) -> CloudResult<()> {
        self.indices
            .lock()
            .expect("indices mutex poisoned")
            .entry(index.to_string())
            .or_default()
            .insert(id.to_string(), document);
        Ok(())
    }

    fn batch_index(
        &self,
        index: &str,
        documents: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<()> {
        for (id, doc) in documents {
            self.index(index, &id, doc)?;
        }
        Ok(())
    }

    fn search(&self, index: &str, query: SearchQuery) -> CloudResult<Vec<SearchHit>> {
        let indices = self.indices.lock().expect("indices mutex poisoned");
        let idx = indices.get(index).ok_or_else(|| {
            CloudIOError::new(ErrorKind::NotFound, format!("Index {index} not found"))
        })?;

        let mut hits: Vec<SearchHit> = idx
            .iter()
            .filter(|(_, doc)| {
                // Simple filtering - check if the query string appears in any field
                doc.values().any(|v| v.contains(&query.query))
                    && query.filters.iter().all(|(k, v)| doc.get(k) == Some(v))
            })
            .map(|(id, fields)| SearchHit {
                id: id.clone(),
                score: 1.0,
                fields: fields.clone(),
            })
            .collect();

        drop(indices);

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .expect("NaN score encountered")
        });

        let start = query.offset as usize;
        let end = std::cmp::min(start + query.limit as usize, hits.len());

        Ok(hits[start..end].to_vec())
    }

    fn delete(&self, index: &str, id: &str) -> CloudResult<()> {
        if let Some(idx) = self
            .indices
            .lock()
            .expect("indices mutex poisoned")
            .get_mut(index)
        {
            idx.remove(id);
        }
        Ok(())
    }

    fn get(&self, index: &str, id: &str) -> CloudResult<Option<HashMap<String, String>>> {
        let indices = self.indices.lock().expect("indices mutex poisoned");
        Ok(indices.get(index).and_then(|idx| idx.get(id)).cloned())
    }

    fn index_exists(&self, index: &str) -> CloudResult<bool> {
        Ok(self
            .indices
            .lock()
            .expect("indices mutex poisoned")
            .contains_key(index))
    }
}

// ============================================================================
// FakeMetricIO
// ============================================================================

#[derive(Clone)]
pub struct FakeMetricIO {
    metrics: Arc<Mutex<HashMap<String, Vec<MetricPoint>>>>,
}

impl FakeMetricIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeMetricIO {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricIO for FakeMetricIO {
    fn put_metric(&self, namespace: &str, metric: MetricPoint) -> CloudResult<()> {
        self.metrics
            .lock()
            .expect("metrics mutex poisoned")
            .entry(namespace.to_string())
            .or_default()
            .push(metric);
        Ok(())
    }

    fn put_metrics(&self, namespace: &str, metrics: Vec<MetricPoint>) -> CloudResult<()> {
        for metric in metrics {
            self.put_metric(namespace, metric)?;
        }
        Ok(())
    }

    fn query_metrics(&self, namespace: &str, query: MetricQuery) -> CloudResult<Vec<MetricPoint>> {
        let metrics = self.metrics.lock().expect("metrics mutex poisoned");
        let ns_metrics = metrics.get(namespace).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::NotFound,
                format!("Namespace {namespace} not found"),
            )
        })?;

        let results: Vec<MetricPoint> = ns_metrics
            .iter()
            .filter(|m| {
                m.name == query.metric_name
                    && m.timestamp >= query.start_time
                    && m.timestamp <= query.end_time
                    && query.tags.iter().all(|(k, v)| m.tags.get(k) == Some(v))
            })
            .cloned()
            .collect();

        drop(metrics);
        Ok(results)
    }

    fn list_metrics(&self, namespace: &str) -> CloudResult<Vec<String>> {
        let metrics = self.metrics.lock().expect("metrics mutex poisoned");
        let ns_metrics = metrics.get(namespace).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::NotFound,
                format!("Namespace {namespace} not found"),
            )
        })?;

        let mut names: Vec<String> = ns_metrics.iter().map(|m| m.name.clone()).collect();
        drop(metrics);
        names.sort();
        names.dedup();
        Ok(names)
    }
}

// ============================================================================
// FakeConfigIO
// ============================================================================

#[derive(Clone)]
pub struct FakeConfigIO {
    config: Arc<Mutex<HashMap<String, ConfigValue>>>,
}

impl FakeConfigIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeConfigIO {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigIO for FakeConfigIO {
    fn get(&self, key: &str) -> CloudResult<ConfigValue> {
        self.config
            .lock()
            .expect("config mutex poisoned")
            .get(key)
            .cloned()
            .ok_or_else(|| {
                CloudIOError::new(ErrorKind::NotFound, format!("Config key {key} not found"))
            })
    }

    fn set(&self, key: &str, value: &str, is_secret: bool) -> CloudResult<()> {
        self.config.lock().expect("config mutex poisoned").insert(
            key.to_string(),
            ConfigValue {
                key: key.to_string(),
                value: value.to_string(),
                version: Some("v1".to_string()),
                is_secret,
            },
        );
        Ok(())
    }

    fn delete(&self, key: &str) -> CloudResult<()> {
        self.config
            .lock()
            .expect("config mutex poisoned")
            .remove(key);
        Ok(())
    }

    fn list(&self, prefix: Option<&str>) -> CloudResult<Vec<String>> {
        let config = self.config.lock().expect("config mutex poisoned");
        let mut keys: Vec<String> = config
            .keys()
            .filter(|k| prefix.is_none_or(|p| k.starts_with(p)))
            .cloned()
            .collect();
        drop(config);
        keys.sort();
        Ok(keys)
    }

    fn batch_get(&self, keys: Vec<String>) -> CloudResult<Vec<Option<ConfigValue>>> {
        let config = self.config.lock().expect("config mutex poisoned");
        Ok(keys.into_iter().map(|k| config.get(&k).cloned()).collect())
    }
}

// ============================================================================
// FakeQueueIO
// ============================================================================

#[derive(Clone)]
pub struct FakeQueueIO {
    queues: Arc<Mutex<HashMap<String, Vec<QueueMessage>>>>,
    message_counter: Arc<Mutex<u64>>,
}

impl FakeQueueIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            queues: Arc::new(Mutex::new(HashMap::new())),
            message_counter: Arc::new(Mutex::new(0)),
        }
    }

    fn next_id(&self) -> String {
        let mut counter = self
            .message_counter
            .lock()
            .expect("message_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("msg-{id}")
    }
}

impl Default for FakeQueueIO {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueIO for FakeQueueIO {
    fn send(
        &self,
        queue: &str,
        body: &str,
        attributes: HashMap<String, String>,
    ) -> CloudResult<String> {
        let msg_id = self.next_id();
        let message = QueueMessage {
            id: msg_id.clone(),
            receipt_handle: format!("receipt-{msg_id}"),
            body: body.to_string(),
            attributes,
            receive_count: 0,
        };

        self.queues
            .lock()
            .expect("queues mutex poisoned")
            .entry(queue.to_string())
            .or_default()
            .push(message);

        Ok(msg_id)
    }

    fn send_batch(
        &self,
        queue: &str,
        messages: Vec<(String, HashMap<String, String>)>,
    ) -> CloudResult<Vec<String>> {
        messages
            .into_iter()
            .map(|(body, attrs)| self.send(queue, &body, attrs))
            .collect()
    }

    fn receive(
        &self,
        queue: &str,
        max_messages: u32,
        _visibility_timeout_secs: u32,
    ) -> CloudResult<Vec<QueueMessage>> {
        let mut queues = self.queues.lock().expect("queues mutex poisoned");
        let q = queues.entry(queue.to_string()).or_default();

        let count = std::cmp::min(max_messages as usize, q.len());
        let messages = q.drain(0..count).collect();
        drop(queues);
        Ok(messages)
    }

    fn delete(&self, _queue: &str, _receipt_handle: &str) -> CloudResult<()> {
        Ok(())
    }

    fn delete_batch(&self, _queue: &str, _receipt_handles: Vec<String>) -> CloudResult<()> {
        Ok(())
    }

    fn queue_size(&self, queue: &str) -> CloudResult<u64> {
        let queues = self.queues.lock().expect("queues mutex poisoned");
        Ok(queues.get(queue).map_or(0, |q| q.len() as u64))
    }

    fn purge(&self, queue: &str) -> CloudResult<()> {
        if let Some(q) = self
            .queues
            .lock()
            .expect("queues mutex poisoned")
            .get_mut(queue)
        {
            q.clear();
        }
        Ok(())
    }
}

// ============================================================================
// FakeCacheIO
// ============================================================================

#[derive(Clone)]
pub struct FakeCacheIO {
    cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl FakeCacheIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for FakeCacheIO {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheIO for FakeCacheIO {
    fn get(&self, key: &str) -> CloudResult<Option<Vec<u8>>> {
        Ok(self
            .cache
            .lock()
            .expect("cache mutex poisoned")
            .get(key)
            .cloned())
    }

    fn set(&self, key: &str, value: &[u8], _ttl_secs: Option<u64>) -> CloudResult<()> {
        self.cache
            .lock()
            .expect("cache mutex poisoned")
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> CloudResult<()> {
        self.cache.lock().expect("cache mutex poisoned").remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> CloudResult<bool> {
        Ok(self
            .cache
            .lock()
            .expect("cache mutex poisoned")
            .contains_key(key))
    }

    fn get_batch(&self, keys: Vec<String>) -> CloudResult<Vec<Option<Vec<u8>>>> {
        let cache = self.cache.lock().expect("cache mutex poisoned");
        Ok(keys.into_iter().map(|k| cache.get(&k).cloned()).collect())
    }

    fn set_batch(&self, items: Vec<(String, Vec<u8>, Option<u64>)>) -> CloudResult<()> {
        let mut cache = self.cache.lock().expect("cache mutex poisoned");
        for (key, value, _ttl) in items {
            cache.insert(key, value);
        }
        drop(cache);
        Ok(())
    }

    fn increment(&self, key: &str, delta: i64) -> CloudResult<i64> {
        let mut cache = self.cache.lock().expect("cache mutex poisoned");
        let current = cache
            .get(key)
            .and_then(|v| String::from_utf8(v.clone()).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let new_value = current + delta;
        cache.insert(key.to_string(), new_value.to_string().into_bytes());
        drop(cache);
        Ok(new_value)
    }

    fn flush(&self) -> CloudResult<()> {
        self.cache.lock().expect("cache mutex poisoned").clear();
        Ok(())
    }
}

// ============================================================================
// FakeGraphIO
// ============================================================================

#[derive(Clone)]
pub struct FakeGraphIO {
    nodes: Arc<Mutex<HashMap<String, GraphNode>>>,
    edges: Arc<Mutex<HashMap<String, GraphEdge>>>,
    node_counter: Arc<Mutex<u64>>,
    edge_counter: Arc<Mutex<u64>>,
}

impl FakeGraphIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
            edges: Arc::new(Mutex::new(HashMap::new())),
            node_counter: Arc::new(Mutex::new(0)),
            edge_counter: Arc::new(Mutex::new(0)),
        }
    }

    fn next_node_id(&self) -> String {
        let mut counter = self
            .node_counter
            .lock()
            .expect("node_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("node-{id}")
    }

    fn next_edge_id(&self) -> String {
        let mut counter = self
            .edge_counter
            .lock()
            .expect("edge_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("edge-{id}")
    }
}

impl Default for FakeGraphIO {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphIO for FakeGraphIO {
    fn add_node(
        &self,
        labels: Vec<String>,
        properties: HashMap<String, String>,
    ) -> CloudResult<String> {
        let id = self.next_node_id();
        let node = GraphNode {
            id: id.clone(),
            labels,
            properties,
        };
        self.nodes
            .lock()
            .expect("nodes mutex poisoned")
            .insert(id.clone(), node);
        Ok(id)
    }

    fn get_node(&self, id: &str) -> CloudResult<Option<GraphNode>> {
        Ok(self
            .nodes
            .lock()
            .expect("nodes mutex poisoned")
            .get(id)
            .cloned())
    }

    fn update_node(&self, id: &str, properties: HashMap<String, String>) -> CloudResult<()> {
        let mut nodes = self.nodes.lock().expect("nodes mutex poisoned");
        if let Some(node) = nodes.get_mut(id) {
            node.properties.extend(properties);
            Ok(())
        } else {
            Err(CloudIOError::new(
                ErrorKind::NotFound,
                format!("Node {id} not found"),
            ))
        }
    }

    fn delete_node(&self, id: &str) -> CloudResult<()> {
        self.nodes.lock().expect("nodes mutex poisoned").remove(id);
        Ok(())
    }

    fn add_edge(
        &self,
        from: &str,
        to: &str,
        label: &str,
        properties: HashMap<String, String>,
    ) -> CloudResult<String> {
        let id = self.next_edge_id();
        let edge = GraphEdge {
            id: id.clone(),
            label: label.to_string(),
            from_node: from.to_string(),
            to_node: to.to_string(),
            properties,
        };
        self.edges
            .lock()
            .expect("edges mutex poisoned")
            .insert(id.clone(), edge);
        Ok(id)
    }

    fn get_edge(&self, id: &str) -> CloudResult<Option<GraphEdge>> {
        Ok(self
            .edges
            .lock()
            .expect("edges mutex poisoned")
            .get(id)
            .cloned())
    }

    fn delete_edge(&self, id: &str) -> CloudResult<()> {
        self.edges.lock().expect("edges mutex poisoned").remove(id);
        Ok(())
    }

    fn query(
        &self,
        _query: &str,
        _params: HashMap<String, String>,
    ) -> CloudResult<Vec<HashMap<String, String>>> {
        // Simple fake - return empty results
        Ok(Vec::new())
    }

    fn get_neighbors(
        &self,
        node_id: &str,
        direction: EdgeDirection,
    ) -> CloudResult<Vec<GraphNode>> {
        let edges = self.edges.lock().expect("edges mutex poisoned");

        let neighbor_ids: Vec<String> = edges
            .values()
            .filter_map(|edge| match direction {
                EdgeDirection::Outgoing => {
                    if edge.from_node == node_id {
                        Some(edge.to_node.clone())
                    } else {
                        None
                    }
                }
                EdgeDirection::Incoming => {
                    if edge.to_node == node_id {
                        Some(edge.from_node.clone())
                    } else {
                        None
                    }
                }
                EdgeDirection::Both => {
                    if edge.from_node == node_id {
                        Some(edge.to_node.clone())
                    } else if edge.to_node == node_id {
                        Some(edge.from_node.clone())
                    } else {
                        None
                    }
                }
            })
            .collect();

        drop(edges);

        let nodes = self.nodes.lock().expect("nodes mutex poisoned");
        Ok(neighbor_ids
            .iter()
            .filter_map(|id| nodes.get(id).cloned())
            .collect())
    }
}

// ============================================================================
// FakeComputeIO
// ============================================================================

#[derive(Clone)]
pub struct FakeComputeIO {
    functions: FunctionMap,
    invocation_counter: Arc<Mutex<u64>>,
}

impl FakeComputeIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            functions: Arc::new(Mutex::new(HashMap::new())),
            invocation_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Registers a function handler for testing.
    ///
    /// # Panics
    ///
    /// Panics if the mutex protecting the functions is poisoned.
    pub fn register_function<F>(&self, name: &str, func: F)
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        self.functions
            .lock()
            .expect("functions mutex poisoned")
            .insert(name.to_string(), Box::new(func));
    }

    fn next_invocation_id(&self) -> String {
        let mut counter = self
            .invocation_counter
            .lock()
            .expect("invocation_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("inv-{id}")
    }
}

impl Default for FakeComputeIO {
    fn default() -> Self {
        Self::new()
    }
}

impl ComputeIO for FakeComputeIO {
    fn invoke(&self, function_name: &str, payload: &[u8]) -> CloudResult<ComputeResult> {
        let functions = self.functions.lock().expect("functions mutex poisoned");
        let func = functions.get(function_name).ok_or_else(|| {
            CloudIOError::new(
                ErrorKind::NotFound,
                format!("Function {function_name} not found"),
            )
        })?;

        let output = func(payload);
        drop(functions);

        Ok(ComputeResult {
            status_code: 200,
            output,
            logs: Some("Fake function executed".to_string()),
            execution_time_ms: 10,
        })
    }

    fn invoke_async(&self, _function_name: &str, _payload: &[u8]) -> CloudResult<String> {
        Ok(self.next_invocation_id())
    }

    fn get_invocation_status(&self, _invocation_id: &str) -> CloudResult<InvocationStatus> {
        Ok(InvocationStatus::Succeeded)
    }

    fn list_functions(&self) -> CloudResult<Vec<String>> {
        let functions = self.functions.lock().expect("functions mutex poisoned");
        let mut names: Vec<String> = functions.keys().cloned().collect();
        drop(functions);
        names.sort();
        Ok(names)
    }
}

// ============================================================================
// FakeNotificationIO
// ============================================================================

#[derive(Clone)]
pub struct FakeNotificationIO {
    topics: Arc<Mutex<HashMap<String, Vec<Notification>>>>,
    subscriptions: Arc<Mutex<HashMap<String, String>>>,
    message_counter: Arc<Mutex<u64>>,
}

impl FakeNotificationIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            topics: Arc::new(Mutex::new(HashMap::new())),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            message_counter: Arc::new(Mutex::new(0)),
        }
    }

    fn next_id(&self) -> String {
        let mut counter = self
            .message_counter
            .lock()
            .expect("message_counter mutex poisoned");
        *counter += 1;
        let id = *counter;
        drop(counter);
        format!("msg-{id}")
    }
}

impl Default for FakeNotificationIO {
    fn default() -> Self {
        Self::new()
    }
}

impl NotificationIO for FakeNotificationIO {
    fn send(&self, notification: Notification) -> CloudResult<NotificationResult> {
        let msg_id = self.next_id();
        let target = notification.target.clone();
        self.topics
            .lock()
            .expect("topics mutex poisoned")
            .entry(target)
            .or_default()
            .push(notification);

        Ok(NotificationResult {
            message_id: msg_id,
            status: NotificationStatus::Sent,
        })
    }

    fn send_batch(&self, notifications: Vec<Notification>) -> CloudResult<Vec<NotificationResult>> {
        notifications.into_iter().map(|n| self.send(n)).collect()
    }

    fn subscribe(&self, topic: &str, endpoint: &str, _protocol: &str) -> CloudResult<String> {
        let sub_id = format!("sub-{topic}-{endpoint}");
        self.subscriptions
            .lock()
            .expect("subscriptions mutex poisoned")
            .insert(sub_id.clone(), topic.to_string());
        Ok(sub_id)
    }

    fn unsubscribe(&self, subscription_id: &str) -> CloudResult<()> {
        self.subscriptions
            .lock()
            .expect("subscriptions mutex poisoned")
            .remove(subscription_id);
        Ok(())
    }

    fn create_topic(&self, name: &str) -> CloudResult<String> {
        self.topics
            .lock()
            .expect("topics mutex poisoned")
            .insert(name.to_string(), Vec::new());
        Ok(name.to_string())
    }

    fn delete_topic(&self, topic: &str) -> CloudResult<()> {
        self.topics
            .lock()
            .expect("topics mutex poisoned")
            .remove(topic);
        Ok(())
    }
}

// ============================================================================
// FakeIntelligenceIO
// ============================================================================

#[derive(Clone)]
pub struct FakeIntelligenceIO {
    models: FunctionMap,
}

impl FakeIntelligenceIO {
    #[must_use]
    pub fn new() -> Self {
        Self {
            models: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Registers a model handler for testing.
    ///
    /// # Panics
    ///
    /// Panics if the mutex protecting the models is poisoned.
    pub fn register_model<F>(&self, name: &str, model: F)
    where
        F: Fn(&[u8]) -> Vec<u8> + Send + Sync + 'static,
    {
        self.models
            .lock()
            .expect("models mutex poisoned")
            .insert(name.to_string(), Box::new(model));
    }
}

impl Default for FakeIntelligenceIO {
    fn default() -> Self {
        Self::new()
    }
}

impl IntelligenceIO for FakeIntelligenceIO {
    fn predict(&self, model_name: &str, input: InferenceInput) -> CloudResult<InferenceOutput> {
        let models = self.models.lock().expect("models mutex poisoned");
        let model = models.get(model_name).ok_or_else(|| {
            CloudIOError::new(ErrorKind::NotFound, format!("Model {model_name} not found"))
        })?;

        let output_data = model(&input.data);
        drop(models);

        Ok(InferenceOutput {
            data: output_data,
            content_type: "application/json".to_string(),
            model_version: Some("v1".to_string()),
            inference_time_ms: 50,
        })
    }

    fn predict_batch(
        &self,
        model_name: &str,
        inputs: Vec<InferenceInput>,
    ) -> CloudResult<Vec<InferenceOutput>> {
        inputs
            .into_iter()
            .map(|input| self.predict(model_name, input))
            .collect()
    }

    fn list_models(&self) -> CloudResult<Vec<String>> {
        let models = self.models.lock().expect("models mutex poisoned");
        let mut names: Vec<String> = models.keys().cloned().collect();
        drop(models);
        names.sort();
        Ok(names)
    }

    fn get_model_info(&self, model_name: &str) -> CloudResult<HashMap<String, String>> {
        let models = self.models.lock().expect("models mutex poisoned");
        if models.contains_key(model_name) {
            let mut info = HashMap::new();
            info.insert("name".to_string(), model_name.to_string());
            info.insert("version".to_string(), "v1".to_string());
            Ok(info)
        } else {
            Err(CloudIOError::new(
                ErrorKind::NotFound,
                format!("Model {model_name} not found"),
            ))
        }
    }
}
