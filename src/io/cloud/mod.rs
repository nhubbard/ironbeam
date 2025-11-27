//! Cloud I/O abstractions for cloud service integrations.
//!
//! This module provides **generic traits** for interacting with cloud services in a
//! provider-agnostic way. Each trait represents a category of cloud service (object storage,
//! databases, queues, etc.), and comes with:
//!
//! - **Synchronous interface** - All operations are blocking by design, wrapping async
//!   operations internally where needed
//! - **Fake implementations** - In-memory implementations for unit testing without external
//!   dependencies
//! - **Generic helpers** - Utilities for retry logic, batching, pagination, and more
//!
//! ## Available Cloud Service Traits
//!
//! ### Storage & Databases
//! - [`WarehouseIO`] - Analytical data warehouses (`BigQuery`, `Redshift`, `Snowflake`)
//! - [`ObjectIO`] - Object storage (S3, GCS, Azure Blob)
//! - [`DatabaseIO`] - Relational databases (RDS, Cloud SQL, Aurora)
//! - [`KeyValueIO`] - Document/NoSQL stores (`DynamoDB`, `Firestore`, Cosmos DB)
//! - [`GraphIO`] - Graph databases (Neptune, `Neo4j` on cloud)
//!
//! ### Messaging & Streaming
//! - [`PubSubIO`] - Pub/sub messaging (Kafka, Kinesis, Cloud Pub/Sub)
//! - [`QueueIO`] - Message queues (SQS, Cloud Tasks, Service Bus)
//! - [`NotificationIO`] - Push notifications (SNS, FCM, `APNs`)
//!
//! ### Compute & Intelligence
//! - [`ComputeIO`] - Serverless compute (Lambda, Cloud Functions, Azure Functions)
//! - [`IntelligenceIO`] - ML model inference (`SageMaker`, Vertex AI, Azure ML)
//!
//! ### Observability & Configuration
//! - [`MetricIO`] - Metrics & monitoring (`CloudWatch`, `Stackdriver`, Azure Monitor)
//! - [`SearchIO`] - Search & logs (`Elasticsearch`, `CloudWatch` Logs, Splunk)
//! - [`ConfigIO`] - Configuration services (Parameter Store, Secret Manager)
//! - [`CacheIO`] - In-memory caching (Redis, Memcached, `ElastiCache`)
//!
//! ## Design Principles
//!
//! ### Provider Agnostic
//! All traits use generic types and error kinds, not provider-specific SDKs. This allows
//! you to:
//! - Write unit tests with fake implementations
//! - Switch cloud providers without changing business logic
//! - Use multiple providers in the same pipeline
//!
//! ### Synchronous by Design
//! Unlike most cloud SDKs which are async-first, these traits are synchronous. This matches
//! Ironbeam's execution model and avoids the complexity of async Rust. Implementations can
//! use `tokio` or similar internally, but expose a blocking interface.
//!
//! ### Credential & Config Abstraction
//! - [`CloudCredentials`] - Trait for passing authentication credentials
//! - [`CloudConfig`] - Trait for passing service configuration
//!
//! Both use trait objects to allow any implementation without generic parameters.
//!
//! ## Usage Patterns
//!
//! ### Unit Testing with Fakes
//! ```
//! use ironbeam::io::cloud::*;
//! use std::collections::HashMap;
//!
//! # fn test_my_logic() -> Result<()> {
//! // Create fake implementations (all state is in-memory)
//! let storage = FakeObjectIO::new();
//! let queue = FakeQueueIO::new();
//!
//! // Use exactly like real cloud services
//! storage.put_object("bucket", "key", b"data")?;
//! queue.send("work-queue", "task", HashMap::new())?;
//!
//! // Verify behavior
//! assert!(storage.object_exists("bucket", "key")?);
//! assert_eq!(queue.queue_size("work-queue")?, 1);
//! # Ok(())
//! # }
//! ```
//!
//! ### Implementing for Real Providers
//! ```ignore
//! // Example: AWS S3 implementation of ObjectIO
//! use ironbeam::io::cloud::*;
//! use aws_sdk_s3::Client;
//!
//! struct S3ObjectIO {
//!     client: Client,
//! }
//!
//! impl ObjectIO for S3ObjectIO {
//!     fn put_object(&self, bucket: &str, key: &str, data: &[u8]) -> Result<()> {
//!         // Use helpers::retry_with_backoff for resilience
//!         let rt = tokio::runtime::Runtime::new()?;
//!         rt.block_on(async {
//!             self.client.put_object()
//!                 .bucket(bucket)
//!                 .key(key)
//!                 .body(data.into())
//!                 .send()
//!                 .await
//!         })?;
//!         Ok(())
//!     }
//!     // ... implement other methods
//! }
//! ```
//!
//! ### Using Helpers
//! ```
//! use ironbeam::io::cloud::helpers::*;
//! use ironbeam::io::cloud::*;
//!
//! # fn example() -> Result<()> {
//! // Retry with exponential backoff
//! let config = RetryConfig::default();
//! let result = retry_with_backoff(&config, || {
//!     // Your cloud API call that might fail transiently
//!     Ok(42)
//! })?;
//!
//! // Batch processing with chunking
//! let items = vec![1, 2, 3, 4, 5];
//! let results = batch_in_chunks(&items, 2, |chunk| {
//!     // Process each chunk
//!     Ok(chunk.iter().map(|x| x * 2).collect())
//! })?;
//!
//! // Parse resource URIs
//! let (provider, parts) = parse_resource_uri("s3://my-bucket/path/to/file")?;
//! assert_eq!(provider, "s3");
//! assert_eq!(parts, vec!["my-bucket", "path", "to", "file"]);
//! # Ok(())
//! # }
//! ```
//!
//! ## Error Handling
//!
//! All operations return [`Result<T>`] where the error is [`CloudIOError`].
//! Errors are categorized by [`ErrorKind`]:
//! - `Authentication` / `Authorization` - Credential issues
//! - `NotFound` / `AlreadyExists` - Resource state
//! - `Network` / `Timeout` / `ServiceUnavailable` - Transient failures
//! - `RateLimited` - Throttling (retry-able)
//! - `InvalidInput` - Bad parameters
//! - `InternalError` / `Other` - Catch-all
//!
//! The `helpers` module provides utilities for automatically retrying transient errors.
//!
//! ## Module Structure
//!
//! - [`traits`] - Core trait definitions and types
//! - [`fake`] - In-memory fake implementations for testing
//! - [`helpers`] - Generic utilities for implementing cloud I/O
//!
//! ## Examples
//!
//! See `examples/cloud_io_demo.rs` for a complete demonstration of all cloud I/O traits,
//! and `tests/cloud_io_tests.rs` for comprehensive integration tests.

pub mod fake;
pub mod helpers;
pub mod traits;

pub use fake::*;
pub use traits::*;
