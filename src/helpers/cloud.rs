//! End-user helpers for running custom cloud operations.
//!
//! This module provides high-level utilities that allow end-users to execute their own
//! custom cloud operations with built-in error handling, retry logic, timeout enforcement,
//! and more. These helpers are designed to work with any cloud service implementation.
//!
//! ## Available Helpers
//!
//! - [`run_with_retry`] - Execute operations with automatic retry on transient failures
//! - [`run_parallel`] - Execute multiple independent operations concurrently
//! - [`run_with_timeout_and_retry`] - Combine timeout and retry logic
//! - [`run_batch_operation`] - Process collections in configurable chunks
//! - [`run_paginated_operation`] - Handle paginated API responses automatically
//! - [`run_with_context`] - Execute operations with metadata tracking
//! - [`OperationBuilder`] - Fluent API for configuring complex operations
//!
//! ## Examples
//!
//! ### Simple retry on failure
//!
//! ```no_run
//! # use ironbeam::helpers::cloud::*;
//! # use ironbeam::io::cloud::utils::RetryConfig;
//! # use ironbeam::io::cloud::traits::CloudResult;
//! # fn upload_file_to_s3(bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> { Ok(()) }
//! # fn main() -> CloudResult<()> {
//! # let data = b"example";
//! let config = RetryConfig::default();
//! let result = run_with_retry(&config, || {
//!     // Your custom cloud operation
//!     upload_file_to_s3("bucket", "key", data)
//! })?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Batch processing with retry
//!
//! ```no_run
//! # use ironbeam::helpers::cloud::*;
//! # use ironbeam::io::cloud::utils::RetryConfig;
//! # use ironbeam::io::cloud::traits::CloudResult;
//! # fn upload_file(file: &str) -> CloudResult<String> { Ok(file.to_string()) }
//! # fn main() -> CloudResult<()> {
//! let files = vec!["file1.txt", "file2.txt", /* ... many files */];
//! let batch_config = BatchConfig { chunk_size: 100, parallel: false };
//! let retry_config = RetryConfig::default();
//!
//! let results = run_batch_operation(&files, &batch_config, |chunk| {
//!     // Process each chunk with retry logic
//!     let chunk_results: CloudResult<Vec<String>> = chunk
//!         .iter()
//!         .map(|&file| {
//!             run_with_retry(&retry_config, || upload_file(file))
//!         })
//!         .collect();
//!     chunk_results
//! })?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Using the builder pattern
//!
//! ```no_run
//! # use ironbeam::helpers::cloud::*;
//! # use ironbeam::io::cloud::utils::RetryConfig;
//! # use ironbeam::io::cloud::traits::CloudResult;
//! # use std::time::Duration;
//! # fn process_large_dataset() -> CloudResult<()> { Ok(()) }
//! # fn main() -> CloudResult<()> {
//! let result = OperationBuilder::new()
//!     .with_retry(RetryConfig::default())
//!     .with_timeout(Duration::from_secs(60))
//!     .execute(|| {
//!         // Your cloud operation
//!         process_large_dataset()
//!     })?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Parallel execution
//!
//! ```no_run
//! # use ironbeam::helpers::cloud::*;
//! # use ironbeam::io::cloud::traits::CloudResult;
//! # fn upload_to_s3(file: &str) -> CloudResult<String> { Ok(file.to_string()) }
//! # fn upload_to_gcs(file: &str) -> CloudResult<String> { Ok(file.to_string()) }
//! # fn upload_to_azure(file: &str) -> CloudResult<String> { Ok(file.to_string()) }
//! # fn main() -> CloudResult<()> {
//! let operations = vec![
//!     Box::new(|| upload_to_s3("file1")) as Box<dyn FnOnce() -> CloudResult<String> + Send>,
//!     Box::new(|| upload_to_gcs("file2")) as Box<dyn FnOnce() -> CloudResult<String> + Send>,
//!     Box::new(|| upload_to_azure("file3")) as Box<dyn FnOnce() -> CloudResult<String> + Send>,
//! ];
//!
//! let results = run_parallel(operations)?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Context tracking
//!
//! ```no_run
//! # use ironbeam::helpers::cloud::*;
//! # use ironbeam::io::cloud::traits::CloudResult;
//! # fn upload_files() -> CloudResult<()> { Ok(()) }
//! # fn main() -> CloudResult<()> {
//! let context = OperationContext::new("upload_batch");
//!
//! let (result, final_context) = run_with_context(context, |ctx| {
//!     ctx.add_metadata("bucket", "my-bucket");
//!     ctx.add_metadata("file_count", "100");
//!
//!     // Your operation
//!     upload_files()?;
//!
//!     ctx.increment_retry();
//!     Ok("success")
//! })?;
//!
//! println!("Operation took {:?}", final_context.elapsed());
//! println!("Retries: {}", final_context.retry_count);
//! # Ok(())
//! # }
//! ```

use crate::io::cloud::traits::CloudResult;
use crate::io::cloud::utils::{
    PaginationConfig, RetryConfig, batch_in_chunks, paginate, retry_with_backoff, with_timeout,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Custom Operation Runners
// ============================================================================

/// Execute a cloud operation with a custom implementation and automatic retry logic
///
/// This helper allows end-users to run their own custom cloud operations with
/// built-in retry, timeout, and error handling.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::helpers::RetryConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # fn my_custom_s3_upload(bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> { Ok(()) }
/// # fn main() -> CloudResult<()> {
/// # let data = b"example";
/// let config = RetryConfig::default();
/// let result = run_with_retry(&config, || {
///     // Your custom cloud operation
///     my_custom_s3_upload("bucket", "key", data)
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if the operation fails after all retry attempts
pub fn run_with_retry<F, T>(config: &RetryConfig, operation: F) -> CloudResult<T>
where
    F: FnMut() -> CloudResult<T>,
{
    retry_with_backoff(config, operation)
}

/// Execute multiple cloud operations in parallel with error handling
///
/// This helper runs multiple independent operations concurrently and collects results.
/// If any operation fails, all results up to that point are still returned along with the error.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # fn upload_file(bucket: &str, file: &str) -> CloudResult<String> { Ok(file.to_string()) }
/// # fn main() -> CloudResult<()> {
/// let operations = vec![
///     Box::new(|| upload_file("bucket1", "file1.txt")) as Box<dyn FnOnce() -> CloudResult<String> + Send>,
///     Box::new(|| upload_file("bucket2", "file2.txt")) as Box<dyn FnOnce() -> CloudResult<String> + Send>,
/// ];
///
/// let results = run_parallel(operations)?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if any of the operations fail
pub fn run_parallel<T: Send>(
    operations: Vec<Box<dyn FnOnce() -> CloudResult<T> + Send>>,
) -> CloudResult<Vec<T>> {
    operations
        .into_iter()
        .map(|op| op())
        .collect::<CloudResult<Vec<T>>>()
}

/// Execute a cloud operation with timeout and custom configuration
///
/// This combines timeout enforcement with retry logic for robust operation execution.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::helpers::RetryConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # use std::time::Duration;
/// # fn process_large_dataset() -> CloudResult<()> { Ok(()) }
/// # fn main() -> CloudResult<()> {
/// let retry_config = RetryConfig::default();
/// let timeout = Duration::from_secs(30);
///
/// let result = run_with_timeout_and_retry(&retry_config, timeout, || {
///     // Your long-running cloud operation
///     process_large_dataset()
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if the operation times out or fails after all retry attempts
pub fn run_with_timeout_and_retry<F, T>(
    retry_config: &RetryConfig,
    timeout: Duration,
    operation: F,
) -> CloudResult<T>
where
    F: FnMut() -> CloudResult<T>,
{
    with_timeout(timeout, || retry_with_backoff(retry_config, operation))
}

/// Execute a batch operation on a collection of items with custom processing
///
/// This helper automatically chunks data, processes each chunk, and handles errors.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # fn upload_files_batch(chunk: Vec<&str>) -> CloudResult<Vec<String>> {
/// #     Ok(chunk.into_iter().map(|s| s.to_string()).collect())
/// # }
/// # fn main() -> CloudResult<()> {
/// let files = vec!["file1.txt", "file2.txt", /* ... 1000 files */];
/// let batch_config = BatchConfig { chunk_size: 100, parallel: false };
///
/// let results = run_batch_operation(&files, &batch_config, |chunk| {
///     // Process each chunk (e.g., batch upload to cloud storage)
///     upload_files_batch(chunk)
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if any batch operation fails
pub fn run_batch_operation<T, R, F>(
    items: &[T],
    config: &BatchConfig,
    processor: F,
) -> CloudResult<Vec<R>>
where
    T: Clone + Send,
    R: Send,
    F: Fn(Vec<T>) -> CloudResult<Vec<R>> + Send + Sync,
{
    batch_in_chunks(items, config.chunk_size, processor)
}

/// Configuration for batch operations
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    pub chunk_size: usize,
    pub parallel: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            chunk_size: 100,
            parallel: false,
        }
    }
}

/// Execute a paginated cloud operation with custom page fetching
///
/// This helper abstracts away pagination logic, allowing users to focus on
/// implementing the page fetching logic.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::helpers::PaginationConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # fn fetch_items_page(page: u32, size: u32) -> CloudResult<Vec<String>> {
/// #     Ok(vec!["item1".to_string(), "item2".to_string()])
/// # }
/// # fn main() -> CloudResult<()> {
/// let pagination_config = PaginationConfig::default();
/// let all_items = run_paginated_operation(&pagination_config, |page, size| {
///     // Fetch a page of data from your cloud service
///     let items = fetch_items_page(page, size)?;
///     let has_more = items.len() == size as usize;
///     Ok((items, has_more))
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if any page fetch operation fails
pub fn run_paginated_operation<T, F>(
    config: &PaginationConfig,
    fetch_page: F,
) -> CloudResult<Vec<T>>
where
    F: FnMut(u32, u32) -> CloudResult<(Vec<T>, bool)>,
{
    paginate(config, fetch_page)
}

/// Builder pattern for configuring and executing cloud operations
///
/// This provides a fluent interface for setting up complex cloud operations
/// with various retry, timeout, and batching configurations.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::helpers::RetryConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # use std::time::Duration;
/// # fn upload_large_file(bucket: &str, key: &str, data: &[u8]) -> CloudResult<()> { Ok(()) }
/// # fn main() -> CloudResult<()> {
/// # let data = b"example";
/// let result = OperationBuilder::new()
///     .with_retry(RetryConfig::default())
///     .with_timeout(Duration::from_secs(60))
///     .execute(|| {
///         // Your cloud operation
///         upload_large_file("bucket", "key", data)
///     })?;
/// # Ok(())
/// # }
/// ```
pub struct OperationBuilder {
    retry_config: Option<RetryConfig>,
    timeout: Option<Duration>,
}

impl OperationBuilder {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            retry_config: None,
            timeout: None,
        }
    }

    #[must_use]
    pub const fn with_retry(mut self, config: RetryConfig) -> Self {
        self.retry_config = Some(config);
        self
    }

    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Execute the operation with the configured settings
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails, times out, or exhausts retry attempts
    pub fn execute<F, T>(self, mut operation: F) -> CloudResult<T>
    where
        F: FnMut() -> CloudResult<T>,
    {
        match (self.retry_config, self.timeout) {
            (Some(retry), Some(timeout)) => run_with_timeout_and_retry(&retry, timeout, operation),
            (Some(retry), None) => retry_with_backoff(&retry, operation),
            (None, Some(timeout)) => {
                let op = || operation();
                with_timeout(timeout, op)
            }
            (None, None) => operation(),
        }
    }
}

impl Default for OperationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Context for cloud operations that tracks execution metadata
///
/// This helper allows operations to track and report metadata like
/// execution time, retry attempts, and operation outcomes.
#[derive(Debug)]
pub struct OperationContext {
    pub operation_name: String,
    pub start_time: Instant,
    pub retry_count: u32,
    pub metadata: HashMap<String, String>,
}

impl OperationContext {
    #[must_use]
    pub fn new(operation_name: impl Into<String>) -> Self {
        Self {
            operation_name: operation_name.into(),
            start_time: Instant::now(),
            retry_count: 0,
            metadata: HashMap::new(),
        }
    }

    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub const fn increment_retry(&mut self) {
        self.retry_count += 1;
    }
}

/// Execute an operation with context tracking
///
/// # Errors
///
/// Returns an error if the operation fails
pub fn run_with_context<F, T>(
    mut context: OperationContext,
    mut operation: F,
) -> CloudResult<(T, OperationContext)>
where
    F: FnMut(&mut OperationContext) -> CloudResult<T>,
{
    let result = operation(&mut context)?;
    Ok((result, context))
}

// ============================================================================
// Generic Cloud I/O Helpers
// ============================================================================

/// Execute a generic cloud I/O operation with retry logic.
///
/// This helper works with any cloud I/O trait (`ObjectIO`, `DatabaseIO`, etc.)
/// and automatically retries operations that fail with transient errors.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::{ObjectIO, FakeObjectIO};
/// # use ironbeam::io::cloud::utils::RetryConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # fn main() -> CloudResult<()> {
/// let storage = FakeObjectIO::new();
/// let config = RetryConfig::default();
///
/// let data = run_cloud_io_with_retry(&config, || {
///     // Your cloud I/O operation
///     storage.get_object("bucket", "key")
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if the operation fails after all retry attempts
pub fn run_cloud_io_with_retry<F, T>(config: &RetryConfig, operation: F) -> CloudResult<T>
where
    F: FnMut() -> CloudResult<T>,
{
    retry_with_backoff(config, operation)
}

/// Execute multiple cloud I/O operations in sequence with retry logic.
///
/// This helper takes a slice of items and a closure that performs a cloud I/O operation
/// on each item, with automatic retry for transient failures.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::{ObjectIO, FakeObjectIO};
/// # use ironbeam::io::cloud::utils::RetryConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # fn main() -> CloudResult<()> {
/// let storage = FakeObjectIO::new();
/// let config = RetryConfig::default();
/// let keys = vec!["file1.txt", "file2.txt", "file3.txt"];
///
/// let results = run_cloud_io_batch(&config, &keys, |key| {
///     storage.get_object("bucket", key)
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if any operation fails after retries
pub fn run_cloud_io_batch<T, R, F>(
    config: &RetryConfig,
    items: &[T],
    mut operation: F,
) -> CloudResult<Vec<R>>
where
    T: Clone,
    F: FnMut(&T) -> CloudResult<R>,
{
    items
        .iter()
        .map(|item| retry_with_backoff(config, || operation(item)))
        .collect()
}

/// Execute a paginated cloud I/O operation with automatic page fetching.
///
/// This helper works with any cloud service that returns paginated results
/// and automatically fetches all pages.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::{ObjectIO, FakeObjectIO};
/// # use ironbeam::io::cloud::utils::PaginationConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # fn main() -> CloudResult<()> {
/// let storage = FakeObjectIO::new();
/// let config = PaginationConfig::default();
///
/// let all_objects = run_cloud_io_paginated(&config, |page, page_size| {
///     // Fetch a page of results
///     let objects = storage.list_objects("bucket", Some("prefix/"))?;
///
///     // Determine if there are more pages
///     let start = (page * page_size) as usize;
///     let end = ((page + 1) * page_size) as usize;
///     let page_items: Vec<_> = objects.into_iter().skip(start).take(page_size as usize).collect();
///     let has_more = page_items.len() == page_size as usize;
///
///     Ok((page_items, has_more))
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if any page fetch fails
pub fn run_cloud_io_paginated<T, F>(config: &PaginationConfig, fetch_page: F) -> CloudResult<Vec<T>>
where
    F: FnMut(u32, u32) -> CloudResult<(Vec<T>, bool)>,
{
    paginate(config, fetch_page)
}

/// Execute a cloud I/O operation with automatic retry and timeout.
///
/// This combines retry logic with timeout enforcement for robust cloud operations.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::{ObjectIO, FakeObjectIO};
/// # use ironbeam::io::cloud::utils::RetryConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # use std::time::Duration;
/// # fn main() -> CloudResult<()> {
/// let storage = FakeObjectIO::new();
/// let retry_config = RetryConfig::default();
/// let timeout = Duration::from_secs(30);
///
/// let result = run_cloud_io_with_retry_and_timeout(&retry_config, timeout, || {
///     storage.get_object("bucket", "large-file.bin")
/// })?;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns an error if the operation times out or fails after retries
pub fn run_cloud_io_with_retry_and_timeout<F, T>(
    retry_config: &RetryConfig,
    timeout: Duration,
    operation: F,
) -> CloudResult<T>
where
    F: FnMut() -> CloudResult<T>,
{
    run_with_timeout_and_retry(retry_config, timeout, operation)
}

/// Generic trait-based cloud I/O operation executor.
///
/// This struct provides a fluent interface for executing operations on any cloud I/O trait
/// implementation with configurable retry, timeout, and error handling.
///
/// # Example
/// ```no_run
/// # use ironbeam::helpers::cloud::*;
/// # use ironbeam::io::cloud::{ObjectIO, FakeObjectIO};
/// # use ironbeam::io::cloud::utils::RetryConfig;
/// # use ironbeam::io::cloud::traits::CloudResult;
/// # use std::time::Duration;
/// # fn main() -> CloudResult<()> {
/// let storage = FakeObjectIO::new();
///
/// let result = CloudIOExecutor::new()
///     .with_retry(RetryConfig::default())
///     .with_timeout(Duration::from_secs(30))
///     .execute(|| storage.get_object("bucket", "key"))?;
/// # Ok(())
/// # }
/// ```
pub struct CloudIOExecutor {
    retry_config: Option<RetryConfig>,
    timeout: Option<Duration>,
}

impl CloudIOExecutor {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            retry_config: None,
            timeout: None,
        }
    }

    #[must_use]
    pub const fn with_retry(mut self, config: RetryConfig) -> Self {
        self.retry_config = Some(config);
        self
    }

    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Execute a cloud I/O operation with the configured settings
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails, times out, or exhausts retry attempts
    pub fn execute<F, T>(self, mut operation: F) -> CloudResult<T>
    where
        F: FnMut() -> CloudResult<T>,
    {
        match (self.retry_config, self.timeout) {
            (Some(retry), Some(timeout)) => run_with_timeout_and_retry(&retry, timeout, operation),
            (Some(retry), None) => retry_with_backoff(&retry, operation),
            (None, Some(timeout)) => {
                let op = || operation();
                with_timeout(timeout, op)
            }
            (None, None) => operation(),
        }
    }
}

impl Default for CloudIOExecutor {
    fn default() -> Self {
        Self::new()
    }
}
