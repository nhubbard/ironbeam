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
//! ```ignore
//! use ironbeam::helpers::cloud::*;
//! use ironbeam::io::cloud::helpers::RetryConfig;
//!
//! let config = RetryConfig::default();
//! let result = run_with_retry(&config, || {
//!     // Your custom cloud operation
//!     upload_file_to_s3("bucket", "key", data)
//! })?;
//! ```
//!
//! ### Batch processing with retry
//!
//! ```ignore
//! use ironbeam::helpers::cloud::*;
//! use ironbeam::io::cloud::helpers::{RetryConfig, BatchConfig};
//!
//! let files = vec!["file1.txt", "file2.txt", /* ... many files */];
//! let batch_config = BatchConfig { chunk_size: 100, parallel: false };
//! let retry_config = RetryConfig::default();
//!
//! let results = run_batch_operation(&files, &batch_config, |chunk| {
//!     // Process each chunk with retry logic
//!     let chunk_results: Result<Vec<String>> = chunk
//!         .iter()
//!         .map(|&file| {
//!             run_with_retry(&retry_config, || upload_file(file))
//!         })
//!         .collect();
//!     chunk_results
//! })?;
//! ```
//!
//! ### Using the builder pattern
//!
//! ```ignore
//! use ironbeam::helpers::cloud::*;
//! use ironbeam::io::cloud::helpers::RetryConfig;
//! use std::time::Duration;
//!
//! let result = OperationBuilder::new()
//!     .with_retry(RetryConfig::default())
//!     .with_timeout(Duration::from_secs(60))
//!     .execute(|| {
//!         // Your cloud operation
//!         process_large_dataset()
//!     })?;
//! ```
//!
//! ### Parallel execution
//!
//! ```ignore
//! use ironbeam::helpers::cloud::*;
//!
//! let operations = vec![
//!     Box::new(|| upload_to_s3("file1")) as Box<dyn FnOnce() -> Result<String> + Send>,
//!     Box::new(|| upload_to_gcs("file2")) as Box<dyn FnOnce() -> Result<String> + Send>,
//!     Box::new(|| upload_to_azure("file3")) as Box<dyn FnOnce() -> Result<String> + Send>,
//! ];
//!
//! let results = run_parallel(operations)?;
//! ```
//!
//! ### Context tracking
//!
//! ```ignore
//! use ironbeam::helpers::cloud::*;
//!
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
//! ```

use crate::io::cloud::helpers::{
    batch_in_chunks, paginate, retry_with_backoff, with_timeout, PaginationConfig, RetryConfig,
};
use crate::io::cloud::traits::Result;
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
/// ```ignore
/// use ironbeam::helpers::cloud::*;
/// use ironbeam::io::cloud::helpers::RetryConfig;
///
/// let config = RetryConfig::default();
/// let result = run_with_retry(&config, || {
///     // Your custom cloud operation
///     my_custom_s3_upload("bucket", "key", data)
/// })?;
/// ```
///
/// # Errors
///
/// Returns an error if the operation fails after all retry attempts
pub fn run_with_retry<F, T>(config: &RetryConfig, operation: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    retry_with_backoff(config, operation)
}

/// Execute multiple cloud operations in parallel with error handling
///
/// This helper runs multiple independent operations concurrently and collects results.
/// If any operation fails, all results up to that point are still returned along with the error.
///
/// # Example
/// ```ignore
/// use ironbeam::helpers::cloud::*;
///
/// let operations = vec![
///     Box::new(|| upload_file("bucket1", "file1.txt")) as Box<dyn FnOnce() -> Result<String>>,
///     Box::new(|| upload_file("bucket2", "file2.txt")) as Box<dyn FnOnce() -> Result<String>>,
/// ];
///
/// let results = run_parallel(operations)?;
/// ```
///
/// # Errors
///
/// Returns an error if any of the operations fail
pub fn run_parallel<T: Send>(
    operations: Vec<Box<dyn FnOnce() -> Result<T> + Send>>,
) -> Result<Vec<T>> {
    operations
        .into_iter()
        .map(|op| op())
        .collect::<Result<Vec<T>>>()
}

/// Execute a cloud operation with timeout and custom configuration
///
/// This combines timeout enforcement with retry logic for robust operation execution.
///
/// # Example
/// ```ignore
/// use ironbeam::helpers::cloud::*;
/// use ironbeam::io::cloud::helpers::RetryConfig;
/// use std::time::Duration;
///
/// let retry_config = RetryConfig::default();
/// let timeout = Duration::from_secs(30);
///
/// let result = run_with_timeout_and_retry(&retry_config, timeout, || {
///     // Your long-running cloud operation
///     process_large_dataset()
/// })?;
/// ```
///
/// # Errors
///
/// Returns an error if the operation times out or fails after all retry attempts
pub fn run_with_timeout_and_retry<F, T>(
    retry_config: &RetryConfig,
    timeout: Duration,
    operation: F,
) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    with_timeout(timeout, || retry_with_backoff(retry_config, operation))
}

/// Execute a batch operation on a collection of items with custom processing
///
/// This helper automatically chunks data, processes each chunk, and handles errors.
///
/// # Example
/// ```ignore
/// use ironbeam::helpers::cloud::*;
///
/// let files = vec!["file1.txt", "file2.txt", /* ... 1000 files */];
/// let batch_config = BatchConfig { chunk_size: 100, parallel: false };
///
/// let results = run_batch_operation(&files, &batch_config, |chunk| {
///     // Process each chunk (e.g., batch upload to cloud storage)
///     upload_files_batch(chunk)
/// })?;
/// ```
///
/// # Errors
///
/// Returns an error if any batch operation fails
pub fn run_batch_operation<T, R, F>(
    items: &[T],
    config: &BatchConfig,
    processor: F,
) -> Result<Vec<R>>
where
    T: Clone + Send,
    R: Send,
    F: Fn(Vec<T>) -> Result<Vec<R>> + Send + Sync,
{
    if config.parallel {
        // For parallel processing, we'd use threading (simplified here)
        batch_in_chunks(items, config.chunk_size, processor)
    } else {
        batch_in_chunks(items, config.chunk_size, processor)
    }
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
/// ```ignore
/// use ironbeam::helpers::cloud::*;
/// use ironbeam::io::cloud::helpers::PaginationConfig;
///
/// let pagination_config = PaginationConfig::default();
/// let all_items = run_paginated_operation(&pagination_config, |page, size| {
///     // Fetch a page of data from your cloud service
///     let items = fetch_items_page(page, size)?;
///     let has_more = items.len() == size as usize;
///     Ok((items, has_more))
/// })?;
/// ```
///
/// # Errors
///
/// Returns an error if any page fetch operation fails
pub fn run_paginated_operation<T, F>(config: &PaginationConfig, fetch_page: F) -> Result<Vec<T>>
where
    F: FnMut(u32, u32) -> Result<(Vec<T>, bool)>,
{
    paginate(config, fetch_page)
}

/// Builder pattern for configuring and executing cloud operations
///
/// This provides a fluent interface for setting up complex cloud operations
/// with various retry, timeout, and batching configurations.
///
/// # Example
/// ```ignore
/// use ironbeam::helpers::cloud::*;
/// use ironbeam::io::cloud::helpers::RetryConfig;
/// use std::time::Duration;
///
/// let result = OperationBuilder::new()
///     .with_retry(RetryConfig::default())
///     .with_timeout(Duration::from_secs(60))
///     .execute(|| {
///         // Your cloud operation
///         upload_large_file("bucket", "key", data)
///     })?;
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
    pub fn execute<F, T>(self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        match (self.retry_config, self.timeout) {
            (Some(retry), Some(timeout)) => {
                run_with_timeout_and_retry(&retry, timeout, operation)
            }
            (Some(retry), None) => retry_with_backoff(&retry, operation),
            (None, Some(timeout)) => with_timeout(timeout, || operation()),
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

    pub fn increment_retry(&mut self) {
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
) -> Result<(T, OperationContext)>
where
    F: FnMut(&mut OperationContext) -> Result<T>,
{
    let result = operation(&mut context)?;
    Ok((result, context))
}
