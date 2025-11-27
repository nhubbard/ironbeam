//! Generic cloud helpers for implementing custom cloud IO solutions.
//!
//! These helpers provide common patterns and utilities that can be reused
//! when implementing cloud IO traits for specific providers.
//!
//! ## End-User Helpers
//!
//! High-level helpers for running custom cloud operations have been moved to
//! [`crate::helpers::cloud`]. See that module for:
//!
//! - [`crate::helpers::cloud::run_with_retry`] - Execute with automatic retry
//! - [`crate::helpers::cloud::run_parallel`] - Execute multiple operations concurrently
//! - [`crate::helpers::cloud::run_with_timeout_and_retry`] - Combine timeout and retry
//! - [`crate::helpers::cloud::run_batch_operation`] - Process in configurable chunks
//! - [`crate::helpers::cloud::run_paginated_operation`] - Handle paginated responses
//! - [`crate::helpers::cloud::OperationBuilder`] - Fluent API for operation configuration
//! - [`crate::helpers::cloud::run_with_context`] - Track execution metadata
//!
//! ## Implementation Helpers
//!
//! Lower-level utilities for implementing cloud service integrations:
//!
//! - [`retry_with_backoff`] - Retry with exponential backoff
//! - [`with_timeout`] - Execute with timeout enforcement
//! - [`batch_in_chunks`] - Split data into chunks for batch processing
//! - [`paginate`] - Fetch all pages from paginated APIs
//! - [`parse_resource_uri`] - Parse cloud resource URIs
//! - [`validate_resource_name`] - Validate resource names
//! - [`ConnectionPool`] - Simple connection pooling

use crate::io::cloud::fake::{FakeConfig, FakeCredentials};
use crate::io::cloud::traits::{CloudConfig, CloudCredentials, CloudIOError, ErrorKind, Result};
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ============================================================================
// Retry Helper
// ============================================================================

/// Configuration for retry behavior
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        }
    }
}

/// Retry a function with exponential backoff
///
/// This helper automatically retries operations that fail due to transient errors
/// like network issues or rate limiting.
///
/// # Example
/// ```ignore
/// let result = retry_with_backoff(&retry_config, || {
///     // Your cloud API call here
///     api_client.get_data()
/// })?;
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The operation fails with a non-retryable error kind
/// - The maximum number of retry attempts is exceeded
/// - The underlying operation returns an error that should not be retried
pub fn retry_with_backoff<F, T>(config: &RetryConfig, mut operation: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let mut attempt = 0;
    let mut delay_ms = config.initial_delay_ms;

    loop {
        attempt += 1;
        match operation() {
            Ok(result) => return Ok(result),
            Err(err) => {
                // Check if we should retry based on error kind
                let should_retry = matches!(
                    err.kind,
                    ErrorKind::Network
                        | ErrorKind::Timeout
                        | ErrorKind::ServiceUnavailable
                        | ErrorKind::RateLimited
                );

                if !should_retry || attempt >= config.max_attempts {
                    return Err(err);
                }

                // Sleep before retry
                std::thread::sleep(Duration::from_millis(delay_ms));

                // Calculate next delay with exponential backoff
                // We use saturating operations to avoid overflow
                // For a typical backoff multiplier of 2.0, we just double the delay
                let new_delay = if config.backoff_multiplier >= 2.0 {
                    delay_ms.saturating_mul(2)
                } else {
                    delay_ms
                };
                delay_ms = new_delay.min(config.max_delay_ms);
            }
        }
    }
}

// ============================================================================
// Timeout Helper
// ============================================================================

/// Execute an operation with a timeout
///
/// This helper ensures that operations complete within a specified time limit.
/// Note: This uses a simple polling approach for synchronous operations.
///
/// # Example
/// ```ignore
/// let result = with_timeout(Duration::from_secs(30), || {
///     // Your long-running operation
///     process_large_file()
/// })?;
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The operation itself returns an error
/// - The operation exceeds the specified timeout duration
pub fn with_timeout<F, T>(timeout: Duration, operation: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    let start = Instant::now();
    let result = operation()?;

    if start.elapsed() > timeout {
        Err(CloudIOError::new(
            ErrorKind::Timeout,
            format!("Operation exceeded timeout of {timeout:?}"),
        ))
    } else {
        Ok(result)
    }
}

// ============================================================================
// Batch Helper
// ============================================================================

/// Split a batch operation into smaller chunks
///
/// Many cloud APIs have limits on batch sizes. This helper automatically
/// chunks large batches and processes them in multiple requests.
///
/// # Example
/// ```ignore
/// let items = vec![...]; // 1000 items
/// let results = batch_in_chunks(&items, 100, |chunk| {
///     api_client.batch_upload(chunk)
/// })?;
/// ```
///
/// # Errors
///
/// Returns an error if any chunk processing operation fails
pub fn batch_in_chunks<T, R, F>(
    items: &[T],
    chunk_size: usize,
    mut process_chunk: F,
) -> Result<Vec<R>>
where
    T: Clone,
    F: FnMut(Vec<T>) -> Result<Vec<R>>,
{
    let mut results = Vec::new();

    for chunk in items.chunks(chunk_size) {
        let chunk_results = process_chunk(chunk.to_vec())?;
        results.extend(chunk_results);
    }

    Ok(results)
}

// ============================================================================
// Pagination Helper
// ============================================================================

/// Configuration for pagination
#[derive(Debug, Clone, Copy)]
pub struct PaginationConfig {
    pub page_size: u32,
    pub max_pages: Option<u32>,
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self {
            page_size: 100,
            max_pages: None,
        }
    }
}

/// Helper for paginating through results
///
/// Many cloud APIs return paginated results. This helper automatically
/// fetches all pages and combines them.
///
/// # Example
/// ```ignore
/// let config = PaginationConfig { page_size: 50, max_pages: Some(10) };
/// let all_items = paginate(&config, |page, page_size| {
///     api_client.list_items(page, page_size)
/// })?;
/// ```
///
/// # Errors
///
/// Returns an error if any page fetch operation fails
pub fn paginate<T, F>(config: &PaginationConfig, mut fetch_page: F) -> Result<Vec<T>>
where
    F: FnMut(u32, u32) -> Result<(Vec<T>, bool)>, // Returns (items, has_more)
{
    let mut all_items = Vec::new();
    let mut page = 0;

    loop {
        let (items, has_more) = fetch_page(page, config.page_size)?;

        if items.is_empty() {
            break;
        }

        all_items.extend(items);
        page += 1;

        if !has_more {
            break;
        }

        if let Some(max_pages) = config.max_pages
            && page >= max_pages
        {
            break;
        }
    }

    Ok(all_items)
}

// ============================================================================
// Connection Pool Helper
// ============================================================================

/// Simple connection pool for cloud services
///
/// This provides basic connection pooling to reuse connections across
/// multiple operations, which can improve performance.
pub struct ConnectionPool<T> {
    connections: Vec<T>,
    max_size: usize,
}

impl<T> ConnectionPool<T> {
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            connections: Vec::with_capacity(max_size),
            max_size,
        }
    }

    /// Acquire a connection from the pool or create a new one
    ///
    /// # Errors
    ///
    /// Returns an error if the connection creation function fails
    pub fn acquire<F>(&mut self, create: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        self.connections.pop().map_or_else(create, |conn| Ok(conn))
    }

    pub fn release(&mut self, connection: T) {
        if self.connections.len() < self.max_size {
            self.connections.push(connection);
        }
    }

    #[must_use]
    pub const fn size(&self) -> usize {
        self.connections.len()
    }
}

// ============================================================================
// Credential Helpers
// ============================================================================

/// Helper for loading credentials from environment variables
///
/// # Errors
///
/// Returns an error if no environment variables matching the prefix are found
pub fn credentials_from_env(prefix: &str) -> Result<HashMap<String, String>> {
    let mut creds = HashMap::new();

    for (key, value) in std::env::vars() {
        if key.starts_with(prefix) {
            let key_name = key.strip_prefix(prefix).unwrap_or(&key);
            creds.insert(key_name.to_lowercase(), value);
        }
    }

    if creds.is_empty() {
        return Err(CloudIOError::new(
            ErrorKind::Authentication,
            format!("No credentials found with prefix '{prefix}'"),
        ));
    }

    Ok(creds)
}

/// Helper for loading config from environment variables
#[must_use]
pub fn config_from_env(prefix: &str) -> HashMap<String, String> {
    let mut config = HashMap::new();

    for (key, value) in std::env::vars() {
        if key.starts_with(prefix) {
            let key_name = key.strip_prefix(prefix).unwrap_or(&key);
            config.insert(key_name.to_lowercase(), value);
        }
    }

    config
}

// ============================================================================
// Resource Identifier Parsing
// ============================================================================

/// Parse a resource identifier from a URI-like string
///
/// # Examples
/// - `s3://bucket-name/key` -> `ObjectIO` resource
/// - `bigquery://project/dataset/table` -> `WarehouseIO` resource
/// - `dynamodb://table-name` -> `KeyValueIO` resource
///
/// # Errors
///
/// Returns an error if the URI format is invalid (missing `://` separator)
pub fn parse_resource_uri(uri: &str) -> Result<(String, Vec<String>)> {
    let parts: Vec<&str> = uri.splitn(2, "://").collect();

    if parts.len() != 2 {
        return Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            format!("Invalid resource URI format: {uri}"),
        ));
    }

    let provider = parts[0].to_string();
    let path_parts: Vec<String> = parts[1]
        .split('/')
        .map(std::string::ToString::to_string)
        .collect();

    Ok((provider, path_parts))
}

// ============================================================================
// Async to Sync Wrapper
// ============================================================================

/// Helper to wrap async operations in a synchronous interface
///
/// This uses a simple blocking approach to execute async operations.
/// For production use, you may want to use a more sophisticated runtime.
///
/// # Example
/// ```ignore
/// let result = block_on_async(async {
///     async_api_client.get_data().await
/// })?;
/// ```
///
/// Note: This is a placeholder function. To enable async support,
/// you would need to add tokio or async-std as a dependency and
/// implement this function using their runtime.
///
/// # Errors
///
/// This function is unimplemented and will panic if called.
/// It exists as a placeholder for future async support.
///
/// # Panics
///
/// This function always panics with an unimplemented message.
/// To use async operations, you need to add a runtime dependency.
#[allow(dead_code)]
pub fn block_on_async<F, T>(_future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    // Note: This is a placeholder. In a real implementation, you'd use
    // tokio::runtime::Runtime or similar to execute the async operation.
    unimplemented!("Async runtime support requires additional dependencies")
}

// ============================================================================
// Error Conversion Helpers
// ============================================================================

/// Convert common error types to `CloudIOError`
pub trait IntoCloudError<T> {
    /// Convert a standard Result to a cloud IO Result
    ///
    /// # Errors
    ///
    /// Returns a `CloudIOError` with the specified kind if the Result is an error
    fn into_cloud_error(self, kind: ErrorKind) -> Result<T>;
}

impl<T, E: std::error::Error> IntoCloudError<T> for std::result::Result<T, E> {
    fn into_cloud_error(self, kind: ErrorKind) -> Result<T> {
        self.map_err(|e| CloudIOError::new(kind, e.to_string()))
    }
}

// ============================================================================
// Validation Helpers
// ============================================================================

/// Validate a resource name according to common cloud provider rules
///
/// # Errors
///
/// Returns an error if:
/// - The resource name is empty
/// - The resource name exceeds 255 characters
/// - The resource name contains invalid characters (only alphanumeric, hyphens, underscores, and periods are allowed)
pub fn validate_resource_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "Resource name cannot be empty",
        ));
    }

    if name.len() > 255 {
        return Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "Resource name too long (max 255 characters)",
        ));
    }

    // Check for invalid characters (basic validation)
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "Resource name contains invalid characters",
        ));
    }

    Ok(())
}

/// Validate a key path (for object storage, etc.)
///
/// # Errors
///
/// Returns an error if:
/// - The key path is empty
/// - The key path starts with a forward slash
pub fn validate_key_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "Key path cannot be empty",
        ));
    }

    if path.starts_with('/') {
        return Err(CloudIOError::new(
            ErrorKind::InvalidInput,
            "Key path cannot start with '/'",
        ));
    }

    Ok(())
}

// ============================================================================
// Testing Helpers
// ============================================================================

/// Helper to create test credentials
#[must_use]
pub fn test_credentials() -> impl CloudCredentials {
    FakeCredentials {
        identifier: "test-user".to_string(),
        credential_type: "test".to_string(),
    }
}

/// Helper to create test config
#[must_use]
pub fn test_config() -> impl CloudConfig {
    FakeConfig::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_with_backoff() {
        let config = RetryConfig::default();
        let mut attempts = 0;

        let result = retry_with_backoff(&config, || {
            attempts += 1;
            if attempts < 3 {
                Err(CloudIOError::new(ErrorKind::Network, "Temporary failure"))
            } else {
                Ok(42)
            }
        });

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts, 3);
    }

    #[test]
    fn test_batch_in_chunks() {
        let items: Vec<i32> = (1..=10).collect();
        let result = batch_in_chunks(&items, 3, |chunk| Ok(chunk.iter().map(|x| x * 2).collect()));

        assert!(result.is_ok());
        let results = result.unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(results[0], 2);
        assert_eq!(results[9], 20);
    }

    #[test]
    fn test_parse_resource_uri() {
        let (provider, parts) = parse_resource_uri("s3://my-bucket/my-key").unwrap();
        assert_eq!(provider, "s3");
        assert_eq!(parts, vec!["my-bucket", "my-key"]);

        let (provider, parts) = parse_resource_uri("bigquery://project/dataset/table").unwrap();
        assert_eq!(provider, "bigquery");
        assert_eq!(parts, vec!["project", "dataset", "table"]);
    }

    #[test]
    fn test_validate_resource_name() {
        assert!(validate_resource_name("my-resource").is_ok());
        assert!(validate_resource_name("my_resource").is_ok());
        assert!(validate_resource_name("my.resource").is_ok());
        assert!(validate_resource_name("").is_err());
        assert!(validate_resource_name("invalid name with spaces").is_err());
    }

    #[test]
    fn test_validate_key_path() {
        assert!(validate_key_path("path/to/key").is_ok());
        assert!(validate_key_path("key").is_ok());
        assert!(validate_key_path("").is_err());
        assert!(validate_key_path("/absolute/path").is_err());
    }

    #[test]
    fn test_connection_pool() {
        let mut pool = ConnectionPool::new(2);

        // Acquire first connection
        let conn1 = pool.acquire(|| Ok(1)).unwrap();
        assert_eq!(conn1, 1);
        assert_eq!(pool.size(), 0);

        // Release and reacquire
        pool.release(conn1);
        assert_eq!(pool.size(), 1);

        let conn2 = pool.acquire(|| Ok(2)).unwrap();
        assert_eq!(conn2, 1); // Should get the released connection
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_pagination() {
        let config = PaginationConfig {
            page_size: 10,
            max_pages: Some(3),
        };

        let result = paginate(&config, |page, page_size| {
            let start = u64::from(page * page_size);
            let end = start + u64::from(page_size);
            let items: Vec<i32> = (start..end)
                .map(|x| i32::try_from(x).unwrap_or(0))
                .collect();
            let has_more = page < 5;
            Ok((items, has_more))
        });

        assert!(result.is_ok());
        let items = result.unwrap();
        assert_eq!(items.len(), 30); // 3 pages * 10 items
    }
}
