//! Deprecated module - use [`crate::io::cloud::utils`] instead.
//!
//! This module has been reorganized for clarity:
//!
//! ## Implementation Helpers (Moved to [`crate::io::cloud::utils`])
//!
//! Low-level utilities for implementing cloud service integrations have been moved to
//! [`crate::io::cloud::utils`]. See that module for:
//!
//! - [`retry_with_backoff`] - Retry with exponential backoff
//! - [`with_timeout`] - Execute with timeout enforcement
//! - [`batch_in_chunks`] - Split data into chunks for batch processing
//! - [`paginate`] - Fetch all pages from paginated APIs
//! - [`parse_resource_uri`] - Parse cloud resource URIs
//! - [`validate_resource_name`] - Validate resource names
//! - [`ConnectionPool`] - Simple connection pooling
//!
//! ## End-User Helpers (Available at [`crate::helpers::cloud`])
//!
//! High-level helpers for running custom cloud operations are available at
//! [`crate::helpers::cloud`]. See that module for:
//!
//! - [`crate::helpers::cloud::run_with_retry`] - Execute with automatic retry
//! - [`crate::helpers::cloud::run_parallel`] - Execute multiple operations concurrently
//! - [`crate::helpers::cloud::run_with_timeout_and_retry`] - Combine timeout and retry
//! - [`crate::helpers::cloud::run_batch_operation`] - Process in configurable chunks
//! - [`crate::helpers::cloud::run_paginated_operation`] - Handle paginated responses
//! - [`crate::helpers::cloud::OperationBuilder`] - Fluent API for operation configuration
//! - [`crate::helpers::cloud::run_with_context`] - Track execution metadata

// Re-export everything from utils for backward compatibility
pub use crate::io::cloud::utils::*;
