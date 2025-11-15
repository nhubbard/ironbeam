//! File globbing utilities for batch file processing.
//!
//! This module provides pattern matching functionality for reading multiple files
//! that match a glob pattern, enabling batch ETL operations on file sets.
//!
//! # Features
//!
//! - **Glob pattern matching** - Support for standard glob patterns like `logs/*.jsonl`
//! - **Date-based partitions** - Special handling for date/time partition patterns
//! - **Sorted results** - Files are returned in deterministic, sorted order
//! - **Error handling** - Clear error messages for invalid patterns or I/O failures
//!
//! # Examples
//!
//! ```no_run
//! use ironbeam::io::glob::expand_glob;
//!
//! // Match all JSONL files in a directory
//! let files = expand_glob("logs/*.jsonl")?;
//!
//! // Match files with date-based partitions
//! let partitions = expand_glob("data/events/year=2024/month=*/day=*/*.parquet")?;
//! # use anyhow::Error; Ok::<(), Error>(())
//! ```

use anyhow::{Context, Result, bail};
use glob::glob;
use std::path::PathBuf;

/// Expand a glob pattern into a sorted vector of matching file paths.
///
/// This function takes a file pattern (e.g., `logs/*.jsonl`, `data/year=2024/*/*.csv`)
/// and returns all files that match the pattern, sorted lexicographically for
/// deterministic processing order.
///
/// # Pattern Syntax
///
/// Supports standard glob patterns:
/// - `*` matches any sequence of characters within a path component
/// - `?` matches any single character
/// - `**` matches zero or more directories
/// - `[abc]` matches any character in the set
/// - `[!abc]` matches any character not in the set
///
/// # Examples
///
/// ```no_run
/// use ironbeam::io::glob::expand_glob;
///
/// // All JSON files in logs directory
/// let files = expand_glob("logs/*.json")?;
///
/// // All CSV files in nested directories
/// let files = expand_glob("data/**/*.csv")?;
///
/// // Date-based partitions
/// let files = expand_glob("events/year=2024/month=01/day=*/data.parquet")?;
/// # use anyhow::Error; Ok::<(), Error>(())
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The pattern is invalid
/// - There are I/O errors accessing the filesystem
/// - No files match the pattern (returns empty vector, not an error)
///
/// # Returns
///
/// A sorted vector of absolute file paths matching the pattern.
pub fn expand_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    let paths = glob(pattern).with_context(|| format!("invalid glob pattern: {pattern}"))?;

    let mut result = Vec::new();
    for entry in paths {
        let path =
            entry.with_context(|| format!("error reading glob entry for pattern: {pattern}"))?;
        // Only include actual files, not directories
        if path.is_file() {
            result.push(path);
        }
    }

    // Sort for deterministic order
    result.sort();

    Ok(result)
}

/// Expand a glob pattern, returning an error if no files are found.
///
/// This is a stricter variant of [`expand_glob`] that treats zero matches as an error,
/// which is useful when you expect at least one file to exist.
///
/// # Examples
///
/// ```no_run
/// use ironbeam::io::glob::expand_glob_required;
///
/// // Will error if no files match
/// let files = expand_glob_required("logs/*.jsonl")?;
/// # use anyhow::Error; Ok::<(), Error>(())
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The pattern is invalid
/// - There are I/O errors accessing the filesystem
/// - No files match the pattern (returns an error, unlike [`expand_glob`])
pub fn expand_glob_required(pattern: &str) -> Result<Vec<PathBuf>> {
    let files = expand_glob(pattern)?;
    if files.is_empty() {
        bail!("no files found matching pattern: {pattern}");
    }
    Ok(files)
}
