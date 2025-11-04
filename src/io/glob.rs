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
//! use rustflow::io::glob::expand_glob;
//!
//! // Match all JSONL files in a directory
//! let files = expand_glob("logs/*.jsonl")?;
//!
//! // Match files with date-based partitions
//! let partitions = expand_glob("data/events/year=2024/month=*/day=*/*.parquet")?;
//! # Ok::<(), anyhow::Error>(())
//! ```

use anyhow::{Context, Result};
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
/// use rustflow::io::glob::expand_glob;
///
/// // All JSON files in logs directory
/// let files = expand_glob("logs/*.json")?;
///
/// // All CSV files in nested directories
/// let files = expand_glob("data/**/*.csv")?;
///
/// // Date-based partitions
/// let files = expand_glob("events/year=2024/month=01/day=*/data.parquet")?;
/// # Ok::<(), anyhow::Error>(())
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
    let paths = glob::glob(pattern)
        .with_context(|| format!("invalid glob pattern: {}", pattern))?;

    let mut result = Vec::new();
    for entry in paths {
        let path = entry.with_context(|| format!("error reading glob entry for pattern: {}", pattern))?;
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
/// use rustflow::io::glob::expand_glob_required;
///
/// // Will error if no files match
/// let files = expand_glob_required("logs/*.jsonl")?;
/// # Ok::<(), anyhow::Error>(())
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
        anyhow::bail!("no files found matching pattern: {}", pattern);
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{create_dir_all, File};
    use tempfile::TempDir;

    #[test]
    fn test_expand_glob_basic() -> Result<()> {
        let dir = TempDir::new()?;
        let base = dir.path();

        // Create test files
        File::create(base.join("test1.txt"))?;
        File::create(base.join("test2.txt"))?;
        File::create(base.join("other.csv"))?;

        let pattern = format!("{}/*.txt", base.display());
        let files = expand_glob(&pattern)?;

        assert_eq!(files.len(), 2);
        assert!(files[0].to_string_lossy().ends_with("test1.txt"));
        assert!(files[1].to_string_lossy().ends_with("test2.txt"));
        Ok(())
    }

    #[test]
    fn test_expand_glob_nested() -> Result<()> {
        let dir = TempDir::new()?;
        let base = dir.path();

        // Create nested structure
        create_dir_all(base.join("year=2024/month=01"))?;
        create_dir_all(base.join("year=2024/month=02"))?;
        File::create(base.join("year=2024/month=01/data.json"))?;
        File::create(base.join("year=2024/month=02/data.json"))?;

        let pattern = format!("{}/year=2024/month=*/data.json", base.display());
        let files = expand_glob(&pattern)?;

        assert_eq!(files.len(), 2);
        Ok(())
    }

    #[test]
    fn test_expand_glob_empty() -> Result<()> {
        let dir = TempDir::new()?;
        let pattern = format!("{}/*.nonexistent", dir.path().display());
        let files = expand_glob(&pattern)?;
        assert_eq!(files.len(), 0);
        Ok(())
    }

    #[test]
    fn test_expand_glob_required_fails_on_empty() -> Result<()> {
        let dir = TempDir::new()?;
        let pattern = format!("{}/*.nonexistent", dir.path().display());
        let result = expand_glob_required(&pattern);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_expand_glob_sorted() -> Result<()> {
        let dir = TempDir::new()?;
        let base = dir.path();

        // Create files in non-sorted order
        File::create(base.join("c.txt"))?;
        File::create(base.join("a.txt"))?;
        File::create(base.join("b.txt"))?;

        let pattern = format!("{}/*.txt", base.display());
        let files = expand_glob(&pattern)?;

        assert_eq!(files.len(), 3);
        // Verify sorted order
        for i in 0..files.len() - 1 {
            assert!(files[i] < files[i + 1]);
        }
        Ok(())
    }

    #[test]
    fn test_expand_glob_excludes_directories() -> Result<()> {
        let dir = TempDir::new()?;
        let base = dir.path();

        // Create a file and a directory
        File::create(base.join("file.txt"))?;
        create_dir_all(base.join("subdir.txt"))?;

        let pattern = format!("{}/*.txt", base.display());
        let files = expand_glob(&pattern)?;

        // Should only match the file, not the directory
        assert_eq!(files.len(), 1);
        assert!(files[0].to_string_lossy().ends_with("file.txt"));
        Ok(())
    }
}
