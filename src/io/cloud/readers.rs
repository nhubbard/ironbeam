//! Cloud file readers with transparent compression and globbing support.
//!
//! This module provides high-level utilities for reading and writing data from cloud object
//! storage services (S3, GCS, Azure Blob, etc.) with automatic compression detection and
//! glob pattern matching, similar to local file I/O operations.
//!
//! ## Features
//!
//! - **Transparent compression** - Automatic detection and decompression based on file extensions
//! - **Glob pattern matching** - List objects matching wildcard patterns
//! - **Format-agnostic** - Works with JSON, JSONL, CSV, or any serializable format
//! - **Provider-agnostic** - Works with any [`ObjectIO`] implementation
//!
//! ## Usage Patterns
//!
//! ### Reading from Cloud Storage
//!
//! ```ignore
//! use ironbeam::io::cloud::readers::read_cloud_jsonl_vec;
//! use ironbeam::io::cloud::FakeObjectIO;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Record {
//!     id: u64,
//!     value: String,
//! }
//!
//! let storage = FakeObjectIO::new();
//! let records: Vec<Record> = read_cloud_jsonl_vec(
//!     &storage,
//!     "my-bucket",
//!     "data/records.jsonl.gz", // Automatically decompressed
//! )?;
//! ```
//!
//! ### Writing to Cloud Storage
//!
//! ```ignore
//! use ironbeam::io::cloud::readers::write_cloud_jsonl_vec;
//!
//! write_cloud_jsonl_vec(
//!     &storage,
//!     "my-bucket",
//!     "output/results.jsonl.zst", // Automatically compressed
//!     &records,
//! )?;
//! ```
//!
//! ### Glob Pattern Matching
//!
//! ```ignore
//! use ironbeam::io::cloud::readers::expand_cloud_glob;
//!
//! // List all JSONL files in a prefix
//! let files = expand_cloud_glob(&storage, "my-bucket", "data/*.jsonl")?;
//!
//! // Date-based partitions
//! let files = expand_cloud_glob(
//!     &storage,
//!     "my-bucket",
//!     "events/year=2024/month=*/day=*/*.parquet",
//! )?;
//! ```
//!
//! ### Reading Multiple Files with Glob
//!
//! ```ignore
//! use ironbeam::io::cloud::readers::read_cloud_jsonl_glob;
//!
//! // Read all matching files and combine results
//! let all_records: Vec<Record> = read_cloud_jsonl_glob(
//!     &storage,
//!     "my-bucket",
//!     "data/2024-*/events.jsonl.gz",
//! )?;
//! ```

use crate::io::cloud::traits::{CloudIOError, CloudResult, ErrorKind, ObjectIO};
use crate::io::compression::auto_detect_reader;
#[cfg(feature = "compression-bzip2")]
use bzip2::{Compression as BzCompression, write::BzEncoder};
#[cfg(feature = "compression-gzip")]
use flate2::{Compression as GzCompression, write::GzEncoder};
use regex::Regex;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
#[cfg(feature = "compression-xz")]
use xz2::write::XzEncoder;
#[cfg(feature = "compression-zstd")]
use zstd::stream::write::Encoder as ZstdEncoder;

// ============================================================================
// Core Reading/Writing Functions
// ============================================================================

/// Read a JSONL file from cloud storage into a typed `Vec<T>`.
///
/// Each non-empty line is parsed as a JSON document and deserialized to `T`.
///
/// **Compression**: Automatically detects and decompresses gzip, zstd, bzip2, and xz
/// formats based on key extension (when respective feature flags are enabled).
///
/// # Errors
///
/// Returns an error if:
/// - The object doesn't exist in the bucket
/// - The object cannot be read
/// - Any line fails to parse into `T`
/// - Compression detection or decompression fails
///
/// # Example
///
/// ```ignore
/// use ironbeam::io::cloud::readers::read_cloud_jsonl_vec;
/// use ironbeam::io::cloud::FakeObjectIO;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Event {
///     timestamp: u64,
///     message: String,
/// }
///
/// let storage = FakeObjectIO::new();
/// let events: Vec<Event> = read_cloud_jsonl_vec(&storage, "logs", "events.jsonl.gz")?;
/// # use ironbeam::io::cloud::traits::Result;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn read_cloud_jsonl_vec<T, O>(storage: &O, bucket: &str, key: &str) -> CloudResult<Vec<T>>
where
    T: DeserializeOwned,
    O: ObjectIO,
{
    // Get object from cloud storage
    let data = storage.get_object(bucket, key)?;

    // Create a cursor over the data
    let cursor = std::io::Cursor::new(data);

    // Apply automatic decompression based on the key extension
    let reader = auto_detect_reader(cursor, key).map_err(|e| {
        CloudIOError::new(
            ErrorKind::InternalError,
            format!("Failed to setup decompression for {key}: {e}"),
        )
    })?;

    // Read and parse JSONL
    let buffered = BufReader::new(reader);
    let mut results = Vec::new();

    for (line_num, line) in buffered.lines().enumerate() {
        let line = line.map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!(
                    "Failed to read line {} from {}/{}: {}",
                    line_num + 1,
                    bucket,
                    key,
                    e
                ),
            )
        })?;

        // Skip empty lines
        if line.trim().is_empty() {
            continue;
        }

        let value: T = serde_json::from_str(&line).map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!(
                    "Failed to parse JSONL line {} in {}/{}: {}",
                    line_num + 1,
                    bucket,
                    key,
                    e
                ),
            )
        })?;

        results.push(value);
    }

    Ok(results)
}

/// Write a typed slice as a JSONL file to cloud storage.
///
/// Each element is serialized with Serde to a single line, followed by `\n`.
///
/// **Compression**: Automatically compresses output based on key extension
/// (e.g., `.gz`, `.zst`, `.bz2`, `.xz`) when respective feature flags are enabled.
///
/// # Returns
///
/// The number of items written (`data.len()`).
///
/// # Errors
///
/// Returns an error if:
/// - The bucket doesn't exist
/// - Permissions are insufficient
/// - Any item fails to serialize
/// - Compression fails
/// - The upload fails
///
/// # Example
///
/// ```ignore
/// use ironbeam::io::cloud::readers::write_cloud_jsonl_vec;
///
/// write_cloud_jsonl_vec(
///     &storage,
///     "results",
///     "output/data.jsonl.gz",  // Automatically compressed with gzip
///     &vec![Event { timestamp: 123, message: "test".into() }],
/// )?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn write_cloud_jsonl_vec<T, O>(
    storage: &O,
    bucket: &str,
    key: &str,
    data: &[T],
) -> CloudResult<usize>
where
    T: Serialize,
    O: ObjectIO,
{
    // Determine the compression format from the file extension
    let key_lower = key.to_lowercase();
    let extension = Path::new(&key_lower).extension();
    let final_buffer = if extension
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz") || ext.eq_ignore_ascii_case("gzip"))
    {
        #[cfg(feature = "compression-gzip")]
        {
            compress_jsonl_gzip(data, bucket, key)?
        }
        #[cfg(not(feature = "compression-gzip"))]
        {
            return Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Cannot write {key}: gzip compression not enabled (enable 'compression-gzip' feature)"
                ),
            ));
        }
    } else if extension
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst") || ext.eq_ignore_ascii_case("zstd"))
    {
        #[cfg(feature = "compression-zstd")]
        {
            compress_jsonl_zstd(data, bucket, key)?
        }
        #[cfg(not(feature = "compression-zstd"))]
        {
            return Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Cannot write {key}: zstd compression not enabled (enable 'compression-zstd' feature)"
                ),
            ));
        }
    } else if extension
        .is_some_and(|ext| ext.eq_ignore_ascii_case("bz2") || ext.eq_ignore_ascii_case("bzip2"))
    {
        #[cfg(feature = "compression-bzip2")]
        {
            compress_jsonl_bzip2(data, bucket, key)?
        }
        #[cfg(not(feature = "compression-bzip2"))]
        {
            return Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Cannot write {key}: bzip2 compression not enabled (enable 'compression-bzip2' feature)"
                ),
            ));
        }
    } else if extension.is_some_and(|ext| ext.eq_ignore_ascii_case("xz")) {
        #[cfg(feature = "compression-xz")]
        {
            compress_jsonl_xz(data, bucket, key)?
        }
        #[cfg(not(feature = "compression-xz"))]
        {
            return Err(CloudIOError::new(
                ErrorKind::InvalidInput,
                format!(
                    "Cannot write {key}: xz compression not enabled (enable 'compression-xz' feature)"
                ),
            ));
        }
    } else {
        // No compression - write uncompressed JSONL
        serialize_jsonl_uncompressed(data, bucket, key)?
    };

    // Upload to cloud storage
    storage.put_object(bucket, key, &final_buffer)?;

    Ok(data.len())
}

/// Serialize JSONL data without compression
fn serialize_jsonl_uncompressed<T: Serialize>(
    data: &[T],
    bucket: &str,
    key: &str,
) -> CloudResult<Vec<u8>> {
    let mut buffer = Vec::new();
    for (i, item) in data.iter().enumerate() {
        serde_json::to_writer(&mut buffer, item).map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to serialize item {i} to {bucket}/{key}: {e}"),
            )
        })?;
        buffer.push(b'\n');
    }
    Ok(buffer)
}

/// Compress JSONL data with gzip
#[cfg(feature = "compression-gzip")]
fn compress_jsonl_gzip<T: Serialize>(data: &[T], bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
    let buffer = Vec::new();
    let mut encoder = GzEncoder::new(buffer, GzCompression::default());

    for (i, item) in data.iter().enumerate() {
        serde_json::to_writer(&mut encoder, item).map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to serialize item {i} to {bucket}/{key}: {e}"),
            )
        })?;
        encoder.write_all(b"\n").map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to write newline after item {i} to {bucket}/{key}: {e}"),
            )
        })?;
    }

    // Finish compression and return the inner buffer
    encoder.finish().map_err(|e| {
        CloudIOError::new(
            ErrorKind::InternalError,
            format!("Failed to finish gzip compression for {bucket}/{key}: {e}"),
        )
    })
}

/// Compress JSONL data with zstd
#[cfg(feature = "compression-zstd")]
fn compress_jsonl_zstd<T: Serialize>(data: &[T], bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
    let buffer = Vec::new();
    let mut encoder = ZstdEncoder::new(buffer, 3).map_err(|e| {
        CloudIOError::new(
            ErrorKind::InternalError,
            format!("Failed to create zstd encoder for {bucket}/{key}: {e}"),
        )
    })?;

    for (i, item) in data.iter().enumerate() {
        serde_json::to_writer(&mut encoder, item).map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to serialize item {i} to {bucket}/{key}: {e}"),
            )
        })?;
        encoder.write_all(b"\n").map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to write newline after item {i} to {bucket}/{key}: {e}"),
            )
        })?;
    }

    // Finish compression and return the inner buffer
    encoder.finish().map_err(|e| {
        CloudIOError::new(
            ErrorKind::InternalError,
            format!("Failed to finish zstd compression for {bucket}/{key}: {e}"),
        )
    })
}

/// Compress JSONL data with bzip2
#[cfg(feature = "compression-bzip2")]
fn compress_jsonl_bzip2<T: Serialize>(data: &[T], bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
    let buffer = Vec::new();
    let mut encoder = BzEncoder::new(buffer, BzCompression::default());

    for (i, item) in data.iter().enumerate() {
        serde_json::to_writer(&mut encoder, item).map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to serialize item {i} to {bucket}/{key}: {e}"),
            )
        })?;
        encoder.write_all(b"\n").map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to write newline after item {i} to {bucket}/{key}: {e}"),
            )
        })?;
    }

    // Finish compression and return the inner buffer
    encoder.finish().map_err(|e| {
        CloudIOError::new(
            ErrorKind::InternalError,
            format!("Failed to finish bzip2 compression for {bucket}/{key}: {e}"),
        )
    })
}

/// Compress JSONL data with xz
#[cfg(feature = "compression-xz")]
fn compress_jsonl_xz<T: Serialize>(data: &[T], bucket: &str, key: &str) -> CloudResult<Vec<u8>> {
    let buffer = Vec::new();
    let mut encoder = XzEncoder::new(buffer, 6);

    for (i, item) in data.iter().enumerate() {
        serde_json::to_writer(&mut encoder, item).map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to serialize item {i} to {bucket}/{key}: {e}"),
            )
        })?;
        encoder.write_all(b"\n").map_err(|e| {
            CloudIOError::new(
                ErrorKind::InternalError,
                format!("Failed to write newline after item {i} to {bucket}/{key}: {e}"),
            )
        })?;
    }

    // Finish compression and return the inner buffer
    encoder.finish().map_err(|e| {
        CloudIOError::new(
            ErrorKind::InternalError,
            format!("Failed to finish xz compression for {bucket}/{key}: {e}"),
        )
    })
}

// ============================================================================
// Glob Pattern Matching
// ============================================================================

/// Expand a glob pattern to matching object keys in cloud storage.
///
/// This function takes a glob pattern (e.g., `logs/*.jsonl`, `data/year=2024/*/*.parquet`)
/// and returns all object keys that match the pattern, sorted lexicographically for
/// deterministic processing order.
///
/// # Pattern Syntax
///
/// Supports glob patterns similar to local file globbing:
/// - `*` matches any sequence of characters within a path segment
/// - `?` matches any single character
/// - `**` matches zero or more path segments
///
/// # Examples
///
/// ```ignore
/// use ironbeam::io::cloud::readers::expand_cloud_glob;
///
/// // All JSONL files in logs prefix
/// let files = expand_cloud_glob(&storage, "bucket", "logs/*.jsonl")?;
///
/// // Date-based partitions
/// let files = expand_cloud_glob(
///     &storage,
///     "bucket",
///     "events/year=2024/month=*/day=*/data.parquet",
/// )?;
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The bucket doesn't exist
/// - Permissions are insufficient
/// - The pattern is invalid
/// - The listing operation fails
///
/// # Returns
///
/// A sorted vector of object keys matching the pattern.
pub fn expand_cloud_glob<O>(storage: &O, bucket: &str, pattern: &str) -> CloudResult<Vec<String>>
where
    O: ObjectIO,
{
    // Convert glob pattern to regex
    let regex_pattern = glob_to_regex(pattern);
    let regex = Regex::new(&regex_pattern).map_err(|e| {
        CloudIOError::new(
            ErrorKind::InvalidInput,
            format!("Invalid glob pattern '{pattern}': {e}"),
        )
    })?;

    // Determine the prefix to use for listing (everything before the first wildcard)
    let prefix = extract_prefix_before_wildcard(pattern);

    // List objects with the determined prefix
    let objects = storage.list_objects(bucket, prefix.as_deref())?;

    // Filter objects that match the pattern
    let mut matched_keys: Vec<String> = objects
        .into_iter()
        .filter_map(|obj| {
            if regex.is_match(&obj.key) {
                Some(obj.key)
            } else {
                None
            }
        })
        .collect();

    // Sort for deterministic order
    matched_keys.sort();

    Ok(matched_keys)
}

/// Convert a glob pattern to a regex pattern.
///
/// This converts common glob wildcards to their regex equivalents:
/// - `*` becomes `[^/]*` (match anything except path separator)
/// - `**` becomes `.*` (match anything including path separators)
/// - `?` becomes `.` (match any single character)
/// - `.` becomes `\.` (literal dot)
fn glob_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '*' => {
                // Check if this is `**`
                if chars.peek() == Some(&'*') {
                    chars.next(); // consume the second `*`
                    regex.push_str(".*");
                } else {
                    regex.push_str("[^/]*");
                }
            }
            '?' => regex.push('.'),
            '.' => regex.push_str(r"\."),
            // Escape other regex special characters
            '+' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }

    regex.push('$');
    regex
}

/// Extract the static prefix before the first wildcard in a glob pattern.
///
/// This allows us to use the cloud storage API's prefix listing for better performance.
fn extract_prefix_before_wildcard(pattern: &str) -> Option<String> {
    pattern.find(['*', '?']).map_or_else(
        || Some(pattern.to_string()),
        |pos| {
            if pos == 0 {
                None
            } else {
                Some(pattern[..pos].to_string())
            }
        },
    )
}

// ============================================================================
// Glob-based Reading
// ============================================================================

/// Read multiple JSONL files matching a glob pattern and combine results.
///
/// This is a convenience function that combines [`expand_cloud_glob`] and
/// [`read_cloud_jsonl_vec`] to read all matching files and concatenate their contents.
///
/// # Example
///
/// ```ignore
/// use ironbeam::io::cloud::readers::read_cloud_jsonl_glob;
///
/// // Read all event files for January 2024
/// let events: Vec<Event> = read_cloud_jsonl_glob(
///     &storage,
///     "logs",
///     "events/2024-01-*/data.jsonl.gz",
/// )?;
/// # Ok::<(), anyhow::Error>(())
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The glob pattern is invalid
/// - Any matching file cannot be read
/// - Any line in any file fails to parse
pub fn read_cloud_jsonl_glob<T, O>(storage: &O, bucket: &str, pattern: &str) -> CloudResult<Vec<T>>
where
    T: DeserializeOwned,
    O: ObjectIO,
{
    let keys = expand_cloud_glob(storage, bucket, pattern)?;

    let mut all_results = Vec::new();

    for key in keys {
        let results = read_cloud_jsonl_vec(storage, bucket, &key)?;
        all_results.extend(results);
    }

    Ok(all_results)
}

/// Expand a cloud glob pattern, returning an error if no objects are found.
///
/// This is a stricter variant of [`expand_cloud_glob`] that treats zero matches as an error,
/// which is useful when you expect at least one object to exist.
///
/// # Errors
///
/// Returns an error if:
/// - The glob pattern is invalid
/// - The bucket doesn't exist or cannot be accessed
/// - No objects match the pattern
pub fn expand_cloud_glob_required<O>(
    storage: &O,
    bucket: &str,
    pattern: &str,
) -> CloudResult<Vec<String>>
where
    O: ObjectIO,
{
    let keys = expand_cloud_glob(storage, bucket, pattern)?;

    if keys.is_empty() {
        return Err(CloudIOError::new(
            ErrorKind::NotFound,
            format!("No objects found matching pattern '{pattern}' in bucket '{bucket}'"),
        ));
    }

    Ok(keys)
}
