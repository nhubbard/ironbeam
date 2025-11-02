//! CSV I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O** with Serde: [`read_csv_vec`] and [`write_csv_vec`]
//! - **Deterministic parallel writer**: [`write_csv_par`] (feature `parallel-io`)
//! - **Streaming ingestion** by sharding rows: [`CsvShards`], [`build_csv_shards`], [`read_csv_range`]
//! - **Execution runner integration**: [`CsvVecOps<T>`] implements [`VecOps`] over `CsvShards`
//!
//! # Design notes
//! - All typed I/O is Serde-backed (`DeserializeOwned`/`Serialize`).
//! - Sharding is **row-count based** (header excluded), not byte-range based.
//! - The parallel writer preserves **deterministic final order** by writing shard
//!   buffers in index order after parallel serialization.

use crate::io::compression::{auto_detect_reader, auto_detect_writer};
use crate::type_token::VecOps;
use crate::Partition;
use anyhow::{Context, Result};
use csv::WriterBuilder;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::any::Any;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Read a CSV file into a typed `Vec<T>`.
///
/// Rows are deserialized with Serde using `T: DeserializeOwned`.
///
/// * If `has_headers` is `true`, the first row is treated as a header and
///   not deserialized into `T`.
/// * Errors are annotated with row numbers for easier debugging.
///
/// **Compression**: Automatically detects and decompresses gzip, zstd, bzip2, and xz
/// formats based on file extension or magic bytes (when respective feature flags are enabled).
///
/// # Errors
/// Returns an error if the file cannot be opened or if any row fails to
/// deserialize into `T`.
pub fn read_csv_vec<T: DeserializeOwned>(
    path: impl AsRef<Path>,
    has_headers: bool,
) -> Result<Vec<T>> {
    let path = path.as_ref();
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let rdr = auto_detect_reader(f, path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .from_reader(rdr);
    let mut out = Vec::<T>::new();
    for (i, rec) in rdr.deserialize::<T>().enumerate() {
        let v = rec.with_context(|| format!("parse CSV record #{}", i + 1))?;
        out.push(v);
    }
    Ok(out)
}

/// Write a typed slice to a CSV file.
///
/// Rows are serialized with Serde using `T: Serialize`.
///
/// * Creates parent directories if they don't exist.
/// * Emits a header row when `has_headers` is `true` and the `Serialize`
///   implementation for `T` supports headers (via `csv` conventions).
///
/// **Compression**: Automatically detects and compresses output based on file extension
/// (e.g., `.gz`, `.zst`) when respective feature flags are enabled.
///
/// # Returns
/// The number of rows written (i.e., `data.len()`).
///
/// # Errors
/// Returns an error if the file/dirs cannot be created or any row fails to
/// serialize/flush.
pub fn write_csv_vec<T: Serialize>(
    path: impl AsRef<Path>,
    has_headers: bool,
    data: &[T],
) -> Result<usize> {
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let w = auto_detect_writer(f, path)
        .with_context(|| format!("setup compression for {}", path.display()))?;
    let mut wtr = WriterBuilder::new()
        .has_headers(has_headers)
        .from_writer(w);
    for (i, row) in data.iter().enumerate() {
        wtr.serialize(row)
            .with_context(|| format!("serialize CSV row #{}", i + 1))?;
    }
    wtr.flush()?;
    Ok(data.len())
}

/// Sharding metadata for streaming CSV ingestion.
///
/// The CSV is split into contiguous row ranges (start-inclusive, end-exclusive),
/// excluding an optional header row from the count.
///
/// Construct with [`build_csv_shards`] and read ranges with [`read_csv_range`].
#[derive(Clone)]
pub struct CsvShards {
    /// Source file path.
    pub path: PathBuf,
    /// Row ranges as `(start_row, end_row)`, 0-based, end-exclusive.
    pub ranges: Vec<(u64, u64)>,
    /// Total number of data rows (excluding header).
    pub total_rows: u64,
    /// Whether the source has a header row.
    pub has_headers: bool,
}

/// Build [`CsvShards`] by scanning row count and slicing into `rows_per_shard`.
///
/// * Counting is performed by iterating over CSV records, respecting `has_headers`.
/// * For empty files, returns `CsvShards` with no ranges.
///
/// **Compression**: Automatically detects and decompresses compressed files for row counting.
/// Note that compressed files require full decompression during this step.
///
/// # Errors
/// Returns an error if the file cannot be opened or read as CSV.
pub fn build_csv_shards(
    path: impl AsRef<Path>,
    has_headers: bool,
    rows_per_shard: usize,
) -> Result<CsvShards> {
    let path = path.as_ref().to_path_buf();
    let f = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let rdr = auto_detect_reader(f, &path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .from_reader(rdr);
    let mut total: u64 = 0;
    let it = rdr.records();
    for _ in it {
        total += 1;
    }
    if total == 0 {
        return Ok(CsvShards {
            path,
            ranges: vec![],
            total_rows: 0,
            has_headers,
        });
    }
    let rps = rows_per_shard.max(1) as u64;
    let shards = total.div_ceil(rps) as usize;
    let mut ranges = Vec::with_capacity(shards);
    for i in 0..shards {
        let start = (i as u64) * rps;
        let end = ((i as u64 + 1) * rps).min(total);
        ranges.push((start, end));
    }
    Ok(CsvShards {
        path,
        ranges,
        total_rows: total,
        has_headers,
    })
}

/// Read a single shard (row range) from a CSV described by [`CsvShards`].
///
/// `start` and `end` are row indices into the data region (excluding header).
///
/// **Compression**: Automatically detects and decompresses compressed files. Note that
/// compressed streams don't support seeking, so the entire file is decompressed from
/// the start and rows are skipped until reaching `start`.
///
/// # Errors
/// Returns an error if the file cannot be opened or if deserialization of any
/// row in the range fails.
pub fn read_csv_range<T: DeserializeOwned>(
    src: &CsvShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let f = File::open(&src.path).with_context(|| format!("open {}", src.path.display()))?;
    let rdr = auto_detect_reader(f, &src.path)
        .with_context(|| format!("setup decompression for {}", src.path.display()))?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(src.has_headers)
        .from_reader(rdr);
    // skip rows
    let mut out = Vec::<T>::new();
    for (i, rec) in rdr.deserialize::<T>().enumerate() {
        let i = i as u64;
        if i < start {
            continue;
        }
        if i >= end {
            break;
        }
        out.push(rec?);
    }
    Ok(out)
}

/// `VecOps` adapter for streaming CSV via [`CsvShards`].
///
/// This allows the execution engine to:
/// * introspect a logical length (`len`) without loading all rows,
/// * split the input into concrete partitions (`split`) by row range,
/// * clone the entire dataset as a single partition (`clone_any`) for
///   sequential execution paths.
///
/// Requires `T: DeserializeOwned + Clone + Send + Sync + 'static`.
pub struct CsvVecOps<T>(std::marker::PhantomData<T>);

impl<T> CsvVecOps<T> {
    /// Construct an `Arc` to the adapter.
    pub fn new() -> Arc<Self> {
        Arc::new(Self(std::marker::PhantomData))
    }
}

impl<T> VecOps for CsvVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<CsvShards>()?;
        Some(s.total_rows as usize)
    }
    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<CsvShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_csv_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }
    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<CsvShards>()?;
        let v: Vec<T> = read_csv_range::<T>(s, 0, s.total_rows).ok()?;
        Some(Box::new(v) as Partition)
    }
}

/// Convenience wrapper that accepts `&Vec<T>` for writing.
///
/// Equivalent to `write_csv_vec(path, has_headers, data.as_slice())`.
///
/// # Errors
/// See [`write_csv_vec`].
pub fn write_csv<T: Serialize>(
    path: impl AsRef<Path>,
    has_headers: bool,
    data: &Vec<T>,
) -> Result<usize> {
    write_csv_vec(path, has_headers, data.as_slice())
}

/// Parallel CSV writer with **deterministic final order**.
///
/// Each shard (a contiguous sub-slice of `data`) is serialized into an
/// in-memory buffer **in parallel**, then all buffers are concatenated in shard
/// index order into a single file. This preserves stable, predictable file
/// ordering regardless of thread scheduling.
///
/// * `shards`: optional shard count. If `None`, defaults to `2 * num_cpus()`,
///   clamped to `[1, data.len()]`.
/// * `has_headers`: if `true`, only shard 0 writes the header (once).
///
/// # Returns
/// The number of rows written (i.e., `data.len()`).
///
/// # Errors
/// Returns an error on serialization or file I/O failures.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(feature = "parallel-io")]
pub fn write_csv_par<T: Serialize + Sync>(
    path: impl AsRef<Path>,
    data: &[T],
    shards: Option<usize>,
    has_headers: bool,
) -> Result<usize> {
    let n = data.len();
    let path = path.as_ref();

    // Empty case: create/truncate file, nothing to do.
    if n == 0 {
        let _ = File::create(path).with_context(|| format!("create {}", path.display()))?;
        return Ok(0);
    }

    let shard_count = shards
        .unwrap_or_else(|| 2 * num_cpus::get().max(2))
        .clamp(1, n);
    let ranges = split_ranges(n, shard_count);

    // Serialize each range into a buffer in parallel. Only chunk 0 emits headers if requested.
    let mut buffers: Vec<(usize, Vec<u8>)> = ranges
        .into_par_iter()
        .map(|(idx, start, end)| {
            let slice = &data[start..end];
            let mut buf: Vec<u8> = Vec::with_capacity((end - start).saturating_mul(64)); // heuristic
            {
                let mut wtr = WriterBuilder::new()
                    .has_headers(has_headers && idx == 0)
                    .from_writer(&mut buf);
                for rec in slice {
                    wtr.serialize(rec)?;
                }
                wtr.flush()?;
            }
            Ok::<_, anyhow::Error>((idx, buf))
        })
        .collect::<Result<Vec<_>>>()?;

    // Concatenate buffers in deterministic order into the final file.
    buffers.sort_by_key(|(idx, _)| *idx);

    let mut file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    for (_, buf) in buffers {
        file.write_all(&buf)?;
    }
    file.flush()?;

    Ok(n)
}

/// Split `[0, len)` into `parts` contiguous ranges as `(chunk_idx, start, end)`.
///
/// Ensures `parts  in  [1, len]` (when `len > 0`) and distributes remainder fairly.
/// Ranges are non-empty and cover the entire domain.
///
/// This is not published to keep the public API focused on CSV semantics.
fn split_ranges(len: usize, parts: usize) -> Vec<(usize, usize, usize)> {
    let parts = parts.max(1).min(len.max(1));
    let base = len / parts;
    let rem = len % parts;

    let mut out = Vec::with_capacity(parts);
    let mut start = 0usize;
    for idx in 0..parts {
        let extra = if idx < rem { 1 } else { 0 };
        let end = start + base + extra;
        if start < end {
            out.push((idx, start, end));
        }
        start = end;
    }
    out
}
