//! JSON Lines (JSONL) I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O** with Serde: [`read_jsonl_vec`] and [`write_jsonl_vec`]
//! - **Deterministic parallel writer**: [`write_jsonl_par`] (feature `parallel-io`)
//! - **Streaming ingestion** by line ranges: [`JsonlShards`], [`build_jsonl_shards`], [`read_jsonl_range`]
//! - **Execution runner integration**: [`JsonlVecOps<T>`] implements [`VecOps`] over `JsonlShards`
//!
//! # Notes
//! - Files are newline-delimited JSON; empty/whitespace-only lines are skipped on read.
//! - Sharding is **line-count based**; it does not rely on byte offsets.
//! - The parallel writer preserves **deterministic file order** by joining shard
//!   outputs in index order.

use crate::io::compression::{auto_detect_reader, auto_detect_writer};
use crate::type_token::VecOps;
use crate::Partition;
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::any::Any;
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Read a JSONL file into a typed `Vec<T>`.
///
/// Each non-empty line is parsed as a JSON document and deserialized to `T`.
///
/// **Compression**: Automatically detects and decompresses gzip, zstd, bzip2, and xz
/// formats based on file extension or magic bytes (when respective feature flags are enabled).
///
/// # Errors
/// Returns an error if the file cannot be opened, read, or if any line fails
/// to parse into `T`. Errors include contextual information (line number).
pub fn read_jsonl_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let rdr = auto_detect_reader(f, path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    let rdr = BufReader::new(rdr);
    let mut out = Vec::<T>::new();
    for (i, line) in rdr.lines().enumerate() {
        let line = line.with_context(|| format!("read line {} in {}", i + 1, path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let v: T = serde_json::from_str(&line).with_context(|| {
            format!("parse JSONL line {} in {}: {}", i + 1, path.display(), line)
        })?;
        out.push(v);
    }
    Ok(out)
}

/// Write a typed slice as a JSONL file (one JSON value per line).
///
/// Each element is serialized with Serde to a single line, followed by `\n`.
/// Parent directories are created as needed.
///
/// **Compression**: Automatically detects and compresses output based on file extension
/// (e.g., `.gz`, `.zst`) when respective feature flags are enabled.
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the file/dirs cannot be created or any item fails to
/// serialize/flush.
pub fn write_jsonl_vec<T: Serialize>(path: impl AsRef<Path>, data: &[T]) -> Result<usize> {
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut w = auto_detect_writer(f, path)
        .with_context(|| format!("setup compression for {}", path.display()))?;
    for (i, item) in data.iter().enumerate() {
        serde_json::to_writer(&mut w, item)
            .with_context(|| format!("serialize item #{} to {}", i, path.display()))?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(data.len())
}

/// Write JSONL in parallel while keeping **deterministic final order**.
///
/// The input slice is split into contiguous shards; each shard is serialized to
/// a temporary part file in parallel, then all parts are concatenated in shard
/// index order into the final file. Temporary files are removed at the end.
///
/// * `shards`: if `None`, defaults to `num_cpus::get().max(2)`, clamped to `[1,n]`.
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if part or output files cannot be created/written.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(feature = "parallel-io")]
pub fn write_jsonl_par<T: Serialize + Send + Sync>(
    path: impl AsRef<Path>,
    data: &[T],
    shards: Option<usize>,
) -> Result<usize> {
    use rayon::prelude::*;
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let n = data.len();
    if n == 0 {
        File::create(path)?; // touch
        return Ok(0);
    }
    let shards = shards.unwrap_or_else(|| num_cpus::get().max(2)).clamp(1, n);
    let chunk = n.div_ceil(shards);

    let shard_paths: Vec<PathBuf> = (0..shards)
        .map(|i| path.with_extension(format!("jsonl.part{i}")))
        .collect();

    shard_paths
        .par_iter()
        .enumerate()
        .try_for_each(|(i, p)| -> Result<()> {
            let start = i * chunk;
            let end = ((i + 1) * chunk).min(n);
            let f = File::create(p).with_context(|| format!("create {}", p.display()))?;
            let mut w = BufWriter::new(f);
            for item in &data[start..end] {
                serde_json::to_writer(&mut w, item)?;
                w.write_all(b"\n")?;
            }
            w.flush()?;
            Ok(())
        })?;

    // concat in order
    let mut out = BufWriter::new(File::create(path)?);
    for p in &shard_paths {
        let mut r = BufReader::new(File::open(p)?);
        std::io::copy(&mut r, &mut out)?;
    }
    out.flush()?;
    for p in shard_paths {
        let _ = std::fs::remove_file(p);
    }
    Ok(n)
}

/// Streaming JSONL sharding metadata.
///
/// Produced by [`build_jsonl_shards`] and consumed by [`read_jsonl_range`]
/// and the execution engine via [`JsonlVecOps`].
#[derive(Clone)]
pub struct JsonlShards {
    /// Source file path.
    pub path: PathBuf,
    /// Line ranges `(start, end)` (0-based, end-exclusive). Empty/whitespace-only lines
    /// are still counted when building ranges; they are skipped at parse time.
    pub ranges: Vec<(u64, u64)>,
    /// Total number of lines considered for sharding.
    pub total_lines: u64,
}

/// Build [`JsonlShards`] by counting lines and slicing into `lines_per_shard`.
///
/// For an empty file, returns an empty set of ranges.
///
/// **Compression**: Automatically detects and decompresses compressed files for line counting.
/// Note that compressed files require full decompression during this step.
///
/// # Errors
/// Returns an error if the file cannot be opened or read.
pub fn build_jsonl_shards(path: impl AsRef<Path>, lines_per_shard: usize) -> Result<JsonlShards> {
    let path = path.as_ref().to_path_buf();
    let f = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let rdr = auto_detect_reader(f, &path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    let rdr = BufReader::new(rdr);
    let mut total: u64 = 0;
    for line in rdr.lines() {
        let _ = line?;
        total += 1;
    }
    if total == 0 {
        return Ok(JsonlShards {
            path,
            ranges: vec![],
            total_lines: 0,
        });
    }
    let lps = lines_per_shard.max(1) as u64;
    let shards = total.div_ceil(lps) as usize;
    let mut ranges = Vec::with_capacity(shards);
    for i in 0..shards {
        let start = (i as u64) * lps;
        let end = ((i as u64 + 1) * lps).min(total);
        ranges.push((start, end));
    }
    Ok(JsonlShards {
        path,
        ranges,
        total_lines: total,
    })
}

/// Read a `[start, end)` line range from a JSONL file into `Vec<T>`.
///
/// Lines that are empty/whitespace are skipped. Each remaining line is parsed as JSON.
///
/// **Compression**: Automatically detects and decompresses compressed files. Note that
/// compressed streams don't support seeking, so the entire file is decompressed from
/// the start and lines are skipped until reaching `start`.
///
/// # Errors
/// Returns an error if the file cannot be opened or any selected line fails to
/// parse into `T`.
pub fn read_jsonl_range<T: DeserializeOwned>(
    src: &JsonlShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let f = File::open(&src.path).with_context(|| format!("open {}", src.path.display()))?;
    let rdr = auto_detect_reader(f, &src.path)
        .with_context(|| format!("setup decompression for {}", src.path.display()))?;
    let rdr = BufReader::new(rdr);
    let mut out = Vec::<T>::new();
    for (i, line) in rdr.lines().enumerate() {
        let i = i as u64;
        let line = line?;
        if i < start {
            continue;
        }
        if i >= end {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let v: T = serde_json::from_str(&line)
            .with_context(|| format!("parse JSONL line {} in {}", i + 1, src.path.display()))?;
        out.push(v);
    }
    Ok(out)
}

/// `VecOps` adapter for streaming JSONL via [`JsonlShards`].
///
/// This enables the execution engine to determine total length (`len`), split
/// into concrete partitions (`split` by line range), and read the entire dataset
/// (`clone_any`) for sequential paths.
///
/// Requires `T: DeserializeOwned + Clone + Send + Sync + 'static`.
pub struct JsonlVecOps<T>(std::marker::PhantomData<T>);

impl<T> JsonlVecOps<T> {
    /// Construct an `Arc` to the adapter.
    pub fn new() -> Arc<Self> {
        Arc::new(Self(std::marker::PhantomData))
    }
}

impl<T> VecOps for JsonlVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<JsonlShards>()?;
        Some(s.total_lines as usize)
    }
    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<JsonlShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_jsonl_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }
    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        // read full file if sequential
        let s = data.downcast_ref::<JsonlShards>()?;
        let v: Vec<T> = read_jsonl_range::<T>(s, 0, s.total_lines).ok()?;
        Some(Box::new(v) as Partition)
    }
}
