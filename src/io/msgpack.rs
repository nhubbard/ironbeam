//! `MessagePack` I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O** with Serde: [`read_msgpack_vec`] and [`write_msgpack_vec`]
//! - **Deterministic parallel writer**: [`write_msgpack_par`] (feature `parallel-io`)
//! - **Streaming ingestion** by record ranges: [`MsgpackShards`], [`build_msgpack_shards`], [`read_msgpack_range`]
//! - **Execution runner integration**: [`MsgpackVecOps<T>`] implements [`VecOps`] over `MsgpackShards`
//!
//! # Notes
//! - A `MessagePack` file is a flat concatenation of self-delimiting `MessagePack`
//!   values (one per record). Unlike Avro, there is no per-file header or sync
//!   marker, so shard part-files are byte-concatenable just like JSONL.
//! - Sharding is **record-count-based**; it does not rely on byte offsets.
//! - Compression is detected automatically based on file extension or magic bytes
//!   (when the respective feature flags are enabled).
//! - Values are encoded with [`rmp_serde::encode::write`], which serializes
//!   structs compactly as arrays; reads use the matching default decoder.

use crate::Partition;
use crate::io::compression::{auto_detect_reader, auto_detect_writer};
use crate::type_token::VecOps;
use anyhow::{Context, Result};
use rmp_serde::Deserializer;
use rmp_serde::decode::Error as DecodeError;
use serde::{Serialize, de::Deserialize, de::DeserializeOwned, de::IgnoredAny};
use std::any::Any;
use std::fs::{File, create_dir_all, remove_file};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write, copy};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ── Private helpers ────────────────────────────────────────────────────────────

/// Return `true` if `err` represents a clean end-of-stream at a record boundary.
///
/// `rmp-serde` reports this as an [`DecodeError::InvalidMarkerRead`] whose inner
/// I/O error has kind [`ErrorKind::UnexpectedEof`]: the decoder tried to read the
/// marker byte of the next value, but the stream was already exhausted.
fn is_clean_eof(err: &DecodeError) -> bool {
    matches!(err, DecodeError::InvalidMarkerRead(io) if io.kind() == ErrorKind::UnexpectedEof)
}

/// Deserialize every record from `reader` into a typed `Vec<T>`.
///
/// Stops cleanly when the stream is exhausted at a record boundary; any other
/// decode error is propagated with the offending record index.
fn msgpack_read_loop<T: DeserializeOwned, R: Read>(reader: R, path: &Path) -> Result<Vec<T>> {
    let mut de = Deserializer::new(reader);
    let mut out = Vec::<T>::new();
    loop {
        match T::deserialize(&mut de) {
            Ok(v) => out.push(v),
            Err(ref e) if is_clean_eof(e) => break,
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!(
                    "deserialize MessagePack record #{} in {}",
                    out.len() + 1,
                    path.display()
                )));
            }
        }
    }
    Ok(out)
}

/// Count every record reachable from `reader`.
///
/// Decodes each value into [`IgnoredAny`] purely to advance past it, so the count
/// never depends on a concrete `T`.
fn msgpack_count_records<R: Read>(reader: R, path: &Path) -> Result<u64> {
    let mut de = Deserializer::new(reader);
    let mut n = 0u64;
    loop {
        match IgnoredAny::deserialize(&mut de) {
            Ok(_) => n += 1,
            Err(ref e) if is_clean_eof(e) => break,
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!(
                    "count MessagePack record #{} in {}",
                    n + 1,
                    path.display()
                )));
            }
        }
    }
    Ok(n)
}

/// Build [`MsgpackShards`] from a pre-counted total and `records_per_shard`.
fn make_msgpack_shards(path: PathBuf, total: u64, records_per_shard: usize) -> MsgpackShards {
    if total == 0 {
        return MsgpackShards {
            path,
            ranges: vec![],
            total_records: 0,
        };
    }
    let rps = records_per_shard.max(1) as u64;
    let n_shards = usize::try_from(total.div_ceil(rps)).expect("overflow while calculating shards");
    let ranges = (0..n_shards)
        .map(|i| (i as u64 * rps, ((i as u64 + 1) * rps).min(total)))
        .collect();
    MsgpackShards {
        path,
        ranges,
        total_records: total,
    }
}

/// Open `path` with compression auto-detection and return a buffered reader.
fn open_msgpack_reader(path: &Path) -> Result<BufReader<Box<dyn Read>>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let inner = auto_detect_reader(f, path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    Ok(BufReader::new(inner))
}

// ── Vector I/O ───────────────────────────────────────────────────────────────

/// Read a `MessagePack` file into a typed `Vec<T>`.
///
/// The file is treated as a concatenation of `MessagePack` values, each
/// deserialized into `T`. Compression is auto-detected.
///
/// # Errors
/// Returns an error if the file cannot be opened or read, or if any record fails
/// to deserialize into `T`.
pub fn read_msgpack_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let rdr = open_msgpack_reader(path)?;
    msgpack_read_loop(rdr, path)
}

/// Write a typed slice as a `MessagePack` file (one value per record).
///
/// Each element is serialized with [`rmp_serde::encode::write`] and appended to
/// the file. Parent directories are created as needed. Compression is
/// auto-detected from the file extension (e.g., `.gz`, `.zst`).
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the file/dirs cannot be created or any item fails to
/// serialize/flush.
pub fn write_msgpack_vec<T: Serialize>(path: impl AsRef<Path>, data: &[T]) -> Result<usize> {
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
        rmp_serde::encode::write(&mut w, item).with_context(|| {
            format!("serialize item #{} to MessagePack in {}", i, path.display())
        })?;
    }
    w.flush().context("flush MessagePack writer")?;
    Ok(data.len())
}

/// Write `MessagePack` in parallel while keeping **deterministic final order**.
///
/// The input slice is split into contiguous shards; each shard is serialized to a
/// temporary part file in parallel, then all parts are concatenated in shard
/// index order into the final file. Temporary files are removed at the end.
///
/// Because `MessagePack` records are self-delimiting and carry no per-file framing,
/// concatenating shard byte streams yields a valid combined file.
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
pub fn write_msgpack_par<T: Serialize + Send + Sync>(
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
        File::create(path).with_context(|| format!("create {}", path.display()))?; // touch
        return Ok(0);
    }
    let requested_shards = shards.unwrap_or_else(|| num_cpus::get().max(2));
    // Clamp shards to the data length to avoid creating empty shards or going out of bounds.
    let actual_shards = requested_shards.clamp(1, n);
    let chunk = n.div_ceil(actual_shards);
    // When chunk * actual_shards > n, some trailing shards would be empty.
    let non_empty_shards = n.div_ceil(chunk);

    let shard_paths: Vec<PathBuf> = (0..non_empty_shards)
        .map(|i| path.with_extension(format!("msgpack.part{i}")))
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
                rmp_serde::encode::write(&mut w, item)
                    .with_context(|| format!("serialize record to {}", p.display()))?;
            }
            w.flush()?;
            Ok(())
        })?;

    let mut out =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    for p in &shard_paths {
        let mut r = BufReader::new(File::open(p).with_context(|| format!("open {}", p.display()))?);
        copy(&mut r, &mut out)?;
    }
    out.flush()?;
    for p in shard_paths {
        let _ = remove_file(p);
    }
    Ok(n)
}

// ── Streaming sharding ─────────────────────────────────────────────────────────

/// Streaming `MessagePack` sharding metadata.
///
/// Produced by [`build_msgpack_shards`] and consumed by [`read_msgpack_range`]
/// and the execution engine via [`MsgpackVecOps`].
#[derive(Clone)]
pub struct MsgpackShards {
    /// Source file path.
    pub path: PathBuf,
    /// Record ranges `(start, end)` (0-based, end-exclusive).
    pub ranges: Vec<(u64, u64)>,
    /// Total number of records considered for sharding.
    pub total_records: u64,
}

/// Build [`MsgpackShards`] by counting records and slicing into `records_per_shard`.
///
/// For an empty file, returns an empty set of ranges.
///
/// **Compression**: Automatically detects and decompresses compressed files for
/// record counting. Note that compressed files require full decompression here.
///
/// # Errors
/// Returns an error if the file cannot be opened or read.
///
/// # Panics
/// If the shard calculation overflows.
pub fn build_msgpack_shards(
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<MsgpackShards> {
    let path = path.as_ref().to_path_buf();
    let rdr = open_msgpack_reader(&path)?;
    let total = msgpack_count_records(rdr, &path)?;
    Ok(make_msgpack_shards(path, total, records_per_shard))
}

/// Read a `[start, end)` record range from a `MessagePack` file into `Vec<T>`.
///
/// Compression is auto-detected. Because `MessagePack` streams are not seekable,
/// the file is decoded from the start and records before `start` are skipped.
///
/// # Errors
/// Returns an error if the file cannot be opened or if any selected record fails
/// to deserialize into `T`.
pub fn read_msgpack_range<T: DeserializeOwned>(
    src: &MsgpackShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let rdr = open_msgpack_reader(&src.path)?;
    let mut de = Deserializer::new(rdr);
    let mut out = Vec::<T>::new();
    let mut i = 0u64;
    loop {
        if i >= end {
            break;
        }
        match T::deserialize(&mut de) {
            Ok(v) => {
                if i >= start {
                    out.push(v);
                }
                i += 1;
            }
            Err(ref e) if is_clean_eof(e) => break,
            Err(e) => {
                return Err(anyhow::Error::new(e).context(format!(
                    "deserialize MessagePack record #{} in {}",
                    i + 1,
                    src.path.display()
                )));
            }
        }
    }
    Ok(out)
}

/// `VecOps` adapter for streaming `MessagePack` via [`MsgpackShards`].
///
/// This enables the execution engine to determine total length (`len`), split
/// into concrete partitions (`split`) by record range, and read the entire
/// dataset (`clone_any`) for sequential paths.
///
/// Requires `T: DeserializeOwned + Clone + Send + Sync + 'static`.
pub struct MsgpackVecOps<T>(PhantomData<T>);

impl<T> MsgpackVecOps<T> {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self(PhantomData))
    }
}

impl<T> VecOps for MsgpackVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<MsgpackShards>()?;
        usize::try_from(s.total_records).ok()
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<MsgpackShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_msgpack_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<MsgpackShards>()?;
        let v: Vec<T> = read_msgpack_range::<T>(s, 0, s.total_records).ok()?;
        Some(Box::new(v) as Partition)
    }
}
