//! CBOR I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O** with Serde: [`read_cbor_vec`] and [`write_cbor_vec`]
//! - **Deterministic parallel writer**: [`write_cbor_par`] (feature `parallel-io`)
//! - **Streaming ingestion** by record ranges: [`CborShards`], [`build_cbor_shards`], [`read_cbor_range`]
//! - **Execution runner integration**: [`CborVecOps<T>`] implements [`VecOps`] over [`CborShards`]
//!
//! # Feature gating
//! The entire public surface of this module is **always available in the ABI**,
//! regardless of whether the `io-cbor` feature is enabled. When the feature is
//! disabled, the read/write functions are compiled as stubs that return an error
//! at runtime instead of breaking compilation. This lets dependent code (the
//! [`helpers`](crate::helpers) layer, the runner) link unconditionally while the
//! `ciborium` dependency stays out of builds that don't opt in.
//!
//! # Notes
//! - A CBOR file is a flat concatenation of self-delimiting CBOR values (one per
//!   record). CBOR values carry their own length, so shard part-files are
//!   byte-concatenable just like JSONL and `MessagePack`.
//! - Sharding is **record-count-based**; it does not rely on byte offsets.
//! - Compression is detected automatically based on file extension or magic bytes
//!   (when the respective feature flags are enabled).
//! - Values are encoded with `ciborium::ser::into_writer` and decoded with
//!   `ciborium::de::from_reader`.

use crate::Partition;
use crate::type_token::VecOps;
use anyhow::Result;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::Any;
use std::marker::PhantomData;
use std::path::PathBuf;

#[cfg(feature = "io-cbor")]
use crate::io::compression::{auto_detect_reader, auto_detect_writer};
#[cfg(feature = "io-cbor")]
use anyhow::Context;
#[cfg(feature = "io-cbor")]
use serde::de::IgnoredAny;
#[cfg(feature = "io-cbor")]
use std::fs::{File, create_dir_all};
#[cfg(feature = "io-cbor")]
use std::io::{BufReader, ErrorKind, Read, Write};
#[cfg(feature = "io-cbor")]
use std::path::Path;

// ── Streaming sharding metadata (always available) ────────────────────────────

/// Streaming CBOR sharding metadata.
///
/// Produced by [`build_cbor_shards`] and consumed by [`read_cbor_range`]
/// and the execution engine via [`CborVecOps`].
#[derive(Clone)]
pub struct CborShards {
    /// Source file path.
    pub path: PathBuf,
    /// Record ranges `(start, end)` (0-based, end-exclusive).
    pub ranges: Vec<(u64, u64)>,
    /// Total number of records in the file.
    pub total_records: u64,
}

// ── Private helpers (only compiled with the feature) ─────────────────────────

/// Return `true` if `err` represents a clean end-of-stream at a record boundary.
///
/// `ciborium` reports this as an `Io` variant whose inner error has kind
/// [`ErrorKind::UnexpectedEof`]: the decoder tried to read the first byte of the
/// next CBOR item but the stream was already exhausted.
#[cfg(feature = "io-cbor")]
fn is_clean_eof(err: &ciborium::de::Error<std::io::Error>) -> bool {
    matches!(err, ciborium::de::Error::Io(e) if e.kind() == ErrorKind::UnexpectedEof)
}

/// Deserialize every record from `reader` into a typed `Vec<T>`.
///
/// Stops cleanly when the stream is exhausted at a record boundary; any other
/// decode error is propagated with the offending record index.
#[cfg(feature = "io-cbor")]
fn cbor_read_loop<T: DeserializeOwned, R: Read>(mut reader: R, path: &Path) -> Result<Vec<T>> {
    let mut out = Vec::<T>::new();
    loop {
        match ciborium::de::from_reader::<T, _>(&mut reader) {
            Ok(v) => out.push(v),
            Err(ref e) if is_clean_eof(e) => break,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "deserialize CBOR record #{} in {}: {e}",
                    out.len() + 1,
                    path.display()
                ));
            }
        }
    }
    Ok(out)
}

/// Count every record reachable from `reader`.
///
/// Decodes each value as [`IgnoredAny`] to advance past it efficiently;
/// the count never depends on a concrete `T`.
#[cfg(feature = "io-cbor")]
fn cbor_count_records<R: Read>(mut reader: R, path: &Path) -> Result<u64> {
    let mut n = 0u64;
    loop {
        match ciborium::de::from_reader::<IgnoredAny, _>(&mut reader) {
            Ok(_) => n += 1,
            Err(ref e) if is_clean_eof(e) => break,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "count CBOR record #{} in {}: {e}",
                    n + 1,
                    path.display()
                ));
            }
        }
    }
    Ok(n)
}

/// Build [`CborShards`] from a pre-counted total and `records_per_shard`.
#[cfg(feature = "io-cbor")]
fn make_cbor_shards(path: PathBuf, total: u64, records_per_shard: usize) -> CborShards {
    if total == 0 {
        return CborShards {
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
    CborShards {
        path,
        ranges,
        total_records: total,
    }
}

/// Open `path` with compression auto-detection and return a buffered reader.
#[cfg(feature = "io-cbor")]
fn open_cbor_reader(path: &Path) -> Result<BufReader<Box<dyn Read>>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let inner = auto_detect_reader(f, path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    Ok(BufReader::new(inner))
}

// ── Vector I/O ────────────────────────────────────────────────────────────────

/// Read a CBOR file into a typed `Vec<T>`.
///
/// The file is treated as a concatenation of CBOR values, each deserialized
/// into `T`. Compression is auto-detected.
///
/// # Errors
/// Returns an error if the file cannot be opened or read, or if any record fails
/// to deserialize into `T`. When the `io-cbor` feature is disabled, always
/// returns an error.
#[cfg(feature = "io-cbor")]
pub fn read_cbor_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let rdr = open_cbor_reader(path)?;
    cbor_read_loop(rdr, path)
}

/// Write a typed slice as a CBOR file (one CBOR value per record).
///
/// Each element is serialized with `ciborium::ser::into_writer` and appended to
/// the file. Parent directories are created as needed. Compression is auto-detected
/// from the file extension (e.g., `.gz`, `.zst`).
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the file/dirs cannot be created or any item fails to
/// serialize/flush. When the `io-cbor` feature is disabled, always returns an
/// error.
#[cfg(feature = "io-cbor")]
pub fn write_cbor_vec<T: Serialize>(path: impl AsRef<Path>, data: &[T]) -> Result<usize> {
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
        ciborium::ser::into_writer(item, &mut w)
            .with_context(|| format!("serialize item #{i} to CBOR in {}", path.display()))?;
    }
    w.flush().context("flush CBOR writer")?;
    Ok(data.len())
}

/// Write CBOR in parallel while keeping **deterministic final order**.
///
/// The input slice is split into contiguous shards; each shard is serialized to a
/// temporary part file in parallel, then all parts are concatenated in shard index
/// order into the final file. Temporary files are removed at the end.
///
/// Because CBOR values are self-delimiting and carry no per-file framing,
/// concatenating shard byte streams yields a valid combined file.
///
/// * `shards`: if `None`, defaults to `num_cpus::get().max(2)`, clamped to `[1,n]`.
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if part or output files cannot be created/written. When the
/// `io-cbor` feature is disabled, always returns an error.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(all(feature = "parallel-io", feature = "io-cbor"))]
pub fn write_cbor_par<T: Serialize + Send + Sync>(
    path: impl AsRef<Path>,
    data: &[T],
    shards: Option<usize>,
) -> Result<usize> {
    use rayon::prelude::*;
    use std::fs::remove_file;
    use std::io::{BufWriter, copy};

    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let n = data.len();
    if n == 0 {
        File::create(path).with_context(|| format!("create {}", path.display()))?;
        return Ok(0);
    }
    let requested_shards = shards.unwrap_or_else(|| num_cpus::get().max(2));
    let actual_shards = requested_shards.clamp(1, n);
    let chunk = n.div_ceil(actual_shards);
    let non_empty_shards = n.div_ceil(chunk);

    let shard_paths: Vec<PathBuf> = (0..non_empty_shards)
        .map(|i| path.with_extension(format!("cbor.part{i}")))
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
                ciborium::ser::into_writer(item, &mut w)
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

// ── Streaming sharding ────────────────────────────────────────────────────────

/// Build [`CborShards`] by counting records and slicing into `records_per_shard`.
///
/// For an empty file, returns an empty set of ranges.
///
/// **Compression**: Automatically detects and decompresses compressed files for
/// record counting. Note that compressed files require full decompression here.
///
/// # Errors
/// Returns an error if the file cannot be opened or read. When the `io-cbor`
/// feature is disabled, always returns an error.
///
/// # Panics
/// If the shard calculation overflows.
#[cfg(feature = "io-cbor")]
pub fn build_cbor_shards(path: impl AsRef<Path>, records_per_shard: usize) -> Result<CborShards> {
    let path = path.as_ref().to_path_buf();
    let rdr = open_cbor_reader(&path)?;
    let total = cbor_count_records(rdr, &path)?;
    Ok(make_cbor_shards(path, total, records_per_shard))
}

/// Read a `[start, end)` record range from a CBOR file into `Vec<T>`.
///
/// Compression is auto-detected. Because CBOR streams are not seekable, the
/// file is decoded from the start and records before `start` are skipped.
///
/// # Errors
/// Returns an error if the file cannot be opened or if any selected record fails
/// to deserialize into `T`. When the `io-cbor` feature is disabled, always
/// returns an error.
#[cfg(feature = "io-cbor")]
pub fn read_cbor_range<T: DeserializeOwned>(
    src: &CborShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let mut rdr = open_cbor_reader(&src.path)?;
    let mut out = Vec::<T>::new();
    let mut i = 0u64;
    loop {
        if i >= end {
            break;
        }
        match ciborium::de::from_reader::<T, _>(&mut rdr) {
            Ok(v) => {
                if i >= start {
                    out.push(v);
                }
                i += 1;
            }
            Err(ref e) if is_clean_eof(e) => break,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "deserialize CBOR record #{} in {}: {e}",
                    i + 1,
                    src.path.display()
                ));
            }
        }
    }
    Ok(out)
}

// ── Disabled-feature stubs ────────────────────────────────────────────────────
//
// When `io-cbor` is off, the functions above are not compiled. These stubs keep
// the public ABI identical and fail at runtime instead.

/// Stub returned when the `io-cbor` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-cbor` feature is not enabled.
#[cfg(not(feature = "io-cbor"))]
pub fn read_cbor_vec<T: DeserializeOwned>(_path: impl AsRef<std::path::Path>) -> Result<Vec<T>> {
    anyhow::bail!("the `io-cbor` feature is not enabled")
}

/// Stub returned when the `io-cbor` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-cbor` feature is not enabled.
#[cfg(not(feature = "io-cbor"))]
pub fn write_cbor_vec<T: Serialize>(
    _path: impl AsRef<std::path::Path>,
    _data: &[T],
) -> Result<usize> {
    anyhow::bail!("the `io-cbor` feature is not enabled")
}

/// Stub returned when the `io-cbor` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-cbor` feature is not enabled.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(all(feature = "parallel-io", not(feature = "io-cbor")))]
pub fn write_cbor_par<T: Serialize + Send + Sync>(
    _path: impl AsRef<std::path::Path>,
    _data: &[T],
    _shards: Option<usize>,
) -> Result<usize> {
    anyhow::bail!("the `io-cbor` feature is not enabled")
}

/// Stub returned when the `io-cbor` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-cbor` feature is not enabled.
#[cfg(not(feature = "io-cbor"))]
pub fn build_cbor_shards(
    _path: impl AsRef<std::path::Path>,
    _records_per_shard: usize,
) -> Result<CborShards> {
    anyhow::bail!("the `io-cbor` feature is not enabled")
}

/// Stub returned when the `io-cbor` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-cbor` feature is not enabled.
#[cfg(not(feature = "io-cbor"))]
pub fn read_cbor_range<T: DeserializeOwned>(
    _src: &CborShards,
    _start: u64,
    _end: u64,
) -> Result<Vec<T>> {
    anyhow::bail!("the `io-cbor` feature is not enabled")
}

// ── VecOps adapter (always available) ────────────────────────────────────────

/// `VecOps` adapter for streaming CBOR via [`CborShards`].
///
/// This enables the execution engine to determine total length (`len`), split
/// into concrete partitions (`split`) by record range, and read the entire dataset
/// (`clone_any`) for sequential paths.
///
/// Requires `T: DeserializeOwned + Clone + Send + Sync + 'static`.
///
/// When the `io-cbor` feature is disabled, `split`/`clone_any` yield `None`
/// because the underlying range reader stub errors — but a disabled source can
/// never be constructed in the first place.
pub struct CborVecOps<T>(PhantomData<T>);

impl<T> CborVecOps<T> {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self(PhantomData))
    }
}

impl<T> VecOps for CborVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<CborShards>()?;
        usize::try_from(s.total_records).ok()
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<CborShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_cbor_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<CborShards>()?;
        let v: Vec<T> = read_cbor_range::<T>(s, 0, s.total_records).ok()?;
        Some(Box::new(v) as Partition)
    }
}
