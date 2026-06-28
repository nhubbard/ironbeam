//! `TFRecord` I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Raw-bytes vector I/O**: [`read_tfrecord_vec`] and [`write_tfrecord_vec`]
//! - **Deterministic parallel writer**: [`write_tfrecord_par`] (feature `parallel-io`)
//! - **Streaming ingestion** by record ranges: [`TFRecordShards`],
//!   [`build_tfrecord_shards`], [`read_tfrecord_range`]
//! - **`tf.Example` decoding**: `read_tfrecord_examples_vec` (requires both
//!   `io-tfrecord` and `io-protobuf`)
//! - **Execution runner integration**: [`TFRecordVecOps`] implements [`VecOps`]
//!   over [`TFRecordShards`]
//!
//! # Feature gating
//! The entire public surface is **always available in the ABI**, regardless of
//! whether `io-tfrecord` is enabled. When the feature is disabled, the read/write
//! functions compile as stubs that return a runtime error. The element type is
//! always `Vec<u8>` (raw bytes), so no feature-specific trait bounds appear in
//! stub signatures.
//!
//! # Wire format
//! Each `TFRecord` is a self-delimiting length-delimited block:
//! ```text
//! ┌────────────────────────────────────────────────────────┐
//! │  uint64LE   byte length of the payload                 │  8 bytes
//! │  uint32LE   masked CRC-32C of those 8 length bytes     │  4 bytes
//! │  <bytes>    raw payload (length bytes)                 │
//! │  uint32LE   masked CRC-32C of the payload bytes        │  4 bytes
//! └────────────────────────────────────────────────────────┘
//! ```
//! Because each record is self-delimiting, shard part-files are
//! byte-concatenable, enabling the same parallel-write strategy as JSONL/MsgPack.
//!
//! CRC masking uses TensorFlow's standard formula:
//! `masked = ((crc >> 15) | (crc << 17)).wrapping_add(0xa282ea_d8)`.

use crate::Partition;
use crate::type_token::VecOps;
use anyhow::Result;
use std::any::Any;
use std::path::PathBuf;

#[cfg(feature = "io-tfrecord")]
use crate::io::compression::{auto_detect_reader, auto_detect_writer};
#[cfg(feature = "io-tfrecord")]
use anyhow::Context;
#[cfg(feature = "io-tfrecord")]
use std::fs::{File, create_dir_all};
#[cfg(feature = "io-tfrecord")]
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
#[cfg(feature = "io-tfrecord")]
use std::path::Path;

// ── CRC masking (always compiled — trivially unit-testable) ──────────────────

/// Apply TensorFlow's CRC masking to a raw CRC-32C value.
///
/// Mask formula: `((crc >> 15) | (crc << 17)).wrapping_add(0xa282ead8)`.
#[must_use]
pub const fn mask_crc(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(0xa282_ead8_u32)
}

/// Undo TensorFlow's CRC masking, recovering the original CRC-32C value.
///
/// Inverse of [`mask_crc`].
#[must_use]
pub const fn unmask_crc(masked: u32) -> u32 {
    let rot = masked.wrapping_sub(0xa282_ead8_u32);
    rot.rotate_left(15)
}

// ── Streaming sharding metadata (always compiled) ─────────────────────────────

/// Streaming `TFRecord` sharding metadata.
///
/// Produced by [`build_tfrecord_shards`] and consumed by [`read_tfrecord_range`]
/// and the execution engine via [`TFRecordVecOps`].
#[derive(Clone)]
pub struct TFRecordShards {
    /// Source file path.
    pub path: PathBuf,
    /// Record ranges `(start, end)` (0-based, end-exclusive).
    pub ranges: Vec<(u64, u64)>,
    /// Total number of records in the file.
    pub total_records: u64,
}

// ── Private helpers (only compiled with the feature) ─────────────────────────

/// Read a single `TFRecord` entry from `reader`.
///
/// Returns `Ok(None)` on a clean EOF at the first byte of a new record.
/// Returns `Ok(Some(data))` on success.
/// Returns `Err` on CRC mismatch, truncation, or other I/O errors.
#[cfg(feature = "io-tfrecord")]
fn read_tfrecord_entry<R: Read>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    // Detect clean EOF at record boundary via the first byte of the length field.
    let mut first = [0u8; 1];
    match reader.read_exact(&mut first) {
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
        Ok(()) => {}
    }

    // Read the remaining 7 bytes of the 8-byte little-endian length.
    let mut length_bytes = [0u8; 8];
    length_bytes[0] = first[0];
    reader.read_exact(&mut length_bytes[1..])?;

    // Verify length CRC.
    let mut len_crc_bytes = [0u8; 4];
    reader.read_exact(&mut len_crc_bytes)?;
    let stored_len_crc = u32::from_le_bytes(len_crc_bytes);
    let computed_len_crc = mask_crc(crc32c::crc32c(&length_bytes));
    if stored_len_crc != computed_len_crc {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "TFRecord length CRC mismatch",
        ));
    }

    // Read the payload.
    #[allow(clippy::cast_possible_truncation)] // TFRecord files > 4 GiB per record not realistic
    let length = u64::from_le_bytes(length_bytes) as usize;
    let mut data = vec![0u8; length];
    reader.read_exact(&mut data)?;

    // Verify data CRC.
    let mut data_crc_bytes = [0u8; 4];
    reader.read_exact(&mut data_crc_bytes)?;
    let stored_data_crc = u32::from_le_bytes(data_crc_bytes);
    let computed_data_crc = mask_crc(crc32c::crc32c(&data));
    if stored_data_crc != computed_data_crc {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "TFRecord data CRC mismatch",
        ));
    }

    Ok(Some(data))
}

/// Write a single `TFRecord` entry to `writer`.
#[cfg(feature = "io-tfrecord")]
fn write_tfrecord_entry<W: Write>(writer: &mut W, data: &[u8]) -> std::io::Result<()> {
    let length = data.len() as u64;
    let length_bytes = length.to_le_bytes();
    writer.write_all(&length_bytes)?;
    writer.write_all(&mask_crc(crc32c::crc32c(&length_bytes)).to_le_bytes())?;
    writer.write_all(data)?;
    writer.write_all(&mask_crc(crc32c::crc32c(data)).to_le_bytes())?;
    Ok(())
}

/// Fast-skip one record from `reader` (reads length + length-CRC, skips payload
/// bytes and data-CRC without decoding).
///
/// Returns `Ok(true)` on success, `Ok(false)` on clean EOF, `Err` on any error.
#[cfg(feature = "io-tfrecord")]
fn skip_tfrecord_entry<R: Read>(reader: &mut R) -> std::io::Result<bool> {
    let mut first = [0u8; 1];
    match reader.read_exact(&mut first) {
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(false),
        Err(e) => return Err(e),
        Ok(()) => {}
    }
    let mut length_bytes = [0u8; 8];
    length_bytes[0] = first[0];
    reader.read_exact(&mut length_bytes[1..])?;

    // Verify the length CRC so corruption is caught during counting too.
    let mut len_crc_bytes = [0u8; 4];
    reader.read_exact(&mut len_crc_bytes)?;
    let stored = u32::from_le_bytes(len_crc_bytes);
    let computed = mask_crc(crc32c::crc32c(&length_bytes));
    if stored != computed {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "TFRecord length CRC mismatch",
        ));
    }

    // Skip payload + data-CRC without reading.
    #[allow(clippy::cast_possible_truncation)] // TFRecord files > 4 GiB per record not realistic
    let length = u64::from_le_bytes(length_bytes) as usize;
    let to_skip = length + 4; // payload + 4-byte data-CRC
    let mut skip_buf = vec![0u8; to_skip];
    reader.read_exact(&mut skip_buf)?;

    Ok(true)
}

/// Count every record in `reader` using the fast-skip path. O(n), O(1) memory.
#[cfg(feature = "io-tfrecord")]
fn tfrecord_count_records<R: Read>(mut reader: R, path: &Path) -> Result<u64> {
    let mut n = 0u64;
    loop {
        match skip_tfrecord_entry(&mut reader) {
            Ok(true) => n += 1,
            Ok(false) => break,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "count TFRecord entry #{} in {}: {e}",
                    n + 1,
                    path.display()
                ));
            }
        }
    }
    Ok(n)
}

/// Build [`TFRecordShards`] from a pre-counted total and `records_per_shard`.
#[cfg(feature = "io-tfrecord")]
fn make_tfrecord_shards(path: PathBuf, total: u64, records_per_shard: usize) -> TFRecordShards {
    if total == 0 {
        return TFRecordShards {
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
    TFRecordShards {
        path,
        ranges,
        total_records: total,
    }
}

/// Open `path` with compression auto-detection and return a buffered reader.
#[cfg(feature = "io-tfrecord")]
fn open_tfrecord_reader(path: &Path) -> Result<BufReader<Box<dyn Read>>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let inner = auto_detect_reader(f, path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    Ok(BufReader::new(inner))
}

// ── Vector I/O ────────────────────────────────────────────────────────────────

/// Read a `TFRecord` file into a `Vec<Vec<u8>>` (one raw byte record per element).
///
/// Each record's bytes are returned as-is; use [`read_tfrecord_examples_vec`]
/// to additionally decode records as `tf.Example` protobufs.
/// Compression is auto-detected from the file extension.
///
/// # Errors
/// Returns an error if the file cannot be opened, a CRC check fails, or the
/// file is truncated mid-record. When the `io-tfrecord` feature is disabled,
/// always returns an error.
#[cfg(feature = "io-tfrecord")]
pub fn read_tfrecord_vec(path: impl AsRef<Path>) -> Result<Vec<Vec<u8>>> {
    let path = path.as_ref();
    let mut rdr = open_tfrecord_reader(path)?;
    let mut out = Vec::new();
    loop {
        match read_tfrecord_entry(&mut rdr) {
            Ok(None) => break,
            Ok(Some(data)) => out.push(data),
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "read TFRecord entry #{} in {}: {e}",
                    out.len() + 1,
                    path.display()
                ));
            }
        }
    }
    Ok(out)
}

/// Write a `Vec<Vec<u8>>` as a `TFRecord` file (one raw-bytes record per element).
///
/// Parent directories are created as needed. Compression is auto-detected from
/// the file extension (e.g. `.gz`, `.zst`).
///
/// # Returns
/// The number of records written (`data.len()`).
///
/// # Errors
/// Returns an error if the file/dirs cannot be created or any entry cannot be
/// written. When the `io-tfrecord` feature is disabled, always returns an error.
#[cfg(feature = "io-tfrecord")]
pub fn write_tfrecord_vec(path: impl AsRef<Path>, data: &[Vec<u8>]) -> Result<usize> {
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut w = auto_detect_writer(f, path)
        .with_context(|| format!("setup compression for {}", path.display()))?;
    for (i, record) in data.iter().enumerate() {
        write_tfrecord_entry(&mut w, record)
            .with_context(|| format!("write TFRecord entry #{i} to {}", path.display()))?;
    }
    w.flush().context("flush TFRecord writer")?;
    Ok(data.len())
}

/// Write `TFRecord` data in parallel while keeping **deterministic final order**.
///
/// Because `TFRecord` records are self-delimiting and the format has no per-file
/// header, shard byte streams can be directly concatenated.
///
/// * `shards`: if `None`, defaults to `num_cpus::get().max(2)`.
///
/// # Returns
/// The number of records written (`data.len()`).
///
/// # Errors
/// Returns an error if any shard file cannot be written. When the `io-tfrecord`
/// feature is disabled, always returns an error.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(all(feature = "parallel-io", feature = "io-tfrecord"))]
pub fn write_tfrecord_par(
    path: impl AsRef<Path>,
    data: &[Vec<u8>],
    shards: Option<usize>,
) -> Result<usize> {
    use rayon::prelude::*;
    use std::fs::remove_file;
    use std::io::copy;

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
        .map(|i| path.with_extension(format!("tfrecord.part{i}")))
        .collect();

    shard_paths
        .par_iter()
        .enumerate()
        .try_for_each(|(i, p)| -> Result<()> {
            let start = i * chunk;
            let end = ((i + 1) * chunk).min(n);
            let f = File::create(p).with_context(|| format!("create {}", p.display()))?;
            let mut w = BufWriter::new(f);
            for record in &data[start..end] {
                write_tfrecord_entry(&mut w, record)
                    .with_context(|| format!("write TFRecord entry to {}", p.display()))?;
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

/// Build [`TFRecordShards`] by counting records and slicing into `records_per_shard`.
///
/// For an empty file, returns an empty set of ranges.
///
/// # Errors
/// Returns an error if the file cannot be opened or a record is corrupt.
/// When the `io-tfrecord` feature is disabled, always returns an error.
///
/// # Panics
/// If the shard calculation overflows.
#[cfg(feature = "io-tfrecord")]
pub fn build_tfrecord_shards(
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<TFRecordShards> {
    let path = path.as_ref().to_path_buf();
    let rdr = open_tfrecord_reader(&path)?;
    let total = tfrecord_count_records(rdr, &path)?;
    Ok(make_tfrecord_shards(path, total, records_per_shard))
}

/// Read a `[start, end)` record range from a `TFRecord` file into `Vec<Vec<u8>>`.
///
/// Records before `start` are fast-skipped without CRC-verifying payloads.
/// Records in `[start, end)` are fully read and CRC-verified.
///
/// # Errors
/// Returns an error if the file cannot be opened or a record is corrupt.
/// When the `io-tfrecord` feature is disabled, always returns an error.
#[cfg(feature = "io-tfrecord")]
pub fn read_tfrecord_range(src: &TFRecordShards, start: u64, end: u64) -> Result<Vec<Vec<u8>>> {
    let mut rdr = open_tfrecord_reader(&src.path)?;
    let mut out = Vec::new();
    let mut i = 0u64;
    loop {
        if i >= end {
            break;
        }
        if i < start {
            match skip_tfrecord_entry(&mut rdr) {
                Ok(true) => {
                    i += 1;
                    continue;
                }
                Ok(false) => break,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "skip TFRecord entry #{} in {}: {e}",
                        i + 1,
                        src.path.display()
                    ));
                }
            }
        }
        match read_tfrecord_entry(&mut rdr) {
            Ok(None) => break,
            Ok(Some(data)) => {
                out.push(data);
                i += 1;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "read TFRecord entry #{} in {}: {e}",
                    i + 1,
                    src.path.display()
                ));
            }
        }
    }
    Ok(out)
}

// ── tf.Example decoding (requires both io-tfrecord and io-protobuf) ───────────

/// Read a `TFRecord` file and decode every record as a `tf.Example` protobuf.
///
/// Combines [`read_tfrecord_vec`] with `prost::Message::decode` to return the
/// structured `Example` type defined in [`crate::io::tfrecord_proto`].
///
/// # Errors
/// Returns an error if the file cannot be opened, a CRC check fails, the file
/// is truncated, or any record cannot be decoded as an `Example`.
#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
pub fn read_tfrecord_examples_vec(
    path: impl AsRef<Path>,
) -> Result<Vec<crate::io::tfrecord_proto::Example>> {
    use prost::Message;
    let path = path.as_ref();
    let raw = read_tfrecord_vec(path)?;
    raw.into_iter()
        .enumerate()
        .map(|(i, bytes)| {
            crate::io::tfrecord_proto::Example::decode(&bytes[..]).with_context(|| {
                format!("decode tf.Example record #{} in {}", i + 1, path.display())
            })
        })
        .collect()
}

// ── Disabled-feature stubs ────────────────────────────────────────────────────

/// Stub: always errors when the `io-tfrecord` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-tfrecord` feature is not enabled.
#[cfg(not(feature = "io-tfrecord"))]
pub fn read_tfrecord_vec(_path: impl AsRef<std::path::Path>) -> Result<Vec<Vec<u8>>> {
    anyhow::bail!("the `io-tfrecord` feature is not enabled")
}

/// Stub: always errors when the `io-tfrecord` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-tfrecord` feature is not enabled.
#[cfg(not(feature = "io-tfrecord"))]
pub fn write_tfrecord_vec(_path: impl AsRef<std::path::Path>, _data: &[Vec<u8>]) -> Result<usize> {
    anyhow::bail!("the `io-tfrecord` feature is not enabled")
}

/// Stub: always errors when the `io-tfrecord` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-tfrecord` feature is not enabled.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(all(feature = "parallel-io", not(feature = "io-tfrecord")))]
pub fn write_tfrecord_par(
    _path: impl AsRef<std::path::Path>,
    _data: &[Vec<u8>],
    _shards: Option<usize>,
) -> Result<usize> {
    anyhow::bail!("the `io-tfrecord` feature is not enabled")
}

/// Stub: always errors when the `io-tfrecord` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-tfrecord` feature is not enabled.
///
/// # Panics
/// Never panics — this is a stub.
#[cfg(not(feature = "io-tfrecord"))]
pub fn build_tfrecord_shards(
    _path: impl AsRef<std::path::Path>,
    _records_per_shard: usize,
) -> Result<TFRecordShards> {
    anyhow::bail!("the `io-tfrecord` feature is not enabled")
}

/// Stub: always errors when the `io-tfrecord` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-tfrecord` feature is not enabled.
#[cfg(not(feature = "io-tfrecord"))]
pub fn read_tfrecord_range(_src: &TFRecordShards, _start: u64, _end: u64) -> Result<Vec<Vec<u8>>> {
    anyhow::bail!("the `io-tfrecord` feature is not enabled")
}

// ── VecOps adapter (always compiled) ─────────────────────────────────────────

/// `VecOps` adapter for streaming `TFRecord` data via [`TFRecordShards`].
///
/// The element type is always `Vec<u8>` (raw record bytes). Implements [`VecOps`]
/// so the execution engine can split and read shard ranges.
///
/// When `io-tfrecord` is disabled, `split`/`clone_any` yield `None` because
/// the range reader stub errors — but a disabled source can never be constructed.
pub struct TFRecordVecOps;

impl TFRecordVecOps {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self)
    }
}

impl VecOps for TFRecordVecOps {
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<TFRecordShards>()?;
        usize::try_from(s.total_records).ok()
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<TFRecordShards>()?;
        let mut parts = Vec::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<Vec<u8>> = read_tfrecord_range(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<TFRecordShards>()?;
        let v: Vec<Vec<u8>> = read_tfrecord_range(s, 0, s.total_records).ok()?;
        Some(Box::new(v) as Partition)
    }
}
