//! Protocol Buffers I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O** via `prost`: `read_proto_vec` and `write_proto_vec`
//! - **Deterministic parallel writer**: `write_proto_par` (feature `parallel-io`)
//! - **Streaming ingestion** by record ranges: [`ProtoShards`], [`build_proto_shards`],
//!   `read_proto_range`
//! - **Execution runner integration**: [`ProtoVecOps<T>`] implements [`VecOps`] over
//!   [`ProtoShards`]
//!
//! # Feature gating
//!
//! Unlike serde-based connectors, not all functions have runtime stubs when the
//! `io-protobuf` feature is disabled, because `prost::Message` is a crate trait that
//! is unavailable without the dependency. Functions whose signatures reference
//! `prost::Message` are **fully gated** (not compiled when the feature is off). Only
//! [`build_proto_shards`] provides a stub because its return type ([`ProtoShards`]) uses
//! no prost types. The [`ProtoVecOps<T>`] struct is always compiled; its [`VecOps`]
//! implementation is gated on the feature.
//!
//! # Notes
//! - A protobuf file is a flat concatenation of **length-delimited records**: each
//!   record is a varint-encoded byte length followed by the raw encoded protobuf bytes.
//!   This framing is self-delimiting and byte-concatenable, so shard part-files can be
//!   concatenated just like JSONL or `MessagePack`.
//! - Sharding is **record-count-based**; it does not rely on byte offsets.
//! - Compression is detected automatically based on file extension or magic bytes
//!   (when the respective feature flags are enabled).
//! - Values are encoded with `prost::Message::encode_to_vec` and decoded with
//!   `prost::Message::decode`.

use crate::Partition;
use crate::type_token::VecOps;
use anyhow::Result;
use std::any::Any;
use std::marker::PhantomData;
use std::path::PathBuf;

#[cfg(feature = "io-protobuf")]
use crate::io::compression::{auto_detect_reader, auto_detect_writer};
#[cfg(feature = "io-protobuf")]
use anyhow::Context;
#[cfg(feature = "io-protobuf")]
use std::fs::{File, create_dir_all};
#[cfg(feature = "io-protobuf")]
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
#[cfg(feature = "io-protobuf")]
use std::path::Path;

// ── Streaming sharding metadata (always available) ────────────────────────────

/// Streaming Protocol Buffers sharding metadata.
///
/// Produced by [`build_proto_shards`] and consumed by `read_proto_range`
/// and the execution engine via [`ProtoVecOps`].
#[derive(Clone)]
pub struct ProtoShards {
    /// Source file path.
    pub path: PathBuf,
    /// Record ranges `(start, end)` (0-based, end-exclusive).
    pub ranges: Vec<(u64, u64)>,
    /// Total number of records in the file.
    pub total_records: u64,
}

// ── Private helpers (only compiled with the feature) ─────────────────────────

/// Read a varint from `reader`.
///
/// Returns `Ok(None)` on a clean EOF at the first byte of the varint (record
/// boundary). Returns `Ok(Some(len))` on success. Returns `Err` on any other
/// I/O error or a malformed (overlong) varint.
#[cfg(feature = "io-protobuf")]
fn read_varint<R: Read>(reader: &mut R) -> std::io::Result<Option<usize>> {
    let mut result = 0usize;
    let mut shift = 0usize;
    let mut first = true;
    loop {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte) {
            Err(e) if e.kind() == ErrorKind::UnexpectedEof && first => return Ok(None),
            Err(e) => return Err(e),
            Ok(()) => {}
        }
        first = false;
        let b = byte[0];
        result |= ((b & 0x7F) as usize) << shift;
        if b & 0x80 == 0 {
            return Ok(Some(result));
        }
        shift += 7;
        if shift >= 63 {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "protobuf length varint too large",
            ));
        }
    }
}

/// Write `value` as a varint to `writer`.
#[cfg(feature = "io-protobuf")]
fn write_varint<W: Write>(writer: &mut W, mut value: usize) -> std::io::Result<()> {
    loop {
        #[allow(clippy::cast_possible_truncation)] // masked to 7 bits, always ≤ 127
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            writer.write_all(&[byte])?;
            break;
        }
        writer.write_all(&[byte | 0x80])?;
    }
    Ok(())
}

/// Deserialize every record from `reader` into a typed `Vec<T>`.
///
/// Stops cleanly when the stream is exhausted at a record boundary (varint EOF);
/// any other error is propagated with the offending record index.
#[cfg(feature = "io-protobuf")]
fn proto_read_loop<T: prost::Message + Default, R: Read>(
    mut reader: R,
    path: &Path,
) -> Result<Vec<T>> {
    let mut out = Vec::<T>::new();
    loop {
        let len = match read_varint(&mut reader) {
            Ok(None) => break,
            Ok(Some(l)) => l,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "read proto record #{} length in {}: {e}",
                    out.len() + 1,
                    path.display()
                ));
            }
        };
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data).with_context(|| {
            format!(
                "read proto record #{} data in {}",
                out.len() + 1,
                path.display()
            )
        })?;
        let msg = T::decode(&data[..]).with_context(|| {
            format!(
                "decode proto record #{} in {}",
                out.len() + 1,
                path.display()
            )
        })?;
        out.push(msg);
    }
    Ok(out)
}

/// Count every record reachable from `reader` by reading the length prefix and
/// skipping the payload. O(n) records, O(1) memory.
#[cfg(feature = "io-protobuf")]
fn proto_count_records<R: Read>(mut reader: R, path: &Path) -> Result<u64> {
    let mut n = 0u64;
    loop {
        let len = match read_varint(&mut reader) {
            Ok(None) => break,
            Ok(Some(l)) => l,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "count proto record #{} length in {}: {e}",
                    n + 1,
                    path.display()
                ));
            }
        };
        let mut skip = vec![0u8; len];
        reader
            .read_exact(&mut skip)
            .with_context(|| format!("count proto record #{} data in {}", n + 1, path.display()))?;
        n += 1;
    }
    Ok(n)
}

/// Build [`ProtoShards`] from a pre-counted total and `records_per_shard`.
#[cfg(feature = "io-protobuf")]
fn make_proto_shards(path: PathBuf, total: u64, records_per_shard: usize) -> ProtoShards {
    if total == 0 {
        return ProtoShards {
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
    ProtoShards {
        path,
        ranges,
        total_records: total,
    }
}

/// Open `path` with compression auto-detection and return a buffered reader.
#[cfg(feature = "io-protobuf")]
fn open_proto_reader(path: &Path) -> Result<BufReader<Box<dyn Read>>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let inner = auto_detect_reader(f, path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    Ok(BufReader::new(inner))
}

// ── Vector I/O ────────────────────────────────────────────────────────────────

/// Read a length-delimited protobuf file into a typed `Vec<T>`.
///
/// Each record is a varint-encoded byte length followed by the raw protobuf
/// bytes. Compression is auto-detected.
///
/// # Errors
/// Returns an error if the file cannot be opened or read, or if any record fails
/// to decode. When the `io-protobuf` feature is disabled this function does not
/// compile.
#[cfg(feature = "io-protobuf")]
pub fn read_proto_vec<T: prost::Message + Default>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let rdr = open_proto_reader(path)?;
    proto_read_loop(rdr, path)
}

/// Write a typed slice as a length-delimited protobuf file (one record per entry).
///
/// Each element is encoded with `prost::Message::encode_to_vec`; the byte length is
/// written as a varint prefix. Parent directories are created as needed. Compression
/// is auto-detected from the file extension.
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the file/dirs cannot be created or any item fails to
/// encode/flush. When the `io-protobuf` feature is disabled this function does not
/// compile.
#[cfg(feature = "io-protobuf")]
pub fn write_proto_vec<T: prost::Message>(path: impl AsRef<Path>, data: &[T]) -> Result<usize> {
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
        let buf = item.encode_to_vec();
        write_varint(&mut w, buf.len())
            .with_context(|| format!("write proto record #{i} length in {}", path.display()))?;
        w.write_all(&buf)
            .with_context(|| format!("write proto record #{i} data in {}", path.display()))?;
    }
    w.flush().context("flush proto writer")?;
    Ok(data.len())
}

/// Write protobuf records in parallel while keeping **deterministic final order**.
///
/// The input slice is split into contiguous shards; each shard is serialized to a
/// temporary part file in parallel, then all parts are concatenated in shard index
/// order into the final file. Temporary files are removed at the end.
///
/// Because length-delimited protobuf records are self-delimiting, concatenating shard
/// byte streams yields a valid combined file.
///
/// * `shards`: if `None`, defaults to `num_cpus::get().max(2)`, clamped to `[1, n]`.
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if part or output files cannot be created/written. When the
/// `io-protobuf` feature is disabled this function does not compile.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(all(feature = "parallel-io", feature = "io-protobuf"))]
pub fn write_proto_par<T: prost::Message + Send + Sync>(
    path: impl AsRef<Path>,
    data: &[T],
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
    let requested = shards.unwrap_or_else(|| num_cpus::get().max(2));
    let actual = requested.clamp(1, n);
    let chunk = n.div_ceil(actual);
    let non_empty = n.div_ceil(chunk);

    let shard_paths: Vec<PathBuf> = (0..non_empty)
        .map(|i| path.with_extension(format!("proto.part{i}")))
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
                let buf = item.encode_to_vec();
                write_varint(&mut w, buf.len())
                    .with_context(|| format!("write proto record length in {}", p.display()))?;
                w.write_all(&buf)
                    .with_context(|| format!("write proto record data in {}", p.display()))?;
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

/// Build [`ProtoShards`] by counting records and slicing into `records_per_shard`.
///
/// For an empty file, returns an empty set of ranges.
///
/// **Compression**: Automatically detects and decompresses compressed files for
/// record counting.
///
/// # Errors
/// Returns an error if the file cannot be opened or read. When the `io-protobuf`
/// feature is disabled, always returns an error.
///
/// # Panics
/// If the shard calculation overflows.
#[cfg(feature = "io-protobuf")]
pub fn build_proto_shards(path: impl AsRef<Path>, records_per_shard: usize) -> Result<ProtoShards> {
    let path = path.as_ref().to_path_buf();
    let rdr = open_proto_reader(&path)?;
    let total = proto_count_records(rdr, &path)?;
    Ok(make_proto_shards(path, total, records_per_shard))
}

/// Stub returned when the `io-protobuf` feature is disabled.
///
/// # Errors
/// Always returns an error: the `io-protobuf` feature is not enabled.
#[cfg(not(feature = "io-protobuf"))]
pub fn build_proto_shards(
    _path: impl AsRef<std::path::Path>,
    _records_per_shard: usize,
) -> Result<ProtoShards> {
    anyhow::bail!("the `io-protobuf` feature is not enabled")
}

/// Read a `[start, end)` record range from a protobuf file into `Vec<T>`.
///
/// Compression is auto-detected. Because the stream is sequential, records before
/// `start` are decoded and discarded.
///
/// # Errors
/// Returns an error if the file cannot be opened or if any selected record fails to
/// decode. When the `io-protobuf` feature is disabled this function does not compile.
#[cfg(feature = "io-protobuf")]
pub fn read_proto_range<T: prost::Message + Default>(
    src: &ProtoShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let mut rdr = open_proto_reader(&src.path)?;
    let mut out = Vec::<T>::new();
    let mut i = 0u64;
    loop {
        if i >= end {
            break;
        }
        let len = match read_varint(&mut rdr) {
            Ok(None) => break,
            Ok(Some(l)) => l,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "read proto record #{} length in {}: {e}",
                    i + 1,
                    src.path.display()
                ));
            }
        };
        let mut data = vec![0u8; len];
        rdr.read_exact(&mut data).with_context(|| {
            format!(
                "read proto record #{} data in {}",
                i + 1,
                src.path.display()
            )
        })?;
        if i >= start {
            let msg = T::decode(&data[..]).with_context(|| {
                format!("decode proto record #{} in {}", i + 1, src.path.display())
            })?;
            out.push(msg);
        }
        i += 1;
    }
    Ok(out)
}

// ── VecOps adapter (struct always available, impl feature-gated) ──────────────

/// `VecOps` adapter for streaming protobuf via [`ProtoShards`].
///
/// The struct is always compiled; the [`VecOps`] implementation is gated on the
/// `io-protobuf` feature. When the feature is disabled, the adapter cannot be
/// instantiated as a `VecOps` — but that is fine because the helpers that create
/// streaming sources are also fully gated.
pub struct ProtoVecOps<T>(PhantomData<T>);

impl<T> ProtoVecOps<T> {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self(PhantomData))
    }
}

#[cfg(feature = "io-protobuf")]
impl<T> VecOps for ProtoVecOps<T>
where
    T: prost::Message + Default + Clone + Send + Sync + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<ProtoShards>()?;
        usize::try_from(s.total_records).ok()
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<ProtoShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_proto_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<ProtoShards>()?;
        let v: Vec<T> = read_proto_range::<T>(s, 0, s.total_records).ok()?;
        Some(Box::new(v) as Partition)
    }
}
