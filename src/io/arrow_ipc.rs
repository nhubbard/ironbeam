//! Arrow IPC I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Batch-level vector I/O**: [`read_arrow_ipc_vec`] and [`write_arrow_ipc_vec`]
//!   (element type: [`ArrowBatch`], a serde-compatible wrapper around `RecordBatch`)
//! - **Row-level vector I/O**: [`read_arrow_ipc_rows_vec`] and
//!   [`write_arrow_ipc_rows_vec`] (element type: any `T: Serialize + DeserializeOwned`)
//! - **Deterministic parallel writers**: [`write_arrow_ipc_par`] and
//!   [`write_arrow_ipc_rows_par`] (feature `parallel-io`)
//! - **Streaming ingestion** by batch ranges: [`ArrowShards`],
//!   [`build_arrow_shards`], [`read_arrow_ipc_range`], [`read_arrow_ipc_rows_range`]
//! - **Execution runner integration**: [`ArrowBatchVecOps`] and [`ArrowRowVecOps`]
//!   implement [`VecOps`] over [`ArrowShards`]
//!
//! # Feature gating
//! Unlike most other I/O modules, **this module provides no stubs**. All public
//! functions reference Arrow types (`RecordBatch`, `ArrowBatch`) in their
//! signatures and therefore cannot compile without `arrow`/`serde_arrow`. All
//! items except [`ArrowShards`] are gated on `#[cfg(feature = "io-arrow")]`.
//!
//! # Wire format
//! Uses the Arrow IPC **file format** (magic `ARROW1` + schema + record blocks
//! and footer). This format requires a complete file before reading, so shard
//! part-files are each a complete IPC file (not byte-concatenable). The parallel
//! writer serializes each shard to its own temp file in parallel and then merges
//! all temp files into one final IPC file.
//!
//! # Sharding unit
//! [`ArrowShards`] shards by **`RecordBatch` count**, not individual rows.
//! `ranges` contains `(start_batch, end_batch)` pairs (end-exclusive).
//! `total_rows` is a separate field for `VecOps` row-count reporting.

use crate::type_token::VecOps;
use std::path::PathBuf;

#[cfg(feature = "io-arrow")]
use crate::Partition;
#[cfg(feature = "io-arrow")]
use anyhow::Context;
#[cfg(feature = "io-arrow")]
use anyhow::Result;
#[cfg(feature = "io-arrow")]
use arrow::datatypes::{FieldRef, Schema};
#[cfg(feature = "io-arrow")]
use arrow::ipc::{reader::FileReader, writer::FileWriter};
#[cfg(feature = "io-arrow")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "io-arrow")]
use serde::Serialize;
#[cfg(feature = "io-arrow")]
use serde::de::DeserializeOwned;
#[cfg(feature = "io-arrow")]
use serde_arrow::schema::{SchemaLike, TracingOptions};
#[cfg(feature = "io-arrow")]
use serde_arrow::{from_record_batch, to_record_batch};
#[cfg(feature = "io-arrow")]
use std::any::Any;
#[cfg(feature = "io-arrow")]
use std::fs::{File, create_dir_all, remove_file};
#[cfg(feature = "io-arrow")]
use std::io::BufReader;
#[cfg(feature = "io-arrow")]
use std::marker::PhantomData;
#[cfg(feature = "io-arrow")]
use std::path::Path;
#[cfg(feature = "io-arrow")]
use std::sync::Arc;

// ── Always-compiled sharding metadata ────────────────────────────────────────

/// Streaming Arrow IPC sharding metadata.
///
/// Produced by [`build_arrow_shards`] and consumed by [`read_arrow_ipc_range`],
/// [`read_arrow_ipc_rows_range`], [`ArrowBatchVecOps`], and [`ArrowRowVecOps`].
///
/// Sharding is by **`RecordBatch` count**. `ranges` contains `(start_batch,
/// end_batch)` pairs (end-exclusive). `total_rows` accumulates the sum of
/// `num_rows()` over all batches, used by [`ArrowRowVecOps::len`].
#[derive(Clone)]
pub struct ArrowShards {
    /// Source file path.
    pub path: PathBuf,
    /// Batch-index ranges `(start, end)` (0-based, end-exclusive).
    pub ranges: Vec<(usize, usize)>,
    /// Total number of `RecordBatch`es in the file.
    pub total_batches: usize,
    /// Total number of rows across all batches.
    pub total_rows: u64,
}

// ── ArrowBatch: serde-compatible RecordBatch wrapper ─────────────────────────

/// A serde-serializable wrapper around [`arrow::record_batch::RecordBatch`].
///
/// The inner `RecordBatch` is encoded as Arrow IPC file bytes for serialization
/// and decoded from those bytes for deserialization. This makes `RecordBatch`
/// usable as a [`crate::Element`] in `PCollection<ArrowBatch>` even when the
/// `coders` feature is enabled (which requires `Serialize + DeserializeOwned`).
///
/// Access the inner batch via the public `.0` field.
#[cfg(feature = "io-arrow")]
#[derive(Clone)]
pub struct ArrowBatch(pub RecordBatch);

#[cfg(feature = "io-arrow")]
impl serde::Serialize for ArrowBatch {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let bytes = batch_to_ipc_bytes(&self.0).map_err(serde::ser::Error::custom)?;
        bytes.serialize(serializer)
    }
}

#[cfg(feature = "io-arrow")]
impl<'de> serde::Deserialize<'de> for ArrowBatch {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let cursor = std::io::Cursor::new(bytes);
        let mut reader = FileReader::try_new(cursor, None).map_err(serde::de::Error::custom)?;
        match reader.next() {
            Some(Ok(b)) => Ok(Self(b)),
            Some(Err(e)) => Err(serde::de::Error::custom(e)),
            None => Err(serde::de::Error::custom(
                "empty IPC file in ArrowBatch deserialization",
            )),
        }
    }
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// Encode a single `RecordBatch` as Arrow IPC file bytes.
#[cfg(feature = "io-arrow")]
fn batch_to_ipc_bytes(batch: &RecordBatch) -> arrow::error::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let schema = batch.schema();
    let mut writer = FileWriter::try_new(&mut buf, schema.as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Write `batches` to a file, creating parent directories as needed.
#[cfg(feature = "io-arrow")]
fn write_batches_to_path(path: &Path, batches: &[RecordBatch]) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent)
            .with_context(|| format!("create parent dir {}", parent.display()))?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let schema = if batches.is_empty() {
        Arc::new(Schema::new(Vec::<FieldRef>::new()))
    } else {
        batches[0].schema()
    };
    let mut writer = FileWriter::try_new(file, schema.as_ref()).context("create IPC FileWriter")?;
    for batch in batches {
        writer.write(batch).context("write RecordBatch")?;
    }
    writer.finish().context("finish IPC file")?;
    Ok(())
}

/// Open a `FileReader` for the given path.
#[cfg(feature = "io-arrow")]
fn open_file_reader(path: &Path) -> Result<FileReader<BufReader<File>>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    FileReader::try_new(BufReader::new(file), None)
        .with_context(|| format!("open IPC FileReader for {}", path.display()))
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Read all `RecordBatch`es from an Arrow IPC file.
///
/// # Errors
/// Returns an error if the file cannot be opened or any batch cannot be decoded.
#[cfg(feature = "io-arrow")]
pub fn read_arrow_ipc_vec(path: impl AsRef<Path>) -> Result<Vec<RecordBatch>> {
    let path = path.as_ref();
    open_file_reader(path)?
        .map(|b| b.context("read RecordBatch"))
        .collect()
}

/// Write `RecordBatch`es to an Arrow IPC file.
///
/// Returns the total number of rows written. When `batches` is empty, a
/// zero-batch IPC file with an empty schema is written.
///
/// # Errors
/// Returns an error if the file cannot be created or written.
#[cfg(feature = "io-arrow")]
pub fn write_arrow_ipc_vec(path: impl AsRef<Path>, batches: &[RecordBatch]) -> Result<usize> {
    write_batches_to_path(path.as_ref(), batches)?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

/// Read an Arrow IPC file and deserialize every row as `T`.
///
/// Iterates all batches in the file and calls `serde_arrow::from_record_batch`
/// for each.
///
/// # Errors
/// Returns an error if the file cannot be read or any batch cannot be
/// deserialized to `T`.
#[cfg(feature = "io-arrow")]
pub fn read_arrow_ipc_rows_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let mut rows = Vec::new();
    for batch in open_file_reader(path)? {
        let batch = batch.context("read RecordBatch")?;
        let chunk: Vec<T> = from_record_batch(&batch).context("deserialize RecordBatch to T")?;
        rows.extend(chunk);
    }
    Ok(rows)
}

/// Serialize `rows` as a single `RecordBatch` and write to an Arrow IPC file.
///
/// Schema is inferred from `T` using `serde_arrow`'s `SchemaLike::from_type`.
///
/// # Errors
/// Returns an error if schema inference, conversion, or file I/O fails.
#[cfg(feature = "io-arrow")]
pub fn write_arrow_ipc_rows_vec<T: Serialize + DeserializeOwned + 'static>(
    path: impl AsRef<Path>,
    rows: &[T],
) -> Result<usize> {
    let fields: Vec<FieldRef> = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .context("infer Arrow schema from type T")?;
    let batch = to_record_batch(&fields, &rows).context("convert rows to RecordBatch")?;
    write_batches_to_path(path.as_ref(), &[batch])?;
    Ok(rows.len())
}

/// Write `RecordBatch`es to an Arrow IPC file in parallel.
///
/// The input batches are split into `shards` chunks; each chunk is written to
/// a temp IPC file concurrently via Rayon. The temp files are then merged in
/// order into the final file and deleted.
///
/// `shards = None` defaults to `num_cpus::get().max(2)`, clamped to
/// `[1, batches.len()]`.
///
/// # Errors
/// Returns an error if any temp file cannot be written or the final merge fails.
#[cfg(all(feature = "io-arrow", feature = "parallel-io"))]
pub fn write_arrow_ipc_par(
    path: impl AsRef<Path>,
    batches: &[RecordBatch],
    shards: Option<usize>,
) -> Result<usize> {
    use rayon::prelude::*;

    let path = path.as_ref();
    let n = batches.len();

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }

    if n == 0 {
        write_batches_to_path(path, &[])?;
        return Ok(0);
    }

    let requested = shards.unwrap_or_else(|| num_cpus::get().max(2));
    let actual = requested.clamp(1, n);
    let chunk_size = n.div_ceil(actual);
    let n_chunks = n.div_ceil(chunk_size);

    let temp_paths: Vec<PathBuf> = (0..n_chunks)
        .map(|i| path.with_extension(format!("arrow.part{i}")))
        .collect();

    temp_paths
        .par_iter()
        .enumerate()
        .try_for_each(|(i, temp)| -> Result<()> {
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(n);
            write_batches_to_path(temp, &batches[start..end])
                .with_context(|| format!("write shard {i} to {}", temp.display()))
        })?;

    let mut merged: Vec<RecordBatch> = Vec::new();
    for temp in &temp_paths {
        let part = read_arrow_ipc_vec(temp)
            .with_context(|| format!("read shard from {}", temp.display()))?;
        merged.extend(part);
    }
    write_batches_to_path(path, &merged)?;

    for temp in &temp_paths {
        let _ = remove_file(temp);
    }
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

/// Serialize `rows` to Arrow IPC in parallel (row-level).
///
/// Each shard chunk is converted to a `RecordBatch` and written to a temp IPC
/// file in parallel. Temp files are merged in order and deleted.
///
/// `shards = None` defaults to `num_cpus::get().max(2)`, clamped to
/// `[1, rows.len()]`.
///
/// # Errors
/// Returns an error if schema inference, conversion, or I/O fails.
#[cfg(all(feature = "io-arrow", feature = "parallel-io"))]
pub fn write_arrow_ipc_rows_par<T>(
    path: impl AsRef<Path>,
    rows: &[T],
    shards: Option<usize>,
) -> Result<usize>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    use rayon::prelude::*;

    let path = path.as_ref();
    let n = rows.len();

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }

    if n == 0 {
        write_arrow_ipc_rows_vec::<T>(path, &[])?;
        return Ok(0);
    }

    let requested = shards.unwrap_or_else(|| num_cpus::get().max(2));
    let actual = requested.clamp(1, n);
    let chunk_size = n.div_ceil(actual);
    let n_chunks = n.div_ceil(chunk_size);

    let temp_paths: Vec<PathBuf> = (0..n_chunks)
        .map(|i| path.with_extension(format!("arrow.part{i}")))
        .collect();

    temp_paths
        .par_iter()
        .enumerate()
        .try_for_each(|(i, temp)| -> Result<()> {
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(n);
            let fields: Vec<FieldRef> = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
                .context("infer Arrow schema from type T")?;
            let batch = to_record_batch(&fields, &&rows[start..end])
                .context("convert rows chunk to RecordBatch")?;
            write_batches_to_path(temp, &[batch])
                .with_context(|| format!("write shard {i} to {}", temp.display()))
        })?;

    let mut merged: Vec<RecordBatch> = Vec::new();
    for temp in &temp_paths {
        let part = read_arrow_ipc_vec(temp)
            .with_context(|| format!("read shard from {}", temp.display()))?;
        merged.extend(part);
    }
    write_batches_to_path(path, &merged)?;

    for temp in &temp_paths {
        let _ = remove_file(temp);
    }
    Ok(n)
}

/// Scan an Arrow IPC file and build [`ArrowShards`] with batch-index ranges.
///
/// Each shard covers at most `batches_per_shard` record batches. If the file
/// has zero batches, the returned shards have empty ranges. If
/// `batches_per_shard == 0`, it is treated as 1.
///
/// # Errors
/// Returns an error if the file cannot be opened or batch counts cannot be read.
#[cfg(feature = "io-arrow")]
pub fn build_arrow_shards(path: impl AsRef<Path>, batches_per_shard: usize) -> Result<ArrowShards> {
    let batches_per_shard = batches_per_shard.max(1);
    let path = path.as_ref().to_path_buf();

    let reader = open_file_reader(&path)?;
    let total_batches = reader.num_batches();

    // Count total rows (reads each batch header via iterator).
    let reader2 = open_file_reader(&path)?;
    let total_rows: u64 = reader2
        .map(|b| b.map(|batch| batch.num_rows() as u64))
        .collect::<Result<Vec<_>, _>>()
        .context("count rows in IPC file")?
        .into_iter()
        .sum();

    if total_batches == 0 {
        return Ok(ArrowShards {
            path,
            ranges: vec![],
            total_batches: 0,
            total_rows: 0,
        });
    }

    let mut ranges = Vec::new();
    let mut start = 0;
    while start < total_batches {
        let end = (start + batches_per_shard).min(total_batches);
        ranges.push((start, end));
        start = end;
    }

    Ok(ArrowShards {
        path,
        ranges,
        total_batches,
        total_rows,
    })
}

/// Read a batch-index range from an Arrow IPC file.
///
/// Returns all `RecordBatch`es whose indices fall in `[start_batch, end_batch)`.
///
/// # Errors
/// Returns an error if the file cannot be read or any batch is malformed.
#[cfg(feature = "io-arrow")]
pub fn read_arrow_ipc_range(
    shards: &ArrowShards,
    start_batch: usize,
    end_batch: usize,
) -> Result<Vec<RecordBatch>> {
    open_file_reader(&shards.path)?
        .skip(start_batch)
        .take(end_batch.saturating_sub(start_batch))
        .map(|b| b.context("read RecordBatch"))
        .collect()
}

/// Read a batch-index range and deserialize every row as `T`.
///
/// # Errors
/// Returns an error if the file cannot be read or any batch cannot be
/// deserialized.
#[cfg(feature = "io-arrow")]
pub fn read_arrow_ipc_rows_range<T: DeserializeOwned>(
    shards: &ArrowShards,
    start_batch: usize,
    end_batch: usize,
) -> Result<Vec<T>> {
    let batches = read_arrow_ipc_range(shards, start_batch, end_batch)?;
    let mut rows: Vec<T> = Vec::new();
    for batch in batches {
        let chunk: Vec<T> = from_record_batch(&batch).context("deserialize RecordBatch to T")?;
        rows.extend(chunk);
    }
    Ok(rows)
}

// ── VecOps adapters (fully gated) ────────────────────────────────────────────

/// `VecOps` adapter for streaming Arrow IPC at the `RecordBatch` level.
///
/// Element type is [`ArrowBatch`] (a serde-compatible wrapper around
/// `RecordBatch`). Requires the `io-arrow` feature.
///
/// When the feature is enabled, `split` reads each shard's batch range and
/// wraps each `RecordBatch` in `ArrowBatch`.
#[cfg(feature = "io-arrow")]
pub struct ArrowBatchVecOps;

#[cfg(feature = "io-arrow")]
impl ArrowBatchVecOps {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

#[cfg(feature = "io-arrow")]
impl VecOps for ArrowBatchVecOps {
    fn len(&self, data: &dyn Any) -> Option<usize> {
        data.downcast_ref::<ArrowShards>().map(|s| s.total_batches)
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<ArrowShards>()?;
        let mut parts = Vec::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let batches = read_arrow_ipc_range(s, start, end).ok()?;
            let wrapped: Vec<ArrowBatch> = batches.into_iter().map(ArrowBatch).collect();
            parts.push(Box::new(wrapped) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<ArrowShards>()?;
        let batches = read_arrow_ipc_range(s, 0, s.total_batches).ok()?;
        let wrapped: Vec<ArrowBatch> = batches.into_iter().map(ArrowBatch).collect();
        Some(Box::new(wrapped) as Partition)
    }
}

/// `VecOps` adapter for streaming Arrow IPC at the row level.
///
/// Element type is `T: DeserializeOwned + Clone + Send + Sync + 'static`.
/// `len` reports `total_rows` (not `total_batches`). `split` reads each shard
/// and deserializes all rows in that batch range.
#[cfg(feature = "io-arrow")]
pub struct ArrowRowVecOps<T>(PhantomData<T>);

#[cfg(feature = "io-arrow")]
impl<T> ArrowRowVecOps<T> {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self(PhantomData))
    }
}

#[cfg(feature = "io-arrow")]
impl<T> VecOps for ArrowRowVecOps<T>
where
    T: DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<ArrowShards>()?;
        usize::try_from(s.total_rows).ok()
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<ArrowShards>()?;
        let mut parts = Vec::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let rows: Vec<T> = read_arrow_ipc_rows_range(s, start, end).ok()?;
            parts.push(Box::new(rows) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<ArrowShards>()?;
        let rows: Vec<T> = read_arrow_ipc_rows_range(s, 0, s.total_batches).ok()?;
        Some(Box::new(rows) as Partition)
    }
}
