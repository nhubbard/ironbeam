//! Parquet I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O** powered by Serde + Arrow + Parquet:
//!   - [`write_parquet_vec`] to write `&Vec<T>`
//!   - [`read_parquet_vec`] to read an entire file into `Vec<T>`
//! - **Streaming ingestion** by row-group ranges:
//!   - [`ParquetShards`] metadata (row-group slicing)
//!   - [`build_parquet_shards`] to compute ranges
//!   - [`read_parquet_row_group_range`] to read only selected groups
//! - **Execution runner integration**: [`ParquetVecOps<T>`] implements [`VecOps`]
//!   over [`ParquetShards`] so sources can be split/counted/cloned deterministically.
//!
//! Uses Arrow 56 and `serde_arrow` 0.13 (`SchemaLike::from_type` and
//! `to_record_batch`/`from_record_batch`).

use crate::type_token::VecOps;
use crate::Partition;
use anyhow::{Context, Result};
use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::{de::DeserializeOwned, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::{from_record_batch, to_record_batch};
use std::any::Any;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Write a typed `Vec<T>` to a Parquet file.
///
/// Internally:
/// 1. Infers an Arrow schema from `T` using `SchemaLike::from_type`.
/// 2. Converts `&Vec<T>` into a `RecordBatch` via `to_record_batch`.
/// 3. Writes the batch with `parquet::arrow::ArrowWriter`.
///
/// This works even when `data` is empty (a zero-row batch is written).
///
/// # Type bounds
/// `T` must be Serde-serializable/deserializable so `serde_arrow` can map it.
///
/// # Returns
/// Number of rows written (`data.len()`).
///
/// # Errors
/// An error is returned if the schema inference, conversion, file creation, or writing fails.
pub fn write_parquet_vec<T: Serialize + serde::Deserialize<'static>>(
    path: impl AsRef<Path>,
    data: &Vec<T>,
) -> Result<usize> {
    let path = path.as_ref();

    // 1) Infer fields from T (works even if data.is_empty()).
    let fields: Vec<FieldRef> = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .context("infer Arrow schema from type T")?;

    // 2) Build a RecordBatch from data (allowing zero rows).
    let batch: RecordBatch =
        to_record_batch(&fields, data).context("convert rows to RecordBatch")?;

    // 3) Open the writer with the batch schema and always close it.
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), Some(props)).context("create ArrowWriter")?;

    // Writing a zero-row batch is fine; alternatively, you could skip write() when empty.
    writer.write(&batch).context("write batch to parquet")?;
    writer.close().context("close ArrowWriter")?;

    Ok(data.len())
}

/// Read a Parquet file into a typed `Vec<T>`.
///
/// Uses `ParquetRecordBatchReaderBuilder` to iterate Arrow batches from the file
/// and converts each `RecordBatch` to `Vec<T>` via `serde_arrow::from_record_batch`,
/// appending into one final vector.
///
/// # Errors
/// Returns an error if the file cannot be opened, the reader cannot be built,
/// batch iteration fails, or conversion to `T` fails.
pub fn read_parquet_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;

    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context("open ParquetRecordBatchReader")?;
    let mut reader = builder
        .with_batch_size(64 * 1024)
        .build()
        .context("build ParquetRecordBatchReader")?;

    let mut out: Vec<T> = Vec::new();
    while let Some(batch) = reader.next().transpose().context("read next batch")? {
        let mut rows: Vec<T> =
            from_record_batch(&batch).context("deserialize RecordBatch rows to T")?;
        out.append(&mut rows);
    }
    Ok(out)
}

/// Sharding metadata for streaming Parquet reads by row groups.
///
/// Produced by [`build_parquet_shards`] and consumed by [`read_parquet_row_group_range`]
/// and the execution engine via [`ParquetVecOps`].
#[derive(Clone)]
pub struct ParquetShards {
    /// Source file path.
    pub path: PathBuf,
    /// Row-group index ranges `(start_group, end_group)` (end-exclusive).
    pub group_ranges: Vec<(usize, usize)>,
    /// Total number of rows across all row groups.
    pub total_rows: u64,
}

/// Inspect Parquet metadata and partition into ranges of `groups_per_shard` row groups.
///
/// If the file has zero row groups, it returns an empty set of ranges. If
/// `groups_per_shard == 0`, it is treated as 1.
///
/// # Errors
/// Returns an error if the file cannot be opened or metadata cannot be read.
pub fn build_parquet_shards(
    path: impl AsRef<Path>,
    groups_per_shard: usize,
) -> Result<ParquetShards> {
    let path = path.as_ref().to_path_buf();
    let f = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let reader = SerializedFileReader::new(f).context("open SerializedFileReader")?;
    let meta = reader.metadata();

    let num_groups = meta.num_row_groups();
    if num_groups == 0 {
        return Ok(ParquetShards {
            path,
            group_ranges: vec![],
            total_rows: 0,
        });
    }

    let total_rows: u64 = (0..num_groups)
        .map(|i| meta.row_group(i).num_rows().cast_unsigned())
        .sum();

    let g = groups_per_shard.max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    while start < num_groups {
        let end = (start + g).min(num_groups);
        ranges.push((start, end));
        start = end;
    }

    Ok(ParquetShards {
        path,
        group_ranges: ranges,
        total_rows,
    })
}

/// Read a row-group range `[start_group, end_group)` into `Vec<T>`.
///
/// Builds a `ParquetRecordBatchReader` restricted to the specified row groups,
/// then converts each `RecordBatch` into typed rows with
/// `serde_arrow::from_record_batch`.
///
/// # Errors
/// Returns an error if the file cannot be opened, the range reader cannot be
/// built, batch iteration fails, or conversion to `T` fails.
pub fn read_parquet_row_group_range<T: DeserializeOwned>(
    src: &ParquetShards,
    start_group: usize,
    end_group: usize,
) -> Result<Vec<T>> {
    let f = File::open(&src.path).with_context(|| format!("open {}", src.path.display()))?;
    let b = ParquetRecordBatchReaderBuilder::try_new(f).context("open ParquetRecordBatchReader")?;
    // select only the row groups for this shard
    let groups: Vec<usize> = (start_group..end_group).collect();
    let mut reader = b
        .with_row_groups(groups)
        .build()
        .context("build row-group reader")?;

    let mut out: Vec<T> = Vec::new();
    while let Some(batch) = reader.next().transpose().context("read batch")? {
        let mut rows: Vec<T> =
            from_record_batch(&batch).context("deserialize RecordBatch rows to T")?;
        out.append(&mut rows);
    }
    Ok(out)
}

/// `VecOps` adapter for streaming Parquet via [`ParquetShards`].
///
/// Enables the engine to:
/// - get total length (`len`)
/// - split into partitions by row-group ranges (`split`)
/// - read the entire dataset for sequential paths (`clone_any`)
#[cfg(feature = "io-parquet")]
pub struct ParquetVecOps<T>(std::marker::PhantomData<T>);

#[cfg(feature = "io-parquet")]
impl<T> ParquetVecOps<T> {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self(std::marker::PhantomData))
    }
}

#[cfg(feature = "io-parquet")]
impl<T> VecOps for ParquetVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<ParquetShards>()?;
        usize::try_from(s.total_rows).ok()
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<ParquetShards>()?;
        let mut parts: Vec<Partition> = Vec::with_capacity(s.group_ranges.len());
        for &(start, end) in &s.group_ranges {
            let v: Vec<T> = read_parquet_row_group_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<ParquetShards>()?;
        let v: Vec<T> = read_parquet_row_group_range::<T>(
            s,
            0,
            s.group_ranges.last().map_or(0, |&(_, e)| e),
        )
        .ok()?;
        Some(Box::new(v) as Partition)
    }
}
