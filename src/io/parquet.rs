use std::any::Any;
use anyhow::{Context, Result};
use arrow::datatypes::FieldRef;                 // <-- needed for SchemaLike
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{de::DeserializeOwned, Serialize};
use serde_arrow::{from_record_batch, to_record_batch};
use serde_arrow::schema::{SchemaLike, TracingOptions}; // <-- from_type lives on SchemaLike
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parquet::file::reader::{FileReader, SerializedFileReader};
use crate::Partition;
use crate::type_token::VecOps;

/// Write a typed Vec<T> to Parquet.
/// Accepts &Vec<T> (Sized) because to_record_batch requires a Sized input.
pub fn write_parquet_vec<T: Serialize + serde::Deserialize<'static>>(
    path: impl AsRef<Path>,
    data: &Vec<T>,
) -> Result<usize> {
    let path = path.as_ref();

    if data.is_empty() {
        let _ = File::create(path).with_context(|| format!("create {}", path.display()))?;
        return Ok(0);
    }

    // Infer Arrow fields from T using SchemaLike::from_type
    let fields: Vec<FieldRef> =
        Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
            .context("infer Arrow schema from type T")?;

    // Convert rows -> RecordBatch
    let batch: RecordBatch =
        to_record_batch(&fields, data).context("convert rows to RecordBatch")?;

    // Write Parquet
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .context("create ArrowWriter")?;
    writer.write(&batch).context("write batch to parquet")?;
    writer.close().context("close ArrowWriter")?;

    Ok(data.len())
}

/// Read a Parquet file into a typed Vec<T>.
pub fn read_parquet_vec<T: DeserializeOwned>(
    path: impl AsRef<Path>,
) -> Result<Vec<T>> {
    let path = path.as_ref();
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("open ParquetRecordBatchReader")?;
    let mut reader = builder.with_batch_size(64 * 1024).build()
        .context("build ParquetRecordBatchReader")?;

    let mut out: Vec<T> = Vec::new();
    while let Some(batch) = reader.next().transpose().context("read next batch")? {
        let mut rows: Vec<T> = from_record_batch(&batch)
            .context("deserialize RecordBatch rows to T")?;
        out.append(&mut rows);
    }
    Ok(out)
}

/// Partition a Parquet file by **row groups** into shard ranges.
#[derive(Clone)]
pub struct ParquetShards {
    pub path: PathBuf,
    /// [start_group, end_group) in row-group indices
    pub group_ranges: Vec<(usize, usize)>,
    pub total_rows: u64,
}

/// Inspect file metadata and create shard ranges of `groups_per_shard` row groups each.
/// If `groups_per_shard == 0`, defaults to 1.
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
        return Ok(ParquetShards { path, group_ranges: vec![], total_rows: 0 });
    }

    let total_rows: u64 = (0..num_groups)
        .map(|i| meta.row_group(i).num_rows() as u64)
        .sum();

    let g = groups_per_shard.max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    while start < num_groups {
        let end = (start + g).min(num_groups);
        ranges.push((start, end));
        start = end;
    }

    Ok(ParquetShards { path, group_ranges: ranges, total_rows })
}

/// Read the given row-group range [start_group, end_group) into a typed Vec<T>.
pub fn read_parquet_row_group_range<T: DeserializeOwned>(
    src: &ParquetShards,
    start_group: usize,
    end_group: usize,
) -> Result<Vec<T>> {
    let f = File::open(&src.path).with_context(|| format!("open {}", src.path.display()))?;
    let b = ParquetRecordBatchReaderBuilder::try_new(f)
        .context("open ParquetRecordBatchReader")?;
    // select only the row groups for this shard
    let groups: Vec<usize> = (start_group..end_group).collect();
    let mut reader = b.with_row_groups(groups).build().context("build row-group reader")?;

    let mut out: Vec<T> = Vec::new();
    while let Some(batch) = reader.next().transpose().context("read batch")? {
        let mut rows: Vec<T> =
            from_record_batch(&batch).context("deserialize RecordBatch rows to T")?;
        out.append(&mut rows);
    }
    Ok(out)
}

#[cfg(feature = "io-parquet")]
pub struct ParquetVecOps<T>(std::marker::PhantomData<T>);
#[cfg(feature = "io-parquet")]
impl<T> ParquetVecOps<T> {
    pub fn new() -> Arc<Self> { Arc::new(Self(std::marker::PhantomData)) }
}

#[cfg(feature = "io-parquet")]
impl<T> VecOps for ParquetVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<ParquetShards>()?;
        Some(s.total_rows as usize)
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
        let v: Vec<T> =
            read_parquet_row_group_range::<T>(s, 0, s.group_ranges.last().map(|&(_, e)| e).unwrap_or(0)).ok()?;
        Some(Box::new(v) as Partition)
    }
}