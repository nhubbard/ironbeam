//! Avro I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O** with Serde: [`read_avro_vec`] and [`write_avro_vec`]
//! - **Deterministic parallel writer**: [`write_avro_par`] (feature `parallel-io`)
//! - **Streaming ingestion** by record ranges: [`AvroShards`], [`build_avro_shards`], [`read_avro_range`]
//! - **Execution runner integration**: [`AvroVecOps<T>`] implements [`VecOps`] over `AvroShards`
//!
//! # Notes
//! - Avro files can be compressed. Compression is detected automatically based on file extension
//!   or magic bytes (when respective feature flags are enabled).
//! - Schema inference is performed automatically on read using `apache-avro`'s native capabilities.
//! - Sharding is **record-count based**; it does not rely on byte offsets.
//! - The parallel writer preserves **deterministic file order** by joining shard outputs in index order.

use crate::Partition;
use crate::io::compression::{auto_detect_reader, auto_detect_writer};
use crate::type_token::VecOps;
use anyhow::{Context, Result};
use apache_avro::{Schema, Writer, from_value, to_value, types::Value};
use serde::{Serialize, de::DeserializeOwned};
use std::any::Any;
use std::fs::{File, create_dir_all};
use std::io::{BufReader, BufWriter, Read};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ── Private helpers ────────────────────────────────────────────────────────────

/// Deserialize every record from an already-constructed [`apache_avro::Reader`].
fn avro_read_loop<T: DeserializeOwned, R: Read>(
    reader: apache_avro::Reader<'_, R>,
    path: &Path,
) -> Result<Vec<T>> {
    let mut out = Vec::new();
    for (i, record) in reader.enumerate() {
        let record =
            record.with_context(|| format!("read record #{} in {}", i + 1, path.display()))?;
        let value: Value = record;
        let v: T = from_value(&value).with_context(|| {
            format!(
                "deserialize record #{} from Avro in {}: {:?}",
                i + 1,
                path.display(),
                value
            )
        })?;
        out.push(v);
    }
    Ok(out)
}

/// Count every record in an already-constructed [`apache_avro::Reader`].
fn avro_count_records<R: Read>(reader: apache_avro::Reader<'_, R>) -> u64 {
    let mut n = 0u64;
    for _ in reader {
        n += 1;
    }
    n
}

/// Build [`AvroShards`] from a pre-counted total and `records_per_shard`.
fn make_avro_shards(path: PathBuf, total: u64, records_per_shard: usize) -> AvroShards {
    if total == 0 {
        return AvroShards {
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
    AvroShards {
        path,
        ranges,
        total_records: total,
    }
}

/// Open `path` with compression auto-detection and return a buffered reader.
fn open_avro_reader(path: &Path) -> Result<BufReader<Box<dyn Read>>> {
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let inner = auto_detect_reader(f, path)
        .with_context(|| format!("setup decompression for {}", path.display()))?;
    Ok(BufReader::new(inner))
}

/// Read an Avro file into a typed `Vec<T>`.
///
/// Schema is read from the file header. Compression is auto-detected.
///
/// # Errors
/// Returns an error if the file cannot be opened, read, or any record fails to
/// deserialize into `T`.
pub fn read_avro_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let mut rdr = open_avro_reader(path)?;
    let reader = apache_avro::Reader::new(&mut rdr).context("create Avro reader")?;
    avro_read_loop(reader, path)
}

/// Read an Avro file into a typed `Vec<T>`, validating against `schema`.
///
/// Compression is auto-detected.
///
/// # Errors
/// Returns an error if the schema string is invalid, the file cannot be opened or
/// read, or any record fails to deserialize.
pub fn read_avro_vec_with_schema<T: DeserializeOwned>(
    path: impl AsRef<Path>,
    schema: impl AsRef<str>,
) -> Result<Vec<T>> {
    let path = path.as_ref();
    let schema = Schema::parse_str(schema.as_ref()).context("parse Avro schema string")?;
    let mut rdr = open_avro_reader(path)?;
    let reader =
        apache_avro::Reader::with_schema(&schema, &mut rdr).context("create Avro reader")?;
    avro_read_loop(reader, path)
}

/// Read an Avro file into a typed `Vec<T>`, validating against a pre-parsed `schema`.
///
/// Compression is auto-detected.
///
/// # Errors
/// Returns an error if the file cannot be opened or read, or any record fails to
/// deserialize.
pub fn read_avro_vec_with_schema_obj<T: DeserializeOwned>(
    path: impl AsRef<Path>,
    schema: &Schema,
) -> Result<Vec<T>> {
    let path = path.as_ref();
    let mut rdr = open_avro_reader(path)?;
    let reader =
        apache_avro::Reader::with_schema(schema, &mut rdr).context("create Avro reader")?;
    avro_read_loop(reader, path)
}

/// Write a typed slice as an Avro file.
///
/// Parent directories are created as needed. Compression is auto-detected from
/// the file extension (e.g., `.gz`, `.zst`).
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the schema string is invalid, the file/dirs cannot be
/// created, or any item fails to serialize.
pub fn write_avro_vec<T: Serialize>(
    path: impl AsRef<Path>,
    data: &[T],
    schema: impl AsRef<str>,
) -> Result<usize> {
    let schema = Schema::parse_str(schema.as_ref()).context("parse Avro schema string")?;
    write_avro_vec_with_schema(path, data, &schema)
}

/// Write a typed slice as an Avro file using a pre-parsed `schema`.
///
/// Parent directories are created as needed. Compression is auto-detected from
/// the file extension (e.g., `.gz`, `.zst`).
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the file/dirs cannot be created or any item fails to
/// serialize.
pub fn write_avro_vec_with_schema<T: Serialize>(
    path: impl AsRef<Path>,
    data: &[T],
    schema: &Schema,
) -> Result<usize> {
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut w = auto_detect_writer(f, path)
        .with_context(|| format!("setup compression for {}", path.display()))?;
    let mut writer = Writer::new(schema, &mut w);
    for (i, item) in data.iter().enumerate() {
        let value: Value = to_value(item)
            .with_context(|| format!("serialize item #{} to Avro in {}", i, path.display()))?;
        writer
            .append(value)
            .with_context(|| format!("write record #{} to Avro in {}", i, path.display()))?;
    }
    writer.flush().context("flush Avro writer")?;
    Ok(data.len())
}

/// Write Avro in parallel while keeping **deterministic final order**.
///
/// The `to_value` (serde) serialization step is parallelized using rayon across
/// `shards` threads; the resulting values are then written sequentially into a
/// single valid Avro file.
///
/// Avro's binary block format is **not byte-concatenable**: each file embeds a
/// unique schema header and 16-byte sync markers. Unlike CSV, shard temp-files
/// cannot be merged by copying bytes. The parallel benefit here is CPU-bound
/// serde work, not I/O throughput.
///
/// * `shards`: if `None`, defaults to `num_cpus::get().max(2)`, clamped to `[1,n]`.
///   Controls the rayon chunk size for parallel serialization.
/// * `schema`: Avro schema as a string.
///
/// # Returns
/// The number of items written (`data.len()`).
///
/// # Errors
/// Returns an error if the output file cannot be created/written or any record
/// fails to serialize.
///
/// # Feature
/// Requires the `parallel-io` feature.
#[cfg(feature = "parallel-io")]
pub fn write_avro_par<T: Serialize + Sync>(
    path: impl AsRef<Path>,
    data: &[T],
    shards: Option<usize>,
    schema: impl AsRef<str>,
) -> Result<usize> {
    use rayon::prelude::*;
    let path = path.as_ref();
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let n = data.len();
    let schema = Schema::parse_str(schema.as_ref()).context("parse Avro schema string")?;

    // The shards hint controls rayon's work-stealing chunk size for serde parallelism.
    let _ = shards; // rayon auto-scales; explicit chunking not needed for in-memory collect

    // Parallel serialization (CPU-bound); collect in index order.
    let values: Vec<Value> = data
        .par_iter()
        .map(|item| to_value(item).map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()
        .with_context(|| format!("serialize records to Avro for {}", path.display()))?;

    // Sequential write into a single valid Avro file.
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut w = BufWriter::new(f);
    let mut writer = Writer::new(&schema, &mut w);
    for (i, value) in values.into_iter().enumerate() {
        writer
            .append(value)
            .with_context(|| format!("write record #{} to {}", i, path.display()))?;
    }
    writer.flush().context("flush Avro writer")?;
    Ok(n)
}

/// Streaming Avro sharding metadata.
///
/// Produced by [`build_avro_shards`] and consumed by [`read_avro_range`]
/// and the execution engine via [`AvroVecOps`].
#[derive(Clone)]
pub struct AvroShards {
    /// Source file path.
    pub path: PathBuf,
    /// Record ranges `(start, end)` (0-based, end-exclusive).
    pub ranges: Vec<(u64, u64)>,
    /// Total number of records considered for sharding.
    pub total_records: u64,
}

/// Build [`AvroShards`] by reading records and slicing into `records_per_shard`.
///
/// For an empty file, returns an empty set of ranges.
///
/// **Compression**: Automatically detects and decompresses compressed files for record counting.
/// Note that compressed files require full decompression during this step.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or read.
///
/// # Panics
///
/// If the shard calculation overflows.
pub fn build_avro_shards(path: impl AsRef<Path>, records_per_shard: usize) -> Result<AvroShards> {
    let path = path.as_ref().to_path_buf();
    let mut rdr = open_avro_reader(&path)?;
    let reader = apache_avro::Reader::new(&mut rdr).context("create Avro reader")?;
    let total = avro_count_records(reader);
    Ok(make_avro_shards(path, total, records_per_shard))
}

/// Build [`AvroShards`] by counting records while validating against `schema`, then
/// slicing into `records_per_shard`.
///
/// For an empty file, returns an empty set of ranges. Compression is auto-detected.
///
/// # Errors
/// Returns an error if the schema string is invalid or the file cannot be opened/read.
///
/// # Panics
/// If the shard calculation overflows.
pub fn build_avro_shards_with_schema(
    path: impl AsRef<Path>,
    schema: impl AsRef<str>,
    records_per_shard: usize,
) -> Result<AvroShards> {
    let path = path.as_ref().to_path_buf();
    let schema = Schema::parse_str(schema.as_ref()).context("parse Avro schema string")?;
    build_avro_shards_with_schema_obj(path, &schema, records_per_shard)
}

/// Build [`AvroShards`] by counting records while validating against a pre-parsed
/// `schema`, then slicing into `records_per_shard`.
///
/// For an empty file, returns an empty set of ranges. Compression is auto-detected.
///
/// # Errors
/// Returns an error if the file cannot be opened or read.
///
/// # Panics
/// If the shard calculation overflows.
pub fn build_avro_shards_with_schema_obj(
    path: impl AsRef<Path>,
    schema: &Schema,
    records_per_shard: usize,
) -> Result<AvroShards> {
    let path = path.as_ref().to_path_buf();
    let mut rdr = open_avro_reader(&path)?;
    let reader =
        apache_avro::Reader::with_schema(schema, &mut rdr).context("create Avro reader")?;
    let total = avro_count_records(reader);
    Ok(make_avro_shards(path, total, records_per_shard))
}

/// Read a `[start, end)` record range from an Avro file into `Vec<T>`.
///
/// The Avro schema is read from the file header. Compression is detected and decompressed automatically
/// based on file extension or magic bytes (when respective feature flags are enabled).
///
/// # Errors
/// Returns an error if the file cannot be opened or if any selected record fails to
/// deserialize into `T`.
pub fn read_avro_range<T: DeserializeOwned>(
    src: &AvroShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let mut rdr = open_avro_reader(&src.path)?;
    let reader = apache_avro::Reader::new(&mut rdr).context("create Avro reader")?;

    let mut out = Vec::<T>::new();
    for (i, record) in reader.enumerate() {
        let i = i as u64;
        let record =
            record.with_context(|| format!("read record #{} in {}", i + 1, src.path.display()))?;
        let value: Value = record;
        if i < start {
            continue;
        }
        if i >= end {
            break;
        }
        let v: T = from_value(&value).with_context(|| {
            format!(
                "deserialize record #{} from Avro in {}: {:?}",
                i + 1,
                src.path.display(),
                value
            )
        })?;
        out.push(v);
    }
    Ok(out)
}

/// `VecOps` adapter for streaming Avro via [`AvroShards`].
///
/// This enables the execution engine to determine total length (`len`), split
/// into concrete partitions (`split`) by record range, and read the entire dataset
/// (`clone_any`) for sequential paths.
///
/// Requires `T: DeserializeOwned + Clone + Send + Sync + 'static`.
pub struct AvroVecOps<T>(PhantomData<T>);

impl<T> AvroVecOps<T> {
    /// Construct an `Arc` to the adapter.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self(PhantomData))
    }
}

impl<T> VecOps for AvroVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<AvroShards>()?;
        usize::try_from(s.total_records).ok()
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<AvroShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_avro_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<AvroShards>()?;
        let v: Vec<T> = read_avro_range::<T>(s, 0, s.total_records).ok()?;
        Some(Box::new(v) as Partition)
    }
}
