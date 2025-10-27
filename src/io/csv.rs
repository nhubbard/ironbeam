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

/// ---------- CSV helpers (typed; serde-backed) ----------
pub fn read_csv_vec<T: DeserializeOwned>(
    path: impl AsRef<Path>,
    has_headers: bool,
) -> Result<Vec<T>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .from_path(path.as_ref())
        .with_context(|| "open csv")?;
    let mut out = Vec::<T>::new();
    for (i, rec) in rdr.deserialize::<T>().enumerate() {
        let v = rec.with_context(|| format!("parse CSV record #{}", i + 1))?;
        out.push(v);
    }
    Ok(out)
}

pub fn write_csv_vec<T: Serialize>(
    path: impl AsRef<Path>,
    has_headers: bool,
    data: &[T],
) -> Result<usize> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() && !parent.as_os_str().is_empty() {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let mut wtr = WriterBuilder::new()
        .has_headers(has_headers)
        .from_path(path)
        .with_context(|| format!("create csv {}", path.display()))?;
    for (i, row) in data.iter().enumerate() {
        wtr.serialize(row)
            .with_context(|| format!("serialize CSV row #{}", i + 1))?;
    }
    wtr.flush()?;
    Ok(data.len())
}

/// Streaming CSV: shard by row count (excluding header).
#[derive(Clone)]
pub struct CsvShards {
    pub path: PathBuf,
    pub ranges: Vec<(u64, u64)>, // [start_row, end_row)
    pub total_rows: u64,
    pub has_headers: bool,
}

pub fn build_csv_shards(
    path: impl AsRef<Path>,
    has_headers: bool,
    rows_per_shard: usize,
) -> Result<CsvShards> {
    let path = path.as_ref().to_path_buf();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .from_path(&path)?;
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

pub fn read_csv_range<T: DeserializeOwned>(
    src: &CsvShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(src.has_headers)
        .from_path(&src.path)?;
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

pub struct CsvVecOps<T>(std::marker::PhantomData<T>);

impl<T> CsvVecOps<T> {
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

/// Convenience wrapper that accepts &Vec<T>.
pub fn write_csv<T: Serialize>(
    path: impl AsRef<Path>,
    has_headers: bool,
    data: &Vec<T>,
) -> Result<usize> {
    write_csv_vec(path, has_headers, data.as_slice())
}

/// Parallel CSV writer that keeps the final file **deterministically ordered**.
/// It serializes contiguous chunks in parallel into per-chunk buffers, then
/// writes header (once) + buffers in chunk index order into a single file.
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

/// Split [0, len) into `parts` contiguous ranges. Returns (chunk_idx, start, end).
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
