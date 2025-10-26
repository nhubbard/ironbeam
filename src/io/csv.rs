use std::any::Any;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::Partition;
use crate::type_token::VecOps;

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
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
        }
    }
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(has_headers)
        .from_path(path)
        .with_context(|| format!("create csv {}", path.display()))?;
    for (i, row) in data.iter().enumerate() {
        wtr.serialize(row).with_context(|| format!("serialize CSV row #{}", i + 1))?;
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
    let mut rdr = csv::ReaderBuilder::new().has_headers(has_headers).from_path(&path)?;
    let mut total: u64 = 0;
    let mut it = rdr.records();
    while let Some(_) = it.next() { total += 1; }
    if total == 0 {
        return Ok(CsvShards { path, ranges: vec![], total_rows: 0, has_headers });
    }
    let rps = rows_per_shard.max(1) as u64;
    let shards = ((total + rps - 1) / rps) as usize;
    let mut ranges = Vec::with_capacity(shards);
    for i in 0..shards {
        let start = (i as u64) * rps;
        let end = ((i as u64 + 1) * rps).min(total);
        ranges.push((start, end));
    }
    Ok(CsvShards { path, ranges, total_rows: total, has_headers })
}

pub fn read_csv_range<T: DeserializeOwned>(
    src: &CsvShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let mut rdr = csv::ReaderBuilder::new().has_headers(src.has_headers).from_path(&src.path)?;
    // skip rows
    let mut out = Vec::<T>::new();
    for (i, rec) in rdr.deserialize::<T>().enumerate() {
        let i = i as u64;
        if i < start { continue; }
        if i >= end { break; }
        out.push(rec?);
    }
    Ok(out)
}

pub struct CsvVecOps<T>(std::marker::PhantomData<T>);

impl<T> CsvVecOps<T> { pub fn new() -> Arc<Self> { Arc::new(Self(std::marker::PhantomData)) } }

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