use crate::type_token::VecOps;
use crate::Partition;
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::any::Any;
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn read_jsonl_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let rdr = BufReader::new(f);
    let mut out = Vec::<T>::new();
    for (i, line) in rdr.lines().enumerate() {
        let line = line.with_context(|| format!("read line {} in {}", i + 1, path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        let v: T = serde_json::from_str(&line).with_context(|| {
            format!("parse JSONL line {} in {}: {}", i + 1, path.display(), line)
        })?;
        out.push(v);
    }
    Ok(out)
}

pub fn write_jsonl_vec<T: Serialize>(path: impl AsRef<Path>, data: &[T]) -> Result<usize> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
        }
    }
    let f = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut w = BufWriter::new(f);
    for (i, item) in data.iter().enumerate() {
        serde_json::to_writer(&mut w, item)
            .with_context(|| format!("serialize item #{} to {}", i, path.display()))?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(data.len())
}

#[cfg(feature = "parallel-io")]
pub fn write_jsonl_par<T: Serialize + Send + Sync>(
    path: impl AsRef<Path>,
    data: &[T],
    shards: Option<usize>,
) -> Result<usize> {
    use rayon::prelude::*;
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
        }
    }
    let n = data.len();
    if n == 0 {
        File::create(path)?; // touch
        return Ok(0);
    }
    let shards = shards.unwrap_or_else(|| num_cpus::get().max(2)).clamp(1, n);
    let chunk = (n + shards - 1) / shards;

    let shard_paths: Vec<PathBuf> = (0..shards)
        .map(|i| path.with_extension(format!("jsonl.part{i}")))
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
                serde_json::to_writer(&mut w, item)?;
                w.write_all(b"\n")?;
            }
            w.flush()?;
            Ok(())
        })?;

    // concat in order
    let mut out = BufWriter::new(File::create(path)?);
    for p in &shard_paths {
        let mut r = BufReader::new(File::open(p)?);
        std::io::copy(&mut r, &mut out)?;
    }
    out.flush()?;
    for p in shard_paths {
        let _ = std::fs::remove_file(p);
    }
    Ok(n)
}

/// ---------- streaming JSONL source support ----------
/// We pre-scan the file to build (start_line, end_line) shard ranges.
/// `split()` reads and parses only the lines in its own range, returning a Vec<T>.
#[derive(Clone)]
pub struct JsonlShards {
    pub path: PathBuf,
    pub ranges: Vec<(u64, u64)>, // [start, end)
    pub total_lines: u64,
}

pub fn build_jsonl_shards(path: impl AsRef<Path>, lines_per_shard: usize) -> Result<JsonlShards> {
    let path = path.as_ref().to_path_buf();
    let f = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let rdr = BufReader::new(f);
    let mut total: u64 = 0;
    for line in rdr.lines() {
        let _ = line?;
        total += 1;
    }
    if total == 0 {
        return Ok(JsonlShards {
            path,
            ranges: vec![],
            total_lines: 0,
        });
    }
    let lps = lines_per_shard.max(1) as u64;
    let shards = ((total + lps - 1) / lps) as usize;
    let mut ranges = Vec::with_capacity(shards);
    for i in 0..shards {
        let start = (i as u64) * lps;
        let end = ((i as u64 + 1) * lps).min(total);
        ranges.push((start, end));
    }
    Ok(JsonlShards {
        path,
        ranges,
        total_lines: total,
    })
}

/// Read a single [start,end) range into Vec<T>
pub fn read_jsonl_range<T: DeserializeOwned>(
    src: &JsonlShards,
    start: u64,
    end: u64,
) -> Result<Vec<T>> {
    let f = File::open(&src.path).with_context(|| format!("open {}", src.path.display()))?;
    let rdr = BufReader::new(f);
    let mut out = Vec::<T>::new();
    for (i, line) in rdr.lines().enumerate() {
        let i = i as u64;
        let line = line?;
        if i < start {
            continue;
        }
        if i >= end {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let v: T = serde_json::from_str(&line)
            .with_context(|| format!("parse JSONL line {} in {}", i + 1, src.path.display()))?;
        out.push(v);
    }
    Ok(out)
}

/// VecOps for JSONL shards, typed over T.
pub struct JsonlVecOps<T>(std::marker::PhantomData<T>);

impl<T> JsonlVecOps<T> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self(std::marker::PhantomData))
    }
}

impl<T> VecOps for JsonlVecOps<T>
where
    T: DeserializeOwned + Send + Sync + Clone + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        let s = data.downcast_ref::<JsonlShards>()?;
        Some(s.total_lines as usize)
    }
    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<JsonlShards>()?;
        let mut parts = Vec::<Partition>::with_capacity(s.ranges.len());
        for &(start, end) in &s.ranges {
            let v: Vec<T> = read_jsonl_range::<T>(s, start, end).ok()?;
            parts.push(Box::new(v) as Partition);
        }
        Some(parts)
    }
    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        // read full file if sequential
        let s = data.downcast_ref::<JsonlShards>()?;
        let v: Vec<T> = read_jsonl_range::<T>(s, 0, s.total_lines).ok()?;
        Some(Box::new(v) as Partition)
    }
}
