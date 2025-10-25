use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// ---------- existing helpers (unchanged) ----------
pub fn read_jsonl_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let rdr = BufReader::new(f);
    let mut out = Vec::<T>::new();
    for (i, line) in rdr.lines().enumerate() {
        let line = line.with_context(|| format!("read line {} in {}", i + 1, path.display()))?;
        if line.trim().is_empty() { continue; }
        let v: T = serde_json::from_str(&line)
            .with_context(|| format!("parse JSONL line {} in {}: {}", i + 1, path.display(), line))?;
        out.push(v);
    }
    Ok(out)
}

pub fn write_jsonl_vec<T: Serialize>(path: impl AsRef<Path>, data: &[T]) -> Result<usize> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() && !parent.as_os_str().is_empty() {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
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

/// ---------- parallel JSONL writer (stable order) ----------
pub fn write_jsonl_par<T: Serialize + Send + Sync>(
    path: impl AsRef<Path>,
    data: &[T],
    shards: Option<usize>,
) -> Result<usize> {
    use rayon::prelude::*;
    let path = path.as_ref();
    if let Some(parent) = path.parent() && !parent.as_os_str().is_empty() {
        create_dir_all(parent).with_context(|| format!("mkdir -p {}", parent.display()))?;
    }

    let n = data.len();
    if n == 0 {
        File::create(path)?; // touch
        return Ok(0);
    }

    let shards = shards.unwrap_or_else(|| num_cpus::get().max(2));
    let shards = shards.min(n).max(1);
    let chunk = (n + shards - 1) / shards;

    // write each shard in parallel to a temp file (stable index)
    let mut shard_files: Vec<PathBuf> = Vec::with_capacity(shards);
    for _ in 0..shards {
        let tmp = path.with_extension(format!(
            "{}.part{}",
            path.extension()
                .and_then(|s| s.to_str())
                .unwrap_or("jsonl"),
            shard_files.len()
        ));
        shard_files.push(tmp);
    }

    shard_files
        .par_iter()
        .enumerate()
        .try_for_each(|(idx, tmp)| -> Result<()> {
            let start = idx * chunk;
            let end = ((idx + 1) * chunk).min(n);
            if start >= end {
                File::create(tmp)?; // empty shard file
                return Ok(());
            }
            let f = File::create(tmp).with_context(|| format!("create {}", tmp.display()))?;
            let mut w = BufWriter::new(f);
            for (i, item) in data[start..end].iter().enumerate() {
                serde_json::to_writer(&mut w, item)
                    .with_context(|| format!("serialize shard {} item #{}", idx, i))?;
                w.write_all(b"\n")?;
            }
            w.flush()?;
            Ok(())
        })?;

    // concatenate shards (in order) into final
    {
        let mut out = BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
        for tmp in &shard_files {
            let f = File::open(tmp).with_context(|| format!("open shard {}", tmp.display()))?;
            let mut r = BufReader::new(f);
            std::io::copy(&mut r, &mut out)?;
        }
        out.flush()?;
    }

    // cleanup
    for tmp in shard_files {
        let _ = std::fs::remove_file(tmp);
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
        return Ok(JsonlShards { path, ranges: vec![], total_lines: 0 });
    }
    let lps = lines_per_shard.max(1) as u64;
    let shards = ((total + lps - 1) / lps) as usize;
    let mut ranges = Vec::with_capacity(shards);
    for i in 0..shards {
        let start = (i as u64) * lps;
        let end = ((i as u64 + 1) * lps).min(total);
        ranges.push((start, end));
    }
    Ok(JsonlShards { path, ranges, total_lines: total })
}

/// Read a single [start,end) range into Vec<T>
pub fn read_jsonl_range<T: DeserializeOwned>(src: &JsonlShards, start: u64, end: u64) -> Result<Vec<T>> {
    let f = File::open(&src.path).with_context(|| format!("open {}", src.path.display()))?;
    let rdr = BufReader::new(f);
    let mut out = Vec::<T>::new();
    for (i, line) in rdr.lines().enumerate() {
        let i = i as u64;
        let line = line?;
        if i < start { continue; }
        if i >= end { break; }
        if line.trim().is_empty() { continue; }
        let v: T = serde_json::from_str(&line)
            .with_context(|| format!("parse JSONL line {} in {}", i + 1, src.path.display()))?;
        out.push(v);
    }
    Ok(out)
}

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