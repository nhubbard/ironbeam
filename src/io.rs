use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{create_dir_all, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

/// Read newline-delimited JSON (JSONL) into a Vec<T>.
/// Each line must be a valid JSON object/array matching T.
pub fn read_jsonl_vec<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(f);

    // Fast line-by-line; gives better error messages (line numbers).
    let mut out = Vec::<T>::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read line {} in {}", idx + 1, path.display()))?;
        if line.trim().is_empty() { continue; }
        let v: T = serde_json::from_str(&line).with_context(|| {
            format!("parse JSONL line {} in {}: {}", idx + 1, path.display(), line)
        })?;
        out.push(v);
    }
    Ok(out)
}

/// Write a Vec<T> to JSONL; one compact JSON value per line.
/// Creates parent directories if needed.
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