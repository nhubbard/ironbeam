//! JSON Lines file and object sources and deferred sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink, TypedSourceIterator};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};

#[cfg(feature = "cloud")]
mod cloud_io;
#[cfg(feature = "cloud")]
pub use cloud_io::{cloud_jsonl_glob_source, cloud_jsonl_sink, cloud_jsonl_source};

fn read_jsonl<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    use std::io::BufRead;
    std::io::BufReader::new(bytes)
        .lines()
        .enumerate()
        .filter_map(|(line, value)| match value {
            Ok(value) if value.trim().is_empty() => None,
            other => Some((line, other)),
        })
        .map(|(line, value)| {
            let value = value.map_err(|e| ironbeam_io::io("jsonl", e))?;
            serde_json::from_str(&value)
                .map_err(|e| ironbeam_io::io("jsonl", format!("line {}: {e}", line + 1)))
        })
        .collect()
}
fn write_jsonl<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    use std::io::Write;
    let file = ironbeam_io::open_encoded_output(path, "jsonl")?;
    let mut file = std::io::BufWriter::new(file);
    for value in values {
        serde_json::to_writer(&mut file, value).map_err(|e| ironbeam_io::io("jsonl", e))?;
        file.write_all(b"\n")
            .map_err(|e| ironbeam_io::io("jsonl", e))?;
    }
    file.flush().map_err(|e| ironbeam_io::io("jsonl", e))
}
pub fn jsonl_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::source(pipeline, key, path, "jsonl", read_jsonl::<T>)
}
#[allow(clippy::needless_pass_by_value)] // The shared lazy-source factory takes PathBuf.
fn read_jsonl_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    use std::io::BufRead;
    let reader = std::io::BufReader::new(ironbeam_io::open_lazy_input(&path, "jsonl")?);
    let rows = reader
        .lines()
        .enumerate()
        .filter_map(|(line, result)| match result {
            Ok(value) if value.trim().is_empty() => None,
            other => Some((line, other)),
        })
        .map(|(line, result)| {
            let value = result.map_err(|e| ironbeam_io::io("jsonl", e))?;
            serde_json::from_str(&value)
                .map_err(|e| ironbeam_io::io("jsonl", format!("line {}: {e}", line + 1)))
        });
    Ok(Box::new(rows))
}
pub fn jsonl_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::lazy_source(pipeline, key, path, "jsonl-streaming", read_jsonl_lazy::<T>)
}
fn count_jsonl_lines(path: &Path) -> Result<usize, Error> {
    use std::io::BufRead;
    std::io::BufReader::new(ironbeam_io::open_lazy_input(path, "jsonl")?)
        .lines()
        .try_fold(0_usize, |count, line| {
            line.map_err(|e| ironbeam_io::io("jsonl", e))?;
            count
                .checked_add(1)
                .ok_or_else(|| ironbeam_io::io("jsonl", "line count overflow"))
        })
}
#[allow(clippy::needless_pass_by_value)] // The shared sharded-source factory takes PathBuf.
fn read_jsonl_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    use std::io::BufRead;
    let reader = std::io::BufReader::new(ironbeam_io::open_lazy_input(&path, "jsonl")?);
    let rows = reader
        .lines()
        .enumerate()
        .skip(start)
        .take(end - start)
        .filter_map(|(line, result)| match result {
            Ok(value) if value.trim().is_empty() => None,
            other => Some((line, other)),
        })
        .map(|(line, result)| {
            let value = result.map_err(|e| ironbeam_io::io("jsonl", e))?;
            serde_json::from_str(&value)
                .map_err(|e| ironbeam_io::io("jsonl", format!("line {}: {e}", line + 1)))
        });
    Ok(Box::new(rows))
}
/// Create stable bounded JSONL line-range sources; blank lines count toward ranges.
pub fn jsonl_sharded_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    lines_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_sharded_source(
        pipeline,
        key,
        path,
        lines_per_shard,
        "jsonl",
        count_jsonl_lines,
        read_jsonl_range::<T>,
    )
}
pub fn jsonl_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    ironbeam_io::sink(input, key, path, write_jsonl::<T>)
}

#[cfg(feature = "parallel-io")]
/// Serialize contiguous shards concurrently and publish their ordered JSONL output at job commit.
pub fn jsonl_parallel_sink<T: Element + Serialize + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    if shards == 0 {
        return Err(Error::graph(
            key,
            "parallel writer needs at least one shard",
        ));
    }
    ironbeam_io::sink(input, key, path, move |path, values| {
        ironbeam_io::parallel_file_writer(path, values, shards, |part, chunk, _| {
            write_jsonl(part, chunk)
        })
    })
}

/// Expand a JSON Lines file glob into stable bounded source nodes.
pub fn jsonl_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        jsonl_source(pipeline, key, path)
    })
}
