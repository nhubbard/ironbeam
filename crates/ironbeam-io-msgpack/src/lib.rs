//! `MessagePack` sources and deferred final-result sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink, TypedSourceIterator};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};

fn read_msgpack<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let mut cursor = std::io::Cursor::new(bytes);
    let mut rows = Vec::new();
    while cursor.position() < bytes.len() as u64 {
        rows.push(rmp_serde::from_read(&mut cursor).map_err(|e| ironbeam_io::io("msgpack", e))?);
    }
    Ok(rows)
}
fn write_msgpack<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    let mut file = ironbeam_io::open_encoded_output(path, "msgpack")?;
    for value in values {
        rmp_serde::encode::write(&mut file, value).map_err(|e| ironbeam_io::io("msgpack", e))?;
    }
    Ok(())
}
pub fn msgpack_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::source(pipeline, key, path, "msgpack", read_msgpack::<T>)
}
// The shared lazy-source callback owns its path even though opening only borrows it.
#[allow(clippy::needless_pass_by_value)]
fn read_msgpack_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    use std::io::BufRead;
    let file = ironbeam_io::open_lazy_input(&path, "msgpack")?;
    let mut reader = std::io::BufReader::new(file);
    let mut done = false;
    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }
        match reader.fill_buf() {
            Ok([]) => None,
            Ok(_) => {
                let item =
                    rmp_serde::from_read(&mut reader).map_err(|e| ironbeam_io::io("msgpack", e));
                if item.is_err() {
                    done = true;
                }
                Some(item)
            }
            Err(error) => {
                done = true;
                Some(Err(ironbeam_io::io("msgpack", error)))
            }
        }
    })))
}
pub fn msgpack_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::lazy_source(
        pipeline,
        key,
        path,
        "msgpack-streaming",
        read_msgpack_lazy::<T>,
    )
}
pub fn msgpack_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    ironbeam_io::sink(input, key, path, write_msgpack::<T>)
}

#[cfg(feature = "parallel-io")]
pub fn msgpack_parallel_sink<T: Element + Serialize + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    ironbeam_io::framed_parallel_sink(input, key, path, shards, write_msgpack::<T>)
}

/// Expand a `MessagePack` file glob into stable bounded source nodes.
pub fn msgpack_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        msgpack_source(pipeline, key, path)
    })
}

fn count_msgpack_records<T: Element + DeserializeOwned>(path: &Path) -> Result<usize, Error> {
    ironbeam_io::count_lazy_records(path, read_msgpack_lazy::<T>)
}

fn read_msgpack_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    ironbeam_io::read_lazy_range(path, start, end, read_msgpack_lazy::<T>)
}

/// Create stable bounded `MessagePack` record-range sources.
pub fn msgpack_sharded_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        "msgpack",
        count_msgpack_records::<T>,
        read_msgpack_range::<T>,
    )
}
