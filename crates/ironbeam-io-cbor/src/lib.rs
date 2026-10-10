//! CBOR sources and deferred final-result sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink, TypedSourceIterator};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};

fn read_cbor<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let mut cursor = std::io::Cursor::new(bytes);
    let mut rows = Vec::new();
    while (cursor.position() as usize) < bytes.len() {
        rows.push(ciborium::de::from_reader(&mut cursor).map_err(|e| ironbeam_io::io("cbor", e))?);
    }
    Ok(rows)
}
fn write_cbor<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    let mut file = ironbeam_io::open_encoded_output(path, "cbor")?;
    for value in values {
        ciborium::ser::into_writer(value, &mut file).map_err(|e| ironbeam_io::io("cbor", e))?;
    }
    Ok(())
}
pub fn cbor_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::source(pipeline, key, path, "cbor", read_cbor::<T>)
}
fn read_cbor_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    use std::io::BufRead;
    let file = ironbeam_io::open_lazy_input(&path, "cbor")?;
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
                    ciborium::de::from_reader(&mut reader).map_err(|e| ironbeam_io::io("cbor", e));
                if item.is_err() {
                    done = true;
                }
                Some(item)
            }
            Err(error) => {
                done = true;
                Some(Err(ironbeam_io::io("cbor", error)))
            }
        }
    })))
}
pub fn cbor_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::lazy_source(pipeline, key, path, "cbor-streaming", read_cbor_lazy::<T>)
}
pub fn cbor_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    ironbeam_io::sink(input, key, path, write_cbor::<T>)
}

#[cfg(feature = "parallel-io")]
pub fn cbor_parallel_sink<T: Element + Serialize + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    ironbeam_io::framed_parallel_sink(input, key, path, shards, write_cbor::<T>)
}

/// Expand a CBOR file glob into stable bounded source nodes.
pub fn cbor_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        cbor_source(pipeline, key, path)
    })
}

fn count_cbor_records<T: Element + DeserializeOwned>(path: &Path) -> Result<usize, Error> {
    ironbeam_io::count_lazy_records(path, read_cbor_lazy::<T>)
}

fn read_cbor_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    ironbeam_io::read_lazy_range(path, start, end, read_cbor_lazy::<T>)
}

/// Create stable bounded CBOR record-range sources.
pub fn cbor_sharded_source<T: Element + DeserializeOwned>(
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
        "cbor",
        count_cbor_records::<T>,
        read_cbor_range::<T>,
    )
}
