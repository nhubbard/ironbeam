//! CSV sources and deferred final-result sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink, TypedSourceIterator};
#[cfg(feature = "parallel-io")]
use ironbeam_io::parallel_file_writer;
use ironbeam_io::{
    file_glob_source, file_sharded_source, io, lazy_source, open_encoded_output, open_lazy_input,
    sink, source,
};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};

fn read_csv<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    csv::Reader::from_reader(bytes)
        .deserialize()
        .map(|value| value.map_err(|e| io("csv", e)))
        .collect()
}

fn require_named_header<T: Serialize>(values: &[T]) -> Result<(), Error> {
    let Some(first) = values.first() else {
        return Ok(());
    };
    let mut auto = csv::Writer::from_writer(Vec::new());
    auto.serialize(first).map_err(|e| io("csv", e))?;
    let auto = auto.into_inner().map_err(|e| io("csv", e))?;
    let mut bare = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    bare.serialize(first).map_err(|e| io("csv", e))?;
    let bare = bare.into_inner().map_err(|e| io("csv", e))?;
    if auto == bare {
        return Err(io(
            "csv",
            "CSV sink requires records with named fields; scalar or tuple values have no header",
        ));
    }
    Ok(())
}

fn write_csv<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    require_named_header(values)?;
    let mut writer = csv::Writer::from_writer(open_encoded_output(path, "csv")?);
    for value in values {
        writer.serialize(value).map_err(|e| io("csv", e))?;
    }
    writer.flush().map_err(|e| io("csv", e))
}

pub fn csv_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "csv", read_csv::<T>)
}

pub fn csv_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        csv_source(pipeline, key, path)
    })
}

#[allow(clippy::needless_pass_by_value)] // The shared lazy-source factory takes PathBuf.
fn read_csv_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    let reader = csv::Reader::from_reader(open_lazy_input(&path, "csv")?);
    Ok(Box::new(
        reader
            .into_deserialize()
            .map(|row| row.map_err(|e| io("csv", e))),
    ))
}

pub fn csv_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    lazy_source(pipeline, key, path, "csv-streaming", read_csv_lazy::<T>)
}

fn count_csv_rows(path: &Path) -> Result<usize, Error> {
    csv::Reader::from_reader(open_lazy_input(path, "csv")?)
        .records()
        .try_fold(0_usize, |count, row| {
            row.map_err(|e| io("csv", e))?;
            count
                .checked_add(1)
                .ok_or_else(|| io("csv", "row count overflow"))
        })
}

#[allow(clippy::needless_pass_by_value)] // The shared sharded-source factory takes PathBuf.
fn read_csv_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    let reader = csv::Reader::from_reader(open_lazy_input(&path, "csv")?);
    Ok(Box::new(
        reader
            .into_deserialize()
            .skip(start)
            .take(end - start)
            .map(|row| row.map_err(|e| io("csv", e))),
    ))
}

/// Create stable bounded CSV row-range sources; the header is excluded from ranges.
pub fn csv_sharded_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    rows_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        rows_per_shard,
        "csv",
        count_csv_rows,
        read_csv_range::<T>,
    )
}

/// Write named records with a header at successful job commit.
/// Scalar and tuple rows return an error instead of losing their first value.
pub fn csv_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_csv::<T>)
}

#[cfg(feature = "parallel-io")]
fn write_csv_fragment<T: Serialize>(path: &Path, values: &[T], first: bool) -> Result<(), Error> {
    if first {
        require_named_header(values)?;
    }
    let file = std::fs::File::create(path).map_err(|e| io("csv", e))?;
    let mut writer = csv::WriterBuilder::new()
        .has_headers(first)
        .from_writer(file);
    for value in values {
        writer.serialize(value).map_err(|e| io("csv", e))?;
    }
    writer.flush().map_err(|e| io("csv", e))
}

#[cfg(feature = "parallel-io")]
/// Serialize named records concurrently with one CSV header and ordered rows.
pub fn csv_parallel_sink<T: Element + Serialize + Sync>(
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
    sink(input, key, path, move |path, values| {
        parallel_file_writer(path, values, shards, write_csv_fragment::<T>)
    })
}
