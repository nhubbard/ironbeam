//! Bounded Parquet file sources and deferred sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SourceProgress, TypedSourceIterator,
};
#[cfg(feature = "parallel-io")]
use ironbeam_io::parallel_container_writer;
use ironbeam_io::{
    count_lazy_records, file_glob_source, file_hash, file_sharded_source, io, lazy_source,
    read_lazy_range, sink, source,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    path::{Path, PathBuf},
};

fn read_parquet<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let data = bytes::Bytes::copy_from_slice(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .map_err(|e| io("parquet", e))?
        .build()
        .map_err(|e| io("parquet", e))?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| io("parquet", e))?;
        rows.extend(
            serde_arrow::from_record_batch::<Vec<T>>(&batch).map_err(|e| io("parquet", e))?,
        );
    }
    Ok(rows)
}
fn write_parquet<T: Serialize + DeserializeOwned>(path: &Path, values: &[T]) -> Result<(), Error> {
    use arrow::datatypes::FieldRef;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};
    let fields =
        Vec::<FieldRef>::from_type::<T>(TracingOptions::default()).map_err(|e| io("parquet", e))?;
    let batch = serde_arrow::to_record_batch(&fields, &values).map_err(|e| io("parquet", e))?;
    let file = fs::File::create(path).map_err(|e| io("parquet", e))?;
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).map_err(|e| io("parquet", e))?;
    writer.write(&batch).map_err(|e| io("parquet", e))?;
    writer.close().map_err(|e| io("parquet", e))?;
    Ok(())
}
pub fn parquet_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "parquet", read_parquet::<T>)
}
fn read_parquet_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = fs::File::open(path).map_err(|e| io("parquet", e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io("parquet", e))?
        .build()
        .map_err(|e| io("parquet", e))?;
    Ok(Box::new(reader.flat_map(|batch| match batch {
        Ok(batch) => match serde_arrow::from_record_batch::<Vec<T>>(&batch) {
            Ok(rows) => rows.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(error) => vec![Err(io("parquet", error))],
        },
        Err(error) => vec![Err(io("parquet", error))],
    })))
}
pub fn parquet_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    lazy_source(
        pipeline,
        key,
        path,
        "parquet-streaming",
        read_parquet_lazy::<T>,
    )
}
fn read_parquet_row_groups_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = fs::File::open(path).map_err(|e| io("parquet", e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io("parquet", e))?
        .with_row_groups((start..end).collect())
        .build()
        .map_err(|e| io("parquet", e))?;
    Ok(Box::new(reader.flat_map(|batch| match batch {
        Ok(batch) => match serde_arrow::from_record_batch::<Vec<T>>(&batch) {
            Ok(rows) => rows.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(error) => vec![Err(io("parquet", error))],
        },
        Err(error) => vec![Err(io("parquet", error))],
    })))
}
/// Plan bounded Parquet splits from row-group metadata, matching v4's
/// `groups_per_shard` behavior without decoding earlier row groups per split.
pub fn parquet_row_group_sharded_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    groups_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    if groups_per_shard == 0 {
        return Err(Error::graph(key, "row groups per shard must be positive"));
    }
    let path = path.as_ref().to_path_buf();
    let expected = file_hash(&path, key)?;
    let file = fs::File::open(&path).map_err(|e| io(key, e))?;
    let metadata = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| io(key, e))?
        .metadata()
        .clone();
    let group_count = metadata.num_row_groups();
    let shard_count = group_count.max(1).div_ceil(groups_per_shard);
    let mut sources = Vec::with_capacity(shard_count);
    for index in 0..shard_count {
        let start = index * groups_per_shard;
        let end = start.saturating_add(groups_per_shard).min(group_count);
        let rows = (start..end).try_fold(0_usize, |total, group| {
            let count = usize::try_from(metadata.row_group(group).num_rows())
                .map_err(|_| Error::resource(key, "row group count exceeds usize"))?;
            total
                .checked_add(count)
                .ok_or_else(|| Error::resource(key, "row group count overflow"))
        })?;
        let child_key = format!("{key}/group-{index}");
        let digest = format!(
            "parquet-row-groups:{}:{expected:02x?}:{start}:{end}",
            path.display()
        );
        let source_path = path.clone();
        let source_expected = expected.clone();
        let name = child_key.clone();
        sources.push(pipeline.bounded_source_with_semantic(
            &child_key,
            "ironbeam/io/parquet-row-group-shard",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            digest,
            Some(rows),
            move || {
                if file_hash(&source_path, &name)? != source_expected {
                    return Err(Error::recovery(
                        &name,
                        "source contents changed after graph construction",
                    ));
                }
                read_parquet_row_groups_lazy(source_path.clone(), start, end)
            },
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}
pub fn parquet_sink<T: Element + Serialize + DeserializeOwned>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_parquet::<T>)
}

#[cfg(feature = "parallel-io")]
pub fn parquet_parallel_sink<T: Element + Serialize + DeserializeOwned + Sync>(
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
        parallel_container_writer(
            path,
            values,
            shards,
            "parquet",
            write_parquet::<T>,
            read_parquet::<T>,
        )
    })
}

/// Expand a sorted local glob into bounded Parquet sources.
pub fn parquet_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        parquet_source(pipeline, key, path)
    })
}

fn count_parquet_records<T: Element + DeserializeOwned>(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_parquet_lazy::<T>)
}
fn read_parquet_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    read_lazy_range(path, start, end, read_parquet_lazy::<T>)
}
/// Split a bounded Parquet file by finite record ranges.
pub fn parquet_sharded_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        "parquet",
        count_parquet_records::<T>,
        read_parquet_range::<T>,
    )
}
