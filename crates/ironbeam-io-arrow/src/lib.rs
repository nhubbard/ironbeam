//! Bounded Arrow IPC row and record-batch adapters for Ironbeam 5.
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

fn read_arrow_ipc<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    use arrow::ipc::reader::FileReader;
    let cursor = std::io::Cursor::new(bytes);
    let reader = FileReader::try_new(cursor, None).map_err(|e| io("arrow-ipc", e))?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| io("arrow-ipc", e))?;
        rows.extend(
            serde_arrow::from_record_batch::<Vec<T>>(&batch).map_err(|e| io("arrow-ipc", e))?,
        );
    }
    Ok(rows)
}
fn write_arrow_ipc<T: Serialize + DeserializeOwned>(
    path: &Path,
    values: &[T],
) -> Result<(), Error> {
    use arrow::datatypes::FieldRef;
    use arrow::ipc::writer::FileWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};
    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .map_err(|e| io("arrow-ipc", e))?;
    let batch = serde_arrow::to_record_batch(&fields, &values).map_err(|e| io("arrow-ipc", e))?;
    let file = fs::File::create(path).map_err(|e| io("arrow-ipc", e))?;
    let mut writer = FileWriter::try_new(file, &batch.schema()).map_err(|e| io("arrow-ipc", e))?;
    writer.write(&batch).map_err(|e| io("arrow-ipc", e))?;
    writer.finish().map_err(|e| io("arrow-ipc", e))?;
    Ok(())
}
pub fn arrow_ipc_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "arrow-ipc", read_arrow_ipc::<T>)
}
fn read_arrow_ipc_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    use arrow::ipc::reader::FileReader;
    let file = fs::File::open(path).map_err(|e| io("arrow-ipc", e))?;
    let reader = FileReader::try_new(file, None).map_err(|e| io("arrow-ipc", e))?;
    Ok(Box::new(reader.flat_map(|batch| match batch {
        Ok(batch) => match serde_arrow::from_record_batch::<Vec<T>>(&batch) {
            Ok(rows) => rows.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(error) => vec![Err(io("arrow-ipc", error))],
        },
        Err(error) => vec![Err(io("arrow-ipc", error))],
    })))
}
fn read_arrow_ipc_indexed_batches(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<ArrowBatch>, Error> {
    use arrow::ipc::reader::FileReader;
    let file = fs::File::open(path).map_err(|e| io("arrow-ipc", e))?;
    let mut reader = FileReader::try_new(file, None).map_err(|e| io("arrow-ipc", e))?;
    if start < end {
        reader.set_index(start).map_err(|e| io("arrow-ipc", e))?;
    }
    Ok(Box::new(reader.take(end - start).map(|batch| {
        batch.map(ArrowBatch).map_err(|e| io("arrow-ipc", e))
    })))
}
fn read_arrow_ipc_indexed_rows<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    let batches = read_arrow_ipc_indexed_batches(path, start, end)?;
    Ok(Box::new(batches.flat_map(|batch| match batch {
        Ok(ArrowBatch(batch)) => match serde_arrow::from_record_batch::<Vec<T>>(&batch) {
            Ok(rows) => rows.into_iter().map(Ok).collect::<Vec<_>>(),
            Err(error) => vec![Err(io("arrow-ipc", error))],
        },
        Err(error) => vec![Err(error)],
    })))
}
fn arrow_ipc_indexed_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    batches_per_shard: usize,
    format: &str,
    batches_are_elements: bool,
    read_range: fn(PathBuf, usize, usize) -> Result<TypedSourceIterator<T>, Error>,
) -> Result<PCollection<T>, Error> {
    use arrow::ipc::reader::FileReader;
    if batches_per_shard == 0 {
        return Err(Error::graph(key, "batches per shard must be positive"));
    }
    let path = path.as_ref().to_path_buf();
    let expected = file_hash(&path, key)?;
    let file = fs::File::open(&path).map_err(|e| io(key, e))?;
    let count = FileReader::try_new(file, None)
        .map_err(|e| io(key, e))?
        .num_batches();
    let shards = count.max(1).div_ceil(batches_per_shard);
    let mut sources = Vec::with_capacity(shards);
    for index in 0..shards {
        let start = index * batches_per_shard;
        let end = start.saturating_add(batches_per_shard).min(count);
        let child_key = format!("{key}/batch-{index}");
        let digest = format!("{format}:{}:{expected:02x?}:{start}:{end}", path.display());
        let source_path = path.clone();
        let source_expected = expected.clone();
        let name = child_key.clone();
        sources.push(pipeline.bounded_source_with_semantic(
            &child_key,
            &format!("ironbeam/io/{format}-indexed-shard"),
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            digest,
            batches_are_elements.then_some(end - start),
            move || {
                if file_hash(&source_path, &name)? != source_expected {
                    return Err(Error::recovery(
                        &name,
                        "source contents changed after graph construction",
                    ));
                }
                read_range(source_path.clone(), start, end)
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
/// Split a bounded Arrow IPC file by its indexed record batches and decode rows.
pub fn arrow_ipc_batch_sharded_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    batches_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    arrow_ipc_indexed_source(
        pipeline,
        key,
        path,
        batches_per_shard,
        "arrow-ipc-row-batches",
        false,
        read_arrow_ipc_indexed_rows::<T>,
    )
}
/// Split a bounded Arrow IPC file by its indexed record batches.
pub fn arrow_ipc_batches_indexed_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    batches_per_shard: usize,
) -> Result<PCollection<ArrowBatch>, Error> {
    arrow_ipc_indexed_source(
        pipeline,
        key,
        path,
        batches_per_shard,
        "arrow-ipc-batches",
        true,
        read_arrow_ipc_indexed_batches,
    )
}
pub fn arrow_ipc_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    lazy_source(
        pipeline,
        key,
        path,
        "arrow-ipc-streaming",
        read_arrow_ipc_lazy::<T>,
    )
}
pub fn arrow_ipc_sink<T: Element + Serialize + DeserializeOwned>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_arrow_ipc::<T>)
}

#[cfg(feature = "parallel-io")]
pub fn arrow_ipc_parallel_sink<T: Element + Serialize + DeserializeOwned + Sync>(
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
            "arrow",
            write_arrow_ipc::<T>,
            read_arrow_ipc::<T>,
        )
    })
}

#[derive(Clone, Debug)]
pub struct ArrowBatch(pub arrow::record_batch::RecordBatch);

impl Serialize for ArrowBatch {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        encode_arrow_batch(&self.0)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for ArrowBatch {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8> as serde::Deserialize>::deserialize(deserializer)?;
        Self::decode(&bytes).map_err(serde::de::Error::custom)
    }
}

fn encode_arrow_batch(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, Error> {
    use arrow::ipc::writer::FileWriter;
    let mut bytes = Vec::new();
    let mut writer =
        FileWriter::try_new(&mut bytes, &batch.schema()).map_err(|e| io("arrow-ipc", e))?;
    writer.write(batch).map_err(|e| io("arrow-ipc", e))?;
    writer.finish().map_err(|e| io("arrow-ipc", e))?;
    Ok(bytes)
}

impl Element for ArrowBatch {
    const TYPE_ID: &'static str = "arrow/record-batch";
    const CODER_ID: &'static str = "ironbeam/arrow-ipc-file-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        encode_arrow_batch(&self.0)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        use arrow::ipc::reader::FileReader;
        let mut reader = FileReader::try_new(std::io::Cursor::new(bytes), None)
            .map_err(|e| io("arrow-ipc", e))?;
        let batch = reader
            .next()
            .ok_or_else(|| Error::incompatible("empty Arrow batch coder payload"))?
            .map_err(|e| io("arrow-ipc", e))?;
        if reader.next().is_some() {
            return Err(Error::incompatible(
                "multiple batches in Arrow batch coder payload",
            ));
        }
        Ok(Self(batch))
    }
}

fn read_arrow_ipc_batches(bytes: &[u8]) -> Result<Vec<ArrowBatch>, Error> {
    use arrow::ipc::reader::FileReader;
    FileReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| io("arrow-ipc", e))?
        .map(|batch| batch.map(ArrowBatch).map_err(|e| io("arrow-ipc", e)))
        .collect()
}
fn write_arrow_ipc_batches(path: &Path, values: &[ArrowBatch]) -> Result<(), Error> {
    use arrow::{
        datatypes::{FieldRef, Schema},
        ipc::writer::FileWriter,
    };
    let schema = values.first().map_or_else(
        || std::sync::Arc::new(Schema::new(Vec::<FieldRef>::new())),
        |batch| batch.0.schema(),
    );
    let file = fs::File::create(path).map_err(|e| io("arrow-ipc", e))?;
    let mut writer = FileWriter::try_new(file, &schema).map_err(|e| io("arrow-ipc", e))?;
    for batch in values {
        writer.write(&batch.0).map_err(|e| io("arrow-ipc", e))?;
    }
    writer.finish().map_err(|e| io("arrow-ipc", e))?;
    Ok(())
}
pub fn arrow_ipc_batches_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<ArrowBatch>, Error> {
    source(
        pipeline,
        key,
        path,
        "arrow-ipc-batches",
        read_arrow_ipc_batches,
    )
}
fn read_arrow_ipc_batches_lazy(path: PathBuf) -> Result<TypedSourceIterator<ArrowBatch>, Error> {
    use arrow::ipc::reader::FileReader;
    let file = fs::File::open(path).map_err(|e| io("arrow-ipc", e))?;
    let reader = FileReader::try_new(file, None).map_err(|e| io("arrow-ipc", e))?;
    Ok(Box::new(reader.map(|batch| {
        batch.map(ArrowBatch).map_err(|e| io("arrow-ipc", e))
    })))
}
pub fn arrow_ipc_batches_streaming_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<ArrowBatch>, Error> {
    lazy_source(
        pipeline,
        key,
        path,
        "arrow-ipc-batches-streaming",
        read_arrow_ipc_batches_lazy,
    )
}
pub fn arrow_ipc_batches_sink(
    input: &PCollection<ArrowBatch>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<ArrowBatch>, Error> {
    sink(input, key, path, write_arrow_ipc_batches)
}

#[cfg(feature = "parallel-io")]
pub fn arrow_ipc_batches_parallel_sink(
    input: &PCollection<ArrowBatch>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<ArrowBatch>, Error> {
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
            "arrow",
            write_arrow_ipc_batches,
            read_arrow_ipc_batches,
        )
    })
}

pub fn arrow_ipc_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        arrow_ipc_source(pipeline, key, path)
    })
}
pub fn arrow_ipc_batches_glob_source(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<ArrowBatch>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        arrow_ipc_batches_source(pipeline, key, path)
    })
}
fn count_arrow_ipc_records<T: Element + DeserializeOwned>(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_arrow_ipc_lazy::<T>)
}
fn read_arrow_ipc_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    read_lazy_range(path, start, end, read_arrow_ipc_lazy::<T>)
}
pub fn arrow_ipc_sharded_source<T: Element + DeserializeOwned>(
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
        "arrow-ipc",
        count_arrow_ipc_records::<T>,
        read_arrow_ipc_range::<T>,
    )
}
fn count_arrow_batches(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_arrow_ipc_batches_lazy)
}
fn read_arrow_batches_range(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<ArrowBatch>, Error> {
    read_lazy_range(path, start, end, read_arrow_ipc_batches_lazy)
}
pub fn arrow_ipc_batches_sharded_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    batches_per_shard: usize,
) -> Result<PCollection<ArrowBatch>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        batches_per_shard,
        "arrow-ipc-batches",
        count_arrow_batches,
        read_arrow_batches_range,
    )
}
