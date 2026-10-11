//! Bounded Arrow IPC row and record-batch adapters for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SourceProgress, TypedSourceIterator,
};
use ironbeam_io::final_result::{AcceptedFragment, LocalFinalResult, PublishedResult};
#[cfg(feature = "parallel-io")]
use ironbeam_io::parallel_container_writer;
use ironbeam_io::sequence_final::{sequence_fragment_sink, sequence_fragment_store};
use ironbeam_io::{
    count_lazy_records, file_glob_source, file_hash, file_sharded_source, io, lazy_source,
    read_lazy_range, sink, source,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs,
    io::{Read, Write},
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

fn row_fragment_format<T: Element>() -> String {
    let type_id = T::type_identity();
    let coder = T::coder_identity();
    format!(
        "arrow-ipc-coded-rows-v1:{}:{type_id}:{}:{coder}:{}:{}",
        type_id.len(),
        coder.len(),
        T::CODER_VERSION.major,
        T::CODER_VERSION.minor
    )
}

/// Reopen a graph and coder-bound staging scope for finalized Arrow IPC rows.
pub struct ArrowIpcRowFragmentStore<T: Element> {
    local: LocalFinalResult,
    marker: std::marker::PhantomData<T>,
}

impl<T: Element> ArrowIpcRowFragmentStore<T> {
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

pub fn arrow_ipc_row_fragment_store<T: Element>(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<ArrowIpcRowFragmentStore<T>, Error> {
    if !T::supports_coder() {
        return Err(Error::unsupported(
            key,
            "arrow-ipc-fragment",
            "element-coder",
        ));
    }
    Ok(ArrowIpcRowFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, &row_fragment_format::<T>())?,
        marker: std::marker::PhantomData,
    })
}

fn encode_fragment_row<T: Element>(value: &T, output: &mut dyn Write) -> Result<(), Error> {
    let bytes = value.encode()?;
    let length = u64::try_from(bytes.len())
        .map_err(|_| Error::resource("arrow-ipc-fragment", "encoded row length overflow"))?;
    output
        .write_all(&length.to_le_bytes())
        .and_then(|()| output.write_all(&bytes))
        .map_err(|error| io("arrow-ipc-fragment", error))
}

/// Stage coded rows per bundle for native Arrow IPC publication after `DONE`.
pub fn arrow_ipc_row_fragment_sink<T: Element + Serialize + DeserializeOwned>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    if !T::supports_coder() {
        return Err(Error::unsupported(
            key,
            "arrow-ipc-fragment",
            "element-coder",
        ));
    }
    sequence_fragment_sink(
        input,
        key,
        staging,
        job,
        graph,
        &row_fragment_format::<T>(),
        encode_fragment_row::<T>,
    )
}

/// Validate coded rows and atomically publish one native Arrow IPC file.
pub fn publish_arrow_ipc_row_fragments<T: Element + Serialize + DeserializeOwned>(
    store: &ArrowIpcRowFragmentStore<T>,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    use arrow::datatypes::{FieldRef, Schema};
    use arrow::ipc::writer::FileWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .map_err(|error| io("arrow-ipc-fragment", error))?;
    let schema = std::sync::Arc::new(Schema::new(fields.clone()));
    store
        .local
        .publish_merged(expected_rows, target, |parts, _, output| {
            let mut writer = FileWriter::try_new(output, &schema)
                .map_err(|error| io("arrow-ipc-fragment", error))?;
            let mut batch_rows = Vec::<T>::with_capacity(1_024);
            let mut rows = 0_u64;
            for part in parts {
                let mut payload = part.open_payload()?;
                let mut remaining = part.payload_len;
                let mut part_rows = 0_u64;
                while remaining > 0 {
                    if remaining < 8 {
                        return Err(Error::incompatible(
                            "truncated Arrow IPC row fragment frame",
                        ));
                    }
                    let mut prefix = [0_u8; 8];
                    payload
                        .read_exact(&mut prefix)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    remaining -= 8;
                    let length = u64::from_le_bytes(prefix);
                    if length > remaining {
                        return Err(Error::incompatible(
                            "Arrow IPC row fragment exceeds payload",
                        ));
                    }
                    let length = usize::try_from(length)
                        .map_err(|_| Error::resource("arrow-ipc-fragment", "row exceeds usize"))?;
                    let mut bytes = Vec::new();
                    bytes.try_reserve_exact(length).map_err(|error| {
                        Error::resource("arrow-ipc-fragment", error.to_string())
                    })?;
                    bytes.resize(length, 0);
                    payload
                        .read_exact(&mut bytes)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    remaining -= length as u64;
                    batch_rows.push(T::decode(&bytes)?);
                    part_rows = part_rows.checked_add(1).ok_or_else(|| {
                        Error::incompatible("Arrow IPC fragment row count overflow")
                    })?;
                    if batch_rows.len() == 1_024 {
                        let batch = serde_arrow::to_record_batch(&fields, &batch_rows)
                            .map_err(|error| io("arrow-ipc-fragment", error))?;
                        writer
                            .write(&batch)
                            .map_err(|error| io("arrow-ipc-fragment", error))?;
                        batch_rows.clear();
                    }
                }
                if part_rows != part.rows {
                    return Err(Error::incompatible(
                        "Arrow IPC fragment decoded row count disagrees",
                    ));
                }
                rows = rows
                    .checked_add(part_rows)
                    .ok_or_else(|| Error::incompatible("Arrow IPC result row count overflow"))?;
            }
            if !batch_rows.is_empty() {
                let batch = serde_arrow::to_record_batch(&fields, &batch_rows)
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
                writer
                    .write(&batch)
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
            }
            writer
                .finish()
                .map_err(|error| io("arrow-ipc-fragment", error))?;
            Ok(rows)
        })
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

fn batch_fragment_format() -> String {
    let type_id = ArrowBatch::type_identity();
    let coder = ArrowBatch::coder_identity();
    format!(
        "arrow-ipc-coded-batches-v1:{}:{type_id}:{}:{coder}:{}:{}",
        type_id.len(),
        coder.len(),
        ArrowBatch::CODER_VERSION.major,
        ArrowBatch::CODER_VERSION.minor
    )
}

/// Reopen staging for native Arrow IPC record-batch final output.
pub struct ArrowIpcBatchFragmentStore {
    local: LocalFinalResult,
}

impl ArrowIpcBatchFragmentStore {
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

pub fn arrow_ipc_batch_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<ArrowIpcBatchFragmentStore, Error> {
    Ok(ArrowIpcBatchFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, &batch_fragment_format())?,
    })
}

/// Stage complete coded record batches per bundle for publication after `DONE`.
pub fn arrow_ipc_batch_fragment_sink(
    input: &PCollection<ArrowBatch>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<ArrowBatch>, Error> {
    sequence_fragment_sink(
        input,
        key,
        staging,
        job,
        graph,
        &batch_fragment_format(),
        encode_fragment_row::<ArrowBatch>,
    )
}

/// Validate coded batches and atomically publish one native Arrow IPC file.
/// `expected_batches` is the committed count of `ArrowBatch` elements.
pub fn publish_arrow_ipc_batch_fragments(
    store: &ArrowIpcBatchFragmentStore,
    expected_batches: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    use arrow::datatypes::Schema;
    use arrow::ipc::writer::FileWriter;

    store
        .local
        .publish_merged(expected_batches, target, |parts, _, output| {
            let mut writer = None;
            let mut schema: Option<arrow::datatypes::SchemaRef> = None;
            let mut batches = 0_u64;
            for part in parts {
                let mut payload = part.open_payload()?;
                let mut remaining = part.payload_len;
                let mut part_batches = 0_u64;
                while remaining > 0 {
                    if remaining < 8 {
                        return Err(Error::incompatible(
                            "truncated Arrow IPC batch fragment frame",
                        ));
                    }
                    let mut prefix = [0_u8; 8];
                    payload
                        .read_exact(&mut prefix)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    remaining -= 8;
                    let length = u64::from_le_bytes(prefix);
                    if length > remaining {
                        return Err(Error::incompatible(
                            "Arrow IPC batch fragment exceeds payload",
                        ));
                    }
                    let length = usize::try_from(length).map_err(|_| {
                        Error::resource("arrow-ipc-fragment", "batch exceeds usize")
                    })?;
                    let mut bytes = Vec::new();
                    bytes.try_reserve_exact(length).map_err(|error| {
                        Error::resource("arrow-ipc-fragment", error.to_string())
                    })?;
                    bytes.resize(length, 0);
                    payload
                        .read_exact(&mut bytes)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    remaining -= length as u64;
                    let batch = ArrowBatch::decode(&bytes)?.0;
                    if let Some(expected) = &schema {
                        if batch.schema().as_ref() != expected.as_ref() {
                            return Err(Error::incompatible(
                                "Arrow IPC fragment batch schema differs",
                            ));
                        }
                    } else {
                        schema = Some(batch.schema());
                        writer = Some(
                            FileWriter::try_new(&mut *output, &batch.schema())
                                .map_err(|error| io("arrow-ipc-fragment", error))?,
                        );
                    }
                    writer
                        .as_mut()
                        .ok_or_else(|| Error::incompatible("Arrow IPC fragment writer missing"))?
                        .write(&batch)
                        .map_err(|error| io("arrow-ipc-fragment", error))?;
                    part_batches = part_batches.checked_add(1).ok_or_else(|| {
                        Error::incompatible("Arrow IPC fragment batch count overflow")
                    })?;
                }
                if part_batches != part.rows {
                    return Err(Error::incompatible(
                        "Arrow IPC fragment decoded batch count disagrees",
                    ));
                }
                batches = batches
                    .checked_add(part_batches)
                    .ok_or_else(|| Error::incompatible("Arrow IPC result batch count overflow"))?;
            }
            if let Some(mut writer) = writer {
                writer
                    .finish()
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
            } else {
                let mut writer = FileWriter::try_new(output, &std::sync::Arc::new(Schema::empty()))
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
                writer
                    .finish()
                    .map_err(|error| io("arrow-ipc-fragment", error))?;
            }
            Ok(batches)
        })
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

#[cfg(test)]
mod row_fragment_tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        id: i32,
        label: String,
    }
    impl Element for Row {
        const TYPE_ID: &'static str = "tests/arrow-fragment-row";
        const CODER_ID: &'static str = "tests/arrow-fragment-json";
        fn supports_coder() -> bool {
            true
        }
        fn encode(&self) -> Result<Vec<u8>, Error> {
            serde_json::to_vec(self).map_err(|error| Error::incompatible(error.to_string()))
        }
        fn decode(bytes: &[u8]) -> Result<Self, Error> {
            serde_json::from_slice(bytes).map_err(|error| Error::incompatible(error.to_string()))
        }
    }

    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    struct Uncoded(i32);
    impl Element for Uncoded {
        const TYPE_ID: &'static str = "tests/arrow-fragment-uncoded";
        const CODER_ID: &'static str = "tests/arrow-fragment-uncoded";
    }

    #[test]
    fn merges_bundles_into_native_arrow_file_and_reopens_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            arrow_ipc_row_fragment_store::<Row>(dir.path().join("stage"), "job", "graph", "sink")
                .unwrap();
        let target = dir.path().join("rows.arrow");
        for (bundle, row) in [
            (
                "a",
                Row {
                    id: 1,
                    label: "Ada".into(),
                },
            ),
            (
                "b",
                Row {
                    id: 2,
                    label: "Lin".into(),
                },
            ),
        ] {
            let mut frame = Vec::new();
            encode_fragment_row(&row, &mut frame).unwrap();
            store.local.stage(bundle, 1, &frame).unwrap();
            store.local.accept(bundle).unwrap();
        }
        assert_eq!(store.accepted_fragments().unwrap().len(), 2);
        let result = publish_arrow_ipc_row_fragments(&store, 2, &target).unwrap();
        assert_eq!(result.rows, 2);
        assert_eq!(result.fragments, 2);
        assert_eq!(
            publish_arrow_ipc_row_fragments(&store, 2, &target).unwrap(),
            result
        );
        let mut actual = read_arrow_ipc::<Row>(&fs::read(&target).unwrap()).unwrap();
        actual.sort_by_key(|row| row.id);
        assert_eq!(
            actual,
            [
                Row {
                    id: 1,
                    label: "Ada".into()
                },
                Row {
                    id: 2,
                    label: "Lin".into()
                },
            ]
        );
        assert!(publish_arrow_ipc_row_fragments(&store, 3, &target).is_err());
    }

    #[test]
    fn rejects_malformed_rows_and_wrong_counts_without_publication() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("bad.arrow");
        let store =
            arrow_ipc_row_fragment_store::<Row>(dir.path().join("bad"), "job", "graph", "sink")
                .unwrap();
        store.local.stage("short", 1, b"abc").unwrap();
        store.local.accept("short").unwrap();
        assert!(publish_arrow_ipc_row_fragments(&store, 1, &target).is_err());
        assert!(!target.exists());
        let store =
            arrow_ipc_row_fragment_store::<Row>(dir.path().join("wrong"), "job", "graph", "sink")
                .unwrap();
        let mut frame = Vec::new();
        encode_fragment_row(
            &Row {
                id: 1,
                label: "Ada".into(),
            },
            &mut frame,
        )
        .unwrap();
        store.local.stage("wrong", 2, &frame).unwrap();
        store.local.accept("wrong").unwrap();
        assert!(publish_arrow_ipc_row_fragments(&store, 2, &target).is_err());
        assert!(!target.exists());
        let store =
            arrow_ipc_row_fragment_store::<Row>(dir.path().join("length"), "job", "graph", "sink")
                .unwrap();
        store
            .local
            .stage("length", 1, &100_u64.to_le_bytes())
            .unwrap();
        store.local.accept("length").unwrap();
        assert!(publish_arrow_ipc_row_fragments(&store, 1, &target).is_err());
        assert!(!target.exists());
    }

    #[test]
    fn writes_multiple_batches_and_empty_file_and_rejects_uncoded_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            arrow_ipc_row_fragment_store::<Row>(dir.path().join("many"), "job", "graph", "sink")
                .unwrap();
        let mut frames = Vec::new();
        for id in 0..1_030 {
            encode_fragment_row(
                &Row {
                    id,
                    label: format!("row-{id}"),
                },
                &mut frames,
            )
            .unwrap();
        }
        store.local.stage("many", 1_030, &frames).unwrap();
        store.local.accept("many").unwrap();
        let target = dir.path().join("many.arrow");
        assert_eq!(
            publish_arrow_ipc_row_fragments(&store, 1_030, &target)
                .unwrap()
                .rows,
            1_030
        );
        assert_eq!(
            read_arrow_ipc::<Row>(&fs::read(&target).unwrap())
                .unwrap()
                .len(),
            1_030
        );
        let empty =
            arrow_ipc_row_fragment_store::<Row>(dir.path().join("empty"), "job", "graph", "sink")
                .unwrap();
        let empty_target = dir.path().join("empty.arrow");
        assert_eq!(
            publish_arrow_ipc_row_fragments(&empty, 0, &empty_target)
                .unwrap()
                .rows,
            0
        );
        assert_eq!(
            read_arrow_ipc::<Row>(&fs::read(empty_target).unwrap()).unwrap(),
            Vec::<Row>::new()
        );
        let pipeline = Pipeline::default();
        let coded = pipeline
            .from_vec(
                "coded",
                vec![Row {
                    id: 1,
                    label: "ok".into(),
                }],
            )
            .unwrap();
        arrow_ipc_row_fragment_sink(
            &coded,
            "coded-sink",
            dir.path().join("stage"),
            "job",
            "graph",
        )
        .unwrap();
        assert!(arrow_ipc_row_fragment_store::<Row>("relative", "job", "graph", "sink").is_err());
        let input = pipeline.from_vec("uncoded", vec![Uncoded(1)]).unwrap();
        assert!(
            arrow_ipc_row_fragment_sink(&input, "sink", dir.path().join("stage"), "job", "graph",)
                .is_err()
        );
        assert!(
            arrow_ipc_row_fragment_store::<Uncoded>(
                dir.path().join("stage"),
                "job",
                "graph",
                "sink",
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod batch_fragment_tests {
    use super::*;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    fn batch(name: &str, values: &[i32]) -> ArrowBatch {
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        ArrowBatch(
            RecordBatch::try_new(
                schema,
                vec![std::sync::Arc::new(Int32Array::from(values.to_vec()))],
            )
            .unwrap(),
        )
    }

    #[test]
    fn publishes_multiple_bundle_batches_as_one_native_file() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            arrow_ipc_batch_fragment_store(dir.path().join("stage"), "job", "graph", "sink")
                .unwrap();
        let target = dir.path().join("batches.arrow");
        for (bundle, values) in [("a", vec![1, 2]), ("b", vec![3])] {
            let mut frame = Vec::new();
            encode_fragment_row(&batch("value", &values), &mut frame).unwrap();
            store.local.stage(bundle, 1, &frame).unwrap();
            store.local.accept(bundle).unwrap();
        }
        assert_eq!(store.accepted_fragments().unwrap().len(), 2);
        let published = publish_arrow_ipc_batch_fragments(&store, 2, &target).unwrap();
        assert_eq!(published.rows, 2);
        assert_eq!(published.fragments, 2);
        assert_eq!(
            publish_arrow_ipc_batch_fragments(&store, 2, &target).unwrap(),
            published
        );
        let decoded = read_arrow_ipc_batches(&fs::read(&target).unwrap()).unwrap();
        assert_eq!(decoded.len(), 2);
        let mut lengths = decoded
            .iter()
            .map(|batch| batch.0.num_rows())
            .collect::<Vec<_>>();
        lengths.sort_unstable();
        assert_eq!(lengths, [1, 2]);
        assert_eq!(decoded[0].0.schema(), decoded[1].0.schema());
        let mut values = decoded
            .iter()
            .flat_map(|batch| {
                batch
                    .0
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, [1, 2, 3]);
        assert!(publish_arrow_ipc_batch_fragments(&store, 3, &target).is_err());
    }

    #[test]
    fn rejects_bad_frames_counts_and_schemas_before_publication() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("bad.arrow");
        let corrupt =
            arrow_ipc_batch_fragment_store(dir.path().join("corrupt"), "job", "graph", "sink")
                .unwrap();
        corrupt
            .local
            .stage("bad", 1, &100_u64.to_le_bytes())
            .unwrap();
        corrupt.local.accept("bad").unwrap();
        assert!(publish_arrow_ipc_batch_fragments(&corrupt, 1, &target).is_err());
        assert!(!target.exists());
        let wrong =
            arrow_ipc_batch_fragment_store(dir.path().join("wrong"), "job", "graph", "sink")
                .unwrap();
        let mut frame = Vec::new();
        encode_fragment_row(&batch("value", &[1]), &mut frame).unwrap();
        wrong.local.stage("wrong", 2, &frame).unwrap();
        wrong.local.accept("wrong").unwrap();
        assert!(publish_arrow_ipc_batch_fragments(&wrong, 2, &target).is_err());
        assert!(!target.exists());
        let mismatch =
            arrow_ipc_batch_fragment_store(dir.path().join("mismatch"), "job", "graph", "sink")
                .unwrap();
        mismatch.local.stage("a", 1, &frame).unwrap();
        mismatch.local.accept("a").unwrap();
        let mut second = Vec::new();
        encode_fragment_row(&batch("other", &[2]), &mut second).unwrap();
        mismatch.local.stage("b", 1, &second).unwrap();
        mismatch.local.accept("b").unwrap();
        assert!(publish_arrow_ipc_batch_fragments(&mismatch, 2, &target).is_err());
        assert!(!target.exists());
    }

    #[test]
    fn empty_file_and_sink_registration() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            arrow_ipc_batch_fragment_store(dir.path().join("stage"), "job", "graph", "sink")
                .unwrap();
        let target = dir.path().join("empty.arrow");
        assert_eq!(
            publish_arrow_ipc_batch_fragments(&store, 0, &target)
                .unwrap()
                .rows,
            0
        );
        assert!(
            read_arrow_ipc_batches(&fs::read(target).unwrap())
                .unwrap()
                .is_empty()
        );
        let pipeline = Pipeline::default();
        let input = pipeline
            .from_vec("batches", vec![batch("value", &[1])])
            .unwrap();
        arrow_ipc_batch_fragment_sink(&input, "sink", dir.path().join("other"), "job", "graph")
            .unwrap();
        assert!(arrow_ipc_batch_fragment_store("relative", "job", "graph", "sink").is_err());
    }
}
