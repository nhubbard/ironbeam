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
    pub fn require_bundle_attempts<I, S>(mut self, bundle_ids: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.local = self.local.require_bundle_attempts(bundle_ids)?;
        Ok(self)
    }

    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}
