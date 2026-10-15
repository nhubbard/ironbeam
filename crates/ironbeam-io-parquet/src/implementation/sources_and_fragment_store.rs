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

pub(crate) fn read_parquet<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
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

fn fragment_format<T: Element>() -> String {
    let type_id = T::type_identity();
    let coder = T::coder_identity();
    format!(
        "parquet-coded-v1:{}:{type_id}:{}:{coder}:{}:{}",
        type_id.len(),
        coder.len(),
        T::CODER_VERSION.major,
        T::CODER_VERSION.minor
    )
}

/// Reopen a graph and coder-bound staging scope for finalized Parquet output.
pub struct ParquetFragmentStore<T: Element> {
    local: LocalFinalResult,
    marker: std::marker::PhantomData<T>,
}

impl<T: Element> ParquetFragmentStore<T> {
    /// Require the publisher to match the runner's completed bundle attempts.
    ///
    /// # Errors
    ///
    /// Rejects empty or duplicate attempt IDs.
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

pub fn parquet_fragment_store<T: Element>(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<ParquetFragmentStore<T>, Error> {
    if !T::supports_coder() {
        return Err(Error::unsupported(key, "parquet-fragment", "element-coder"));
    }
    Ok(ParquetFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, &fragment_format::<T>())?,
        marker: std::marker::PhantomData,
    })
}

fn encode_fragment_row<T: Element>(value: &T, output: &mut dyn Write) -> Result<(), Error> {
    let bytes = value.encode()?;
    let len = u64::try_from(bytes.len())
        .map_err(|_| Error::resource("parquet-fragment", "encoded row length overflow"))?;
    output
        .write_all(&len.to_le_bytes())
        .and_then(|()| output.write_all(&bytes))
        .map_err(|error| io("parquet-fragment", error))
}
