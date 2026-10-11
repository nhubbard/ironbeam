//! Bounded Parquet file sources and deferred sinks for Ironbeam 5.
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

/// Stage coder-framed rows per bundle for a native Parquet result after `DONE`.
pub fn parquet_fragment_sink<T: Element + Serialize + DeserializeOwned>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    if !T::supports_coder() {
        return Err(Error::unsupported(key, "parquet-fragment", "element-coder"));
    }
    sequence_fragment_sink(
        input,
        key,
        staging,
        job,
        graph,
        &fragment_format::<T>(),
        encode_fragment_row::<T>,
    )
}

/// Validate coder-framed rows and atomically publish a native Parquet file.
pub fn publish_parquet_fragments<T: Element + Serialize + DeserializeOwned>(
    store: &ParquetFragmentStore<T>,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    use arrow::datatypes::{FieldRef, Schema};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default())
        .map_err(|error| io("parquet-fragment", error))?;
    let schema = std::sync::Arc::new(Schema::new(fields.clone()));
    store
        .local
        .publish_merged(expected_rows, target, |parts, _, output| {
            let mut writer = ArrowWriter::try_new(output, schema, None)
                .map_err(|error| io("parquet-fragment", error))?;
            let mut batch_rows = Vec::<T>::with_capacity(1_024);
            let mut rows = 0_u64;
            for part in parts {
                let mut payload = part.open_payload()?;
                let mut remaining = part.payload_len;
                let mut part_rows = 0_u64;
                while remaining > 0 {
                    if remaining < 8 {
                        return Err(Error::incompatible("truncated Parquet fragment frame"));
                    }
                    let mut prefix = [0_u8; 8];
                    payload
                        .read_exact(&mut prefix)
                        .map_err(|error| io("parquet-fragment", error))?;
                    remaining -= 8;
                    let length = u64::from_le_bytes(prefix);
                    if length > remaining {
                        return Err(Error::incompatible("Parquet fragment row exceeds payload"));
                    }
                    let length = usize::try_from(length)
                        .map_err(|_| Error::resource("parquet-fragment", "row exceeds usize"))?;
                    let mut bytes = Vec::new();
                    bytes
                        .try_reserve_exact(length)
                        .map_err(|error| Error::resource("parquet-fragment", error.to_string()))?;
                    bytes.resize(length, 0);
                    payload
                        .read_exact(&mut bytes)
                        .map_err(|error| io("parquet-fragment", error))?;
                    remaining -= length as u64;
                    batch_rows.push(T::decode(&bytes)?);
                    part_rows = part_rows.checked_add(1).ok_or_else(|| {
                        Error::incompatible("Parquet fragment row count overflow")
                    })?;
                    if batch_rows.len() == 1_024 {
                        let batch = serde_arrow::to_record_batch(&fields, &batch_rows)
                            .map_err(|error| io("parquet-fragment", error))?;
                        writer
                            .write(&batch)
                            .map_err(|error| io("parquet-fragment", error))?;
                        batch_rows.clear();
                    }
                }
                if part_rows != part.rows {
                    return Err(Error::incompatible(
                        "Parquet fragment decoded row count disagrees",
                    ));
                }
                rows = rows
                    .checked_add(part_rows)
                    .ok_or_else(|| Error::incompatible("Parquet result row count overflow"))?;
            }
            if !batch_rows.is_empty() {
                let batch = serde_arrow::to_record_batch(&fields, &batch_rows)
                    .map_err(|error| io("parquet-fragment", error))?;
                writer
                    .write(&batch)
                    .map_err(|error| io("parquet-fragment", error))?;
            }
            writer
                .close()
                .map_err(|error| io("parquet-fragment", error))?;
            Ok(rows)
        })
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

#[cfg(test)]
mod fragment_tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        id: i32,
        label: String,
    }
    impl Element for Row {
        const TYPE_ID: &'static str = "tests/parquet-fragment-row";
        const CODER_ID: &'static str = "tests/parquet-fragment-json";
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
    struct Uncoded {
        value: i32,
    }
    impl Element for Uncoded {
        const TYPE_ID: &'static str = "tests/parquet-uncoded";
        const CODER_ID: &'static str = "tests/parquet-uncoded";
    }

    #[test]
    fn publishes_multiple_coded_bundles_as_one_native_parquet_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = parquet_fragment_store::<Row>(dir.path().join("stage"), "job", "graph", "sink")
            .unwrap();
        let output = dir.path().join("result.parquet");
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
        let published = publish_parquet_fragments::<Row>(&store, 2, &output).unwrap();
        assert_eq!(published.rows, 2);
        assert_eq!(published.fragments, 2);
        assert_eq!(
            publish_parquet_fragments::<Row>(&store, 2, &output).unwrap(),
            published
        );
        let mut rows = read_parquet::<Row>(&fs::read(&output).unwrap()).unwrap();
        rows.sort_by_key(|row| row.id);
        assert_eq!(
            rows,
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
        assert!(publish_parquet_fragments::<Row>(&store, 3, &output).is_err());
    }

    #[test]
    fn rejects_corrupt_frames_and_wrong_counts_without_publication() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("result.parquet");
        let broken =
            parquet_fragment_store::<Row>(dir.path().join("broken"), "job", "graph", "sink")
                .unwrap();
        broken.local.stage("a", 1, &100_u64.to_le_bytes()).unwrap();
        broken.local.accept("a").unwrap();
        assert!(publish_parquet_fragments::<Row>(&broken, 1, &output).is_err());
        assert!(!output.exists());
        let wrong = parquet_fragment_store::<Row>(dir.path().join("wrong"), "job", "graph", "sink")
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
        wrong.local.stage("a", 2, &frame).unwrap();
        wrong.local.accept("a").unwrap();
        assert!(publish_parquet_fragments::<Row>(&wrong, 2, &output).is_err());
        assert!(!output.exists());
        assert!(parquet_fragment_store::<Row>("relative", "job", "graph", "sink").is_err());
    }

    #[test]
    fn writes_multiple_record_batches_and_empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let store = parquet_fragment_store::<Row>(dir.path().join("stage"), "job", "graph", "sink")
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
        let output = dir.path().join("many.parquet");
        assert_eq!(
            publish_parquet_fragments::<Row>(&store, 1_030, &output)
                .unwrap()
                .rows,
            1_030
        );
        let decoded = read_parquet::<Row>(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(decoded.len(), 1_030);
        assert_eq!(
            decoded.iter().map(|row| row.id).sum::<i32>(),
            (0..1_030).sum::<i32>()
        );
        let empty = parquet_fragment_store::<Row>(dir.path().join("empty"), "job", "graph", "sink")
            .unwrap();
        let empty_output = dir.path().join("empty.parquet");
        assert_eq!(
            publish_parquet_fragments::<Row>(&empty, 0, &empty_output)
                .unwrap()
                .rows,
            0
        );
        assert_eq!(
            read_parquet::<Row>(&fs::read(empty_output).unwrap()).unwrap(),
            Vec::<Row>::new()
        );
    }

    #[test]
    fn rejects_uncoded_types_and_incomplete_frames_before_publishing() {
        let dir = tempfile::tempdir().unwrap();
        let pipeline = Pipeline::default();
        let input = pipeline
            .from_vec(
                "input",
                vec![Row {
                    id: 1,
                    label: "x".into(),
                }],
            )
            .unwrap();
        parquet_fragment_sink(&input, "sink", dir.path().join("stage"), "job", "graph").unwrap();
        let uncoded = pipeline
            .from_vec("uncoded", vec![Uncoded { value: 1 }])
            .unwrap();
        assert!(
            parquet_fragment_sink(&uncoded, "bad", dir.path().join("stage"), "job", "graph")
                .is_err()
        );
        assert!(
            parquet_fragment_store::<Uncoded>(dir.path().join("stage"), "job", "graph", "bad")
                .is_err()
        );
        let store =
            parquet_fragment_store::<Row>(dir.path().join("broken"), "job", "graph", "sink")
                .unwrap();
        assert!(store.accepted_fragments().unwrap().is_empty());
        store.local.stage("short", 1, b"abc").unwrap();
        store.local.accept("short").unwrap();
        assert_eq!(store.accepted_fragments().unwrap().len(), 1);
        let output = dir.path().join("out.parquet");
        assert!(publish_parquet_fragments::<Row>(&store, 1, &output).is_err());
        assert!(!output.exists());
    }
}
