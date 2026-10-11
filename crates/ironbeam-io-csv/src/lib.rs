//! CSV sources and deferred final-result sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, TypedSink,
    TypedSourceIterator,
};
use ironbeam_io::final_result::{AcceptedFragment, LocalFinalResult, PublishedResult};
#[cfg(feature = "parallel-io")]
use ironbeam_io::parallel_file_writer;
use ironbeam_io::{
    file_glob_source, file_sharded_source, io, lazy_source, open_encoded_output, open_lazy_input,
    sink, source,
};
use serde::{Serialize, de::DeserializeOwned};
use std::io::{Seek, SeekFrom};
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

/// A local, graph-bound staging scope for finalized CSV bundles.
#[derive(Clone)]
pub struct CsvFragmentStore {
    local: LocalFinalResult,
}

impl CsvFragmentStore {
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

pub fn csv_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<CsvFragmentStore, Error> {
    let staging = staging.as_ref();
    if !staging.is_absolute() || graph.is_empty() {
        return Err(Error::graph(
            key,
            "CSV fragments need an absolute staging path and graph",
        ));
    }
    let scope = serde_json::to_string(&(graph, "csv", "fragments-v1"))
        .map_err(|error| Error::incompatible(error.to_string()))?;
    Ok(CsvFragmentStore {
        local: LocalFinalResult::new(staging, job, &scope, key)?,
    })
}

struct CsvFragmentSink<T> {
    store: CsvFragmentStore,
    bundle_id: Option<String>,
    writer: Option<csv::Writer<tempfile::NamedTempFile>>,
    rows: u64,
    staged: bool,
    marker: std::marker::PhantomData<T>,
}

impl<T: Element + Serialize> TypedSink<T> for CsvFragmentSink<T> {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        if instruction_id.is_empty() || self.bundle_id.is_some() {
            return Err(Error::incompatible(
                "CSV fragment needs one bundle attempt identity",
            ));
        }
        self.bundle_id = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: T) -> Result<(), Error> {
        if self.bundle_id.is_none() {
            return Err(Error::incompatible(
                "CSV fragment has no bundle attempt identity",
            ));
        }
        if self.writer.is_none() {
            require_named_header(std::slice::from_ref(&value))?;
            self.writer = Some(csv::Writer::from_writer(
                tempfile::NamedTempFile::new().map_err(|error| io("csv-fragment", error))?,
            ));
        }
        self.writer
            .as_mut()
            .expect("writer initialized")
            .serialize(value)
            .map_err(|error| io("csv-fragment", error))?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| Error::incompatible("CSV fragment row count overflow"))?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let bundle_id = self
            .bundle_id
            .as_deref()
            .ok_or_else(|| Error::incompatible("CSV fragment has no bundle attempt identity"))?;
        let mut spool = match self.writer.take() {
            Some(writer) => writer
                .into_inner()
                .map_err(|error| io("csv-fragment", error))?,
            None => tempfile::NamedTempFile::new().map_err(|error| io("csv-fragment", error))?,
        };
        spool
            .as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|error| io("csv-fragment", error))?;
        self.store
            .local
            .stage_reader(bundle_id, self.rows, spool.as_file_mut())?;
        self.staged = true;
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        if self.staged {
            if let Some(bundle_id) = self.bundle_id.as_deref() {
                self.store.local.abandon(bundle_id)?;
            }
            self.staged = false;
        }
        self.writer = None;
        Ok(())
    }

    fn requires_bundle_finalization(&self) -> bool {
        true
    }

    fn finalize_bundle(&mut self) -> Result<(), Error> {
        let bundle_id = self
            .bundle_id
            .as_deref()
            .ok_or_else(|| Error::incompatible("CSV fragment has no bundle attempt identity"))?;
        if !self.staged {
            return Err(Error::incompatible("CSV fragment has not committed"));
        }
        self.store.local.accept(bundle_id)
    }

    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.store.local.accept(instruction_id)
    }
}

/// Stage one header-bearing CSV fragment per bundle for coordinator publication.
pub fn csv_fragment_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    let staging = staging.as_ref();
    let store = csv_fragment_store(staging, job, graph, key)?;
    let config = serde_json::to_string(&("csv-fragments-v1", staging, job, graph, key))
        .map_err(|error| Error::incompatible(error.to_string()))?;
    input.sink_named_with_config_and_finalization(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        config,
        move || {
            Ok(Box::new(CsvFragmentSink::<T> {
                store: store.clone(),
                bundle_id: None,
                writer: None,
                rows: 0,
                staged: false,
                marker: std::marker::PhantomData,
            }))
        },
    )
}

/// Merge accepted bundles into one CSV file after the runner reports success.
pub fn publish_csv_fragments<T: DeserializeOwned>(
    store: &CsvFragmentStore,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    let target = target.as_ref();
    if target
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
    {
        return Err(Error::graph(
            "csv-fragment",
            "compressed final result needs a format merger",
        ));
    }
    store
        .local
        .publish_merged(expected_rows, target, |parts, _, output| {
            let mut writer = csv::WriterBuilder::new()
                .has_headers(false)
                .from_writer(output);
            let mut header: Option<csv::StringRecord> = None;
            let mut rows = 0_u64;
            for part in parts {
                if part.rows == 0 {
                    if part.payload_len != 0 {
                        return Err(Error::incompatible("empty CSV fragment has a payload"));
                    }
                    continue;
                }
                let mut reader = csv::Reader::from_reader(part.open_payload()?);
                let part_header = reader
                    .headers()
                    .map_err(|error| io("csv-fragment", error))?
                    .clone();
                if part_header.is_empty() {
                    return Err(Error::incompatible("CSV fragment is missing a header"));
                }
                match &header {
                    Some(previous) if previous != &part_header => {
                        return Err(Error::incompatible("CSV fragment headers disagree"));
                    }
                    None => {
                        writer
                            .write_record(&part_header)
                            .map_err(|error| io("csv-fragment", error))?;
                        header = Some(part_header);
                    }
                    _ => {}
                }
                let mut part_rows = 0_u64;
                for record in reader.records() {
                    let record = record.map_err(|error| io("csv-fragment", error))?;
                    record
                        .deserialize::<T>(header.as_ref())
                        .map_err(|error| io("csv-fragment", error))?;
                    writer
                        .write_record(&record)
                        .map_err(|error| io("csv-fragment", error))?;
                    part_rows = part_rows
                        .checked_add(1)
                        .ok_or_else(|| Error::incompatible("CSV row count overflow"))?;
                }
                if part_rows != part.rows {
                    return Err(Error::incompatible(
                        "CSV fragment decoded row count disagrees",
                    ));
                }
                rows = rows
                    .checked_add(part_rows)
                    .ok_or_else(|| Error::incompatible("CSV row count overflow"))?;
            }
            writer.flush().map_err(|error| io("csv-fragment", error))?;
            Ok(rows)
        })
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

#[cfg(test)]
mod fragment_tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct Row {
        id: u32,
        name: String,
    }

    impl Element for Row {
        const TYPE_ID: &'static str = "csv-fragment-test-row";
        const CODER_ID: &'static str = "csv-fragment-test-json";
    }

    fn session(store: &CsvFragmentStore) -> CsvFragmentSink<Row> {
        CsvFragmentSink {
            store: store.clone(),
            bundle_id: None,
            writer: None,
            rows: 0,
            staged: false,
            marker: std::marker::PhantomData,
        }
    }

    #[test]
    fn merges_bundles_with_one_header_and_multiline_values() {
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        let store = csv_fragment_store(&stage, "job", "graph", "sink").unwrap();
        let output = dir.path().join("result.csv");
        let mut first = session(&store);
        first.begin_bundle("first").unwrap();
        first
            .write(Row {
                id: 1,
                name: "Ada,\nLovelace".into(),
            })
            .unwrap();
        first.commit().unwrap();
        assert!(publish_csv_fragments::<Row>(&store, 1, &output).is_err());
        first.finalize_bundle().unwrap();
        let mut second = session(&store);
        second.begin_bundle("second").unwrap();
        second
            .write(Row {
                id: 2,
                name: "Lin".into(),
            })
            .unwrap();
        second.commit().unwrap();
        second.finalize_bundle().unwrap();
        let result = publish_csv_fragments::<Row>(&store, 2, &output).unwrap();
        assert_eq!(result.rows, 2);
        assert_eq!(result.fragments, 2);
        assert_eq!(
            publish_csv_fragments::<Row>(&store, 2, &output).unwrap(),
            result
        );
        assert_eq!(
            std::fs::read_to_string(&output)
                .unwrap()
                .matches("id,name")
                .count(),
            1
        );
        let mut rows = read_csv::<Row>(&std::fs::read(&output).unwrap()).unwrap();
        rows.sort_by_key(|row| row.id);
        assert_eq!(
            rows,
            vec![
                Row {
                    id: 1,
                    name: "Ada,\nLovelace".into()
                },
                Row {
                    id: 2,
                    name: "Lin".into()
                },
            ]
        );
        assert!(publish_csv_fragments::<Row>(&store, 3, &output).is_err());
    }

    #[test]
    fn rejects_header_mismatch_and_bad_rows_without_publishing() {
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        let store = csv_fragment_store(&stage, "job", "graph", "sink").unwrap();
        let output = dir.path().join("result.csv");
        store.local.stage("a", 1, b"id,name\n1,Ada\n").unwrap();
        store.local.accept("a").unwrap();
        store.local.stage("b", 1, b"name,id\nLin,2\n").unwrap();
        store.local.accept("b").unwrap();
        assert!(publish_csv_fragments::<Row>(&store, 2, &output).is_err());
        assert!(!output.exists());
        let valid = csv_fragment_store(dir.path().join("other"), "job", "graph", "sink").unwrap();
        valid
            .local
            .stage("a", 1, b"id,name\nnot-a-number,Ada\n")
            .unwrap();
        valid.local.accept("a").unwrap();
        assert!(publish_csv_fragments::<Row>(&valid, 1, &output).is_err());
        assert!(!output.exists());
        assert!(publish_csv_fragments::<Row>(&valid, 1, dir.path().join("result.csv.gz")).is_err());
    }

    #[test]
    fn empty_bundle_and_recovery_are_durable() {
        let dir = tempfile::tempdir().unwrap();
        let store = csv_fragment_store(dir.path().join("stage"), "job", "graph", "sink").unwrap();
        let output = dir.path().join("empty.csv");
        let mut bundle = session(&store);
        assert!(
            bundle
                .write(Row {
                    id: 0,
                    name: String::new()
                })
                .is_err()
        );
        bundle.begin_bundle("empty").unwrap();
        bundle.commit().unwrap();
        let mut restarted = session(&store);
        restarted.recover_bundle_finalization("empty").unwrap();
        bundle.finalize_bundle().unwrap();
        assert_eq!(
            publish_csv_fragments::<Row>(&store, 0, &output)
                .unwrap()
                .rows,
            0
        );
        assert_eq!(std::fs::read(&output).unwrap(), b"");
        assert!(csv_fragment_store("relative", "job", "graph", "sink").is_err());
    }

    #[test]
    fn sink_registration_and_bundle_failures_preserve_staging() {
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        let store = csv_fragment_store(&stage, "job", "graph", "sink").unwrap();
        let pipeline = Pipeline::default();
        let input = pipeline
            .from_vec(
                "input",
                vec![Row {
                    id: 1,
                    name: "Ada".into(),
                }],
            )
            .unwrap();
        csv_fragment_sink(&input, "sink", &stage, "job", "graph").unwrap();
        assert!(csv_fragment_sink(&input, "bad", "relative", "job", "graph").is_err());
        assert!(csv_fragment_store(&stage, "job", "", "sink").is_err());
        let mut bundle = session(&store);
        assert!(bundle.begin_bundle("").is_err());
        assert!(bundle.commit().is_err());
        assert!(bundle.finalize_bundle().is_err());
        bundle.begin_bundle("attempt").unwrap();
        assert!(bundle.begin_bundle("again").is_err());
        assert!(bundle.finalize_bundle().is_err());
        bundle
            .write(Row {
                id: 1,
                name: "Ada".into(),
            })
            .unwrap();
        bundle.commit().unwrap();
        assert!(store.accepted_fragments().unwrap().is_empty());
        bundle.abort().unwrap();
        assert!(store.accepted_fragments().unwrap().is_empty());
        assert!(bundle.finalize_bundle().is_err());
        let mut unnamed = CsvFragmentSink::<i32> {
            store,
            bundle_id: None,
            writer: None,
            rows: 0,
            staged: false,
            marker: std::marker::PhantomData,
        };
        unnamed.begin_bundle("unnamed").unwrap();
        assert!(unnamed.write(1).is_err());
    }

    #[test]
    fn rejects_empty_payload_with_rows_and_wrong_fragment_count() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("result.csv");
        let store =
            csv_fragment_store(dir.path().join("empty-with-data"), "job", "graph", "sink").unwrap();
        store.local.stage("a", 0, b"id,name\n").unwrap();
        store.local.accept("a").unwrap();
        assert!(publish_csv_fragments::<Row>(&store, 0, &output).is_err());
        assert!(!output.exists());
        let store =
            csv_fragment_store(dir.path().join("wrong-count"), "job", "graph", "sink").unwrap();
        store.local.stage("a", 2, b"id,name\n1,Ada\n").unwrap();
        store.local.accept("a").unwrap();
        assert!(publish_csv_fragments::<Row>(&store, 2, &output).is_err());
        assert!(!output.exists());
    }
}
