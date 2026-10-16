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

/// Read a bounded CSV file into typed rows.
pub fn csv_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "csv", read_csv::<T>)
}

/// Read every CSV file matching a local glob pattern.
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

/// Decode a bounded CSV file one row at a time.
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

    /// Return accepted CSV fragments after runner finalization.
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

/// Open a local CSV fragment store for one job and graph.
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
