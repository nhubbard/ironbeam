use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, TypedSink,
    TypedSourceIterator,
};
use ironbeam_io::final_result::{AcceptedFragment, LocalFinalResult, PublishedResult};
use ironbeam_io::sequence_final::{
    publish_sequence_fragments, sequence_fragment_sink, sequence_fragment_store,
};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};

fn read_jsonl<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    bytes
        .split(|&byte| byte == b'\n')
        .enumerate()
        .filter_map(|(line, value)| match std::str::from_utf8(value) {
            Ok(text) if text.trim().is_empty() => None,
            _ => Some((line, value)),
        })
        .map(|(line, value)| {
            serde_json::from_slice(value)
                .map_err(|e| ironbeam_io::io("jsonl", format!("line {}: {e}", line + 1)))
        })
        .collect()
}
fn write_jsonl<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    use std::io::Write;
    let file = ironbeam_io::open_encoded_output(path, "jsonl")?;
    let mut file = std::io::BufWriter::new(file);
    for value in values {
        serde_json::to_writer(&mut file, value).map_err(|e| ironbeam_io::io("jsonl", e))?;
        file.write_all(b"\n")
            .map_err(|e| ironbeam_io::io("jsonl", e))?;
    }
    file.flush().map_err(|e| ironbeam_io::io("jsonl", e))
}
/// Read a bounded JSON Lines file into typed values.
pub fn jsonl_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::source(pipeline, key, path, "jsonl", read_jsonl::<T>)
}
#[allow(clippy::needless_pass_by_value)] // The shared lazy-source factory takes PathBuf.
fn read_jsonl_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    use std::io::BufRead;
    let reader = std::io::BufReader::new(ironbeam_io::open_lazy_input(&path, "jsonl")?);
    let rows = reader
        .lines()
        .enumerate()
        .filter_map(|(line, result)| match result {
            Ok(value) if value.trim().is_empty() => None,
            other => Some((line, other)),
        })
        .map(|(line, result)| {
            let value = result.map_err(|e| ironbeam_io::io("jsonl", e))?;
            serde_json::from_str(&value)
                .map_err(|e| ironbeam_io::io("jsonl", format!("line {}: {e}", line + 1)))
        });
    Ok(Box::new(rows))
}
/// Decode a bounded JSON Lines file one record at a time.
pub fn jsonl_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::lazy_source(pipeline, key, path, "jsonl-streaming", read_jsonl_lazy::<T>)
}
fn count_jsonl_lines(path: &Path) -> Result<usize, Error> {
    use std::io::BufRead;
    std::io::BufReader::new(ironbeam_io::open_lazy_input(path, "jsonl")?)
        .lines()
        .try_fold(0_usize, |count, line| {
            line.map_err(|e| ironbeam_io::io("jsonl", e))?;
            count
                .checked_add(1)
                .ok_or_else(|| ironbeam_io::io("jsonl", "line count overflow"))
        })
}
#[allow(clippy::needless_pass_by_value)] // The shared sharded-source factory takes PathBuf.
fn read_jsonl_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    use std::io::BufRead;
    let reader = std::io::BufReader::new(ironbeam_io::open_lazy_input(&path, "jsonl")?);
    let rows = reader
        .lines()
        .enumerate()
        .skip(start)
        .take(end - start)
        .filter_map(|(line, result)| match result {
            Ok(value) if value.trim().is_empty() => None,
            other => Some((line, other)),
        })
        .map(|(line, result)| {
            let value = result.map_err(|e| ironbeam_io::io("jsonl", e))?;
            serde_json::from_str(&value)
                .map_err(|e| ironbeam_io::io("jsonl", format!("line {}: {e}", line + 1)))
        });
    Ok(Box::new(rows))
}
/// Create stable bounded JSONL line-range sources; blank lines count toward ranges.
pub fn jsonl_sharded_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    lines_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_sharded_source(
        pipeline,
        key,
        path,
        lines_per_shard,
        "jsonl",
        count_jsonl_lines,
        read_jsonl_range::<T>,
    )
}
/// Write typed values to a local JSON Lines file.
pub fn jsonl_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    ironbeam_io::sink(input, key, path, write_jsonl::<T>)
}

/// A local, graph-bound staging scope for finalized JSONL bundles.
#[derive(Clone)]
pub struct JsonlFragmentStore {
    local: LocalFinalResult,
}

impl JsonlFragmentStore {
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

    /// Return accepted JSON Lines fragments after runner finalization.
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

/// Open a local JSON Lines fragment store for one job and graph.
pub fn jsonl_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<JsonlFragmentStore, Error> {
    Ok(JsonlFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, "jsonl")?,
    })
}

fn write_jsonl_record<T: Serialize>(
    value: &T,
    output: &mut dyn std::io::Write,
) -> Result<(), Error> {
    serde_json::to_writer(&mut *output, value)
        .map_err(|error| ironbeam_io::io("jsonl-fragment", error))?;
    output
        .write_all(b"\n")
        .map_err(|error| ironbeam_io::io("jsonl-fragment", error))
}

/// Stage complete JSONL records per bundle and accept them after `FinalizeBundle`.
/// The staging path must be shared with the job coordinator.
pub fn jsonl_fragment_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    sequence_fragment_sink(
        input,
        key,
        staging,
        job,
        graph,
        "jsonl",
        write_jsonl_record::<T>,
    )
}

/// Validate every record and atomically publish one JSONL file after job completion.
pub fn publish_jsonl_fragments<T: DeserializeOwned>(
    store: &JsonlFragmentStore,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    use std::io::BufRead;
    publish_sequence_fragments(&store.local, expected_rows, target, |path| {
        let file =
            std::fs::File::open(path).map_err(|error| ironbeam_io::io("jsonl-fragment", error))?;
        let mut reader = std::io::BufReader::new(file);
        let mut line = String::new();
        let mut rows = 0_u64;
        loop {
            line.clear();
            if reader
                .read_line(&mut line)
                .map_err(|error| ironbeam_io::io("jsonl-fragment", error))?
                == 0
            {
                break;
            }
            if !line.ends_with('\n') {
                return Err(Error::incompatible(
                    "JSONL fragment has an incomplete final line",
                ));
            }
            let _: T = serde_json::from_str(&line)
                .map_err(|error| ironbeam_io::io("jsonl-fragment", error))?;
            rows = rows
                .checked_add(1)
                .ok_or_else(|| Error::incompatible("JSONL fragment row count overflow"))?;
        }
        Ok(rows)
    })
}
