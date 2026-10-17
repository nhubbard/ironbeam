use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink, TypedSourceIterator};
use ironbeam_io::final_result::{AcceptedFragment, LocalFinalResult, PublishedResult};
use ironbeam_io::sequence_final::{
    publish_sequence_fragments, sequence_fragment_sink, sequence_fragment_store,
};
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};

fn read_cbor<T: DeserializeOwned>(bytes: &[u8]) -> Result<Vec<T>, Error> {
    let mut cursor = std::io::Cursor::new(bytes);
    let mut rows = Vec::new();
    while cursor.position() < bytes.len() as u64 {
        rows.push(ciborium::de::from_reader(&mut cursor).map_err(|e| ironbeam_io::io("cbor", e))?);
    }
    Ok(rows)
}
fn write_cbor<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Error> {
    let mut file = ironbeam_io::open_encoded_output(path, "cbor")?;
    for value in values {
        ciborium::ser::into_writer(value, &mut file).map_err(|e| ironbeam_io::io("cbor", e))?;
    }
    Ok(())
}
/// Read a bounded CBOR file into typed values.
pub fn cbor_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::source(pipeline, key, path, "cbor", read_cbor::<T>)
}
// The shared lazy-source callback owns its path even though opening only borrows it.
#[allow(clippy::needless_pass_by_value)]
fn read_cbor_lazy<T: Element + DeserializeOwned>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    use std::io::BufRead;
    let file = ironbeam_io::open_lazy_input(&path, "cbor")?;
    let mut reader = std::io::BufReader::new(file);
    let mut done = false;
    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }
        match reader.fill_buf() {
            Ok([]) => None,
            Ok(_) => {
                let item =
                    ciborium::de::from_reader(&mut reader).map_err(|e| ironbeam_io::io("cbor", e));
                if item.is_err() {
                    done = true;
                }
                Some(item)
            }
            Err(error) => {
                done = true;
                Some(Err(ironbeam_io::io("cbor", error)))
            }
        }
    })))
}
/// Decode a bounded CBOR file incrementally.
pub fn cbor_streaming_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::lazy_source(pipeline, key, path, "cbor-streaming", read_cbor_lazy::<T>)
}
/// Write typed values to a local CBOR file.
pub fn cbor_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    ironbeam_io::sink(input, key, path, write_cbor::<T>)
}

/// Scope for finalized CBOR record sequences.
#[derive(Clone)]
pub struct CborFragmentStore {
    local: LocalFinalResult,
}

impl CborFragmentStore {
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

    /// Return accepted CBOR fragments after runner finalization.
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

/// Open a local CBOR fragment store for one job and graph.
pub fn cbor_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<CborFragmentStore, Error> {
    Ok(CborFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, "cbor")?,
    })
}

fn write_cbor_record<T: Serialize>(
    value: &T,
    output: &mut dyn std::io::Write,
) -> Result<(), Error> {
    ciborium::ser::into_writer(value, output)
        .map_err(|error| ironbeam_io::io("cbor-fragment", error))
}

/// Stage CBOR values as replay-safe bundle fragments.
pub fn cbor_fragment_sink<T: Element + Serialize>(
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
        "cbor",
        write_cbor_record::<T>,
    )
}

/// Publish one validated CBOR sequence after the runner reports DONE.
pub fn publish_cbor_fragments<T: DeserializeOwned>(
    store: &CborFragmentStore,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    use std::io::BufRead;
    publish_sequence_fragments(&store.local, expected_rows, target, |path| {
        let file =
            std::fs::File::open(path).map_err(|error| ironbeam_io::io("cbor-fragment", error))?;
        let mut reader = std::io::BufReader::new(file);
        let mut count = 0_u64;
        while !reader
            .fill_buf()
            .map_err(|error| ironbeam_io::io("cbor-fragment", error))?
            .is_empty()
        {
            let _: T = ciborium::de::from_reader(&mut reader)
                .map_err(|error| ironbeam_io::io("cbor-fragment", error))?;
            count += 1;
        }
        Ok(count)
    })
}

#[cfg(feature = "parallel-io")]
/// Write CBOR values through parallel local output shards.
pub fn cbor_parallel_sink<T: Element + Serialize + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    ironbeam_io::framed_parallel_sink(input, key, path, shards, write_cbor::<T>)
}

/// Expand a CBOR file glob into stable bounded source nodes.
pub fn cbor_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        cbor_source(pipeline, key, path)
    })
}

fn count_cbor_records<T: Element + DeserializeOwned>(path: &Path) -> Result<usize, Error> {
    ironbeam_io::count_lazy_records(path, read_cbor_lazy::<T>)
}

fn read_cbor_range<T: Element + DeserializeOwned>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    ironbeam_io::read_lazy_range(path, start, end, read_cbor_lazy::<T>)
}

/// Create stable bounded CBOR record-range sources.
pub fn cbor_sharded_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        "cbor",
        count_cbor_records::<T>,
        read_cbor_range::<T>,
    )
}

#[cfg(test)]
#[path = "../../tests/unit/lib__fragment_tests.rs"]
mod fragment_tests;
