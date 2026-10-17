//! Durable bundle fragments for formats made of concatenated complete records.

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use ironbeam_core::{Element, Error, PCollection, Replay, Sink, SinkMode, SinkSpec, TypedSink};

use crate::final_result::{AcceptedFragment, LocalFinalResult, PublishedResult};

fn io(error: impl std::fmt::Display) -> Error {
    Error::io("sequence-fragment", "file", error.to_string())
}

struct SequenceFragmentSink<T> {
    store: LocalFinalResult,
    encode: fn(&T, &mut dyn Write) -> Result<(), Error>,
    bundle_id: Option<String>,
    spool: Option<tempfile::NamedTempFile>,
    rows: u64,
    staged: bool,
}

impl<T: Element> TypedSink<T> for SequenceFragmentSink<T> {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        if instruction_id.is_empty() || self.bundle_id.is_some() {
            return Err(Error::incompatible(
                "sequence fragment needs one bundle attempt identity",
            ));
        }
        self.bundle_id = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: T) -> Result<(), Error> {
        if self.bundle_id.is_none() {
            return Err(Error::incompatible(
                "sequence fragment has no bundle attempt identity",
            ));
        }
        let spool = match self.spool.as_mut() {
            Some(spool) => spool,
            None => self
                .spool
                .insert(tempfile::NamedTempFile::new().map_err(io)?),
        };
        (self.encode)(&value, spool.as_file_mut())?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| Error::incompatible("sequence fragment row count overflow"))?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let bundle_id = self.bundle_id.as_deref().ok_or_else(|| {
            Error::incompatible("sequence fragment has no bundle attempt identity")
        })?;
        let spool = match self.spool.as_mut() {
            Some(spool) => spool,
            None => self
                .spool
                .insert(tempfile::NamedTempFile::new().map_err(io)?),
        };
        spool.as_file_mut().seek(SeekFrom::Start(0)).map_err(io)?;
        self.store
            .stage_reader(bundle_id, self.rows, spool.as_file_mut())?;
        self.staged = true;
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        if self.staged {
            if let Some(bundle_id) = self.bundle_id.as_deref() {
                self.store.abandon(bundle_id)?;
            }
            self.staged = false;
        }
        self.spool = None;
        Ok(())
    }

    fn requires_bundle_finalization(&self) -> bool {
        true
    }

    fn finalize_bundle(&mut self) -> Result<(), Error> {
        let bundle_id = self.bundle_id.as_deref().ok_or_else(|| {
            Error::incompatible("sequence fragment has no bundle attempt identity")
        })?;
        if !self.staged {
            return Err(Error::incompatible("sequence fragment has not committed"));
        }
        self.store.accept(bundle_id)
    }

    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.store.accept(instruction_id)
    }
}

/// Reopen the same format-bound local staging scope on a worker or coordinator.
pub fn sequence_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
    format: &str,
) -> Result<LocalFinalResult, Error> {
    let staging = staging.as_ref();
    if !staging.is_absolute() || graph.is_empty() || format.is_empty() {
        return Err(Error::graph(
            key,
            "sequence fragments need an absolute staging path, graph, and format",
        ));
    }
    let scope = serde_json::to_string(&(graph, format, "sequence-v1"))
        .map_err(|error| Error::incompatible(error.to_string()))?;
    LocalFinalResult::new(staging, job, &scope, key)
}

/// Stage encoded records per bundle and accept them only after `FinalizeBundle`.
pub fn sequence_fragment_sink<T: Element>(
    input: &PCollection<T>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    format: &str,
    encode: fn(&T, &mut dyn Write) -> Result<(), Error>,
) -> Result<Sink<T>, Error> {
    let staging = staging.as_ref();
    let store = sequence_fragment_store(staging, job, graph, key, format)?;
    let config =
        serde_json::to_string(&("sequence-fragments-v1", staging, job, graph, key, format))
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
            Ok(Box::new(SequenceFragmentSink {
                store: store.clone(),
                encode,
                bundle_id: None,
                spool: None,
                rows: 0,
                staged: false,
            }))
        },
    )
}

/// Validate and atomically publish the concatenation after a successful job.
/// The validator must decode all records and return their exact count.
pub fn publish_sequence_fragments(
    store: &LocalFinalResult,
    expected_rows: u64,
    target: impl AsRef<Path>,
    validate: impl FnOnce(&Path) -> Result<u64, Error>,
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
            "sequence-fragment",
            "compressed final result needs a format merger",
        ));
    }
    store.publish_merged(
        expected_rows,
        target,
        |parts: &[AcceptedFragment], path, output| {
            for part in parts {
                part.copy_payload(output)?;
            }
            output.flush().map_err(io)?;
            validate(path)
        },
    )
}

#[cfg(test)]
#[path = "../tests/unit/sequence_final__tests.rs"]
mod tests;
