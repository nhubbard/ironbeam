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

/// Stage encoded records per bundle and accept them only after FinalizeBundle.
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
mod tests {
    use super::*;

    fn encode_u32(value: &u32, output: &mut dyn Write) -> Result<(), Error> {
        output.write_all(&value.to_le_bytes()).map_err(io)
    }

    fn session(store: &LocalFinalResult) -> SequenceFragmentSink<u32> {
        SequenceFragmentSink {
            store: store.clone(),
            encode: encode_u32,
            bundle_id: None,
            spool: None,
            rows: 0,
            staged: false,
        }
    }

    fn count_u32(path: &Path) -> Result<u64, Error> {
        let bytes = std::fs::read(path).map_err(io)?;
        if bytes.len() % 4 != 0 {
            return Err(Error::incompatible("incomplete u32 record"));
        }
        Ok((bytes.len() / 4) as u64)
    }

    #[test]
    fn retry_abort_scope_and_decoded_count_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let stage = dir.path().join("stage");
        let store = sequence_fragment_store(&stage, "job", "graph", "sink", "test-u32").unwrap();
        assert!(sequence_fragment_store("relative", "job", "graph", "sink", "test-u32").is_err());
        assert!(sequence_fragment_store(&stage, "job", "", "sink", "test-u32").is_err());
        assert!(sequence_fragment_store(&stage, "job", "graph", "sink", "").is_err());
        let output = dir.path().join("result.bin");
        let mut aborted = session(&store);
        assert!(aborted.write(1).is_err());
        aborted.begin_bundle("aborted").unwrap();
        aborted.write(1).unwrap();
        aborted.commit().unwrap();
        assert!(publish_sequence_fragments(&store, 0, &output, count_u32).is_err());
        aborted.abort().unwrap();
        assert!(store.accepted_fragments().unwrap().is_empty());
        let mut accepted = session(&store);
        accepted.begin_bundle("accepted").unwrap();
        accepted.write(2).unwrap();
        accepted.commit().unwrap();
        accepted.finalize_bundle().unwrap();
        assert!(publish_sequence_fragments(&store, 2, &output, count_u32).is_err());
        assert!(!output.exists());
        let other = sequence_fragment_store(&stage, "job", "graph", "sink", "other").unwrap();
        assert!(other.accepted_fragments().is_err());
        let ambiguous_stage = dir.path().join("ambiguous-stage");
        let first =
            sequence_fragment_store(&ambiguous_stage, "job", "graph", "sink", "test\0u32").unwrap();
        first.stage("bundle", 1, &1_u32.to_le_bytes()).unwrap();
        first.accept("bundle").unwrap();
        let split_scope =
            sequence_fragment_store(&ambiguous_stage, "job", "graph\0test", "sink", "u32").unwrap();
        assert!(split_scope.accepted_fragments().is_err());
        assert_eq!(
            publish_sequence_fragments(&store, 1, &output, count_u32)
                .unwrap()
                .rows,
            1
        );
        assert_eq!(std::fs::read(&output).unwrap(), 2_u32.to_le_bytes());
    }

    #[test]
    fn empty_bundle_requires_commit_and_acceptance() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            sequence_fragment_store(dir.path().join("stage"), "job", "graph", "sink", "test-u32")
                .unwrap();
        let output = dir.path().join("empty.bin");
        let mut sink = session(&store);
        assert!(sink.commit().is_err());
        assert!(sink.finalize_bundle().is_err());
        assert!(sink.begin_bundle("").is_err());
        sink.begin_bundle("empty").unwrap();
        assert!(sink.begin_bundle("again").is_err());
        assert!(sink.finalize_bundle().is_err());
        sink.commit().unwrap();
        assert!(publish_sequence_fragments(&store, 0, &output, count_u32).is_err());
        sink.finalize_bundle().unwrap();
        sink.finalize_bundle().unwrap();
        assert_eq!(
            publish_sequence_fragments(&store, 0, &output, count_u32)
                .unwrap()
                .rows,
            0
        );
        assert!(std::fs::read(&output).unwrap().is_empty());
        sink.abort().unwrap();
        assert_eq!(store.accepted_fragments().unwrap().len(), 1);
    }

    #[test]
    fn malformed_accepted_payload_cannot_publish() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            sequence_fragment_store(dir.path().join("stage"), "job", "graph", "sink", "test-u32")
                .unwrap();
        store.stage("bundle", 1, &[1, 2, 3]).unwrap();
        store.accept("bundle").unwrap();
        let target = dir.path().join("result.bin");
        assert!(publish_sequence_fragments(&store, 1, &target, count_u32).is_err());
        assert!(!target.exists());
    }
}
