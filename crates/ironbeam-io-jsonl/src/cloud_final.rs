//! Portable JSONL result fragments backed by provider-neutral object storage.

use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;

use ironbeam_cloud_objects::ObjectIO;
use ironbeam_core::{Element, Error, PCollection, Replay, Sink, SinkMode, SinkSpec, TypedSink};
use ironbeam_io::object_final::{ObjectFinalResult, ObjectPublishedResult};
use serde::{Serialize, de::DeserializeOwned};

/// Worker memory is bounded per bundle; the completed result streams through disk.
const MAX_BUNDLE_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

struct BoundedBytes<'a> {
    bytes: &'a mut Vec<u8>,
    limit: usize,
}

impl Write for BoundedBytes<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("cloud JSONL bundle exceeds 64 MiB"));
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn read_record(reader: &mut impl BufRead, line: &mut Vec<u8>) -> Result<bool, Error> {
    line.clear();
    loop {
        let available = reader
            .fill_buf()
            .map_err(|error| Error::io("cloud-jsonl-result", "jsonl", error.to_string()))?;
        if available.is_empty() {
            if line.is_empty() {
                return Ok(false);
            }
            return Err(Error::incompatible(
                "cloud JSONL result lacks final newline",
            ));
        }
        let length = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |at| at + 1);
        if length
            > MAX_RECORD_BYTES
                .saturating_add(1)
                .saturating_sub(line.len())
        {
            return Err(Error::incompatible("cloud JSONL record exceeds 64 MiB"));
        }
        let complete = available[length - 1] == b'\n';
        line.extend_from_slice(&available[..length]);
        reader.consume(length);
        if complete {
            line.pop();
            return Ok(true);
        }
    }
}

struct CloudJsonlFragmentSink<T> {
    store: ObjectFinalResult,
    bundle_id: Option<String>,
    bytes: Vec<u8>,
    rows: u64,
    staged: bool,
    failed: bool,
    marker: std::marker::PhantomData<T>,
}

impl<T: Element + Serialize> TypedSink<T> for CloudJsonlFragmentSink<T> {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        if instruction_id.is_empty() || self.bundle_id.is_some() {
            return Err(Error::incompatible(
                "cloud JSONL fragment needs one bundle attempt identity",
            ));
        }
        self.bundle_id = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: T) -> Result<(), Error> {
        if self.bundle_id.is_none() || self.staged || self.failed {
            return Err(Error::incompatible(
                "cloud JSONL fragment has no open bundle",
            ));
        }
        let next_rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| Error::incompatible("cloud JSONL fragment row count overflow"))?;
        if let Err(error) = serde_json::to_writer(
            BoundedBytes {
                bytes: &mut self.bytes,
                limit: MAX_BUNDLE_BYTES,
            },
            &value,
        ) {
            self.failed = true;
            return Err(Error::io(
                "cloud-jsonl-fragment",
                "jsonl",
                error.to_string(),
            ));
        }
        if let Err(error) = (BoundedBytes {
            bytes: &mut self.bytes,
            limit: MAX_BUNDLE_BYTES,
        })
        .write_all(b"\n")
        {
            self.failed = true;
            return Err(Error::io(
                "cloud-jsonl-fragment",
                "jsonl",
                error.to_string(),
            ));
        }
        self.rows = next_rows;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if self.failed {
            return Err(Error::incompatible(
                "cloud JSONL fragment failed before commit",
            ));
        }
        let bundle_id = self.bundle_id.as_deref().ok_or_else(|| {
            Error::incompatible("cloud JSONL fragment has no bundle attempt identity")
        })?;
        self.store.stage(bundle_id, self.rows, &self.bytes)?;
        self.staged = true;
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        if self.staged
            && let Some(bundle_id) = self.bundle_id.as_deref()
        {
            self.store.abandon(bundle_id)?;
        }
        self.bytes.clear();
        self.staged = false;
        self.failed = false;
        Ok(())
    }

    fn requires_bundle_finalization(&self) -> bool {
        true
    }

    fn finalize_bundle(&mut self) -> Result<(), Error> {
        let bundle_id = self.bundle_id.as_deref().ok_or_else(|| {
            Error::incompatible("cloud JSONL fragment has no bundle attempt identity")
        })?;
        if !self.staged {
            return Err(Error::incompatible(
                "cloud JSONL fragment has not committed",
            ));
        }
        self.store.accept(bundle_id)
    }

    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.store.accept(instruction_id)
    }
}

/// Reopen the same object-backed result scope on a worker or coordinator.
/// The target must be outside the staging prefix; this produces uncompressed JSONL.
pub fn cloud_jsonl_fragment_store(
    storage: Arc<dyn ObjectIO>,
    staging: (&str, &str),
    target: (&str, &str),
    job: &str,
    graph: &str,
    key: &str,
) -> Result<ObjectFinalResult, Error> {
    if !target.1.ends_with(".jsonl") {
        return Err(Error::graph(
            key,
            "cloud JSONL fragment target must end in .jsonl",
        ));
    }
    ObjectFinalResult::new(storage, staging, target, job, graph, key)
}

/// Stage each portable bundle, then accept it through `FinalizeBundle`.
/// The coordinator must publish after successful job completion with its exact
/// completed attempt manifest and committed row count.
pub fn cloud_jsonl_fragment_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    storage: Arc<dyn ObjectIO>,
    staging: (&str, &str),
    target: (&str, &str),
    job: &str,
    graph: &str,
) -> Result<Sink<T>, Error> {
    let store = cloud_jsonl_fragment_store(storage, staging, target, job, graph, key)?;
    let config =
        serde_json::to_string(&("cloud-jsonl-fragments-v1", staging, target, job, graph, key))
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
            Ok(Box::new(CloudJsonlFragmentSink::<T> {
                store: store.clone(),
                bundle_id: None,
                bytes: Vec::new(),
                rows: 0,
                staged: false,
                failed: false,
                marker: std::marker::PhantomData,
            }))
        },
    )
}

/// Validate all decoded records and conditionally create the completed result.
/// Bind `store` with `require_bundle_attempts` before calling this function.
pub fn publish_cloud_jsonl_fragments<T: DeserializeOwned>(
    store: &ObjectFinalResult,
    expected_rows: u64,
) -> Result<ObjectPublishedResult, Error> {
    store.publish_validated_streaming(expected_rows, |reader| {
        let mut reader = BufReader::new(reader);
        let mut rows = 0_u64;
        let mut line = Vec::new();
        loop {
            if !read_record(&mut reader, &mut line)? {
                break;
            }
            if line.is_empty() {
                return Err(Error::incompatible(format!(
                    "cloud JSONL result has empty line {}",
                    rows + 1
                )));
            }
            let _: T = serde_json::from_slice(&line).map_err(|error| {
                Error::io(
                    "cloud-jsonl-result",
                    "jsonl",
                    format!("line {}: {error}", rows + 1),
                )
            })?;
            rows = rows
                .checked_add(1)
                .ok_or_else(|| Error::incompatible("cloud JSONL result row count overflow"))?;
        }
        Ok(rows)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironbeam_cloud_objects::LocalObjectIO;

    #[test]
    fn bundle_writer_and_record_reader_enforce_resource_bounds() {
        let mut bytes = Vec::new();
        let mut writer = BoundedBytes {
            bytes: &mut bytes,
            limit: 4,
        };
        writer.write_all(b"1234").unwrap();
        assert!(writer.write_all(b"5").is_err());
        assert_eq!(bytes, b"1234");
        let mut reader = BufReader::new(b"one\ntwo\n".as_slice());
        let mut line = Vec::new();
        assert!(read_record(&mut reader, &mut line).unwrap());
        assert_eq!(line, b"one");
        assert!(read_record(&mut reader, &mut line).unwrap());
        assert_eq!(line, b"two");
        assert!(!read_record(&mut reader, &mut line).unwrap());
        let mut incomplete = BufReader::new(b"missing newline".as_slice());
        assert!(read_record(&mut incomplete, &mut line).is_err());
    }

    #[test]
    fn bundle_finalization_and_publication_round_trip() {
        let directory = tempfile::tempdir().unwrap();
        let storage: Arc<dyn ObjectIO> = Arc::new(LocalObjectIO::new(directory.path()).unwrap());
        let store = cloud_jsonl_fragment_store(
            Arc::clone(&storage),
            ("stage", "job/result"),
            ("output", "result.jsonl"),
            "job",
            "graph",
            "sink",
        )
        .unwrap();
        let mut sink = CloudJsonlFragmentSink::<u64> {
            store: store.clone(),
            bundle_id: None,
            bytes: Vec::new(),
            rows: 0,
            staged: false,
            failed: false,
            marker: std::marker::PhantomData,
        };
        sink.begin_bundle("attempt").unwrap();
        sink.write(3).unwrap();
        sink.write(4).unwrap();
        sink.commit().unwrap();
        assert!(
            publish_cloud_jsonl_fragments::<u64>(
                &store.require_bundle_attempts(["attempt"]).unwrap(),
                2,
            )
            .is_err()
        );
        sink.finalize_bundle().unwrap();
        let completed = store.require_bundle_attempts(["attempt"]).unwrap();
        assert_eq!(
            publish_cloud_jsonl_fragments::<u64>(&completed, 2)
                .unwrap()
                .rows,
            2
        );
        assert_eq!(
            storage.get_object("output", "result.jsonl").unwrap(),
            b"3\n4\n"
        );
        assert_eq!(
            publish_cloud_jsonl_fragments::<u64>(&completed, 2)
                .unwrap()
                .rows,
            2
        );
    }

    #[test]
    fn partial_record_cannot_cross_attempt_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let storage: Arc<dyn ObjectIO> = Arc::new(LocalObjectIO::new(directory.path()).unwrap());
        let store = cloud_jsonl_fragment_store(
            Arc::clone(&storage),
            ("stage", "job/result"),
            ("output", "result.jsonl"),
            "job",
            "graph",
            "sink",
        )
        .unwrap();
        store.stage("a", 0, b"{").unwrap();
        store.accept("a").unwrap();
        store.stage("b", 1, b"}\n").unwrap();
        store.accept("b").unwrap();
        let completed = store.require_bundle_attempts(["a", "b"]).unwrap();
        assert!(publish_cloud_jsonl_fragments::<serde_json::Value>(&completed, 1).is_err());
        assert!(!storage.object_exists("output", "result.jsonl").unwrap());
    }
}
