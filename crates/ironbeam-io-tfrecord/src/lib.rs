//! `TFRecord` file sources, deferred sinks, and native TensorFlow Example schema for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{
    Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, TypedSink, TypedSourceIterator,
};
use ironbeam_io::final_result::{LocalFinalResult, PublishedResult};
#[cfg(feature = "parallel-io")]
use ironbeam_io::framed_parallel_sink;
use ironbeam_io::{
    count_lazy_records, file_glob_source, file_sharded_source, io, lazy_source,
    open_encoded_output, open_lazy_seek, read_lazy_range, sink, source,
};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub mod tfrecord_proto;
pub use tfrecord_proto::{BytesList, Example, Feature, Features, FloatList, Int64List};

pub fn tfrecord_glob_source(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<Vec<u8>>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        tfrecord_source(pipeline, key, path)
    })
}
fn masked_crc(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
        .rotate_right(15)
        .wrapping_add(0xa282_ead8)
}
fn read_tfrecord(mut bytes: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
    let mut rows = Vec::new();
    while !bytes.is_empty() {
        let header = bytes
            .get(..12)
            .ok_or_else(|| io("tfrecord", "truncated record header"))?;
        let length_bytes: [u8; 8] = header[..8]
            .try_into()
            .map_err(|_| io("tfrecord", "invalid length"))?;
        let crc_bytes: [u8; 4] = header[8..12]
            .try_into()
            .map_err(|_| io("tfrecord", "invalid length CRC"))?;
        if u32::from_le_bytes(crc_bytes) != masked_crc(&length_bytes) {
            return Err(io("tfrecord", "length CRC mismatch"));
        }
        let length =
            usize::try_from(u64::from_le_bytes(length_bytes)).map_err(|e| io("tfrecord", e))?;
        let end = 12_usize
            .checked_add(length)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| io("tfrecord", "record length overflow"))?;
        let record = bytes
            .get(12..end)
            .ok_or_else(|| io("tfrecord", "truncated record"))?;
        let payload = &record[..length];
        let stored_crc: [u8; 4] = record[length..]
            .try_into()
            .map_err(|_| io("tfrecord", "invalid data CRC"))?;
        if u32::from_le_bytes(stored_crc) != masked_crc(payload) {
            return Err(io("tfrecord", "data CRC mismatch"));
        }
        rows.push(payload.to_vec());
        bytes = &bytes[end..];
    }
    Ok(rows)
}
fn write_frames(writer: &mut impl Write, values: &[Vec<u8>]) -> Result<(), Error> {
    for value in values {
        let length = (value.len() as u64).to_le_bytes();
        writer.write_all(&length).map_err(|e| io("tfrecord", e))?;
        writer
            .write_all(&masked_crc(&length).to_le_bytes())
            .map_err(|e| io("tfrecord", e))?;
        writer.write_all(value).map_err(|e| io("tfrecord", e))?;
        writer
            .write_all(&masked_crc(value).to_le_bytes())
            .map_err(|e| io("tfrecord", e))?;
    }
    Ok(())
}
fn write_tfrecord(path: &Path, values: &[Vec<u8>]) -> Result<(), Error> {
    let mut file = open_encoded_output(path, "tfrecord")?;
    write_frames(&mut file, values)
}
pub fn tfrecord_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<Vec<u8>>, Error> {
    source(pipeline, key, path, "tfrecord", read_tfrecord)
}
#[allow(clippy::needless_pass_by_value)] // Shared lazy-source factory accepts PathBuf.
fn read_tfrecord_lazy(path: PathBuf) -> Result<TypedSourceIterator<Vec<u8>>, Error> {
    use std::io::{Read, Seek};
    let (file, file_length) = open_lazy_seek(&path, "tfrecord")?;
    let mut reader = std::io::BufReader::new(file);
    let mut done = false;
    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }
        let mut header = [0_u8; 12];
        match reader.read(&mut header[..1]) {
            Ok(0) => return None,
            Ok(_) => {}
            Err(error) => {
                done = true;
                return Some(Err(io("tfrecord", error)));
            }
        }
        if let Err(error) = reader.read_exact(&mut header[1..]) {
            done = true;
            return Some(Err(io("tfrecord", error)));
        }
        let length_bytes: [u8; 8] = header[..8].try_into().expect("fixed-size header");
        let stored_length_crc =
            u32::from_le_bytes(header[8..12].try_into().expect("fixed-size header"));
        if stored_length_crc != masked_crc(&length_bytes) {
            done = true;
            return Some(Err(io("tfrecord", "length CRC mismatch")));
        }
        let length = match usize::try_from(u64::from_le_bytes(length_bytes)) {
            Ok(length) => length,
            Err(error) => {
                done = true;
                return Some(Err(io("tfrecord", error)));
            }
        };
        let position = match reader.stream_position() {
            Ok(position) => position,
            Err(error) => {
                done = true;
                return Some(Err(io("tfrecord", error)));
            }
        };
        if u64::try_from(length)
            .ok()
            .and_then(|length| length.checked_add(4))
            .is_none_or(|needed| needed > file_length.saturating_sub(position))
        {
            done = true;
            return Some(Err(io("tfrecord", "truncated record")));
        }
        let mut payload = vec![0_u8; length];
        let mut crc = [0_u8; 4];
        if let Err(error) = reader
            .read_exact(&mut payload)
            .and_then(|()| reader.read_exact(&mut crc))
        {
            done = true;
            return Some(Err(io("tfrecord", error)));
        }
        if u32::from_le_bytes(crc) != masked_crc(&payload) {
            done = true;
            return Some(Err(io("tfrecord", "data CRC mismatch")));
        }
        Some(Ok(payload))
    })))
}
pub fn tfrecord_streaming_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<Vec<u8>>, Error> {
    lazy_source(
        pipeline,
        key,
        path,
        "tfrecord-streaming",
        read_tfrecord_lazy,
    )
}
pub fn tfrecord_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<Vec<u8>>, Error> {
    sink(input, key, path, write_tfrecord)
}

struct TfrecordAppendSink {
    path: PathBuf,
    key: String,
    values: Vec<Vec<u8>>,
}

impl TypedSink<Vec<u8>> for TfrecordAppendSink {
    fn write(&mut self, value: Vec<u8>) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let mut bytes = Vec::new();
        write_frames(&mut bytes, &self.values)?;
        let parent = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent).map_err(|error| io(&self.key, error))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| io(&self.key, error))?;
        file.write_all(&bytes)
            .map_err(|error| io(&self.key, error))?;
        file.flush().map_err(|error| io(&self.key, error))?;
        self.values.clear();
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}

/// Append complete `TFRecord` frames at each bundle commit.
///
/// Replays may duplicate records. Use `tfrecord_sink` for whole-job replacement.
/// This local filesystem sink requires a path shared by all workers.
pub fn tfrecord_append_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<Vec<u8>>, Error> {
    let path = path.as_ref().to_path_buf();
    if has_compression_extension(&path) {
        return Err(Error::graph(
            key,
            "append sink does not support compressed output",
        ));
    }
    let name = key.to_owned();
    input.sink_named_with_config(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("tfrecord-append:{}", path.display()),
        move || {
            Ok(Box::new(TfrecordAppendSink {
                path: path.clone(),
                key: name.clone(),
                values: Vec::new(),
            }))
        },
    )
}

fn has_compression_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|part| part.to_str())
        .is_some_and(|part| {
            matches!(
                part.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
}

struct TfrecordFragmentSink {
    store: LocalFinalResult,
    bundle_id: Option<String>,
    spool: Option<tempfile::NamedTempFile>,
    rows: u64,
    staged: bool,
}

impl TypedSink<Vec<u8>> for TfrecordFragmentSink {
    fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
        if instruction_id.is_empty() || self.bundle_id.is_some() {
            return Err(Error::incompatible(
                "TFRecord fragment needs one bundle attempt identity",
            ));
        }
        self.bundle_id = Some(instruction_id.to_owned());
        Ok(())
    }

    fn write(&mut self, value: Vec<u8>) -> Result<(), Error> {
        if self.bundle_id.is_none() {
            return Err(Error::incompatible(
                "TFRecord fragment has no bundle attempt identity",
            ));
        }
        let spool = match self.spool.as_mut() {
            Some(spool) => spool,
            None => self.spool.insert(
                tempfile::NamedTempFile::new().map_err(|error| io("tfrecord-fragment", error))?,
            ),
        };
        write_frames(spool, std::slice::from_ref(&value))?;
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| Error::incompatible("TFRecord fragment row count overflow"))?;
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let bundle_id = self.bundle_id.as_deref().ok_or_else(|| {
            Error::incompatible("TFRecord fragment has no bundle attempt identity")
        })?;
        let spool = match self.spool.as_mut() {
            Some(spool) => spool,
            None => self.spool.insert(
                tempfile::NamedTempFile::new().map_err(|error| io("tfrecord-fragment", error))?,
            ),
        };
        spool
            .as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(|error| io("tfrecord-fragment", error))?;
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
            Error::incompatible("TFRecord fragment has no bundle attempt identity")
        })?;
        if !self.staged {
            return Err(Error::incompatible("TFRecord fragment has not committed"));
        }
        self.store.accept(bundle_id)
    }

    fn recover_bundle_finalization(&mut self, instruction_id: &str) -> Result<(), Error> {
        self.store.accept(instruction_id)
    }
}

/// Reopen the same local staging scope on a worker or job coordinator.
/// The staging path must be absolute and visible to both processes.
pub fn tfrecord_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<LocalFinalResult, Error> {
    let staging = staging.as_ref();
    if !staging.is_absolute() {
        return Err(Error::graph(
            key,
            "TFRecord fragment staging path must be absolute",
        ));
    }
    LocalFinalResult::new(staging, job, graph, key)
}

/// Register a bundle-finalized TFRecord fragment sink for portable jobs.
/// The caller must publish after the runner reports DONE. This remains an
/// append-mode graph sink until the whole-job result contract is complete.
pub fn tfrecord_fragment_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
) -> Result<Sink<Vec<u8>>, Error> {
    let staging = staging.as_ref();
    let store = tfrecord_fragment_store(staging, job, graph, key)?;
    let config = serde_json::to_string(&("tfrecord-fragments-v1", staging, job, graph, key))
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
            Ok(Box::new(TfrecordFragmentSink {
                store: store.clone(),
                bundle_id: None,
                spool: None,
                rows: 0,
                staged: false,
            }))
        },
    )
}

/// Publish accepted TFRecord fragments after a successful portable job.
/// Every frame and CRC is checked before the final path becomes visible.
pub fn publish_tfrecord_fragments(
    store: &LocalFinalResult,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    let target = target.as_ref();
    if has_compression_extension(target) {
        return Err(Error::graph(
            "tfrecord-fragment",
            "compressed final result needs a format merger",
        ));
    }
    store.publish_validated_concatenated(expected_rows, target, |path| {
        let mut count = 0_u64;
        for row in read_tfrecord_lazy(path.to_path_buf())? {
            row?;
            count = count
                .checked_add(1)
                .ok_or_else(|| Error::incompatible("TFRecord result row count overflow"))?;
        }
        if count != expected_rows {
            return Err(Error::incompatible(
                "TFRecord result row count does not match runner",
            ));
        }
        Ok(())
    })
}

#[cfg(feature = "parallel-io")]
pub fn tfrecord_parallel_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<Vec<u8>>, Error> {
    framed_parallel_sink(input, key, path, shards, write_tfrecord)
}

fn read_tfrecord_examples(bytes: &[u8]) -> Result<Vec<Example>, Error> {
    read_tfrecord(bytes)?
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| {
            <Example as prost::Message>::decode(bytes.as_slice())
                .map_err(|e| io("tfrecord-examples", format!("record {}: {e}", index + 1)))
        })
        .collect()
}
fn read_tfrecord_examples_lazy(path: PathBuf) -> Result<TypedSourceIterator<Example>, Error> {
    Ok(Box::new(read_tfrecord_lazy(path)?.enumerate().map(
        |(index, result)| {
            let bytes = result?;
            <Example as prost::Message>::decode(bytes.as_slice())
                .map_err(|e| io("tfrecord-examples", format!("record {}: {e}", index + 1)))
        },
    )))
}
/// Read TensorFlow `tf.Example` messages from a bounded `TFRecord` file.
pub fn tfrecord_examples_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<Example>, Error> {
    source(
        pipeline,
        key,
        path,
        "tfrecord-examples",
        read_tfrecord_examples,
    )
}
/// Lazily decode TensorFlow `tf.Example` messages from a bounded `TFRecord` file.
pub fn tfrecord_examples_streaming_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<Example>, Error> {
    lazy_source(
        pipeline,
        key,
        path,
        "tfrecord-examples-streaming",
        read_tfrecord_examples_lazy,
    )
}
fn count_tfrecord_examples(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_tfrecord_examples_lazy)
}
fn read_tfrecord_examples_range(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<Example>, Error> {
    read_lazy_range(path, start, end, read_tfrecord_examples_lazy)
}
pub fn tfrecord_examples_sharded_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<Example>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        "tfrecord-examples",
        count_tfrecord_examples,
        read_tfrecord_examples_range,
    )
}

fn count_tfrecord_records(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_tfrecord_lazy)
}
fn read_tfrecord_range(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<Vec<u8>>, Error> {
    read_lazy_range(path, start, end, read_tfrecord_lazy)
}
pub fn tfrecord_sharded_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<Vec<u8>>, Error> {
    file_sharded_source(
        pipeline,
        key,
        path,
        records_per_shard,
        "tfrecord",
        count_tfrecord_records,
        read_tfrecord_range,
    )
}

#[cfg(test)]
mod fragment_tests {
    use super::*;

    fn session(store: &LocalFinalResult) -> TfrecordFragmentSink {
        TfrecordFragmentSink {
            store: store.clone(),
            bundle_id: None,
            spool: None,
            rows: 0,
            staged: false,
        }
    }

    #[test]
    fn finalized_fragments_publish_valid_tfrecord_only_after_acceptance() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        let output = temp.path().join("rows.tfrecord");
        let mut first = session(&store);
        assert!(first.write(b"early".to_vec()).is_err());
        assert!(first.begin_bundle("").is_err());
        assert!(first.finalize_bundle().is_err());
        first.begin_bundle("bundle-1").unwrap();
        assert!(first.begin_bundle("another").is_err());
        assert!(first.finalize_bundle().is_err());
        first.write(b"one".to_vec()).unwrap();
        first.write(b"two".to_vec()).unwrap();
        first.commit().unwrap();
        assert!(publish_tfrecord_fragments(&store, 2, &output).is_err());
        assert!(!output.exists());
        let mut conflicting = session(&store);
        conflicting.begin_bundle("bundle-1").unwrap();
        conflicting.write(b"other".to_vec()).unwrap();
        assert!(conflicting.commit().is_err());
        conflicting.abort().unwrap();
        let mut restarted = session(&store);
        assert!(restarted.recover_bundle_finalization("missing").is_err());
        restarted.recover_bundle_finalization("bundle-1").unwrap();
        first.finalize_bundle().unwrap();
        first.finalize_bundle().unwrap();
        let mut empty = session(&store);
        empty.begin_bundle("bundle-empty").unwrap();
        empty.commit().unwrap();
        empty.finalize_bundle().unwrap();
        let published = publish_tfrecord_fragments(&store, 2, &output).unwrap();
        assert_eq!(published.rows, 2);
        assert_eq!(published.fragments, 2);
        assert_eq!(
            publish_tfrecord_fragments(&store, 2, &output).unwrap(),
            published
        );
        assert_eq!(
            read_tfrecord(&std::fs::read(&output).unwrap()).unwrap(),
            [b"one".to_vec(), b"two".to_vec()]
        );
        let path = std::fs::read_dir(temp.path().join("stage"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.metadata().unwrap().len() > 120)
            .unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(path, bytes).unwrap();
        assert!(
            publish_tfrecord_fragments(&store, 2, temp.path().join("corrupt.tfrecord")).is_err()
        );
    }

    #[test]
    fn builder_binds_identity_and_aborted_attempt_never_publishes() {
        let temp = tempfile::tempdir().unwrap();
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", vec![b"row".to_vec()]).unwrap();
        assert!(
            tfrecord_fragment_sink(&input, "relative", "relative-stage", "job", "graph").is_err()
        );
        let staging = temp.path().join("stage");
        let sink = tfrecord_fragment_sink(&input, "fragment", &staging, "job", "graph").unwrap();
        let (_, registry) = sink.snapshot().unwrap();
        assert!(
            registry
                .sink_config_digest("fragment")
                .unwrap()
                .contains("tfrecord-fragments-v1")
        );
        assert!(registry.sink_requires_bundle_finalization("fragment"));
        let mut session = registry.open_sink("fragment").unwrap();
        assert!(session.requires_bundle_finalization());
        session.begin_bundle("aborted").unwrap();
        session.commit().unwrap();
        let store = LocalFinalResult::new(&staging, "job", "graph", "fragment").unwrap();
        assert!(store.accepted_fragments().unwrap().is_empty());
        session.abort().unwrap();
        assert!(std::fs::read_dir(&staging).unwrap().next().is_none());
        assert!(
            publish_tfrecord_fragments(&store, 1, temp.path().join("absent.tfrecord")).is_err()
        );
    }

    #[test]
    fn publication_validates_decoded_count_and_crc() {
        let temp = tempfile::tempdir().unwrap();
        let store =
            LocalFinalResult::new(temp.path().join("stage"), "job", "graph", "sink").unwrap();
        let mut frame = Vec::new();
        write_frames(&mut frame, &[b"one".to_vec()]).unwrap();
        store.stage("wrong-count", 2, &frame).unwrap();
        store.accept("wrong-count").unwrap();
        let output = temp.path().join("result.tfrecord");
        assert!(
            publish_tfrecord_fragments(&store, 2, temp.path().join("result.tfrecord.gz")).is_err()
        );
        assert!(publish_tfrecord_fragments(&store, 2, &output).is_err());
        assert!(!output.exists());
        let other =
            LocalFinalResult::new(temp.path().join("other"), "job", "graph", "sink").unwrap();
        let last = frame.len() - 1;
        frame[last] ^= 1;
        other.stage("bad-crc", 1, &frame).unwrap();
        other.accept("bad-crc").unwrap();
        assert!(publish_tfrecord_fragments(&other, 1, &output).is_err());
        assert!(!output.exists());
    }
}
