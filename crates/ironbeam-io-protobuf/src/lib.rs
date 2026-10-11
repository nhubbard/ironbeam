//! Protobuf file sources and deferred sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, TypedSink,
    TypedSourceIterator,
};
use ironbeam_io::final_result::{AcceptedFragment, LocalFinalResult, PublishedResult};
#[cfg(feature = "parallel-io")]
use ironbeam_io::framed_parallel_sink;
use ironbeam_io::sequence_final::{
    publish_sequence_fragments, sequence_fragment_sink, sequence_fragment_store,
};
use ironbeam_io::{
    count_lazy_records, file_glob_source, file_sharded_source, io, lazy_source,
    open_encoded_output, open_lazy_seek, read_lazy_range, sink, source,
};
use std::path::{Path, PathBuf};

pub fn protobuf_glob_source<T: Element + prost::Message + Default>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        protobuf_source(pipeline, key, path)
    })
}
fn read_protobuf<T: prost::Message + Default>(mut bytes: &[u8]) -> Result<Vec<T>, Error> {
    let mut rows = Vec::new();
    while !bytes.is_empty() {
        rows.push(T::decode_length_delimited(&mut bytes).map_err(|e| io("protobuf", e))?);
    }
    Ok(rows)
}
fn write_protobuf<T: prost::Message>(path: &Path, values: &[T]) -> Result<(), Error> {
    use std::io::Write;
    let mut file = open_encoded_output(path, "protobuf")?;
    for value in values {
        file.write_all(&value.encode_length_delimited_to_vec())
            .map_err(|e| io("protobuf", e))?;
    }
    Ok(())
}
pub fn protobuf_source<T: Element + prost::Message + Default>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "protobuf", read_protobuf::<T>)
}
fn read_protobuf_length(reader: &mut impl std::io::Read) -> Result<Option<usize>, Error> {
    let mut value = 0_u64;
    for index in 0..10_u32 {
        let mut byte = [0_u8; 1];
        match reader.read(&mut byte).map_err(|e| io("protobuf", e))? {
            0 if index == 0 => return Ok(None),
            0 => return Err(io("protobuf", "truncated length prefix")),
            _ => {}
        }
        let part = u64::from(byte[0] & 0x7f);
        if index == 9 && part > 1 {
            return Err(io("protobuf", "length prefix overflow"));
        }
        value |= part << (index * 7);
        if byte[0] & 0x80 == 0 {
            return usize::try_from(value)
                .map(Some)
                .map_err(|e| io("protobuf", e));
        }
    }
    Err(io("protobuf", "length prefix exceeds ten bytes"))
}
#[allow(clippy::needless_pass_by_value)] // Shared lazy-source factory accepts PathBuf.
fn read_protobuf_lazy<T: Element + prost::Message + Default>(
    path: PathBuf,
) -> Result<TypedSourceIterator<T>, Error> {
    use std::io::{Read, Seek};
    let (file, file_length) = open_lazy_seek(&path, "protobuf")?;
    let mut reader = std::io::BufReader::new(file);
    let mut done = false;
    Ok(Box::new(std::iter::from_fn(move || {
        if done {
            return None;
        }
        let length = match read_protobuf_length(&mut reader) {
            Ok(Some(length)) => length,
            Ok(None) => return None,
            Err(error) => {
                done = true;
                return Some(Err(error));
            }
        };
        let position = match reader.stream_position() {
            Ok(position) => position,
            Err(error) => {
                done = true;
                return Some(Err(io("protobuf", error)));
            }
        };
        if u64::try_from(length)
            .ok()
            .is_none_or(|length| length > file_length.saturating_sub(position))
        {
            done = true;
            return Some(Err(io("protobuf", "truncated record")));
        }
        let mut bytes = vec![0_u8; length];
        if let Err(error) = reader.read_exact(&mut bytes) {
            done = true;
            return Some(Err(io("protobuf", error)));
        }
        let item = <T as prost::Message>::decode(&bytes[..]).map_err(|e| io("protobuf", e));
        if item.is_err() {
            done = true;
        }
        Some(item)
    })))
}
pub fn protobuf_streaming_source<T: Element + prost::Message + Default>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    lazy_source(
        pipeline,
        key,
        path,
        "protobuf-streaming",
        read_protobuf_lazy::<T>,
    )
}
pub fn protobuf_sink<T: Element + prost::Message>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_protobuf::<T>)
}

/// Scope for finalized length-delimited Protobuf records.
#[derive(Clone)]
pub struct ProtobufFragmentStore {
    local: LocalFinalResult,
}

impl ProtobufFragmentStore {
    pub fn accepted_fragments(&self) -> Result<Vec<AcceptedFragment>, Error> {
        self.local.accepted_fragments()
    }
}

pub fn protobuf_fragment_store(
    staging: impl AsRef<Path>,
    job: &str,
    graph: &str,
    key: &str,
) -> Result<ProtobufFragmentStore, Error> {
    Ok(ProtobufFragmentStore {
        local: sequence_fragment_store(staging, job, graph, key, "protobuf")?,
    })
}

fn write_protobuf_record<T: prost::Message>(
    value: &T,
    output: &mut dyn std::io::Write,
) -> Result<(), Error> {
    output
        .write_all(&value.encode_length_delimited_to_vec())
        .map_err(|error| io("protobuf-fragment", error))
}

pub fn protobuf_fragment_sink<T: Element + prost::Message>(
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
        "protobuf",
        write_protobuf_record::<T>,
    )
}

/// Publish one validated Protobuf file after the runner reports DONE.
pub fn publish_protobuf_fragments<T: Element + prost::Message + Default>(
    store: &ProtobufFragmentStore,
    expected_rows: u64,
    target: impl AsRef<Path>,
) -> Result<PublishedResult, Error> {
    publish_sequence_fragments(&store.local, expected_rows, target, |path| {
        let mut rows = 0_u64;
        for row in read_protobuf_lazy::<T>(path.to_path_buf())? {
            row?;
            rows += 1;
        }
        Ok(rows)
    })
}

struct ProtobufAppendSink<T> {
    path: PathBuf,
    key: String,
    values: Vec<T>,
}

impl<T: Element + prost::Message> TypedSink<T> for ProtobufAppendSink<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        use std::io::Write;
        let mut bytes = Vec::new();
        for value in &self.values {
            value
                .encode_length_delimited(&mut bytes)
                .map_err(|error| io(&self.key, error))?;
        }
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

/// Append length-delimited Protobuf records at each bundle commit.
///
/// Replays may duplicate records. Use `protobuf_sink` for whole-job replacement.
/// This local filesystem sink requires a path shared by all workers.
pub fn protobuf_append_sink<T: Element + prost::Message>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    let path = path.as_ref().to_path_buf();
    if path
        .extension()
        .and_then(|part| part.to_str())
        .is_some_and(|part| {
            matches!(
                part.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
    {
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
        format!("protobuf-append:{}", path.display()),
        move || {
            Ok(Box::new(ProtobufAppendSink::<T> {
                path: path.clone(),
                key: name.clone(),
                values: Vec::new(),
            }))
        },
    )
}

#[cfg(feature = "parallel-io")]
pub fn protobuf_parallel_sink<T: Element + prost::Message + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    framed_parallel_sink(input, key, path, shards, write_protobuf::<T>)
}

fn count_protobuf_records<T: Element + prost::Message + Default>(
    path: &Path,
) -> Result<usize, Error> {
    count_lazy_records(path, read_protobuf_lazy::<T>)
}
fn read_protobuf_range<T: Element + prost::Message + Default>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    read_lazy_range(path, start, end, read_protobuf_lazy::<T>)
}
pub fn protobuf_sharded_source<T: Element + prost::Message + Default>(
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
        "protobuf",
        count_protobuf_records::<T>,
        read_protobuf_range::<T>,
    )
}

#[cfg(test)]
mod fragment_tests {
    use super::*;

    #[test]
    fn malformed_delimited_record_cannot_replace_existing_result() {
        #[derive(Clone, PartialEq, prost::Message)]
        struct Row {
            #[prost(int32, tag = "1")]
            value: i32,
        }
        impl Element for Row {
            const TYPE_ID: &'static str = "protobuf-test/row";
            const CODER_ID: &'static str = "protobuf-test/row-v1";
        }
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("result.pb");
        std::fs::write(&target, b"previous result").unwrap();
        let store =
            protobuf_fragment_store(dir.path().join("stage"), "job", "graph", "sink").unwrap();
        store.local.stage("bundle", 1, &[0xff]).unwrap();
        store.local.accept("bundle").unwrap();
        assert!(publish_protobuf_fragments::<Row>(&store, 1, &target).is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"previous result");
    }
}
