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
    open_encoded_output, open_lazy_input, read_lazy_range, sink, source,
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
    use std::io::Read;
    let mut reader = std::io::BufReader::new(open_lazy_input(&path, "protobuf")?);
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
        let mut bytes = Vec::new();
        if let Err(error) = (&mut reader)
            .take(u64::try_from(length).expect("decoded length fits u64"))
            .read_to_end(&mut bytes)
        {
            done = true;
            return Some(Err(io("protobuf", error)));
        }
        if bytes.len() != length {
            done = true;
            return Some(Err(io("protobuf", "truncated record")));
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
