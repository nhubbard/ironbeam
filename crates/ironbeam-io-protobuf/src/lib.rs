//! Protobuf file sources and deferred sinks for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{Element, Error, PCollection, Pipeline, Sink, TypedSourceIterator};
#[cfg(feature = "parallel-io")]
use ironbeam_io::framed_parallel_sink;
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
