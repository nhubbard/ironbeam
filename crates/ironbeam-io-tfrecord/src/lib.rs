//! `TFRecord` file sources, deferred sinks, and native TensorFlow Example schema for Ironbeam 5.
#![allow(clippy::missing_errors_doc)]

use ironbeam_core::{Error, PCollection, Pipeline, Sink, TypedSourceIterator};
#[cfg(feature = "parallel-io")]
use ironbeam_io::framed_parallel_sink;
use ironbeam_io::{
    count_lazy_records, file_glob_source, file_sharded_source, io, lazy_source,
    open_encoded_output, open_lazy_seek, read_lazy_range, sink, source,
};
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
fn write_tfrecord(path: &Path, values: &[Vec<u8>]) -> Result<(), Error> {
    use std::io::Write;
    let mut file = open_encoded_output(path, "tfrecord")?;
    for value in values {
        let length = (value.len() as u64).to_le_bytes();
        file.write_all(&length).map_err(|e| io("tfrecord", e))?;
        file.write_all(&masked_crc(&length).to_le_bytes())
            .map_err(|e| io("tfrecord", e))?;
        file.write_all(value).map_err(|e| io("tfrecord", e))?;
        file.write_all(&masked_crc(value).to_le_bytes())
            .map_err(|e| io("tfrecord", e))?;
    }
    Ok(())
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
