use ironbeam_core::{
    Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, TypedSink, TypedSourceIterator,
};
use ironbeam_io::final_result::{LocalFinalResult, PublishedResult};
#[cfg(feature = "parallel-io")]
use ironbeam_io::framed_parallel_sink;
use ironbeam_io::{
    count_lazy_records, file_glob_source, file_sharded_source, io, lazy_source,
    open_encoded_output, open_lazy_input, read_lazy_range, sink, source,
};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::Example;
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
    use std::io::Read;
    let mut reader = std::io::BufReader::new(open_lazy_input(&path, "tfrecord")?);
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
        let mut payload = Vec::new();
        if let Err(error) = (&mut reader)
            .take(u64::try_from(length).expect("decoded length fits u64"))
            .read_to_end(&mut payload)
        {
            done = true;
            return Some(Err(io("tfrecord", error)));
        }
        if payload.len() != length {
            done = true;
            return Some(Err(io("tfrecord", "truncated record")));
        }
        let mut crc = [0_u8; 4];
        if let Err(error) = reader.read_exact(&mut crc) {
            done = true;
            return Some(Err(io("tfrecord", format!("truncated record: {error}"))));
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
