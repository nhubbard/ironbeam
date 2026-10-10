#![cfg(any(
    feature = "file",
    feature = "protobuf",
    feature = "tfrecord",
    feature = "cloud"
))]
#![allow(clippy::missing_errors_doc)]
//! Bounded file adapters for the v5 logical graph.
//! Sources bind their input bytes at graph construction. Final-result sinks write
//! to a sibling temporary file and publish only after the whole job succeeds.
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, SourceProgress,
    TypedSink, TypedSourceIterator,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
pub mod tfrecord_proto;
#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
pub use tfrecord_proto::{BytesList, Example, Feature, Features, FloatList, Int64List};

fn read_decoded_file(path: &Path, key: &str) -> Result<Vec<u8>, Error> {
    #[cfg(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    ))]
    {
        use std::io::Read;
        let file = fs::File::open(path).map_err(|e| io(key, e))?;
        let mut reader =
            ironbeam_cloud::compression::auto_detect_reader(file, path).map_err(|e| io(key, e))?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).map_err(|e| io(key, e))?;
        Ok(bytes)
    }
    #[cfg(not(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    )))]
    {
        fs::read(path).map_err(|e| io(key, e))
    }
}

fn compressed_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "gz" | "gzip" | "zst" | "zstd" | "bz2" | "xz"
            )
        })
}

#[doc(hidden)]
pub fn open_encoded_output(path: &Path, key: &str) -> Result<Box<dyn std::io::Write>, Error> {
    let file = fs::File::create(path).map_err(|e| io(key, e))?;
    #[cfg(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    ))]
    {
        ironbeam_cloud::compression::auto_detect_writer(file, path).map_err(|e| io(key, e))
    }
    #[cfg(not(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    )))]
    {
        Ok(Box::new(file))
    }
}

#[doc(hidden)]
pub fn open_lazy_input(path: &Path, key: &str) -> Result<Box<dyn std::io::Read + Send>, Error> {
    if compressed_extension(path) {
        Ok(Box::new(std::io::Cursor::new(read_decoded_file(
            path, key,
        )?)))
    } else {
        Ok(Box::new(fs::File::open(path).map_err(|e| io(key, e))?))
    }
}

#[cfg(any(feature = "protobuf", feature = "tfrecord"))]
trait ReadSeek: std::io::Read + std::io::Seek {}
#[cfg(any(feature = "protobuf", feature = "tfrecord"))]
impl<T: std::io::Read + std::io::Seek> ReadSeek for T {}
#[cfg(any(feature = "protobuf", feature = "tfrecord"))]
fn open_lazy_seek(path: &Path, key: &str) -> Result<(Box<dyn ReadSeek + Send>, u64), Error> {
    if compressed_extension(path) {
        let bytes = read_decoded_file(path, key)?;
        let length = u64::try_from(bytes.len()).map_err(|e| io(key, e))?;
        Ok((Box::new(std::io::Cursor::new(bytes)), length))
    } else {
        let file = fs::File::open(path).map_err(|e| io(key, e))?;
        let length = file.metadata().map_err(|e| io(key, e))?.len();
        Ok((Box::new(file), length))
    }
}

type FileWriter<T> = dyn Fn(&Path, &[T]) -> Result<(), Error> + Send + Sync;

#[cfg(feature = "cloud")]
pub use ironbeam_cloud as cloud;

#[doc(hidden)]
pub fn io(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "file", error.to_string())
}
#[doc(hidden)]
pub fn source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    format: &str,
    read: fn(&[u8]) -> Result<Vec<T>, Error>,
) -> Result<PCollection<T>, Error> {
    let path = path.as_ref().to_path_buf();
    let bytes = fs::read(&path).map_err(|e| io(key, e))?;
    let expected = Sha256::digest(&bytes);
    let hex = expected.iter().fold(String::new(), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    });
    let digest = format!("{}:{}:{}", format, path.display(), hex);
    let name = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        &format!("ironbeam/io/{format}-source"),
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        digest,
        None,
        move || {
            let current = fs::read(&path).map_err(|e| io(&name, e))?;
            if Sha256::digest(&current) != expected {
                return Err(Error::recovery(
                    &name,
                    "source contents changed after graph construction",
                ));
            }
            let decoded = read_decoded_file(&path, &name)?;
            let values = read(&decoded)?;
            Ok(Box::new(values.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
}

#[cfg(any(feature = "file", feature = "protobuf", feature = "tfrecord"))]
#[doc(hidden)]
pub fn file_hash(path: &Path, key: &str) -> Result<Vec<u8>, Error> {
    use std::io::Read;
    let mut file = fs::File::open(path).map_err(|e| io(key, e))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|e| io(key, e))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().to_vec())
}

#[cfg(any(feature = "file", feature = "protobuf", feature = "tfrecord"))]
#[doc(hidden)]
pub fn lazy_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    format: &str,
    read: fn(PathBuf) -> Result<TypedSourceIterator<T>, Error>,
) -> Result<PCollection<T>, Error> {
    let path = path.as_ref().to_path_buf();
    let expected = file_hash(&path, key)?;
    let hex = expected.iter().fold(String::new(), |mut output, byte| {
        let _ = write!(output, "{byte:02x}");
        output
    });
    let digest = format!("{format}:{}:{hex}", path.display());
    let name = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        &format!("ironbeam/io/{format}-source"),
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        digest,
        None,
        move || {
            if file_hash(&path, &name)? != expected {
                return Err(Error::recovery(
                    &name,
                    "source contents changed after graph construction",
                ));
            }
            read(path.clone())
        },
    )
}

/// Expand a local file glob in sorted order and flatten its bounded sources.
/// Each matched file receives a stable child key under `key`.
pub fn file_glob_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
    make_source: impl Fn(&Pipeline, &str, &Path) -> Result<PCollection<T>, Error>,
) -> Result<PCollection<T>, Error> {
    let mut paths = glob::glob(pattern)
        .map_err(|error| Error::graph(key, error.to_string()))?
        .map(|path| path.map_err(|error| io(key, error)))
        .collect::<Result<Vec<_>, _>>()?;
    paths.sort();
    if paths.is_empty() {
        return Err(io(key, format!("no files matched {pattern}")));
    }
    let mut sources = Vec::with_capacity(paths.len());
    for path in &paths {
        let digest = Sha256::digest(path.to_string_lossy().as_bytes());
        let path_hash = digest.iter().fold(String::new(), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        });
        let path_key = format!("{key}/file-{path_hash}");
        sources.push(make_source(pipeline, &path_key, path)?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}

#[cfg(any(feature = "file", feature = "protobuf", feature = "tfrecord"))]
#[doc(hidden)]
pub fn file_sharded_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
    records_per_shard: usize,
    format: &str,
    count: fn(&Path) -> Result<usize, Error>,
    read_range: fn(PathBuf, usize, usize) -> Result<TypedSourceIterator<T>, Error>,
) -> Result<PCollection<T>, Error> {
    if records_per_shard == 0 {
        return Err(Error::graph(key, "records per shard must be positive"));
    }
    let path = path.as_ref().to_path_buf();
    let expected = file_hash(&path, key)?;
    let total = count(&path)?;
    let shards = total.max(1).div_ceil(records_per_shard);
    let mut sources = Vec::with_capacity(shards);
    for index in 0..shards {
        let start = index * records_per_shard;
        let end = start.saturating_add(records_per_shard).min(total);
        let child_key = format!("{key}/shard-{index}");
        let digest = format!("{format}:{}:{expected:02x?}:{start}:{end}", path.display());
        let source_path = path.clone();
        let source_expected = expected.clone();
        let name = child_key.clone();
        sources.push(pipeline.bounded_source_with_semantic(
            &child_key,
            &format!("ironbeam/io/{format}-shard"),
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            digest,
            (format != "jsonl").then_some(end - start),
            move || {
                if file_hash(&source_path, &name)? != source_expected {
                    return Err(Error::recovery(
                        &name,
                        "source contents changed after graph construction",
                    ));
                }
                read_range(source_path.clone(), start, end)
            },
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}

#[cfg(any(feature = "file", feature = "protobuf", feature = "tfrecord"))]
#[doc(hidden)]
pub fn count_lazy_records<T: Element>(
    path: &Path,
    read: fn(PathBuf) -> Result<TypedSourceIterator<T>, Error>,
) -> Result<usize, Error> {
    let mut count = 0_usize;
    for value in read(path.to_path_buf())? {
        value?;
        count = count
            .checked_add(1)
            .ok_or_else(|| io("shard", "record count overflow"))?;
    }
    Ok(count)
}

#[cfg(any(feature = "file", feature = "protobuf", feature = "tfrecord"))]
#[doc(hidden)]
pub fn read_lazy_range<T: Element>(
    path: PathBuf,
    start: usize,
    end: usize,
    read: fn(PathBuf) -> Result<TypedSourceIterator<T>, Error>,
) -> Result<TypedSourceIterator<T>, Error> {
    Ok(Box::new(read(path)?.skip(start).take(end - start)))
}

#[cfg(feature = "protobuf")]
pub fn protobuf_glob_source<T: Element + prost::Message + Default>(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        protobuf_source(pipeline, key, path)
    })
}
#[cfg(feature = "tfrecord")]
pub fn tfrecord_glob_source(
    pipeline: &Pipeline,
    key: &str,
    pattern: &str,
) -> Result<PCollection<Vec<u8>>, Error> {
    file_glob_source(pipeline, key, pattern, |pipeline, key, path| {
        tfrecord_source(pipeline, key, path)
    })
}
struct FileSink<T> {
    path: PathBuf,
    key: String,
    values: Vec<T>,
    write: Arc<FileWriter<T>>,
}
impl<T: Element> TypedSink<T> for FileSink<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|e| io(&self.key, e))?;
        let extension = self
            .path
            .extension()
            .and_then(|part| part.to_str())
            .unwrap_or("");
        let suffix = if extension.is_empty() {
            String::new()
        } else {
            format!(".{extension}")
        };
        let temp = tempfile::Builder::new()
            .suffix(&suffix)
            .tempfile_in(parent)
            .map_err(|e| io(&self.key, e))?;
        (self.write)(temp.path(), &self.values)?;
        temp.as_file().sync_all().map_err(|e| io(&self.key, e))?;
        temp.persist(&self.path).map_err(|e| io(&self.key, e))?;
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}
#[doc(hidden)]
pub fn sink<T: Element>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    write: impl Fn(&Path, &[T]) -> Result<(), Error> + Send + Sync + 'static,
) -> Result<Sink<T>, Error> {
    let path = path.as_ref().to_path_buf();
    let name = key.to_owned();
    let write: Arc<FileWriter<T>> = Arc::new(write);
    input.sink_named(
        key,
        SinkSpec {
            mode: SinkMode::FinalResult,
            replay: Replay::Idempotent,
            commit: true,
        },
        move || {
            Ok(Box::new(FileSink::<T> {
                path: path.clone(),
                key: name.clone(),
                values: Vec::new(),
                write: Arc::clone(&write),
            }))
        },
    )
}

#[cfg(all(
    feature = "parallel-io",
    any(feature = "file", feature = "protobuf", feature = "tfrecord")
))]
#[doc(hidden)]
pub fn parallel_file_writer<T: Sync>(
    path: &Path,
    values: &[T],
    shards: usize,
    write_fragment: impl Fn(&Path, &[T], bool) -> Result<(), Error> + Sync,
) -> Result<(), Error> {
    use std::io::Write;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let fragments = tempfile::tempdir_in(parent).map_err(|e| io("parallel-io", e))?;
    let requested = shards.min(values.len().max(1));
    let chunk_size = values.len().max(1).div_ceil(requested);
    let count = values.len().max(1).div_ceil(chunk_size);
    let parts = (0..count)
        .map(|index| fragments.path().join(format!("part-{index}")))
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        let write_fragment = &write_fragment;
        let tasks = parts
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let start = index * chunk_size;
                let end = (start + chunk_size).min(values.len());
                scope.spawn(move || write_fragment(part, &values[start..end], index == 0))
            })
            .collect::<Vec<_>>();
        for task in tasks {
            task.join().map_err(|_| {
                Error::execution("parallel-io", "direct", "writer shard panicked")
            })??;
        }
        Ok::<(), Error>(())
    })?;
    let file = fs::File::create(path).map_err(|e| io("parallel-io", e))?;
    #[cfg(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    ))]
    let mut output: Box<dyn Write> = ironbeam_cloud::compression::auto_detect_writer(file, path)
        .map_err(|e| io("parallel-io", e))?;
    #[cfg(not(any(
        feature = "compression-gzip",
        feature = "compression-zstd",
        feature = "compression-bzip2",
        feature = "compression-xz"
    )))]
    let mut output: Box<dyn Write> = Box::new(file);
    for part in parts {
        let mut input = fs::File::open(part).map_err(|e| io("parallel-io", e))?;
        std::io::copy(&mut input, &mut output).map_err(|e| io("parallel-io", e))?;
    }
    output.flush().map_err(|e| io("parallel-io", e))
}

#[cfg(all(
    feature = "parallel-io",
    any(feature = "file", feature = "protobuf", feature = "tfrecord")
))]
#[doc(hidden)]
pub fn framed_parallel_sink<T: Element + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
    write: fn(&Path, &[T]) -> Result<(), Error>,
) -> Result<Sink<T>, Error> {
    if shards == 0 {
        return Err(Error::graph(
            key,
            "parallel writer needs at least one shard",
        ));
    }
    sink(input, key, path, move |path, values| {
        parallel_file_writer(path, values, shards, |part, chunk, _| write(part, chunk))
    })
}

#[cfg(all(feature = "parallel-io", feature = "file"))]
#[doc(hidden)]
pub fn parallel_container_writer<T: Sync>(
    path: &Path,
    values: &[T],
    shards: usize,
    extension: &str,
    write: impl Fn(&Path, &[T]) -> Result<(), Error> + Sync,
    read: fn(&[u8]) -> Result<Vec<T>, Error>,
) -> Result<(), Error> {
    if values.is_empty() {
        return write(path, values);
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let fragments = tempfile::tempdir_in(parent).map_err(|e| io("parallel-io", e))?;
    let requested = shards.min(values.len());
    let chunk_size = values.len().div_ceil(requested);
    let count = values.len().div_ceil(chunk_size);
    let parts = (0..count)
        .map(|index| fragments.path().join(format!("part-{index}.{extension}")))
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        let write = &write;
        let tasks = parts
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let start = index * chunk_size;
                let end = (start + chunk_size).min(values.len());
                scope.spawn(move || write(part, &values[start..end]))
            })
            .collect::<Vec<_>>();
        for task in tasks {
            task.join().map_err(|_| {
                Error::execution("parallel-io", "direct", "writer shard panicked")
            })??;
        }
        Ok::<(), Error>(())
    })?;
    let mut merged = Vec::with_capacity(values.len());
    for part in parts {
        let bytes = fs::read(part).map_err(|e| io("parallel-io", e))?;
        merged.extend(read(&bytes)?);
    }
    write(path, &merged)
}

#[cfg(feature = "protobuf")]
fn read_protobuf<T: prost::Message + Default>(mut bytes: &[u8]) -> Result<Vec<T>, Error> {
    let mut rows = Vec::new();
    while !bytes.is_empty() {
        rows.push(T::decode_length_delimited(&mut bytes).map_err(|e| io("protobuf", e))?);
    }
    Ok(rows)
}
#[cfg(feature = "protobuf")]
fn write_protobuf<T: prost::Message>(path: &Path, values: &[T]) -> Result<(), Error> {
    use std::io::Write;
    let mut file = open_encoded_output(path, "protobuf")?;
    for value in values {
        file.write_all(&value.encode_length_delimited_to_vec())
            .map_err(|e| io("protobuf", e))?;
    }
    Ok(())
}
#[cfg(feature = "protobuf")]
pub fn protobuf_source<T: Element + prost::Message + Default>(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, path, "protobuf", read_protobuf::<T>)
}
#[cfg(feature = "protobuf")]
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
#[cfg(feature = "protobuf")]
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
#[cfg(feature = "protobuf")]
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
#[cfg(feature = "protobuf")]
pub fn protobuf_sink<T: Element + prost::Message>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<T>, Error> {
    sink(input, key, path, write_protobuf::<T>)
}

#[cfg(all(feature = "protobuf", feature = "parallel-io"))]
pub fn protobuf_parallel_sink<T: Element + prost::Message + Sync>(
    input: &PCollection<T>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<T>, Error> {
    framed_parallel_sink(input, key, path, shards, write_protobuf::<T>)
}

#[cfg(feature = "tfrecord")]
fn masked_crc(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
        .rotate_right(15)
        .wrapping_add(0xa282_ead8)
}
#[cfg(feature = "tfrecord")]
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
#[cfg(feature = "tfrecord")]
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
#[cfg(feature = "tfrecord")]
pub fn tfrecord_source(
    pipeline: &Pipeline,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<PCollection<Vec<u8>>, Error> {
    source(pipeline, key, path, "tfrecord", read_tfrecord)
}
#[cfg(feature = "tfrecord")]
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
            .and_then(|_| reader.read_exact(&mut crc))
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
#[cfg(feature = "tfrecord")]
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
#[cfg(feature = "tfrecord")]
pub fn tfrecord_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    path: impl AsRef<Path>,
) -> Result<Sink<Vec<u8>>, Error> {
    sink(input, key, path, write_tfrecord)
}

#[cfg(all(feature = "tfrecord", feature = "parallel-io"))]
pub fn tfrecord_parallel_sink(
    input: &PCollection<Vec<u8>>,
    key: &str,
    path: impl AsRef<Path>,
    shards: usize,
) -> Result<Sink<Vec<u8>>, Error> {
    framed_parallel_sink(input, key, path, shards, write_tfrecord)
}

#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
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
#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
fn read_tfrecord_examples_lazy(path: PathBuf) -> Result<TypedSourceIterator<Example>, Error> {
    Ok(Box::new(read_tfrecord_lazy(path)?.enumerate().map(
        |(index, result)| {
            let bytes = result?;
            <Example as prost::Message>::decode(bytes.as_slice())
                .map_err(|e| io("tfrecord-examples", format!("record {}: {e}", index + 1)))
        },
    )))
}
#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
/// Read TensorFlow `tf.Example` messages from a bounded TFRecord file.
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
#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
/// Lazily decode TensorFlow `tf.Example` messages from a bounded TFRecord file.
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
#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
fn count_tfrecord_examples(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_tfrecord_examples_lazy)
}
#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
fn read_tfrecord_examples_range(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<Example>, Error> {
    read_lazy_range(path, start, end, read_tfrecord_examples_lazy)
}
#[cfg(all(feature = "tfrecord", feature = "protobuf"))]
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

#[cfg(feature = "protobuf")]
fn count_protobuf_records<T: Element + prost::Message + Default>(
    path: &Path,
) -> Result<usize, Error> {
    count_lazy_records(path, read_protobuf_lazy::<T>)
}
#[cfg(feature = "protobuf")]
fn read_protobuf_range<T: Element + prost::Message + Default>(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<T>, Error> {
    read_lazy_range(path, start, end, read_protobuf_lazy::<T>)
}
#[cfg(feature = "protobuf")]
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

#[cfg(feature = "tfrecord")]
fn count_tfrecord_records(path: &Path) -> Result<usize, Error> {
    count_lazy_records(path, read_tfrecord_lazy)
}
#[cfg(feature = "tfrecord")]
fn read_tfrecord_range(
    path: PathBuf,
    start: usize,
    end: usize,
) -> Result<TypedSourceIterator<Vec<u8>>, Error> {
    read_lazy_range(path, start, end, read_tfrecord_lazy)
}
#[cfg(feature = "tfrecord")]
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
