//! Verified, bounded-memory object input for JSON Lines.

use super::{cloud_error, expand_cloud_glob_required};
use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::{self, BufRead, Read, Write};
use tempfile::NamedTempFile;

fn hash_stream(mut reader: impl Read, mut writer: impl Write) -> io::Result<(u64, [u8; 32])> {
    let mut hash = Sha256::new();
    let mut length = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = match reader.read(&mut buffer) {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if count == 0 {
            break;
        }
        length = length
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or_else(|| io::Error::other("JSONL object length exceeds u64"))?;
        hash.update(&buffer[..count]);
        writer.write_all(&buffer[..count])?;
    }
    Ok((length, hash.finalize().into()))
}

fn stream_identity(uri: &str, length: u64, content_digest: &[u8]) -> String {
    let mut hash = Sha256::new();
    for part in [uri.as_bytes(), &length.to_le_bytes(), content_digest] {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hash.update(part);
    }
    let mut output = String::from("cloud-jsonl-stream-sha256-v1:");
    for byte in hash.finalize() {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

struct TempJsonlRows<T> {
    lines: io::Lines<io::BufReader<Box<dyn Read + Send>>>,
    line_number: usize,
    key: String,
    _temp: NamedTempFile,
    marker: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned> Iterator for TempJsonlRows<T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = self.lines.next()?;
            self.line_number += 1;
            match line {
                Ok(text) if !text.trim().is_empty() => {
                    return Some(serde_json::from_str(&text).map_err(|error| {
                        Error::io(
                            &self.key,
                            "cloud",
                            format!("line {}: {error}", self.line_number),
                        )
                    }));
                }
                Ok(_) => {}
                Err(error) => {
                    return Some(Err(Error::io(
                        &self.key,
                        "cloud",
                        format!("line {}: {error}", self.line_number),
                    )));
                }
            }
        }
    }
}

/// Read a JSONL object through its provider's sequential reader.
///
/// The object is hashed when the graph is built. At execution it is streamed
/// into a temporary file and checked before the first row is emitted. Rows
/// are then decoded on demand; dropping the source removes the temporary file.
/// The object suffix selects an enabled compression codec.
///
/// # Errors
/// Returns an error for an invalid URI, missing provider, failed read, or
/// changed object content.
pub fn cloud_jsonl_streaming_uri_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<T>, Error> {
    let location = ObjectLocation::parse(uri).map_err(|error| cloud_error(key, error))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| cloud_error(key, error))?;
    let uri = location.uri();
    let reader = provider
        .open_object(&location.bucket, &location.key)
        .map_err(|error| cloud_error(key, error))?;
    let (length, digest) =
        hash_stream(reader, io::sink()).map_err(|error| cloud_error(key, error))?;
    let expected = stream_identity(&uri, length, &digest);
    let source_key = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/cloud-jsonl-streaming-source",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        expected.clone(),
        None,
        move || {
            let reader = provider
                .open_object(&location.bucket, &location.key)
                .map_err(|error| cloud_error(&source_key, error))?;
            let mut temp = NamedTempFile::new().map_err(|error| cloud_error(&source_key, error))?;
            let (length, digest) = hash_stream(reader, temp.as_file_mut())
                .map_err(|error| cloud_error(&source_key, error))?;
            if stream_identity(&uri, length, &digest) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud JSONL object changed after graph construction",
                ));
            }
            let reader = ironbeam_compression::auto_detect_reader(
                temp.reopen()
                    .map_err(|error| cloud_error(&source_key, error))?,
                &location.key,
            )
            .map_err(|error| cloud_error(&source_key, error))?;
            Ok(Box::new(TempJsonlRows::<T> {
                lines: io::BufReader::new(reader).lines(),
                line_number: 0,
                key: source_key.clone(),
                _temp: temp,
                marker: std::marker::PhantomData,
            }) as TypedSourceIterator<T>)
        },
    )
}

/// Bind the sorted objects matching a URI glob to separate streaming sources.
/// The set of object keys and each object's content are fixed in graph identity.
///
/// # Errors
/// Returns an error for an invalid URI, empty glob, missing provider, or
/// failed object read.
pub fn cloud_jsonl_streaming_uri_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_pattern: &str,
) -> Result<PCollection<T>, Error> {
    let location = ObjectLocation::parse(uri_pattern).map_err(|error| cloud_error(key, error))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| cloud_error(key, error))?;
    let objects = expand_cloud_glob_required(&*provider, &location.bucket, &location.key)
        .map_err(|error| cloud_error(key, error))?;
    let mut sources = Vec::with_capacity(objects.len());
    for object in objects {
        let uri = format!("{}://{}/{}", location.scheme, location.bucket, object);
        let mut hash = String::new();
        for byte in Sha256::digest(object.as_bytes()) {
            write!(hash, "{byte:02x}").expect("writing to String cannot fail");
        }
        let child_key = format!("{key}/object-{hash}");
        sources.push(cloud_jsonl_streaming_uri_source::<T>(
            pipeline, &child_key, stores, &uri,
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}
