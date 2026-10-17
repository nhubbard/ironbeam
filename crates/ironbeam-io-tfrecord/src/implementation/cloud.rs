//! Verified streaming `TFRecord` input from registered object providers.

use super::read_tfrecord_lazy;
use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::path::Path;
use tempfile::{Builder, NamedTempFile};

fn cloud_error(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "cloud", error.to_string())
}

fn digest_stream(mut reader: impl Read, mut writer: impl Write) -> io::Result<(u64, [u8; 32])> {
    let mut digest = Sha256::new();
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
            .ok_or_else(|| io::Error::other("TFRecord object length exceeds u64"))?;
        digest.update(&buffer[..count]);
        writer.write_all(&buffer[..count])?;
    }
    Ok((length, digest.finalize().into()))
}

fn stream_identity(uri: &str, length: u64, content_digest: &[u8]) -> String {
    let mut digest = Sha256::new();
    for part in [uri.as_bytes(), &length.to_le_bytes(), content_digest] {
        digest.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        digest.update(part);
    }
    let mut output = String::from("cloud-tfrecord-stream-sha256-v1:");
    for byte in digest.finalize() {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

struct TempRecords {
    records: TypedSourceIterator<Vec<u8>>,
    _temp: NamedTempFile,
}

impl Iterator for TempRecords {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.records.next()
    }
}

/// Read a `TFRecord` object through its provider's sequential reader.
///
/// The source hashes the object when the graph is built. At execution it
/// streams a second read to a temporary file, checks the hash, and decodes
/// frames incrementally. Dropping the source removes the temporary file. The
/// object suffix selects an enabled compression codec.
///
/// # Errors
/// Returns an error for an invalid URI, missing provider, failed read, or
/// changed object content.
pub fn cloud_tfrecord_streaming_uri_source(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<Vec<u8>>, Error> {
    let location = ObjectLocation::parse(uri).map_err(|error| cloud_error(key, error))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| cloud_error(key, error))?;
    let uri = location.uri();
    let reader = provider
        .open_object(&location.bucket, &location.key)
        .map_err(|error| cloud_error(key, error))?;
    let (length, digest) =
        digest_stream(reader, io::sink()).map_err(|error| cloud_error(key, error))?;
    let expected = stream_identity(&uri, length, &digest);
    let suffix = Path::new(&location.key)
        .extension()
        .and_then(|extension| extension.to_str())
        .map_or_else(
            || ".tfrecord".to_owned(),
            |extension| format!(".{extension}"),
        );
    let source_key = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/cloud-tfrecord-streaming-source",
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
            let mut temp = Builder::new()
                .suffix(&suffix)
                .tempfile()
                .map_err(|error| cloud_error(&source_key, error))?;
            let (length, digest) = digest_stream(reader, temp.as_file_mut())
                .map_err(|error| cloud_error(&source_key, error))?;
            if stream_identity(&uri, length, &digest) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud TFRecord object changed after graph construction",
                ));
            }
            let records = read_tfrecord_lazy(temp.path().to_path_buf())?;
            Ok(Box::new(TempRecords {
                records,
                _temp: temp,
            }) as TypedSourceIterator<Vec<u8>>)
        },
    )
}

/// Bind every object under a URI prefix as a separate `TFRecord` source.
/// Object keys are sorted when the graph is built; each object's bytes are
/// included in its source identity.
///
/// # Errors
/// Returns an error for an invalid URI, empty prefix, missing provider, or
/// failed object read.
pub fn cloud_tfrecord_streaming_uri_prefix_source(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
) -> Result<PCollection<Vec<u8>>, Error> {
    let mut uris = stores
        .list(uri_prefix)
        .map_err(|error| cloud_error(key, error))?
        .into_iter()
        .map(|(location, _)| location.uri())
        .collect::<Vec<_>>();
    uris.sort();
    if uris.is_empty() {
        return Err(cloud_error(
            key,
            format!("no TFRecord objects matched {uri_prefix}"),
        ));
    }
    let mut sources = Vec::with_capacity(uris.len());
    for uri in uris {
        let mut hash = String::new();
        for byte in Sha256::digest(uri.as_bytes()) {
            write!(hash, "{byte:02x}").expect("writing to String cannot fail");
        }
        let child_key = format!("{key}/object-{hash}");
        sources.push(cloud_tfrecord_streaming_uri_source(
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
