//! Verified streaming CSV input from registered object providers.

use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::path::Path;
use tempfile::{Builder, NamedTempFile};

use super::read_csv_lazy;

fn cloud_error(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "cloud", error.to_string())
}

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
            .ok_or_else(|| io::Error::other("CSV object length exceeds u64"))?;
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
    let mut output = String::from("cloud-csv-stream-sha256-v1:");
    for byte in hash.finalize() {
        write!(output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

struct TempCsvRows<T> {
    rows: TypedSourceIterator<T>,
    _temp: NamedTempFile,
}

impl<T> Iterator for TempCsvRows<T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }
}

/// Read a CSV object through its provider's sequential reader.
///
/// The source hashes the object when the graph is built. At execution it
/// spools a second read to a temporary file, verifies the hash, and decodes
/// records incrementally. The temporary file is removed when the source is
/// dropped. Its suffix selects an enabled compression codec.
///
/// # Errors
/// Returns an error for an invalid URI, missing provider, failed read, or
/// changed object content.
pub fn cloud_csv_streaming_uri_source<T: Element + DeserializeOwned>(
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
    let suffix = Path::new(&location.key)
        .extension()
        .and_then(|extension| extension.to_str())
        .map_or_else(|| ".csv".to_owned(), |extension| format!(".{extension}"));
    let source_key = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/cloud-csv-streaming-source",
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
            let (length, digest) = hash_stream(reader, temp.as_file_mut())
                .map_err(|error| cloud_error(&source_key, error))?;
            if stream_identity(&uri, length, &digest) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud CSV object changed after graph construction",
                ));
            }
            let rows = read_csv_lazy::<T>(temp.path().to_path_buf())?;
            Ok(Box::new(TempCsvRows { rows, _temp: temp }) as TypedSourceIterator<T>)
        },
    )
}

/// Bind every object under a URI prefix as a separate streaming CSV source.
/// Object keys are sorted when the graph is built; each object's bytes are
/// included in its source identity.
///
/// # Errors
/// Returns an error for an invalid URI, empty prefix, missing provider, or
/// failed object read.
pub fn cloud_csv_streaming_uri_prefix_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
) -> Result<PCollection<T>, Error> {
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
            format!("no CSV objects matched {uri_prefix}"),
        ));
    }
    let mut sources = Vec::with_capacity(uris.len());
    for uri in uris {
        let mut hash = String::new();
        for byte in Sha256::digest(uri.as_bytes()) {
            write!(hash, "{byte:02x}").expect("writing to String cannot fail");
        }
        let child_key = format!("{key}/object-{hash}");
        sources.push(cloud_csv_streaming_uri_source::<T>(
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
