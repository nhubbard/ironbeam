//! Parquet input through a registered object URI provider.

use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::{Read, Write};
use tempfile::NamedTempFile;

use crate::implementation::{read_parquet, read_parquet_lazy};

fn identity(uri: &str, bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    for part in [uri.as_bytes(), bytes] {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hash.update(part);
    }
    let mut digest = String::from("cloud-parquet-sha256-v1:");
    for byte in hash.finalize() {
        write!(digest, "{byte:02x}").expect("writing to String cannot fail");
    }
    digest
}

fn stream_identity(uri: &str, length: u64, content_digest: &[u8]) -> String {
    let mut hash = Sha256::new();
    for part in [uri.as_bytes(), &length.to_le_bytes(), content_digest] {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hash.update(part);
    }
    let mut digest = String::from("cloud-parquet-stream-sha256-v1:");
    for byte in hash.finalize() {
        write!(digest, "{byte:02x}").expect("writing to String cannot fail");
    }
    digest
}

fn hash_stream(
    mut reader: impl Read,
    mut writer: impl Write,
) -> Result<(u64, [u8; 32]), std::io::Error> {
    let mut content = Sha256::new();
    let mut length = 0_u64;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let count = match reader.read(&mut buffer) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if count == 0 {
            break;
        }
        length = length
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("Parquet object length exceeds u64"))?;
        content.update(&buffer[..count]);
        writer.write_all(&buffer[..count])?;
    }
    Ok((length, content.finalize().into()))
}

struct TempParquetRows<T> {
    rows: TypedSourceIterator<T>,
    _temp: NamedTempFile,
}

impl<T> Iterator for TempParquetRows<T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }
}

/// Read one Parquet object from a registered URI provider.
///
/// The URI and complete object bytes identify the source. The provider is read
/// again when execution opens it; changed bytes fail before any row is emitted.
///
/// # Errors
///
/// Returns an error for an invalid URI, missing provider, or failed object read.
pub fn cloud_parquet_uri_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<T>, Error> {
    let location =
        ObjectLocation::parse(uri).map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let uri = location.uri();
    let bytes = provider
        .get_object(&location.bucket, &location.key)
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let expected = identity(&uri, &bytes);
    let source_key = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/cloud-parquet-source",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        expected.clone(),
        None,
        move || {
            let current = provider
                .get_object(&location.bucket, &location.key)
                .map_err(|error| Error::io(&source_key, "cloud", error.to_string()))?;
            if identity(&uri, &current) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud Parquet object changed after graph construction",
                ));
            }
            let values = read_parquet::<T>(&current)?;
            Ok(Box::new(values.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
}

/// Read one Parquet object through a provider's sequential object reader.
///
/// Graph construction hashes the object through `open_object`. At execution,
/// the source streams it into an automatically removed temporary file, checks
/// the hash before emitting a row, then decodes record batches as requested.
/// The provider must implement streaming reads.
///
/// # Errors
///
/// Returns an error if the provider cannot stream the object or if reading
/// the object fails. A changed object fails recovery when the source opens.
pub fn cloud_parquet_streaming_uri_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<T>, Error> {
    let location =
        ObjectLocation::parse(uri).map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let uri = location.uri();
    let reader = provider
        .open_object(&location.bucket, &location.key)
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let (length, digest) = hash_stream(reader, std::io::sink())
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let expected = stream_identity(&uri, length, &digest);
    let source_key = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/cloud-parquet-streaming-source",
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
                .map_err(|error| Error::io(&source_key, "cloud", error.to_string()))?;
            let mut temp = NamedTempFile::new()
                .map_err(|error| Error::io(&source_key, "cloud", error.to_string()))?;
            let (length, digest) = hash_stream(reader, temp.as_file_mut())
                .map_err(|error| Error::io(&source_key, "cloud", error.to_string()))?;
            if stream_identity(&uri, length, &digest) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud Parquet object changed after graph construction",
                ));
            }
            let rows = read_parquet_lazy::<T>(temp.path().to_path_buf())?;
            Ok(Box::new(TempParquetRows { rows, _temp: temp }) as TypedSourceIterator<T>)
        },
    )
}

/// Read every Parquet object under a URI prefix.
///
/// The matching object set is fixed when the graph is built. Each object has
/// its own byte-bound source and stable key. Objects added later are not part
/// of this graph; changed or removed objects fail when execution opens them.
///
/// # Errors
///
/// Returns an error for an invalid or empty prefix, missing provider, or
/// failed object read.
type CloudParquetSource<T> =
    fn(&Pipeline, &str, &ObjectStores, &str) -> Result<PCollection<T>, Error>;

fn prefix_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
    source: CloudParquetSource<T>,
) -> Result<PCollection<T>, Error> {
    let mut locations = stores
        .list(uri_prefix)
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?
        .into_iter()
        .map(|(location, _)| location.uri())
        .collect::<Vec<_>>();
    locations.sort();
    if locations.is_empty() {
        return Err(Error::io(
            key,
            "cloud",
            format!("no Parquet objects matched {uri_prefix}"),
        ));
    }
    let mut sources = Vec::with_capacity(locations.len());
    for uri in locations {
        let mut digest = String::new();
        for byte in Sha256::digest(uri.as_bytes()) {
            write!(digest, "{byte:02x}").expect("writing to String cannot fail");
        }
        let child_key = format!("{key}/object-{digest}");
        sources.push(source(pipeline, &child_key, stores, &uri)?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}

/// Read every Parquet object under a fixed URI prefix.
///
/// The object set is selected when the graph is built. Each object has a
/// stable child key and a byte-bound source configuration.
///
/// # Errors
///
/// Returns an error if the prefix is invalid or empty, a provider is missing,
/// or an object cannot be read.
pub fn cloud_parquet_uri_prefix_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
) -> Result<PCollection<T>, Error> {
    prefix_source(pipeline, key, stores, uri_prefix, cloud_parquet_uri_source)
}

/// Stream every Parquet object under a fixed URI prefix.
///
/// This variant requires providers that implement sequential object reads.
/// Each object is spooled to an automatically removed temporary file and
/// checked before its first row is emitted.
///
/// # Errors
///
/// Returns an error if the prefix is invalid or empty, a provider cannot
/// stream an object, or an object read fails.
pub fn cloud_parquet_streaming_uri_prefix_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
) -> Result<PCollection<T>, Error> {
    prefix_source(
        pipeline,
        key,
        stores,
        uri_prefix,
        cloud_parquet_streaming_uri_source,
    )
}
