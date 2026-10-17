use super::read_xml;
use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::Read;
use std::path::Path;

fn cloud_error(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "cloud", error.to_string())
}

fn object_identity(uri: &str, bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    for part in [uri.as_bytes(), bytes] {
        digest.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        digest.update(part);
    }
    let mut identity = String::from("cloud-xml-sha256-v1:");
    for byte in digest.finalize() {
        write!(identity, "{byte:02x}").expect("writing to String cannot fail");
    }
    identity
}

/// Bind one XML object to a bounded source and reopen it in the worker.
///
/// The URI and original object bytes identify the source. A changed object
/// fails before any XML row is returned. XML is decoded as one document, as
/// with [`crate::xml_source`]. Enabled compression codecs follow the key suffix.
///
/// # Errors
///
/// Returns an error for an invalid URI, missing provider, failed read, or
/// changed object content.
pub fn cloud_xml_uri_source<T: Element + DeserializeOwned>(
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
    let mut bytes = Vec::new();
    provider
        .open_object(&location.bucket, &location.key)
        .map_err(|error| cloud_error(key, error))?
        .read_to_end(&mut bytes)
        .map_err(|error| cloud_error(key, error))?;
    let expected = object_identity(&uri, &bytes);
    let source_key = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/cloud-xml-source",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        expected.clone(),
        None,
        move || {
            let mut bytes = Vec::new();
            provider
                .open_object(&location.bucket, &location.key)
                .map_err(|error| cloud_error(&source_key, error))?
                .read_to_end(&mut bytes)
                .map_err(|error| cloud_error(&source_key, error))?;
            if object_identity(&uri, &bytes) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud XML object changed after graph construction",
                ));
            }
            let decoded =
                ironbeam_io::decode_source_bytes(bytes, Path::new(&location.key), &source_key)?;
            let rows = read_xml::<T>(&decoded)?;
            Ok(Box::new(rows.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
}

/// Bind sorted XML objects under a URI prefix as one bounded collection.
///
/// Each object has a separate source identity. An empty prefix is an error.
///
/// # Errors
///
/// Returns an error for an invalid URI, missing provider, empty prefix, or
/// failed object read.
pub fn cloud_xml_uri_prefix_source<T: Element + DeserializeOwned>(
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
            format!("no XML objects matched {uri_prefix}"),
        ));
    }
    let mut sources = Vec::with_capacity(uris.len());
    for uri in uris {
        let mut hash = String::new();
        for byte in Sha256::digest(uri.as_bytes()) {
            write!(hash, "{byte:02x}").expect("writing to String cannot fail");
        }
        sources.push(cloud_xml_uri_source::<T>(
            pipeline,
            &format!("{key}/object-{hash}"),
            stores,
            &uri,
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}
