//! Parquet input through a registered object URI provider.

use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

use crate::implementation::read_parquet;

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
