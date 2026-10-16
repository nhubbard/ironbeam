use ironbeam_cloud_objects::ObjectStores;
use ironbeam_core::{Element, Error, PCollection, Pipeline};
use serde::de::DeserializeOwned;

use super::read_cbor_lazy;

/// Read concatenated CBOR records from one provider-bound object.
///
/// The source fixes the object's content identity when the graph is built.
/// Each worker open streams the object to a temporary file and rejects changed
/// content before decoding. The worker must register the same URI scheme.
///
/// # Errors
///
/// Returns an error if the URI, provider, or object cannot be read.
pub fn cloud_cbor_streaming_uri_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::provider_streaming_uri_source(
        pipeline,
        key,
        stores,
        uri,
        "ironbeam/io/cloud-cbor-streaming-source/v1",
        "cbor",
        read_cbor_lazy::<T>,
    )
}

/// Read a sorted, fixed set of CBOR objects under a provider prefix.
///
/// Each object becomes a stable source split. Workers need the provider for
/// the URI scheme and temporary disk space for the largest object.
///
/// # Errors
///
/// Returns an error for an invalid or empty prefix or an unreadable object.
pub fn cloud_cbor_streaming_uri_prefix_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
) -> Result<PCollection<T>, Error> {
    ironbeam_io::provider_streaming_uri_prefix_source(
        pipeline,
        key,
        stores,
        uri_prefix,
        "ironbeam/io/cloud-cbor-streaming-source/v1",
        "cbor",
        read_cbor_lazy::<T>,
    )
}
