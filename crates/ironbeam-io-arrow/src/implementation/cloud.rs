use ironbeam_cloud_objects::ObjectStores;
use ironbeam_core::{Element, Error, PCollection, Pipeline};
use serde::de::DeserializeOwned;

use super::{ArrowBatch, read_arrow_ipc_batches_lazy, read_arrow_ipc_lazy};

/// Decode rows incrementally from one provider-bound Arrow IPC file object.
///
/// The graph fixes the object's content identity. A worker copies and verifies
/// the object in a temporary file before opening its indexed IPC reader.
///
/// # Errors
///
/// Returns an error for an invalid URI, missing provider, or unreadable object.
pub fn cloud_arrow_ipc_streaming_uri_source<T: Element + DeserializeOwned>(
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
        "ironbeam/io/cloud-arrow-ipc-row-streaming-source/v1",
        "arrow",
        read_arrow_ipc_lazy::<T>,
    )
}

/// Decode rows from a sorted, fixed set of Arrow IPC file objects.
///
/// # Errors
///
/// Returns an error for an invalid or empty prefix or an unreadable object.
pub fn cloud_arrow_ipc_streaming_uri_prefix_source<T: Element + DeserializeOwned>(
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
        "ironbeam/io/cloud-arrow-ipc-row-streaming-source/v1",
        "arrow",
        read_arrow_ipc_lazy::<T>,
    )
}

/// Read Arrow record batches from one provider-bound IPC file object.
///
/// # Errors
///
/// Returns an error for an invalid URI, missing provider, or unreadable object.
pub fn cloud_arrow_ipc_batches_streaming_uri_source(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<ArrowBatch>, Error> {
    ironbeam_io::provider_streaming_uri_source(
        pipeline,
        key,
        stores,
        uri,
        "ironbeam/io/cloud-arrow-ipc-batch-streaming-source/v1",
        "arrow",
        read_arrow_ipc_batches_lazy,
    )
}

/// Read Arrow record batches from a sorted, fixed set of IPC file objects.
///
/// # Errors
///
/// Returns an error for an invalid or empty prefix or an unreadable object.
pub fn cloud_arrow_ipc_batches_streaming_uri_prefix_source(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
) -> Result<PCollection<ArrowBatch>, Error> {
    ironbeam_io::provider_streaming_uri_prefix_source(
        pipeline,
        key,
        stores,
        uri_prefix,
        "ironbeam/io/cloud-arrow-ipc-batch-streaming-source/v1",
        "arrow",
        read_arrow_ipc_batches_lazy,
    )
}
