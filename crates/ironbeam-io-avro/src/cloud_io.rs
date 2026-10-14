//! Native Avro containers stored behind a provider-neutral object URI.

use crate::implementation::reader_schema_format;
use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::sync::Arc;

fn identity(uri: &str, bytes: &[u8], reader_schema: Option<&str>) -> String {
    let mut hash = Sha256::new();
    for part in [
        uri.as_bytes(),
        bytes,
        reader_schema.unwrap_or("").as_bytes(),
    ] {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hash.update(part);
    }
    let mut digest = String::from("cloud-avro-sha256-v1:");
    for byte in hash.finalize() {
        use std::fmt::Write as _;
        write!(digest, "{byte:02x}").expect("writing to String cannot fail");
    }
    digest
}

fn decode<T: DeserializeOwned>(
    bytes: &[u8],
    reader_schema: Option<&apache_avro::Schema>,
) -> Result<Vec<T>, Error> {
    let reader = if let Some(schema) = reader_schema {
        apache_avro::Reader::builder(bytes)
            .reader_schema(schema)
            .build()
            .map_err(|error| Error::io("avro", "cloud", error.to_string()))?
    } else {
        apache_avro::Reader::new(bytes)
            .map_err(|error| Error::io("avro", "cloud", error.to_string()))?
    };
    reader
        .map(|row| {
            let value = row.map_err(|error| Error::io("avro", "cloud", error.to_string()))?;
            apache_avro::from_value(&value)
                .map_err(|error| Error::io("avro", "cloud", error.to_string()))
        })
        .collect()
}

fn source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
    reader_schema: Option<&str>,
) -> Result<PCollection<T>, Error> {
    let location =
        ObjectLocation::parse(uri).map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let schema = reader_schema
        .map(apache_avro::Schema::parse_str)
        .transpose()
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let uri = location.uri();
    let bytes = provider
        .get_object(&location.bucket, &location.key)
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let expected = identity(&uri, &bytes, reader_schema);
    let semantic = reader_schema.map_or_else(
        || "ironbeam/io/cloud-avro-source".to_owned(),
        |raw| format!("ironbeam/io/cloud-{}-source", reader_schema_format(raw)),
    );
    let source_key = key.to_owned();
    let reader_schema = reader_schema.map(str::to_owned);
    let schema = Arc::new(schema);
    pipeline.bounded_source_with_semantic(
        key,
        &semantic,
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
            if identity(&uri, &current, reader_schema.as_deref()) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud Avro object changed after graph construction",
                ));
            }
            let values = decode::<T>(&current, schema.as_ref().as_ref())?;
            Ok(Box::new(values.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
}

/// Read a native Avro object through a registered URI provider.
///
/// The URI and object bytes are bound to source configuration. The object is
/// read again when the source opens; changed bytes fail recovery validation.
pub fn cloud_avro_uri_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, stores, uri, None)
}

/// Read a native Avro object using its embedded writer schema and a supplied
/// reader schema for projections and defaulted fields.
///
/// The reader schema also becomes part of the graph fingerprint. Invalid
/// schemas fail while the graph is built.
pub fn cloud_avro_uri_source_with_reader_schema<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
    reader_schema: impl AsRef<str>,
) -> Result<PCollection<T>, Error> {
    source(pipeline, key, stores, uri, Some(reader_schema.as_ref()))
}
