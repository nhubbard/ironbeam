//! Verified sequential Avro object input.

use super::prefix_source;
use crate::implementation::{read_avro_lazy, read_avro_lazy_with_schema, reader_schema_format};
use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use tempfile::{Builder, NamedTempFile};

fn stream_identity(
    uri: &str,
    length: u64,
    content_digest: &[u8],
    reader_schema: Option<&str>,
) -> String {
    let mut hash = Sha256::new();
    for part in [
        uri.as_bytes(),
        &length.to_le_bytes(),
        content_digest,
        reader_schema.unwrap_or("").as_bytes(),
    ] {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hash.update(part);
    }
    let mut digest = String::from("cloud-avro-stream-sha256-v1:");
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
            .ok_or_else(|| std::io::Error::other("Avro object length exceeds u64"))?;
        content.update(&buffer[..count]);
        writer.write_all(&buffer[..count])?;
    }
    Ok((length, content.finalize().into()))
}

struct TempAvroRows<T> {
    rows: TypedSourceIterator<T>,
    _temp: NamedTempFile,
}

impl<T> Iterator for TempAvroRows<T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }
}

pub fn streaming_source<T: Element + DeserializeOwned>(
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
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?
        .map(Arc::new);
    let uri = location.uri();
    let reader = provider
        .open_object(&location.bucket, &location.key)
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let (length, digest) = hash_stream(reader, std::io::sink())
        .map_err(|error| Error::io(key, "cloud", error.to_string()))?;
    let expected = stream_identity(&uri, length, &digest, reader_schema);
    let semantic = reader_schema.map_or_else(
        || "ironbeam/io/cloud-avro-streaming-source".to_owned(),
        |raw| {
            format!(
                "ironbeam/io/cloud-{}-streaming-source",
                reader_schema_format(raw)
            )
        },
    );
    let suffix = Path::new(&location.key)
        .extension()
        .and_then(|extension| extension.to_str())
        .map_or_else(|| ".avro".to_owned(), |extension| format!(".{extension}"));
    let source_key = key.to_owned();
    let reader_schema = reader_schema.map(str::to_owned);
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
            let reader = provider
                .open_object(&location.bucket, &location.key)
                .map_err(|error| Error::io(&source_key, "cloud", error.to_string()))?;
            let mut temp = Builder::new()
                .suffix(&suffix)
                .tempfile()
                .map_err(|error| Error::io(&source_key, "cloud", error.to_string()))?;
            let (length, digest) = hash_stream(reader, temp.as_file_mut())
                .map_err(|error| Error::io(&source_key, "cloud", error.to_string()))?;
            if stream_identity(&uri, length, &digest, reader_schema.as_deref()) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud Avro object changed after graph construction",
                ));
            }
            let rows = if let Some(schema) = &schema {
                read_avro_lazy_with_schema::<T>(temp.path().to_path_buf(), Arc::clone(schema))?
            } else {
                read_avro_lazy::<T>(temp.path().to_path_buf())?
            };
            Ok(Box::new(TempAvroRows { rows, _temp: temp }) as TypedSourceIterator<T>)
        },
    )
}

/// Read an Avro object through a provider's sequential reader.
///
/// The object is hashed at graph construction. At execution it is spooled to
/// temporary storage and checked before the first row is emitted. Avro rows
/// are then decoded incrementally; the temporary file is removed on drop.
pub fn cloud_avro_streaming_uri_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<T>, Error> {
    streaming_source(pipeline, key, stores, uri, None)
}

/// Stream an Avro object through a supplied reader schema.
/// The schema also contributes to graph identity.
pub fn cloud_avro_streaming_uri_source_with_reader_schema<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
    reader_schema: impl AsRef<str>,
) -> Result<PCollection<T>, Error> {
    streaming_source(pipeline, key, stores, uri, Some(reader_schema.as_ref()))
}

/// Stream a fixed set of Avro objects under a URI prefix.
/// Each object is bound to the graph by its URI and bytes, and is checked
/// again before it emits records.
pub fn cloud_avro_streaming_uri_prefix_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
) -> Result<PCollection<T>, Error> {
    prefix_source(pipeline, key, stores, uri_prefix, None, true)
}

/// Stream a fixed Avro object set through a supplied reader schema.
pub fn cloud_avro_streaming_uri_prefix_source_with_reader_schema<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
    reader_schema: impl AsRef<str>,
) -> Result<PCollection<T>, Error> {
    prefix_source(
        pipeline,
        key,
        stores,
        uri_prefix,
        Some(reader_schema.as_ref()),
        true,
    )
}
