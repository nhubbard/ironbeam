use ironbeam_cloud_objects::{ObjectLocation, ObjectStores};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, SourceProgress, TypedSourceIterator,
};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use tempfile::{Builder, NamedTempFile};

fn cloud_error(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "cloud", error.to_string())
}

fn copy_and_hash(mut reader: impl Read, mut writer: impl Write) -> io::Result<(u64, [u8; 32])> {
    let mut digest = Sha256::new();
    let mut length = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
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
            .ok_or_else(|| io::Error::other("object length exceeds u64"))?;
        digest.update(&buffer[..count]);
        writer.write_all(&buffer[..count])?;
    }
    Ok((length, digest.finalize().into()))
}

fn object_identity(uri: &str, length: u64, content_digest: &[u8]) -> String {
    let mut digest = Sha256::new();
    for part in [uri.as_bytes(), &length.to_le_bytes(), content_digest] {
        digest.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        digest.update(part);
    }
    let mut identity = String::from("provider-stream-sha256-v1:");
    for byte in digest.finalize() {
        write!(identity, "{byte:02x}").expect("writing to String cannot fail");
    }
    identity
}

struct TempRows<T> {
    rows: TypedSourceIterator<T>,
    _temp: NamedTempFile,
}

impl<T> Iterator for TempRows<T> {
    type Item = Result<T, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }
}

/// Bind an object to a source that decodes a temporary file incrementally.
///
/// The object is hashed when the graph is built and streamed to a temporary
/// file on every open. A changed object fails before the decoder starts. The
/// decoder owns the file path while the temporary file remains alive.
///
/// # Errors
///
/// Returns an error for an invalid URI, missing provider, failed read, or
/// changed object content.
#[doc(hidden)]
pub fn provider_streaming_uri_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
    semantic_id: &str,
    format_name: &'static str,
    decode: fn(PathBuf) -> Result<TypedSourceIterator<T>, Error>,
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
        copy_and_hash(reader, io::sink()).map_err(|error| cloud_error(key, error))?;
    let expected = object_identity(&uri, length, &digest);
    let suffix = Path::new(&location.key)
        .extension()
        .and_then(|extension| extension.to_str())
        .map_or_else(
            || format!(".{format_name}"),
            |extension| format!(".{extension}"),
        );
    let source_key = key.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        semantic_id,
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
            let (length, digest) = copy_and_hash(reader, temp.as_file_mut())
                .map_err(|error| cloud_error(&source_key, error))?;
            if object_identity(&uri, length, &digest) != expected {
                return Err(Error::recovery(
                    &source_key,
                    format!("cloud {format_name} object changed after graph construction"),
                ));
            }
            let rows = decode(temp.path().to_path_buf())?;
            Ok(Box::new(TempRows { rows, _temp: temp }) as TypedSourceIterator<T>)
        },
    )
}

/// Bind a sorted object prefix as one collection of incremental sources.
///
/// # Errors
///
/// Returns an error for an invalid URI, empty prefix, missing provider, or
/// failed object read.
#[doc(hidden)]
pub fn provider_streaming_uri_prefix_source<T: Element>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_prefix: &str,
    semantic_id: &str,
    format_name: &'static str,
    decode: fn(PathBuf) -> Result<TypedSourceIterator<T>, Error>,
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
            format!("no {format_name} objects matched {uri_prefix}"),
        ));
    }
    let mut sources = Vec::with_capacity(uris.len());
    for uri in uris {
        let mut hash = String::new();
        for byte in Sha256::digest(uri.as_bytes()) {
            write!(hash, "{byte:02x}").expect("writing to String cannot fail");
        }
        sources.push(provider_streaming_uri_source::<T>(
            pipeline,
            &format!("{key}/object-{hash}"),
            stores,
            &uri,
            semantic_id,
            format_name,
            decode,
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}
