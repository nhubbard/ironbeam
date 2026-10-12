//! Bounded graph adapters for provider-neutral object storage JSONL.
use ironbeam_cloud_objects::{
    CloudIOError, CloudResult, ErrorKind as CloudErrorKind, ObjectIO, ObjectLocation, ObjectStores,
};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, SourceProgress,
    TypedSink, TypedSourceIterator,
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::io::BufRead;
use std::{fmt::Write as _, sync::Arc};

fn cloud_error(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "cloud", error.to_string())
}
fn digest(bucket: &str, object: &str, bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(bucket.as_bytes());
    hash.update([0]);
    hash.update(object.as_bytes());
    hash.update([0]);
    hash.update(bytes);
    let mut output = String::from("cloud-jsonl-sha256-v1:");
    for byte in hash.finalize() {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn configuration_digest(label: &str, parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(u64::try_from(part.len()).unwrap_or(u64::MAX).to_le_bytes());
        hash.update(part.as_bytes());
    }
    let mut output = format!("{label}:");
    for byte in hash.finalize() {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

/// Write bounded JSONL values to a provider-neutral object store.
/// The object suffix selects an enabled compression codec.
///
/// # Errors
/// Returns an object, serialization, or compression error.
pub fn write_cloud_jsonl_vec<T: Serialize, O: ObjectIO + ?Sized>(
    storage: &O,
    bucket: &str,
    object: &str,
    values: &[T],
) -> CloudResult<usize> {
    let bytes = ironbeam_compression::write_to_vec_for_path(object, |writer| {
        for value in values {
            serde_json::to_writer(&mut *writer, value)?;
            writer.write_all(b"\n")?;
        }
        Ok(())
    })
    .map_err(|error| CloudIOError::new(CloudErrorKind::InvalidInput, error.to_string()))?;
    storage.put_object(bucket, object, &bytes)?;
    Ok(values.len())
}

/// Read a bounded JSONL object into typed values.
/// Blank lines are ignored, and the object suffix selects an enabled decoder.
///
/// # Errors
/// Returns an object, decompression, or line-specific decoding error.
pub fn read_cloud_jsonl_vec<T: DeserializeOwned, O: ObjectIO + ?Sized>(
    storage: &O,
    bucket: &str,
    object: &str,
) -> CloudResult<Vec<T>> {
    let bytes = storage.get_object(bucket, object)?;
    let reader = ironbeam_compression::auto_detect_reader(std::io::Cursor::new(bytes), object)
        .map_err(|error| CloudIOError::new(CloudErrorKind::InvalidInput, error.to_string()))?;
    std::io::BufReader::new(reader)
        .lines()
        .enumerate()
        .filter_map(|(line, result)| match result {
            Ok(ref text) if text.trim().is_empty() => None,
            other => Some((line, other)),
        })
        .map(|(line, result)| {
            let text = result.map_err(|error| {
                CloudIOError::new(
                    CloudErrorKind::InvalidInput,
                    format!("{bucket}/{object} line {}: {error}", line + 1),
                )
            })?;
            serde_json::from_str(&text).map_err(|error| {
                CloudIOError::new(
                    CloudErrorKind::InvalidInput,
                    format!("{bucket}/{object} line {}: {error}", line + 1),
                )
            })
        })
        .collect()
}

fn glob_matches(pattern: &str, key: &str) -> bool {
    // Preserve the v4 glob alphabet: * stays within a path component, while
    // ** crosses separators. A question mark consumes one non-newline scalar.
    let key = key.chars().collect::<Vec<_>>();
    let mut current = vec![false; key.len() + 1];
    current[0] = true;
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        let mut next = vec![false; key.len() + 1];
        match ch {
            '*' if chars.peek() == Some(&'*') => {
                chars.next();
                next[0] = current[0];
                for index in 1..=key.len() {
                    next[index] = current[index] || (key[index - 1] != '\n' && next[index - 1]);
                }
            }
            '*' => {
                next[0] = current[0];
                for index in 1..=key.len() {
                    next[index] = current[index] || (key[index - 1] != '/' && next[index - 1]);
                }
            }
            '?' => {
                for index in 1..=key.len() {
                    next[index] = current[index - 1] && key[index - 1] != '\n';
                }
            }
            literal => {
                for index in 1..=key.len() {
                    next[index] = current[index - 1] && key[index - 1] == literal;
                }
            }
        }
        current = next;
    }
    current[key.len()]
}

/// Return sorted object keys matching the v4 compatible `*`, `**`, and `?` glob alphabet.
/// An unmatched pattern returns an empty vector.
///
/// # Errors
/// Returns the provider's listing error.
pub fn expand_cloud_glob<O: ObjectIO + ?Sized>(
    storage: &O,
    bucket: &str,
    pattern: &str,
) -> CloudResult<Vec<String>> {
    let prefix = pattern.find(['*', '?']).map_or_else(
        || Some(pattern),
        |position| (position > 0).then(|| &pattern[..position]),
    );
    let mut keys = storage
        .list_objects(bucket, prefix)?
        .into_iter()
        .filter_map(|object| glob_matches(pattern, &object.key).then_some(object.key))
        .collect::<Vec<_>>();
    keys.sort();
    Ok(keys)
}

/// Expand a cloud object glob and require at least one match.
///
/// # Errors
/// Returns a provider listing error or `NotFound` when no object matches.
pub fn expand_cloud_glob_required<O: ObjectIO + ?Sized>(
    storage: &O,
    bucket: &str,
    pattern: &str,
) -> CloudResult<Vec<String>> {
    let keys = expand_cloud_glob(storage, bucket, pattern)?;
    if keys.is_empty() {
        return Err(CloudIOError::new(
            CloudErrorKind::NotFound,
            format!("No objects found matching pattern '{pattern}' in bucket '{bucket}'"),
        ));
    }
    Ok(keys)
}

/// Read all matching JSONL objects in sorted object-key order.
/// An unmatched pattern returns an empty vector.
///
/// # Errors
/// Returns a listing, object, decompression, or line-specific decoding error.
pub fn read_cloud_jsonl_glob<T: DeserializeOwned, O: ObjectIO + ?Sized>(
    storage: &O,
    bucket: &str,
    pattern: &str,
) -> CloudResult<Vec<T>> {
    let mut rows = Vec::new();
    for object in expand_cloud_glob(storage, bucket, pattern)? {
        rows.extend(read_cloud_jsonl_vec(storage, bucket, &object)?);
    }
    Ok(rows)
}

/// Bind an object version to a bounded source. The object is read at graph
/// construction for its digest and again when the runner opens the source.
pub fn cloud_jsonl_source<T, O>(
    pipeline: &Pipeline,
    key: &str,
    storage: Arc<O>,
    bucket: &str,
    object: &str,
) -> Result<PCollection<T>, Error>
where
    T: Element + DeserializeOwned,
    O: ObjectIO + ?Sized + 'static,
{
    cloud_jsonl_source_with_location(pipeline, key, storage, bucket, object, None)
}

fn cloud_jsonl_source_with_location<T, O>(
    pipeline: &Pipeline,
    key: &str,
    storage: Arc<O>,
    bucket: &str,
    object: &str,
    location: Option<&str>,
) -> Result<PCollection<T>, Error>
where
    T: Element + DeserializeOwned,
    O: ObjectIO + ?Sized + 'static,
{
    let source_key = key.to_owned();
    let bucket = bucket.to_owned();
    let object = object.to_owned();
    let bytes = storage
        .get_object(&bucket, &object)
        .map_err(|e| cloud_error(key, e))?;
    let expected = digest(&bucket, &object, &bytes);
    let config_digest = location.map_or_else(
        || expected.clone(),
        |uri| configuration_digest("cloud-jsonl-uri-source-sha256-v1", &[uri, &expected]),
    );
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/cloud-jsonl-source",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        config_digest,
        None,
        move || {
            let current = storage
                .get_object(&bucket, &object)
                .map_err(|e| cloud_error(&source_key, e))?;
            if digest(&bucket, &object, &current) != expected {
                return Err(Error::recovery(
                    &source_key,
                    "cloud object changed after graph construction",
                ));
            }
            let reader =
                ironbeam_compression::auto_detect_reader(std::io::Cursor::new(current), &object)
                    .map_err(|e| cloud_error(&source_key, e))?;
            let values = std::io::BufReader::new(reader)
                .lines()
                .filter_map(|line| match line {
                    Ok(ref text) if text.trim().is_empty() => None,
                    _ => Some(line),
                })
                .map(|line| {
                    let line = line.map_err(|e| cloud_error(&source_key, e))?;
                    serde_json::from_str::<T>(&line).map_err(|e| cloud_error(&source_key, e))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Box::new(values.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
}

/// Read a bounded JSONL object by URI through a registered provider.
/// The URI scheme and object bytes both participate in the source identity.
///
/// # Errors
/// Returns an invalid URI, missing provider, object read, or graph error.
pub fn cloud_jsonl_uri_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<PCollection<T>, Error> {
    let location = ObjectLocation::parse(uri).map_err(|error| cloud_error(key, error))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| cloud_error(key, error))?;
    let identity = location.uri();
    cloud_jsonl_source_with_location(
        pipeline,
        key,
        provider,
        &location.bucket,
        &location.key,
        Some(&identity),
    )
}

/// Expand a provider-neutral cloud object glob into stable bounded JSONL sources.
#[allow(clippy::needless_pass_by_value)] // Public graph adapters consistently take an owned provider handle.
pub fn cloud_jsonl_glob_source<T, O>(
    pipeline: &Pipeline,
    key: &str,
    storage: Arc<O>,
    bucket: &str,
    pattern: &str,
) -> Result<PCollection<T>, Error>
where
    T: Element + DeserializeOwned,
    O: ObjectIO + ?Sized + 'static,
{
    cloud_jsonl_glob_source_with_scheme(pipeline, key, storage, bucket, pattern, None)
}

#[allow(clippy::needless_pass_by_value)] // Keeps the owned provider handle consistent with the public graph adapter.
fn cloud_jsonl_glob_source_with_scheme<T, O>(
    pipeline: &Pipeline,
    key: &str,
    storage: Arc<O>,
    bucket: &str,
    pattern: &str,
    scheme: Option<&str>,
) -> Result<PCollection<T>, Error>
where
    T: Element + DeserializeOwned,
    O: ObjectIO + ?Sized + 'static,
{
    let objects =
        expand_cloud_glob_required(&*storage, bucket, pattern).map_err(|e| cloud_error(key, e))?;
    let mut sources = Vec::with_capacity(objects.len());
    for object in objects {
        let hash = Sha256::digest(object.as_bytes());
        let object_hash = hash.iter().fold(String::new(), |mut output, byte| {
            let _ = write!(output, "{byte:02x}");
            output
        });
        let child_key = format!("{key}/object-{object_hash}");
        let identity = scheme.map(|scheme| format!("{scheme}://{bucket}/{object}"));
        sources.push(cloud_jsonl_source_with_location(
            pipeline,
            &child_key,
            Arc::clone(&storage),
            bucket,
            &object,
            identity.as_deref(),
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}

/// Expand a JSONL object glob by URI through a registered provider.
///
/// # Errors
/// Returns an invalid URI, missing provider, listing, object read, or graph error.
pub fn cloud_jsonl_uri_glob_source<T: Element + DeserializeOwned>(
    pipeline: &Pipeline,
    key: &str,
    stores: &ObjectStores,
    uri_pattern: &str,
) -> Result<PCollection<T>, Error> {
    let location = ObjectLocation::parse(uri_pattern).map_err(|error| cloud_error(key, error))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| cloud_error(key, error))?;
    cloud_jsonl_glob_source_with_scheme(
        pipeline,
        key,
        provider,
        &location.bucket,
        &location.key,
        Some(&location.scheme),
    )
}

struct CloudJsonlSink<T, O: ?Sized> {
    key: String,
    storage: Arc<O>,
    bucket: String,
    object: String,
    values: Vec<T>,
}
impl<T: Element + Serialize, O: ObjectIO + ?Sized> TypedSink<T> for CloudJsonlSink<T, O> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        write_cloud_jsonl_vec(&*self.storage, &self.bucket, &self.object, &self.values)
            .map_err(|e| cloud_error(&self.key, e))?;
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}

/// Deferred final-result object write. Repeating a successful commit overwrites
/// the same object key with the same bytes.
pub fn cloud_jsonl_sink<T, O>(
    input: &PCollection<T>,
    key: &str,
    storage: Arc<O>,
    bucket: &str,
    object: &str,
) -> Result<Sink<T>, Error>
where
    T: Element + Serialize,
    O: ObjectIO + ?Sized + 'static,
{
    let config_digest = configuration_digest(
        "cloud-jsonl-sink-sha256-v1",
        &[std::any::type_name::<O>(), bucket, object],
    );
    cloud_jsonl_sink_with_config(input, key, storage, bucket, object, config_digest)
}

fn cloud_jsonl_sink_with_config<T, O>(
    input: &PCollection<T>,
    key: &str,
    storage: Arc<O>,
    bucket: &str,
    object: &str,
    config_digest: String,
) -> Result<Sink<T>, Error>
where
    T: Element + Serialize,
    O: ObjectIO + ?Sized + 'static,
{
    let sink_key = key.to_owned();
    let bucket = bucket.to_owned();
    let object = object.to_owned();
    input.sink_named_with_config(
        key,
        SinkSpec {
            mode: SinkMode::FinalResult,
            replay: Replay::Idempotent,
            commit: true,
        },
        config_digest,
        move || {
            Ok(Box::new(CloudJsonlSink::<T, O> {
                key: sink_key.clone(),
                storage: Arc::clone(&storage),
                bucket: bucket.clone(),
                object: object.clone(),
                values: Vec::new(),
            }))
        },
    )
}

/// Write a deferred JSONL result to a URI through a registered provider.
///
/// The URI participates in the sink identity. Direct commits once per job.
/// Portable jobs should use `cloud_jsonl_fragment_sink` and publish the
/// completed result with `publish_cloud_jsonl_fragments` after runner success.
///
/// # Errors
/// Returns an invalid URI, missing provider, or graph error.
pub fn cloud_jsonl_uri_sink<T: Element + Serialize>(
    input: &PCollection<T>,
    key: &str,
    stores: &ObjectStores,
    uri: &str,
) -> Result<Sink<T>, Error> {
    let location = ObjectLocation::parse(uri).map_err(|error| cloud_error(key, error))?;
    let provider = stores
        .provider(&location.scheme)
        .map_err(|error| cloud_error(key, error))?;
    let config_digest = configuration_digest("cloud-jsonl-uri-sink-sha256-v1", &[&location.uri()]);
    cloud_jsonl_sink_with_config(
        input,
        key,
        provider,
        &location.bucket,
        &location.key,
        config_digest,
    )
}

#[cfg(test)]
mod glob_tests {
    use super::glob_matches;

    #[test]
    fn wildcard_rules_match_the_existing_cloud_glob_alphabet() {
        let cases = [
            ("data/*.jsonl", "data/a.jsonl", true),
            ("data/*.jsonl", "data/nested/a.jsonl", false),
            ("data/**/*.jsonl", "data/nested/a.jsonl", true),
            ("data/**/*.jsonl", "data/a.jsonl", false),
            ("data/?.jsonl", "data/é.jsonl", true),
            ("data/?.jsonl", "data/ab.jsonl", false),
            ("data/a[1]+(x).jsonl", "data/a[1]+(x).jsonl", true),
            ("data/a[1]+(x).jsonl", "data/a11x.jsonl", false),
            ("***", "a/b", true),
            ("**", "a\nb", false),
            ("?", "\n", false),
            ("*", "\n", true),
            ("", "", true),
            ("", "a", false),
        ];
        for (pattern, key, expected) in cases {
            assert_eq!(glob_matches(pattern, key), expected, "{pattern:?} {key:?}");
        }
    }
}
