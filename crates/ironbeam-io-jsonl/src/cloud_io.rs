//! Bounded graph adapters for provider-neutral object storage JSONL.
use ironbeam_cloud_objects::{CloudIOError, CloudResult, ErrorKind as CloudErrorKind, ObjectIO};
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, SourceProgress,
    TypedSink, TypedSourceIterator,
};
use regex::Regex;
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

fn write_cloud_jsonl_vec<T: Serialize, O: ObjectIO + ?Sized>(
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

fn glob_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '*' if chars.peek() == Some(&'*') => {
                chars.next();
                regex.push_str(".*");
            }
            '*' => regex.push_str("[^/]*"),
            '?' => regex.push('.'),
            '.' => regex.push_str(r"\."),
            '+' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    regex
}

fn expand_cloud_glob_required<O: ObjectIO + ?Sized>(
    storage: &O,
    bucket: &str,
    pattern: &str,
) -> CloudResult<Vec<String>> {
    let regex = Regex::new(&glob_to_regex(pattern))
        .map_err(|error| CloudIOError::new(CloudErrorKind::InvalidInput, error.to_string()))?;
    let prefix = pattern.find(['*', '?']).map_or_else(
        || Some(pattern),
        |position| (position > 0).then(|| &pattern[..position]),
    );
    let mut keys = storage
        .list_objects(bucket, prefix)?
        .into_iter()
        .filter_map(|object| regex.is_match(&object.key).then_some(object.key))
        .collect::<Vec<_>>();
    keys.sort();
    if keys.is_empty() {
        return Err(CloudIOError::new(
            CloudErrorKind::NotFound,
            format!("No objects found matching pattern '{pattern}' in bucket '{bucket}'"),
        ));
    }
    Ok(keys)
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
    let source_key = key.to_owned();
    let bucket = bucket.to_owned();
    let object = object.to_owned();
    let bytes = storage
        .get_object(&bucket, &object)
        .map_err(|e| cloud_error(key, e))?;
    let expected = digest(&bucket, &object, &bytes);
    let config_digest = expected.clone();
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
        sources.push(cloud_jsonl_source(
            pipeline,
            &child_key,
            Arc::clone(&storage),
            bucket,
            &object,
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
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
    let sink_key = key.to_owned();
    let bucket = bucket.to_owned();
    let object = object.to_owned();
    input.sink_named(
        key,
        SinkSpec {
            mode: SinkMode::FinalResult,
            replay: Replay::Idempotent,
            commit: true,
        },
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
