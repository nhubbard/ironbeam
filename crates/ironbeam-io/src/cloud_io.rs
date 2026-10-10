//! Bounded graph adapters for provider-neutral object storage JSONL.
use ironbeam_cloud::{ObjectIO, readers::write_cloud_jsonl_vec};
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
    O: ObjectIO + 'static,
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
            let reader = ironbeam_cloud::compression::auto_detect_reader(
                std::io::Cursor::new(current),
                &object,
            )
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

struct CloudJsonlSink<T, O> {
    key: String,
    storage: Arc<O>,
    bucket: String,
    object: String,
    values: Vec<T>,
}
impl<T: Element + Serialize, O: ObjectIO> TypedSink<T> for CloudJsonlSink<T, O> {
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
    O: ObjectIO + 'static,
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
