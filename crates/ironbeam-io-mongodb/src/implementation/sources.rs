use bson::Document;
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, SourceProgress,
    TypedSink, TypedSourceIterator,
};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{collections::VecDeque, fmt::Write as _, future::Future, sync::LazyLock};

static MONGO_RUNTIME: LazyLock<Result<tokio::runtime::Runtime, String>> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())
});
fn runtime(key: &str) -> Result<&'static tokio::runtime::Runtime, Error> {
    MONGO_RUNTIME
        .as_ref()
        .map_err(|message| Error::io(key, "mongodb", message))
}
fn block_on<T: Send>(key: &str, future: impl Future<Output = T> + Send) -> Result<T, Error> {
    let runtime = runtime(key)?;
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            Ok(tokio::task::block_in_place(|| runtime.block_on(future)))
        }
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(move || runtime.block_on(future))
                .join()
                .map_err(|_| Error::execution(key, "mongodb", "MongoDB adapter task panicked"))
        }),
        Err(_) => Ok(runtime.block_on(future)),
    }
}
fn mongo_error(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "mongodb", error.to_string())
}
fn digest(
    uri: &str,
    database: &str,
    collection: &str,
    filter: &Document,
    page_size: Option<usize>,
) -> Result<String, Error> {
    let mut hash = Sha256::new();
    for part in [uri, database, collection] {
        hash.update(part.as_bytes());
        hash.update([0]);
    }
    hash.update(filter.to_vec().map_err(|e| mongo_error("filter", e))?);
    hash.update(page_size.unwrap_or(0).to_le_bytes());
    let mut result = String::from("mongodb-sha256-v1:");
    for byte in hash.finalize() {
        let _ = write!(result, "{byte:02x}");
    }
    Ok(result)
}
fn sink_digest(uri: &str, database: &str, collection: &str) -> String {
    let mut hash = Sha256::new();
    for part in ["mongodb-idempotent-upsert-v1", uri, database, collection] {
        hash.update((part.len() as u64).to_le_bytes());
        hash.update(part.as_bytes());
    }
    let mut result = String::from("mongodb-sink-sha256-v1:");
    for byte in hash.finalize() {
        let _ = write!(result, "{byte:02x}");
    }
    result
}
const fn source_progress() -> SourceProgress {
    SourceProgress {
        stable_split_id: false,
        replay: Replay::AtLeastOnce,
        reports_event_time: false,
    }
}

/// A bounded `find` loaded when Direct opens the source.
pub fn mongodb_source<T>(
    pipeline: &Pipeline,
    key: &str,
    uri: &str,
    database: &str,
    collection: &str,
    filter: Document,
) -> Result<PCollection<T>, Error>
where
    T: Element + DeserializeOwned + Unpin + Sync,
{
    let config_digest = digest(uri, database, collection, &filter, None)?;
    let source_key = key.to_owned();
    let uri = uri.to_owned();
    let database = database.to_owned();
    let collection = collection.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/mongodb-source",
        source_progress(),
        config_digest,
        None,
        move || {
            let rows = block_on(&source_key, async {
                let client = mongodb::Client::with_uri_str(&uri)
                    .await
                    .map_err(|e| mongo_error(&source_key, e))?;
                let collection = client.database(&database).collection::<T>(&collection);
                let mut cursor = collection
                    .find(filter.clone())
                    .await
                    .map_err(|e| mongo_error(&source_key, e))?;
                let mut rows = Vec::new();
                while cursor
                    .advance()
                    .await
                    .map_err(|e| mongo_error(&source_key, e))?
                {
                    rows.push(
                        cursor
                            .deserialize_current()
                            .map_err(|e| mongo_error(&source_key, e))?,
                    );
                }
                Ok::<_, Error>(rows)
            })??;
            Ok(Box::new(rows.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
}

struct MongoPages<T> {
    key: String,
    client: mongodb::Client,
    database: String,
    collection: String,
    filter: Document,
    page_size: usize,
    offset: u64,
    pending: VecDeque<T>,
    done: bool,
}
impl<T: Element + DeserializeOwned + Unpin + Sync> Iterator for MongoPages<T> {
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(value) = self.pending.pop_front() {
            return Some(Ok(value));
        }
        if self.done {
            return None;
        }
        let page = block_on(&self.key, async {
            let collection = self
                .client
                .database(&self.database)
                .collection::<T>(&self.collection);
            let limit = i64::try_from(self.page_size).map_err(|e| mongo_error(&self.key, e))?;
            let mut cursor = collection
                .find(self.filter.clone())
                .skip(self.offset)
                .limit(limit)
                .await
                .map_err(|e| mongo_error(&self.key, e))?;
            let mut rows = Vec::new();
            while cursor
                .advance()
                .await
                .map_err(|e| mongo_error(&self.key, e))?
            {
                rows.push(
                    cursor
                        .deserialize_current()
                        .map_err(|e| mongo_error(&self.key, e))?,
                );
            }
            Ok::<_, Error>(rows)
        })
        .and_then(std::convert::identity);
        match page {
            Ok(rows) => {
                self.done = rows.len() < self.page_size;
                self.offset = self.offset.saturating_add(rows.len() as u64);
                self.pending = rows.into();
                self.pending.pop_front().map(Ok)
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
    }
}

/// Bounded `find` in `skip`/`limit` pages. A changing collection may shift
/// page boundaries; use a stable snapshot when repeatability matters.
pub fn mongodb_streaming_source<T>(
    pipeline: &Pipeline,
    key: &str,
    uri: &str,
    database: &str,
    collection: &str,
    filter: Document,
    docs_per_page: usize,
) -> Result<PCollection<T>, Error>
where
    T: Element + DeserializeOwned + Unpin + Sync,
{
    let page_size = docs_per_page.max(1);
    let config_digest = digest(uri, database, collection, &filter, Some(page_size))?;
    let source_key = key.to_owned();
    let uri = uri.to_owned();
    let database = database.to_owned();
    let collection = collection.to_owned();
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/mongodb-paged-source",
        source_progress(),
        config_digest,
        None,
        move || {
            let client = block_on(&source_key, mongodb::Client::with_uri_str(&uri))?
                .map_err(|e| mongo_error(&source_key, e))?;
            Ok(Box::new(MongoPages::<T> {
                key: source_key.clone(),
                client,
                database: database.clone(),
                collection: collection.clone(),
                filter: filter.clone(),
                page_size,
                offset: 0,
                pending: VecDeque::new(),
                done: false,
            }) as TypedSourceIterator<T>)
        },
    )
}

/// Build stable finite MongoDB range nodes without connecting during graph
/// construction. Ranges sort by `_id`; the caller's `snapshot_id` must identify
/// an unchanged collection snapshot across retries.
pub struct MongoShardSpec<'a> {
    pub uri: &'a str,
    pub database: &'a str,
    pub collection: &'a str,
    pub filter: Document,
    pub total_docs: usize,
    pub docs_per_shard: usize,
    pub snapshot_id: &'a str,
}
