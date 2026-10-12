#![allow(clippy::missing_errors_doc)]
//! MongoDB bounded sources and deferred append sink.
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

pub fn mongodb_sharded_source<T>(
    pipeline: &Pipeline,
    key: &str,
    spec: MongoShardSpec<'_>,
) -> Result<PCollection<T>, Error>
where
    T: Element + DeserializeOwned + Unpin + Sync,
{
    let MongoShardSpec {
        uri,
        database,
        collection,
        filter,
        total_docs,
        docs_per_shard,
        snapshot_id,
    } = spec;
    if docs_per_shard == 0 || snapshot_id.is_empty() {
        return Err(Error::graph(
            key,
            "MongoDB shards require a positive size and snapshot ID",
        ));
    }
    let base_digest = digest(uri, database, collection, &filter, Some(docs_per_shard))?;
    let count = total_docs.max(1).div_ceil(docs_per_shard);
    let mut sources = Vec::with_capacity(count);
    for index in 0..count {
        let start = index * docs_per_shard;
        let end = start.saturating_add(docs_per_shard).min(total_docs);
        let name = format!("{key}/shard-{index}");
        let source_name = name.clone();
        let source_uri = uri.to_owned();
        let source_database = database.to_owned();
        let source_collection = collection.to_owned();
        let source_filter = filter.clone();
        let config_digest = format!("{base_digest}:{snapshot_id}:{start}:{end}");
        sources.push(pipeline.bounded_source_with_semantic(
            &name,
            "ironbeam/io/mongodb-shard-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            config_digest,
            Some(end - start),
            move || {
                if start == end {
                    return Ok(Box::new(std::iter::empty()) as TypedSourceIterator<T>);
                }
                let values = block_on(&source_name, async {
                    let client = mongodb::Client::with_uri_str(&source_uri)
                        .await
                        .map_err(|e| mongo_error(&source_name, e))?;
                    let collection = client
                        .database(&source_database)
                        .collection::<T>(&source_collection);
                    let limit =
                        i64::try_from(end - start).map_err(|e| mongo_error(&source_name, e))?;
                    let offset = u64::try_from(start).map_err(|e| mongo_error(&source_name, e))?;
                    let mut cursor = collection
                        .find(source_filter.clone())
                        .sort(bson::doc! {"_id": 1})
                        .skip(offset)
                        .limit(limit)
                        .await
                        .map_err(|e| mongo_error(&source_name, e))?;
                    let mut rows = Vec::new();
                    while cursor
                        .advance()
                        .await
                        .map_err(|e| mongo_error(&source_name, e))?
                    {
                        rows.push(
                            cursor
                                .deserialize_current()
                                .map_err(|e| mongo_error(&source_name, e))?,
                        );
                    }
                    Ok::<_, Error>(rows)
                })??;
                if values.len() != end - start {
                    return Err(Error::recovery(
                        &source_name,
                        "MongoDB snapshot range length changed",
                    ));
                }
                Ok(Box::new(values.into_iter().map(Ok)) as TypedSourceIterator<T>)
            },
        )?);
    }
    let first = sources.remove(0);
    if sources.is_empty() {
        first.map_named(key, |value| value)
    } else {
        first.flatten_named(key, &sources)
    }
}

struct MongoSink<T> {
    key: String,
    uri: String,
    database: String,
    collection: String,
    values: Vec<T>,
}
fn insert_rows<T: Element + Serialize + Sync>(
    key: &str,
    uri: &str,
    database: &str,
    collection: &str,
    values: &[T],
) -> Result<(), Error> {
    if values.is_empty() {
        return Ok(());
    }
    block_on(key, async {
        let client = mongodb::Client::with_uri_str(uri)
            .await
            .map_err(|e| mongo_error(key, e))?;
        let collection = client.database(database).collection::<T>(collection);
        for batch in values.chunks(100_000) {
            collection
                .insert_many(batch)
                .await
                .map_err(|e| mongo_error(key, e))?;
        }
        Ok(())
    })?
}
impl<T: Element + Serialize + Sync> TypedSink<T> for MongoSink<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        insert_rows(
            &self.key,
            &self.uri,
            &self.database,
            &self.collection,
            &self.values,
        )
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}

/// Deferred append sink. A successful batch is not rolled back if a later batch fails.
pub fn mongodb_sink<T: Element + Serialize + Sync>(
    input: &PCollection<T>,
    key: &str,
    uri: &str,
    database: &str,
    collection: &str,
) -> Result<Sink<T>, Error> {
    let sink_key = key.to_owned();
    let uri = uri.to_owned();
    let database = database.to_owned();
    let collection = collection.to_owned();
    input.sink_named(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::None,
            commit: true,
        },
        move || {
            Ok(Box::new(MongoSink::<T> {
                key: sink_key.clone(),
                uri: uri.clone(),
                database: database.clone(),
                collection: collection.clone(),
                values: Vec::new(),
            }))
        },
    )
}

struct MongoIdempotentSink<T> {
    key: String,
    uri: String,
    database: String,
    collection: String,
    values: Vec<Document>,
    marker: std::marker::PhantomData<T>,
}

impl<T: Element + Serialize + Sync> TypedSink<T> for MongoIdempotentSink<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        let document =
            bson::serialize_to_document(&value).map_err(|error| mongo_error(&self.key, error))?;
        if document.get("_id").is_none_or(|id| *id == bson::Bson::Null) {
            return Err(Error::graph(
                &self.key,
                "idempotent MongoDB sink needs a non-null _id in every row",
            ));
        }
        self.values.push(document);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if self.values.is_empty() {
            return Ok(());
        }
        block_on(&self.key, async {
            let client = mongodb::Client::with_uri_str(&self.uri)
                .await
                .map_err(|error| mongo_error(&self.key, error))?;
            let collection = client
                .database(&self.database)
                .collection::<Document>(&self.collection);
            for value in &self.values {
                let id = value.get("_id").expect("validated at write").clone();
                collection
                    .replace_one(bson::doc! {"_id": id}, value.clone())
                    .upsert(true)
                    .await
                    .map_err(|error| mongo_error(&self.key, error))?;
            }
            Ok(())
        })?
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}

/// Portable MongoDB sink that replaces each row by its stable `_id`.
/// Replaying a bundle produces the same collection state for identical input.
pub fn mongodb_idempotent_sink<T: Element + Serialize + Sync>(
    input: &PCollection<T>,
    key: &str,
    uri: &str,
    database: &str,
    collection: &str,
) -> Result<Sink<T>, Error> {
    let sink_key = key.to_owned();
    let sink_uri = uri.to_owned();
    let sink_database = database.to_owned();
    let sink_collection = collection.to_owned();
    input.sink_named_with_config(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        sink_digest(uri, database, collection),
        move || {
            Ok(Box::new(MongoIdempotentSink::<T> {
                key: sink_key.clone(),
                uri: sink_uri.clone(),
                database: sink_database.clone(),
                collection: sink_collection.clone(),
                values: Vec::new(),
                marker: std::marker::PhantomData,
            }))
        },
    )
}

struct MongoParallelSink<T> {
    inner: MongoSink<T>,
    shards: usize,
}
impl<T: Element + Serialize + Sync> TypedSink<T> for MongoParallelSink<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.inner.write(value)
    }
    fn commit(&mut self) -> Result<(), Error> {
        if self.inner.values.is_empty() {
            return Ok(());
        }
        let chunk_size = self
            .inner
            .values
            .len()
            .div_ceil(self.shards.min(self.inner.values.len()));
        let inner = &self.inner;
        std::thread::scope(|scope| {
            let tasks = inner
                .values
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || {
                        insert_rows(
                            &inner.key,
                            &inner.uri,
                            &inner.database,
                            &inner.collection,
                            chunk,
                        )
                    })
                })
                .collect::<Vec<_>>();
            for task in tasks {
                task.join().map_err(|_| {
                    Error::execution(&inner.key, "direct", "MongoDB writer shard panicked")
                })??;
            }
            Ok(())
        })
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.inner.abort()
    }
}

/// Deferred parallel append sink. Shards use independent clients; one shard
/// can commit before another fails, so retries require application deduplication.
pub fn mongodb_parallel_sink<T: Element + Serialize + Sync>(
    input: &PCollection<T>,
    key: &str,
    uri: &str,
    database: &str,
    collection: &str,
    shards: usize,
) -> Result<Sink<T>, Error> {
    if shards == 0 {
        return Err(Error::graph(
            key,
            "parallel writer needs at least one shard",
        ));
    }
    let sink_key = key.to_owned();
    let uri = uri.to_owned();
    let database = database.to_owned();
    let collection = collection.to_owned();
    input.sink_named(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::None,
            commit: true,
        },
        move || {
            Ok(Box::new(MongoParallelSink {
                inner: MongoSink {
                    key: sink_key.clone(),
                    uri: uri.clone(),
                    database: database.clone(),
                    collection: collection.clone(),
                    values: Vec::new(),
                },
                shards,
            }))
        },
    )
}
