/// Read MongoDB documents through stable caller-selected shards.
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
