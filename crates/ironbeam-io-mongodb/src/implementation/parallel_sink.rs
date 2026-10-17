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
