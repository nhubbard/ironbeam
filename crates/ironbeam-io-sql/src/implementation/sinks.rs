struct SqlSink<T, F> {
    key: String,
    url: String,
    prefix: String,
    bind: Arc<F>,
    values: Vec<T>,
}
fn insert_rows<T, F>(
    key: &str,
    url: &str,
    prefix: &str,
    bind: &F,
    values: &[T],
) -> Result<(), Error>
where
    F: for<'q> Fn(sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>, &T) + Send + Sync,
{
    if values.is_empty() {
        return Ok(());
    }
    sqlx::any::install_default_drivers();
    let mut builder = sqlx::QueryBuilder::<sqlx::Any>::new(prefix.to_owned());
    builder.push_values(values.iter(), bind);
    let statement = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        postgres_placeholders(builder.sql().as_str())
    } else {
        builder.sql().as_str().to_owned()
    };
    let arguments = builder
        .build()
        .take_arguments()
        .map_err(|e| sql_error(key, e))?
        .unwrap_or_default();
    block_on(key, async {
        let pool = sqlx::AnyPool::connect(url)
            .await
            .map_err(|e| sql_error(key, e))?;
        sqlx::query_with::<sqlx::Any, _>(sqlx::AssertSqlSafe(statement), arguments)
            .execute(&pool)
            .await
            .map_err(|e| sql_error(key, e))?;
        Ok(())
    })?
}
impl<T, F> TypedSink<T> for SqlSink<T, F>
where
    T: Element,
    F: for<'q> Fn(sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>, &T)
        + Send
        + Sync
        + 'static,
{
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        insert_rows(
            &self.key,
            &self.url,
            &self.prefix,
            self.bind.as_ref(),
            &self.values,
        )
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}
fn postgres_placeholders(sql: &str) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut index = 0;
    for c in sql.chars() {
        if c == '?' {
            index += 1;
            output.push('$');
            output.push_str(&index.to_string());
        } else {
            output.push(c);
        }
    }
    output
}

/// Deferred, single-statement bulk insert. Its append effect is not replay safe.
pub fn sql_sink_with<T, F>(
    input: &PCollection<T>,
    key: &str,
    url: &str,
    insert_prefix: &str,
    bind: F,
) -> Result<Sink<T>, Error>
where
    T: Element,
    F: for<'q> Fn(sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>, &T)
        + Send
        + Sync
        + 'static,
{
    let sink_key = key.to_owned();
    let sink_url = url.to_owned();
    let sink_prefix = insert_prefix.to_owned();
    let bind = Arc::new(bind);
    input.sink_named(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::None,
            commit: true,
        },
        move || {
            Ok(Box::new(SqlSink::<T, F> {
                key: sink_key.clone(),
                url: sink_url.clone(),
                prefix: sink_prefix.clone(),
                bind: Arc::clone(&bind),
                values: Vec::new(),
            }))
        },
    )
}

/// Portable SQL append sink with a stable destination and binding identity.
///
/// The caller must make the SQL statement idempotent, for example by using a
/// unique key and conflict handling: a portable runner may replay a bundle.
/// Change `version` when the binding callback's behavior changes.
pub fn sql_idempotent_sink_with<T, F>(
    input: &PCollection<T>,
    key: &str,
    url: &str,
    insert_prefix: &str,
    application_id: &str,
    version: Version,
    bind: F,
) -> Result<Sink<T>, Error>
where
    T: Element,
    F: for<'q> Fn(sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>, &T)
        + Send
        + Sync
        + 'static,
{
    if application_id.is_empty() {
        return Err(Error::graph(
            key,
            "SQL sink application identity is required",
        ));
    }
    let sink_key = key.to_owned();
    let sink_url = url.to_owned();
    let sink_prefix = insert_prefix.to_owned();
    let bind = Arc::new(bind);
    input.sink_named_with_config(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::Idempotent,
            commit: true,
        },
        sink_digest(url, insert_prefix, application_id, version),
        move || {
            Ok(Box::new(SqlSink::<T, F> {
                key: sink_key.clone(),
                url: sink_url.clone(),
                prefix: sink_prefix.clone(),
                bind: Arc::clone(&bind),
                values: Vec::new(),
            }))
        },
    )
}

struct SqlParallelSink<T, F> {
    inner: SqlSink<T, F>,
    shards: usize,
}
impl<T, F> TypedSink<T> for SqlParallelSink<T, F>
where
    T: Element + Sync,
    F: for<'q> Fn(sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>, &T)
        + Send
        + Sync
        + 'static,
{
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
                            &inner.url,
                            &inner.prefix,
                            inner.bind.as_ref(),
                            chunk,
                        )
                    })
                })
                .collect::<Vec<_>>();
            for task in tasks {
                task.join().map_err(|_| {
                    Error::execution(&inner.key, "direct", "SQL writer shard panicked")
                })??;
            }
            Ok(())
        })
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.inner.abort()
    }
}

/// Deferred parallel append sink. Shards use independent connections; a
/// successful shard is not rolled back if another shard fails.
pub fn sql_parallel_sink_with<T, F>(
    input: &PCollection<T>,
    key: &str,
    url: &str,
    insert_prefix: &str,
    shards: usize,
    bind: F,
) -> Result<Sink<T>, Error>
where
    T: Element + Sync,
    F: for<'q> Fn(sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>, &T)
        + Send
        + Sync
        + 'static,
{
    if shards == 0 {
        return Err(Error::graph(
            key,
            "parallel writer needs at least one shard",
        ));
    }
    let sink_key = key.to_owned();
    let sink_url = url.to_owned();
    let sink_prefix = insert_prefix.to_owned();
    let bind = Arc::new(bind);
    input.sink_named(
        key,
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::None,
            commit: true,
        },
        move || {
            Ok(Box::new(SqlParallelSink::<T, F> {
                inner: SqlSink {
                    key: sink_key.clone(),
                    url: sink_url.clone(),
                    prefix: sink_prefix.clone(),
                    bind: Arc::clone(&bind),
                    values: Vec::new(),
                },
                shards,
            }))
        },
    )
}
