#![allow(clippy::missing_errors_doc)]
//! SQL adapters. A source opens only after runner validation; a sink
//! issues one bulk insert at commit. Queries and insertion prefixes are trusted
//! application SQL, as in v4.
use ironbeam_core::{
    Element, Error, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, SourceProgress,
    TypedSink, TypedSourceIterator, Version,
};
use sha2::{Digest, Sha256};
use sqlx::Execute;
use std::fmt::Write as _;
use std::future::Future;
use std::{
    collections::VecDeque,
    sync::{Arc, LazyLock},
};

static SQL_RUNTIME: LazyLock<Result<tokio::runtime::Runtime, String>> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())
});

fn runtime(key: &str) -> Result<&'static tokio::runtime::Runtime, Error> {
    SQL_RUNTIME
        .as_ref()
        .map_err(|message| Error::io(key, "sql", message))
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
                .map_err(|_| Error::execution(key, "sql", "SQL adapter task panicked"))
        }),
        Err(_) => Ok(runtime.block_on(future)),
    }
}
fn sql_error(key: &str, error: impl std::fmt::Display) -> Error {
    Error::io(key, "sql", error.to_string())
}
fn digest(url: &str, query: &str, batch_size: Option<usize>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    hasher.update([0]);
    hasher.update(query.as_bytes());
    hasher.update([0]);
    hasher.update(batch_size.unwrap_or(0).to_le_bytes());
    let mut result = String::from("sql-sha256-v1:");
    for byte in hasher.finalize() {
        let _ = write!(result, "{byte:02x}");
    }
    result
}
fn sink_digest(url: &str, insert_prefix: &str, application_id: &str, version: Version) -> String {
    let mut hasher = Sha256::new();
    for part in [url, insert_prefix, application_id] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.update(version.major.to_le_bytes());
    hasher.update(version.minor.to_le_bytes());
    let mut result = String::from("sql-sink-sha256-v1:");
    for byte in hasher.finalize() {
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

/// A bounded SQL query loaded when Direct opens the source.
pub fn sql_source<T>(
    pipeline: &Pipeline,
    key: &str,
    url: &str,
    query: &str,
) -> Result<PCollection<T>, Error>
where
    T: Element + for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Unpin,
{
    let source_key = key.to_owned();
    let source_url = url.to_owned();
    let source_query = query.to_owned();
    let config_digest = digest(url, query, None);
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/sql-source",
        source_progress(),
        config_digest,
        None,
        move || {
            sqlx::any::install_default_drivers();
            let values = block_on(&source_key, async {
                let pool = sqlx::AnyPool::connect(&source_url)
                    .await
                    .map_err(|e| sql_error(&source_key, e))?;
                sqlx::query_as::<sqlx::Any, T>(sqlx::AssertSqlSafe(source_query.clone()))
                    .fetch_all(&pool)
                    .await
                    .map_err(|e| sql_error(&source_key, e))
            })??;
            Ok(Box::new(values.into_iter().map(Ok)) as TypedSourceIterator<T>)
        },
    )
}

struct SqlPages<T> {
    key: String,
    query: String,
    pool: sqlx::AnyPool,
    page_size: usize,
    offset: u64,
    pending: VecDeque<T>,
    done: bool,
}
impl<T> Iterator for SqlPages<T>
where
    T: Element + for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Unpin,
{
    type Item = Result<T, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(value) = self.pending.pop_front() {
            return Some(Ok(value));
        }
        if self.done {
            return None;
        }
        let limit = self.page_size;
        let statement = format!(
            "SELECT * FROM ({}) AS _ironbeam_page LIMIT {limit} OFFSET {}",
            self.query, self.offset
        );
        let rows = block_on(&self.key, async {
            sqlx::query_as::<sqlx::Any, T>(sqlx::AssertSqlSafe(statement))
                .fetch_all(&self.pool)
                .await
                .map_err(|e| sql_error(&self.key, e))
        })
        .and_then(std::convert::identity);
        match rows {
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

/// Read a bounded query in `LIMIT`/`OFFSET` pages. Use a stable `ORDER BY` and
/// an unchanged database snapshot when a repeatable row sequence is required.
pub fn sql_streaming_source<T>(
    pipeline: &Pipeline,
    key: &str,
    url: &str,
    query: &str,
    rows_per_page: usize,
) -> Result<PCollection<T>, Error>
where
    T: Element + for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Unpin,
{
    let page_size = rows_per_page.max(1);
    let source_key = key.to_owned();
    let source_url = url.to_owned();
    let source_query = query.to_owned();
    let config_digest = digest(url, query, Some(page_size));
    pipeline.bounded_source_with_semantic(
        key,
        "ironbeam/io/sql-paged-source",
        source_progress(),
        config_digest,
        None,
        move || {
            sqlx::any::install_default_drivers();
            let pool = block_on(&source_key, sqlx::AnyPool::connect(&source_url))?
                .map_err(|e| sql_error(&source_key, e))?;
            Ok(Box::new(SqlPages::<T> {
                key: source_key.clone(),
                query: source_query.clone(),
                pool,
                page_size,
                offset: 0,
                pending: VecDeque::new(),
                done: false,
            }) as TypedSourceIterator<T>)
        },
    )
}

/// Build stable finite SQL range nodes without connecting during graph construction.
///
/// `query` must use a total `ORDER BY`, and `snapshot_id` must identify an
/// unchanged database snapshot across retries. A changed range length fails recovery.
pub fn sql_sharded_source<T>(
    pipeline: &Pipeline,
    key: &str,
    url: &str,
    query: &str,
    total_rows: usize,
    rows_per_shard: usize,
    snapshot_id: &str,
) -> Result<PCollection<T>, Error>
where
    T: Element + for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Unpin,
{
    if rows_per_shard == 0 || snapshot_id.is_empty() {
        return Err(Error::graph(
            key,
            "SQL shards require a positive size and snapshot ID",
        ));
    }
    let count = total_rows.max(1).div_ceil(rows_per_shard);
    let base_digest = digest(url, query, Some(rows_per_shard));
    let mut sources = Vec::with_capacity(count);
    for index in 0..count {
        let start = index * rows_per_shard;
        let end = start.saturating_add(rows_per_shard).min(total_rows);
        let name = format!("{key}/shard-{index}");
        let source_url = url.to_owned();
        let source_query = query.to_owned();
        let source_name = name.clone();
        let config_digest = format!("{base_digest}:{snapshot_id}:{start}:{end}");
        sources.push(pipeline.bounded_source_with_semantic(
            &name,
            "ironbeam/io/sql-shard-source",
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
                sqlx::any::install_default_drivers();
                let limit = end - start;
                let statement = format!(
                    "SELECT * FROM ({source_query}) AS _ironbeam_shard LIMIT {limit} OFFSET {start}"
                );
                let values = block_on(&source_name, async {
                    let pool = sqlx::AnyPool::connect(&source_url)
                        .await
                        .map_err(|e| sql_error(&source_name, e))?;
                    sqlx::query_as::<sqlx::Any, T>(sqlx::AssertSqlSafe(statement))
                        .fetch_all(&pool)
                        .await
                        .map_err(|e| sql_error(&source_name, e))
                })??;
                if values.len() != limit {
                    return Err(Error::recovery(
                        &source_name,
                        "SQL snapshot range length changed",
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
