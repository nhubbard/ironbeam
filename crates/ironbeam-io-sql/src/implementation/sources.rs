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
