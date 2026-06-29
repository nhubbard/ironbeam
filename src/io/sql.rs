//! SQL Database I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O**: `read_sql_vec` and `write_sql_with` (feature: `io-sql`)
//! - **Deterministic parallel writer**: `write_sql_par_with` (features: `io-sql` + `parallel-io`)
//! - **Streaming ingestion** by row ranges: [`SqlShards`], [`build_sql_shards`], `read_sql_range`
//! - **Execution runner integration**: [`SqlVecOps<T>`] implements [`VecOps`] over [`SqlShards`]
//!
//! # Feature gating
//!
//! [`SqlShards`] and [`SqlVecOps<T>`] are **always available** regardless of the `io-sql`
//! feature, so the helpers layer and runner can link unconditionally. Functions that require
//! `sqlx` types in their signature (e.g., `T: sqlx::FromRow`) are gated with
//! `#[cfg(feature = "io-sql")]` and **have no stub** — they simply do not exist when the
//! feature is off.
//!
//! [`build_sql_shards`] uses only primitive types in its signature and therefore does receive
//! a runtime stub that returns an error when `io-sql` is disabled.
//!
//! # Async bridge
//!
//! `sqlx` is fully async. Since Ironbeam is Rayon-based (synchronous), a single
//! `static SQL_RUNTIME: LazyLock<tokio::runtime::Runtime>` is created once and used to
//! `block_on` every async operation. Calling `block_on` from a Rayon thread is always safe
//! — Rayon threads are not tokio async tasks, so there is no ambient runtime to conflict.
//!
//! # Sharding strategy
//!
//! Shards are computed via:
//! 1. `SELECT COUNT(*) FROM ({query}) AS _q` — full scan to count rows.
//! 2. `SELECT * FROM ({query}) AS _q LIMIT {limit} OFFSET {offset}` per shard.
//!
//! **Limitation**: `LIMIT/OFFSET` pagination is O(N) in offset on most databases. For very
//! large tables with many shards, consider key-based pagination as a future extension.
//!
//! # Connection handling
//!
//! Each public function creates its own `AnyPool` for the duration of the call and drops it
//! on return. This avoids lifetime and ownership complexity in batch pipeline contexts.
//!
//! # Driver registration
//!
//! `sqlx::any::install_default_drivers()` is called at the start of every public SQL
//! function that opens a connection. The `Any` driver dispatches on the URL scheme at
//! runtime and silently fails with "unsupported URL scheme" if this call is omitted.

use crate::Partition;
use crate::type_token::VecOps;
use anyhow::Result;
use std::any::Any;
use std::marker::PhantomData;

// ── Always-available sharding metadata ───────────────────────────────────────

/// Streaming SQL sharding metadata.
///
/// Produced by [`build_sql_shards`] and consumed by `read_sql_range`
/// and the execution engine via [`SqlVecOps`].
///
/// All fields use primitive types so this struct compiles regardless of whether
/// the `io-sql` feature is enabled.
#[derive(Clone, Debug)]
pub struct SqlShards {
    /// Database URL (e.g. `sqlite::memory:` or `postgres://user:pass@host/db`).
    pub url: String,
    /// The user-supplied query whose results are sharded.
    pub query: String,
    /// Total number of rows in the result set.
    pub total_rows: u64,
    /// `(offset, limit)` pairs, one per shard.
    pub ranges: Vec<(u64, u64)>,
}

// ── Async runtime bridge (only compiled with the feature) ────────────────────

#[cfg(feature = "io-sql")]
#[allow(clippy::missing_panics_doc)] // panics only if the OS is out of threads
static SQL_RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect(
                "failed to create SQL I/O tokio runtime — check OS thread limits (ulimit -u)",
            )
    });

// ── Private helpers (only compiled with the feature) ─────────────────────────

/// Build [`SqlShards`] from a pre-counted total row count and a desired shard size.
#[cfg(feature = "io-sql")]
fn make_sql_shards(url: String, query: String, total: u64, rows_per_shard: usize) -> SqlShards {
    if total == 0 {
        return SqlShards {
            url,
            query,
            total_rows: 0,
            ranges: vec![],
        };
    }
    let rps = (rows_per_shard.max(1)) as u64;
    let n_shards =
        usize::try_from(total.div_ceil(rps)).expect("shard count overflow");
    let ranges = (0..n_shards)
        .map(|i| {
            let offset = i as u64 * rps;
            let limit = rps.min(total - offset);
            (offset, limit)
        })
        .collect();
    SqlShards {
        url,
        query,
        total_rows: total,
        ranges,
    }
}

/// Count rows returned by `query` by wrapping it in `SELECT COUNT(*)`.
///
/// The query string is wrapped in `AssertSqlSafe` to allow the runtime-constructed
/// SQL to pass sqlx's injection-audit gate. The caller is responsible for ensuring
/// `query` does not contain untrusted user input.
#[cfg(feature = "io-sql")]
async fn sql_count(pool: &sqlx::AnyPool, query: &str) -> Result<u64> {
    let count_query = format!("SELECT COUNT(*) FROM ({query}) AS _q");
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(count_query))
        .fetch_one(pool)
        .await
        .map_err(|e| anyhow::anyhow!("sql_count: {e}"))?;
    Ok(count.max(0) as u64)
}

/// Fetch rows `[offset, offset+limit)` from the result of `query`.
///
/// The query string is wrapped in `AssertSqlSafe` — see [`sql_count`] for the
/// injection-audit rationale.
#[cfg(feature = "io-sql")]
async fn sql_fetch_range<T>(
    pool: &sqlx::AnyPool,
    query: &str,
    offset: u64,
    limit: u64,
) -> Result<Vec<T>>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Send + Sync + Unpin,
{
    let range_query =
        format!("SELECT * FROM ({query}) AS _q LIMIT {limit} OFFSET {offset}");
    sqlx::query_as::<sqlx::Any, T>(sqlx::AssertSqlSafe(range_query))
        .fetch_all(pool)
        .await
        .map_err(|e| anyhow::anyhow!("sql_fetch_range: {e}"))
}

// ── Vector I/O (feature-gated, no stub) ──────────────────────────────────────

/// Read all rows returned by `query` against the database at `url` into a `Vec<T>`.
///
/// `T` must derive [`sqlx::FromRow`] and the database driver is selected by the URL
/// scheme (e.g. `sqlite:`, `postgres:`, `mysql:`).
///
/// # Errors
/// Returns an error if the connection fails or any row cannot be mapped to `T`.
#[cfg(feature = "io-sql")]
pub fn read_sql_vec<T>(url: &str, query: &str) -> Result<Vec<T>>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Send + Sync + Unpin,
{
    sqlx::any::install_default_drivers();
    let url_owned = url.to_owned();
    let query_owned = query.to_owned();
    SQL_RUNTIME.block_on(async move {
        let pool = sqlx::AnyPool::connect(&url_owned)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {url_owned}: {e}"))?;
        sqlx::query_as::<sqlx::Any, T>(sqlx::AssertSqlSafe(query_owned))
            .fetch_all(&pool)
            .await
            .map_err(|e| anyhow::anyhow!("read_sql_vec query: {e}"))
    })
}

/// Insert `data` into the database at `url` using a `QueryBuilder` bulk-insert.
///
/// `insert_prefix` is the opening SQL (e.g. `"INSERT INTO tbl (a, b)"`).
/// `bind_fn` is called once per row with a [`sqlx::query_builder::Separated`]
/// handle — call `.push_bind(value)` on it for each column.
///
/// Returns the number of rows reported as affected by the database.
///
/// # Errors
/// Returns an error if the connection or insert fails. Returns `Ok(0)` immediately
/// if `data` is empty.
///
/// # Feature
/// Requires `io-sql`.
#[cfg(feature = "io-sql")]
pub fn write_sql_with<T, F>(url: &str, insert_prefix: &str, data: &[T], bind_fn: F) -> Result<usize>
where
    T: Send + Sync,
    F: for<'q> Fn(
            sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>,
            &T,
        ) + Send
        + Sync,
{
    if data.is_empty() {
        return Ok(0);
    }
    sqlx::any::install_default_drivers();
    let url_owned = url.to_owned();
    let prefix_owned = insert_prefix.to_owned();
    SQL_RUNTIME.block_on(async move {
        let pool = sqlx::AnyPool::connect(&url_owned)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {url_owned}: {e}"))?;
        let mut qb = sqlx::QueryBuilder::<sqlx::Any>::new(prefix_owned);
        qb.push_values(data.iter(), &bind_fn);
        let result = qb
            .build()
            .execute(&pool)
            .await
            .map_err(|e| anyhow::anyhow!("write_sql_with execute: {e}"))?;
        Ok(result.rows_affected() as usize)
    })
}

/// Insert `data` in parallel using `shards` independent database connections.
///
/// Each shard opens its own pool and calls [`write_sql_with`] independently.
/// `bind_fn` is passed by reference so it can be shared across Rayon threads;
/// `F: Sync` is required. The total affected rows is returned.
///
/// * `shards`: if `None`, defaults to `num_cpus::get().max(2)`.
///
/// # Errors
/// Returns an error if any shard fails. If `data` is empty, returns `Ok(0)`.
///
/// # Feature
/// Requires both `io-sql` and `parallel-io`.
#[cfg(all(feature = "io-sql", feature = "parallel-io"))]
pub fn write_sql_par_with<T, F>(
    url: &str,
    insert_prefix: &str,
    data: &[T],
    bind_fn: &F,
    shards: Option<usize>,
) -> Result<usize>
where
    T: Send + Sync,
    F: for<'q> Fn(
            sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>,
            &T,
        ) + Send
        + Sync,
{
    use rayon::prelude::*;

    if data.is_empty() {
        return Ok(0);
    }
    let n = data.len();
    let requested = shards.unwrap_or_else(|| num_cpus::get().max(2));
    let actual = requested.clamp(1, n);
    let chunk = n.div_ceil(actual);

    data.par_chunks(chunk)
        .map(|chunk_data| write_sql_with(url, insert_prefix, chunk_data, bind_fn))
        .try_fold(|| 0usize, |acc, r| r.map(|n| acc + n))
        .try_reduce(|| 0usize, |a, b| Ok(a + b))
}

// ── Streaming sharding (feature-gated with stub) ──────────────────────────────

/// Build [`SqlShards`] by counting rows in `query` and slicing into `rows_per_shard`.
///
/// Requires a network round-trip to count rows. For very large result sets, choose
/// a `rows_per_shard` that keeps the shard count small to avoid many OFFSET scans.
///
/// # Errors
/// Returns an error if the connection fails or the count query fails.
/// When the `io-sql` feature is disabled, always returns an error.
#[cfg(feature = "io-sql")]
pub fn build_sql_shards(url: &str, query: &str, rows_per_shard: usize) -> Result<SqlShards> {
    sqlx::any::install_default_drivers();
    let url_owned = url.to_owned();
    let query_owned = query.to_owned();
    SQL_RUNTIME.block_on(async move {
        let pool = sqlx::AnyPool::connect(&url_owned)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {url_owned}: {e}"))?;
        let total = sql_count(&pool, &query_owned).await?;
        Ok(make_sql_shards(url_owned, query_owned, total, rows_per_shard))
    })
}

/// Stub returned when the `io-sql` feature is disabled.
///
/// # Errors
/// Always returns an error.
#[cfg(not(feature = "io-sql"))]
pub fn build_sql_shards(_url: &str, _query: &str, _rows_per_shard: usize) -> Result<SqlShards> {
    anyhow::bail!("the `io-sql` feature is not enabled")
}

// ── Range reader (feature-gated, no stub) ────────────────────────────────────

/// Read the shard described by `(offset, limit)` from the database referenced by `shards`.
///
/// # Errors
/// Returns an error if the connection or query fails.
///
/// # Feature
/// Requires `io-sql`.
#[cfg(feature = "io-sql")]
pub fn read_sql_range<T>(shards: &SqlShards, offset: u64, limit: u64) -> Result<Vec<T>>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Send + Sync + Unpin,
{
    sqlx::any::install_default_drivers();
    let url_owned = shards.url.clone();
    let query_owned = shards.query.clone();
    SQL_RUNTIME.block_on(async move {
        let pool = sqlx::AnyPool::connect(&url_owned)
            .await
            .map_err(|e| anyhow::anyhow!("connect to {url_owned}: {e}"))?;
        sql_fetch_range::<T>(&pool, &query_owned, offset, limit).await
    })
}

// ── VecOps adapter ────────────────────────────────────────────────────────────

/// `VecOps` adapter for streaming SQL reads via [`SqlShards`].
///
/// The struct and its constructor always compile. The [`VecOps`] implementation is
/// gated by `#[cfg(feature = "io-sql")]` — a disabled source can never be
/// constructed in practice, so the runner will never attempt to call these methods
/// when the feature is off.
pub struct SqlVecOps<T>(PhantomData<T>);

impl<T> SqlVecOps<T> {
    /// Construct an `Arc`-wrapped adapter.
    #[must_use]
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self(PhantomData))
    }
}

#[cfg(feature = "io-sql")]
impl<T> VecOps for SqlVecOps<T>
where
    T: for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow>
        + Send
        + Sync
        + Clone
        + Unpin
        + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        data.downcast_ref::<SqlShards>()
            .and_then(|s| usize::try_from(s.total_rows).ok())
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<SqlShards>()?;
        s.ranges
            .iter()
            .map(|&(offset, limit)| {
                let v: Vec<T> = read_sql_range(s, offset, limit).ok()?;
                Some(Box::new(v) as Partition)
            })
            .collect()
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<SqlShards>()?;
        let v: Vec<T> = read_sql_vec::<T>(&s.url, &s.query).ok()?;
        Some(Box::new(v) as Partition)
    }
}
