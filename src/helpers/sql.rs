//! SQL database sources and sinks for [`PCollection`].
//!
//! This module provides typed, `sqlx`-backed SQL I/O that integrates with the Ironbeam
//! pipeline. You can either:
//!
//! - **Vector I/O** — eagerly run a query and load every row into memory:
//!   - [`read_sql`] -> `PCollection<T>`
//!   - [`PCollection::write_sql_with`]
//!   - [`PCollection::write_sql_par_with`] (feature: `parallel-io`)
//!
//! - **Streaming I/O** — build a source that shards a query's result set by row count
//!   (via `LIMIT`/`OFFSET`) and reads each shard lazily in the runner:
//!   - [`read_sql_streaming`] -> `PCollection<T>`
//!
//! Read types must implement `sqlx::FromRow` (typically via `#[derive(sqlx::FromRow)]`).
//! Writes use a `bind_fn` closure that binds each row's columns onto a
//! [`sqlx::query_builder::Separated`] handle — this keeps the write path generic over
//! any table shape without requiring a derive macro.
//!
//! ## Feature flags
//! - `io-sql`: enables SQL helpers. This connector is **not** part of the default
//!   feature set; opt in explicitly to avoid pulling in `sqlx` and `tokio`.
//! - `parallel-io`: enables the parallel writer ([`PCollection::write_sql_par_with`]).
//!
//! ## Note on glob expansion
//! Unlike file-based sources, SQL sources read from a live database connection —
//! there is no filesystem glob to expand. `read_sql` and `read_sql_streaming` always
//! treat `query` as a single SQL statement.
//!
//! ## Examples
//! Read a table with SQLite, transform it, and write it back to a new table:
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! #[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
//! struct Row { id: i64, name: String }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let rows = read_sql::<Row>(&p, "sqlite::memory:", "SELECT id, name FROM people")?;
//! let renamed = rows.map(|r: &Row| Row { id: r.id, name: r.name.to_uppercase() });
//! renamed.write_sql_with(
//!     "sqlite::memory:",
//!     "INSERT INTO people_out (id, name)",
//!     |mut sep, row: &Row| {
//!         sep.push_bind(row.id).push_bind(row.name.clone());
//!     },
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! Streaming read shard-by-shard (useful for large result sets):
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! #[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
//! struct Row { id: i64, name: String }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let stream = read_sql_streaming::<Row>(&p, "sqlite::memory:", "SELECT id, name FROM people", 10_000)?;
//! let out = stream.collect_seq()?;
//! # Ok(())
//! # }
//! ```

use crate::io::sql::{SqlShards, SqlVecOps, build_sql_shards};
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{Element, PCollection, Pipeline, from_vec};
use anyhow::Result;
use std::marker::PhantomData;
use std::sync::Arc;

/// Run `query` against the database at `url` and collect every row into a
/// `PCollection<T>` (vector mode).
///
/// This eagerly executes the query and loads the entire result set into memory.
/// For very large result sets, prefer [`read_sql_streaming`].
///
/// SQL sources are **not** glob-expanded — `query` is always run as a single
/// statement against a single connection.
///
/// *Enabled when the `io-sql` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `url`: Database connection URL (e.g. `sqlite::memory:`, `postgres://...`).
/// - `query`: SQL query whose result set becomes the collection's elements.
///
/// # Errors
/// Returns an error if the connection fails or any row cannot be mapped to `T`.
///
/// # Examples
/// ```no_run
/// use ironbeam::*;
/// use anyhow::Result;
///
/// #[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
/// struct Row { id: i64, name: String }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let rows = read_sql::<Row>(&p, "sqlite::memory:", "SELECT id, name FROM people")?;
/// let out = rows.collect_seq()?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "io-sql")]
pub fn read_sql<T>(p: &Pipeline, url: &str, query: &str) -> Result<PCollection<T>>
where
    T: Element + for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Send + Sync + Unpin,
{
    let v = crate::io::sql::read_sql_vec::<T>(url, query)?;
    Ok(from_vec(p, v))
}

/// Create a **streaming** SQL source, sharded by a fixed number of rows.
///
/// This builds a [`SqlShards`] descriptor (counting rows up front via
/// `SELECT COUNT(*)`) and inserts a `Source` node that runs a `LIMIT`/`OFFSET`
/// query to read only its shard when executed by the runner. Useful for result
/// sets that don't fit comfortably in memory.
///
/// *Enabled when the `io-sql` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `url`: Database connection URL.
/// - `query`: SQL query whose result set is sharded.
/// - `rows_per_shard`: Target number of rows per shard (minimum 1).
///
/// # Errors
/// Returns an error if the connection fails or the count query fails.
///
/// # Example
/// ```no_run
/// use ironbeam::*;
/// use anyhow::Result;
///
/// #[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
/// struct Row { id: i64, name: String }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let stream = read_sql_streaming::<Row>(&p, "sqlite::memory:", "SELECT id, name FROM people", 10_000)?;
/// let out = stream.collect_seq()?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "io-sql")]
pub fn read_sql_streaming<T>(
    p: &Pipeline,
    url: &str,
    query: &str,
    rows_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: Element + for<'r> sqlx::FromRow<'r, sqlx::any::AnyRow> + Send + Sync + Unpin,
{
    let shards: SqlShards = build_sql_shards(url, query, rows_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: SqlVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    p.set_coder::<T>(id);
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}

#[cfg(feature = "io-sql")]
impl<T: Element + Send + Sync> PCollection<T> {
    /// Execute the collection and bulk-insert it into the database at `url`
    /// (sequential).
    ///
    /// The entire collection is first collected into memory (sequentially) to
    /// preserve deterministic ordering, then inserted via a single
    /// `QueryBuilder` bulk-insert built from `insert_prefix` and `bind_fn`.
    ///
    /// `insert_prefix` is the opening SQL (e.g. `"INSERT INTO tbl (a, b)"`).
    /// `bind_fn` is called once per row to bind column values via the
    /// [`sqlx::query_builder::Separated`] handle.
    ///
    /// Returns the number of rows reported as affected by the database.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// #[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
    /// struct Row { id: i64, name: String }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let rows = from_vec(&p, vec![Row { id: 1, name: "a".into() }]);
    /// let n = rows.write_sql_with(
    ///     "sqlite::memory:",
    ///     "INSERT INTO people (id, name)",
    ///     |mut sep, row: &Row| {
    ///         sep.push_bind(row.id).push_bind(row.name.clone());
    ///     },
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Propagates connection and insert errors.
    pub fn write_sql_with<F>(self, url: &str, insert_prefix: &str, bind_fn: F) -> Result<usize>
    where
        F: for<'q> Fn(sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>, &T)
            + Send
            + Sync,
    {
        let rows = self.collect_seq()?;
        crate::io::sql::write_sql_with(url, insert_prefix, &rows, bind_fn)
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "parallel-io")))]
#[cfg(all(feature = "io-sql", feature = "parallel-io"))]
impl<T: Element + Send + Sync> PCollection<T> {
    /// Execute the collection sequentially (to lock in a deterministic order),
    /// then bulk-insert it into the database **in parallel** using independent
    /// connections per shard.
    ///
    /// `shards = Some(n)` sets the number of writer shards; `None` uses a
    /// sensible default (`num_cpus::get().max(2)`).
    ///
    /// Returns the total number of rows reported as affected across all shards.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// #[derive(Clone, Debug, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
    /// struct Row { id: i64, name: String }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let rows = from_vec(&p, vec![Row { id: 1, name: "a".into() }]);
    /// rows.write_sql_par_with(
    ///     "sqlite::memory:",
    ///     "INSERT INTO people (id, name)",
    ///     |mut sep, row: &Row| {
    ///         sep.push_bind(row.id).push_bind(row.name.clone());
    ///     },
    ///     Some(4),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Propagates connection and insert errors from any shard.
    pub fn write_sql_par_with<F>(
        self,
        url: &str,
        insert_prefix: &str,
        bind_fn: F,
        shards: Option<usize>,
    ) -> Result<usize>
    where
        F: for<'q> Fn(sqlx::query_builder::Separated<'q, sqlx::Any, &'static str>, &T)
            + Send
            + Sync,
    {
        let data = self.collect_seq()?;
        crate::io::sql::write_sql_par_with(url, insert_prefix, &data, &bind_fn, shards)
    }
}
