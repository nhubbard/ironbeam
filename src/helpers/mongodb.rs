//! MongoDB sources and sinks for [`PCollection`].
//!
//! This module provides typed, `mongodb`-backed I/O that integrates with the Ironbeam
//! pipeline. You can either:
//!
//! - **Vector I/O** — eagerly run a `find` and load every matching document into memory:
//!   - [`read_mongodb`] -> `PCollection<T>`
//!   - [`PCollection::write_mongodb`]
//!   - [`PCollection::write_mongodb_par`] (feature: `parallel-io`)
//!
//! - **Streaming I/O** — build a source that shards a query's result set by document count
//!   (via `skip`/`limit`) and reads each shard lazily in the runner:
//!   - [`read_mongodb_streaming`] -> `PCollection<T>`
//!
//! Read and write types cross the wire via `serde`: reads require `T: DeserializeOwned` and
//! writes require `T: Serialize`, mirroring how `mongodb::Collection<T>` is generic over the
//! document type. Query filters are built with the [`bson::doc!`] macro.
//!
//! ## Feature flags
//! - `io-mongodb`: enables MongoDB helpers. This connector is **not** part of the default
//!   feature set; opt in explicitly to avoid pulling in `mongodb`, `bson`, and `tokio`.
//! - `parallel-io`: enables the parallel writer ([`PCollection::write_mongodb_par`]).
//!
//! ## Note on glob expansion
//! Unlike file-based sources, MongoDB sources read from a live database connection —
//! there is no filesystem glob to expand. `read_mongodb` and `read_mongodb_streaming` always
//! run a single `find` against a single collection.
//!
//! ## Examples
//! Read a collection, transform it, and write it back to a new collection:
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
//! struct Doc { id: i64, name: String }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let docs = read_mongodb::<Doc>(&p, "mongodb://localhost:27017", "mydb", "people", bson::doc! {})?;
//! let renamed = docs.map(|d: &Doc| Doc { id: d.id, name: d.name.to_uppercase() });
//! renamed.write_mongodb("mongodb://localhost:27017", "mydb", "people_out")?;
//! # Ok(())
//! # }
//! ```
//!
//! Streaming read shard-by-shard (useful for large result sets):
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
//! struct Doc { id: i64, name: String }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let stream = read_mongodb_streaming::<Doc>(&p, "mongodb://localhost:27017", "mydb", "people", bson::doc! {}, 10_000)?;
//! let out = stream.collect_seq()?;
//! # Ok(())
//! # }
//! ```

use crate::PCollection;
#[cfg(feature = "io-mongodb")]
use crate::io::mongodb::build_mongodb_shards;
#[cfg(feature = "io-mongodb")]
use crate::io::mongodb::{MongoShards, MongoVecOps};
#[cfg(feature = "io-mongodb")]
use crate::node::Node;
#[cfg(feature = "io-mongodb")]
use crate::type_token::TypeTag;
#[cfg(feature = "io-mongodb")]
use crate::{Element, Pipeline, from_vec};
#[cfg(feature = "io-mongodb")]
use anyhow::Result;
#[cfg(feature = "io-mongodb")]
use std::marker::PhantomData;
#[cfg(feature = "io-mongodb")]
use std::sync::Arc;

/// Run a `find` with `filter` against `database.collection` and collect every matching
/// document into a `PCollection<T>` (vector mode).
///
/// This eagerly executes the query and loads the entire result set into memory. For very
/// large result sets, prefer [`read_mongodb_streaming`].
///
/// MongoDB sources are **not** glob-expanded — `filter` is always run as a single `find`
/// against a single collection.
///
/// *Enabled when the `io-mongodb` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `uri`: MongoDB connection URI (e.g. `mongodb://localhost:27017`).
/// - `database`: Database name.
/// - `collection`: Collection name.
/// - `filter`: Query filter, typically built with the `bson::doc!` macro.
///
/// # Errors
/// Returns an error if the connection fails or any document cannot be deserialized as `T`.
///
/// # Examples
/// ```no_run
/// use ironbeam::*;
/// use anyhow::Result;
///
/// #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// struct Doc { id: i64, name: String }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let docs = read_mongodb::<Doc>(&p, "mongodb://localhost:27017", "mydb", "people", bson::doc! {})?;
/// let out = docs.collect_seq()?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "io-mongodb")]
pub fn read_mongodb<T>(
    p: &Pipeline,
    uri: &str,
    database: &str,
    collection: &str,
    filter: bson::Document,
) -> Result<PCollection<T>>
where
    T: Element + serde::de::DeserializeOwned + Send + Sync + Unpin,
{
    let v = crate::io::mongodb::read_mongodb_vec::<T>(uri, database, collection, filter)?;
    Ok(from_vec(p, v))
}

/// Create a **streaming** MongoDB source, sharded by a fixed number of documents.
///
/// This builds a [`MongoShards`] descriptor (counting documents up front via
/// `count_documents`) and inserts a `Source` node that runs a `skip`/`limit` query to read
/// only its shard when executed by the runner. Useful for result sets that don't fit
/// comfortably in memory.
///
/// *Enabled when the `io-mongodb` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `uri`: MongoDB connection URI.
/// - `database`: Database name.
/// - `collection`: Collection name.
/// - `filter`: Query filter whose matching documents are sharded.
/// - `docs_per_shard`: Target number of documents per shard (minimum 1).
///
/// # Errors
/// Returns an error if the connection fails or the count command fails.
///
/// # Example
/// ```no_run
/// use ironbeam::*;
/// use anyhow::Result;
///
/// #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// struct Doc { id: i64, name: String }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let stream = read_mongodb_streaming::<Doc>(&p, "mongodb://localhost:27017", "mydb", "people", bson::doc! {}, 10_000)?;
/// let out = stream.collect_seq()?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "io-mongodb")]
pub fn read_mongodb_streaming<T>(
    p: &Pipeline,
    uri: &str,
    database: &str,
    collection: &str,
    filter: bson::Document,
    docs_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: Element + serde::de::DeserializeOwned + Send + Sync + Unpin + Clone,
{
    let shards: MongoShards =
        build_mongodb_shards(uri, database, collection, filter, docs_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: MongoVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    p.set_coder::<T>(id);
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}

#[cfg(feature = "io-mongodb")]
impl<T: Element + serde::Serialize + Send + Sync> PCollection<T> {
    /// Execute the collection and bulk-insert it into `database.collection` at `uri`
    /// (sequential).
    ///
    /// The entire collection is first collected into memory (sequentially) to preserve
    /// deterministic ordering, then inserted via `insert_many` in batches of at most
    /// 100,000 documents.
    ///
    /// Returns the number of documents reported as inserted.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// struct Doc { id: i64, name: String }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let docs = from_vec(&p, vec![Doc { id: 1, name: "a".into() }]);
    /// let n = docs.write_mongodb("mongodb://localhost:27017", "mydb", "people")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Propagates connection and insert errors.
    pub fn write_mongodb(self, uri: &str, database: &str, collection: &str) -> Result<usize> {
        let rows = self.collect_seq()?;
        crate::io::mongodb::write_mongodb_vec(uri, database, collection, &rows)
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "parallel-io")))]
#[cfg(all(feature = "io-mongodb", feature = "parallel-io"))]
impl<T: Element + serde::Serialize + Send + Sync> PCollection<T> {
    /// Execute the collection sequentially (to lock in a deterministic order), then
    /// bulk-insert it into `database.collection` **in parallel** using independent
    /// connections per shard.
    ///
    /// `shards = Some(n)` sets the number of writer shards; `None` uses a sensible default
    /// (`num_cpus::get().max(2)`).
    ///
    /// Returns the total number of documents reported as inserted across all shards.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// struct Doc { id: i64, name: String }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let docs = from_vec(&p, vec![Doc { id: 1, name: "a".into() }]);
    /// docs.write_mongodb_par("mongodb://localhost:27017", "mydb", "people", Some(4))?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Propagates connection and insert errors from any shard.
    pub fn write_mongodb_par(
        self,
        uri: &str,
        database: &str,
        collection: &str,
        shards: Option<usize>,
    ) -> Result<usize> {
        let data = self.collect_seq()?;
        crate::io::mongodb::write_mongodb_par(uri, database, collection, &data, shards)
    }
}
