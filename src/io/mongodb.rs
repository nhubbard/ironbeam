//! MongoDB I/O utilities and `VecOps` integration.
//!
//! This module provides:
//! - **Typed vector I/O**: `read_mongodb_vec` and `write_mongodb_vec` (feature: `io-mongodb`)
//! - **Deterministic parallel writer**: `write_mongodb_par` (features: `io-mongodb` + `parallel-io`)
//! - **Streaming ingestion** by document ranges: [`MongoShards`], `build_mongodb_shards`,
//!   `read_mongodb_range`
//! - **Execution runner integration**: [`MongoVecOps<T>`] implements [`VecOps`] over
//!   [`MongoShards`]
//!
//! # Feature gating
//!
//! [`MongoShards`] and [`MongoVecOps<T>`] are **always available** regardless of the
//! `io-mongodb` feature, so the helper layer and runner can link unconditionally. Functions
//! that require `mongodb`/`bson` types in their signature (e.g., `filter: bson::Document`)
//! are gated with `#[cfg(feature = "io-mongodb")]` and **have no stub** — they simply do not
//! exist when the feature is off, including `build_mongodb_shards`, whose signature requires
//! `bson::Document`.
//!
//! # Async bridge
//!
//! `mongodb` is fully async. Since Ironbeam is Rayon-based (synchronous), a single
//! `static MONGO_RUNTIME: LazyLock<tokio::runtime::Runtime>` is created once and used to
//! `block_on` every async operation. Calling `block_on` from a Rayon thread is always safe
//! — Rayon threads are not tokio async tasks, so there is no ambient runtime to conflict.
//!
//! # Bson serde bridge
//!
//! `T` crosses the wire via `serde`: reads require `T: DeserializeOwned` and writes require
//! `T: Serialize`, mirroring how `mongodb::Collection<T>` is generic over the document type.
//!
//! # Sharding strategy
//!
//! Shards are computed via:
//! 1. `collection.count_documents(filter)` — full count of matching documents.
//! 2. `collection.find(filter).skip(offset).limit(limit)` per shard.
//!
//! The filter is stored in [`MongoShards`] as raw BSON bytes (`Vec<u8>`), since
//! `bson::Document` does not implement the auto traits Ironbeam otherwise requires for
//! always-available data; it is decoded back into a `Document` on each read.
//!
//! # Connection handling
//!
//! Each public function creates its own `mongodb::Client` for the duration of the call and
//! drops it on return. This avoids lifetime and ownership complexity in batch pipeline
//! contexts.

#[cfg(feature = "io-mongodb")]
use crate::Partition;
use crate::type_token::VecOps;
use anyhow::Result;
#[cfg(feature = "io-mongodb")]
use std::any::Any;
use std::marker::PhantomData;

// ── Always-available sharding metadata ───────────────────────────────────────

/// Streaming MongoDB sharding metadata.
///
/// Produced by `build_mongodb_shards` and consumed by `read_mongodb_range` and the
/// execution engine via [`MongoVecOps`].
///
/// All fields use primitive types (the filter is stored as raw BSON bytes), so this struct
/// compiles regardless of the `io-mongodb` feature being enabled.
#[derive(Clone, Debug)]
pub struct MongoShards {
    /// MongoDB connection URI (e.g. `mongodb://localhost:27017`).
    pub uri: String,
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// The query filter, encoded as raw BSON bytes.
    pub filter_bson: Vec<u8>,
    /// Total number of documents matching the filter.
    pub total_docs: u64,
    /// `(skip, limit)` pairs, one per shard.
    pub ranges: Vec<(u64, u64)>,
}

// ── Async runtime bridge (only compiled with the feature) ────────────────────

#[cfg(feature = "io-mongodb")]
#[allow(clippy::missing_panics_doc)] // panics only if the OS is out of threads
static MONGO_RUNTIME: std::sync::LazyLock<tokio::runtime::Runtime> =
    std::sync::LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect(
                "failed to create MongoDB I/O tokio runtime — check OS thread limits (ulimit -u)",
            )
    });

// ── Private helpers (only compiled with the feature) ─────────────────────────

/// Build [`MongoShards`] from a pre-counted total document count and a desired shard size.
#[cfg(feature = "io-mongodb")]
fn make_mongo_shards(
    uri: String,
    database: String,
    collection: String,
    filter_bson: Vec<u8>,
    total: u64,
    docs_per_shard: usize,
) -> MongoShards {
    if total == 0 {
        return MongoShards {
            uri,
            database,
            collection,
            filter_bson,
            total_docs: 0,
            ranges: vec![],
        };
    }
    let dps = (docs_per_shard.max(1)) as u64;
    let n_shards = usize::try_from(total.div_ceil(dps)).expect("shard count overflow");
    let ranges = (0..n_shards)
        .map(|i| {
            let skip = i as u64 * dps;
            let limit = dps.min(total - skip);
            (skip, limit)
        })
        .collect();
    MongoShards {
        uri,
        database,
        collection,
        filter_bson,
        total_docs: total,
        ranges,
    }
}

/// Connect to `uri` and return a fresh [`mongodb::Client`].
#[cfg(feature = "io-mongodb")]
async fn mongo_client(uri: &str) -> Result<mongodb::Client> {
    mongodb::Client::with_uri_str(uri)
        .await
        .map_err(|e| anyhow::anyhow!("connect to {uri}: {e}"))
}

/// Resolve a typed handle to `database.collection` from an existing client.
#[cfg(feature = "io-mongodb")]
fn mongo_collection<T: Send + Sync>(
    client: &mongodb::Client,
    database: &str,
    collection: &str,
) -> mongodb::Collection<T> {
    client.database(database).collection(collection)
}

// ── Vector I/O (feature-gated, no stub) ──────────────────────────────────────

/// Read every document matching `filter` in `database.collection` into a `Vec<T>`.
///
/// # Errors
/// Returns an error if the connection fails or any document cannot be deserialized as `T`.
///
/// # Feature
/// Requires `io-mongodb`.
#[cfg(feature = "io-mongodb")]
pub fn read_mongodb_vec<T>(
    uri: &str,
    database: &str,
    collection: &str,
    filter: bson::Document,
) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned + Send + Sync + Unpin,
{
    let uri_owned = uri.to_owned();
    let database_owned = database.to_owned();
    let collection_owned = collection.to_owned();
    MONGO_RUNTIME.block_on(async move {
        let client = mongo_client(&uri_owned).await?;
        let coll = mongo_collection::<T>(&client, &database_owned, &collection_owned);
        let mut cursor = coll
            .find(filter)
            .await
            .map_err(|e| anyhow::anyhow!("read_mongodb_vec find: {e}"))?;
        let mut out = Vec::new();
        while cursor
            .advance()
            .await
            .map_err(|e| anyhow::anyhow!("read_mongodb_vec advance: {e}"))?
        {
            out.push(
                cursor
                    .deserialize_current()
                    .map_err(|e| anyhow::anyhow!("read_mongodb_vec deserialize: {e}"))?,
            );
        }
        Ok(out)
    })
}

/// Insert `data` into `database.collection`.
///
/// Documents are inserted in batches of at most 100,000 (MongoDB's `maxWriteBatchSize`).
///
/// Returns the number of documents reported as inserted.
///
/// # Errors
/// Returns an error if the connection or insert fails. Returns `Ok(0)` immediately
/// if `data` is empty.
///
/// # Feature
/// Requires `io-mongodb`.
#[cfg(feature = "io-mongodb")]
pub fn write_mongodb_vec<T>(
    uri: &str,
    database: &str,
    collection: &str,
    data: &[T],
) -> Result<usize>
where
    T: serde::Serialize + Send + Sync,
{
    const MAX_BATCH: usize = 100_000;

    if data.is_empty() {
        return Ok(0);
    }
    let uri_owned = uri.to_owned();
    let database_owned = database.to_owned();
    let collection_owned = collection.to_owned();
    MONGO_RUNTIME.block_on(async move {
        let client = mongo_client(&uri_owned).await?;
        let coll = mongo_collection::<T>(&client, &database_owned, &collection_owned);
        let mut inserted = 0usize;
        for chunk in data.chunks(MAX_BATCH) {
            let result = coll
                .insert_many(chunk)
                .await
                .map_err(|e| anyhow::anyhow!("write_mongodb_vec insert_many: {e}"))?;
            inserted += result.inserted_ids.len();
        }
        Ok(inserted)
    })
}

/// Insert `data` in parallel using `shards` independent database connections.
///
/// Each shard opens its own client and calls [`write_mongodb_vec`] independently.
/// The total inserted document count is returned.
///
/// * `shards`: if `None`, it defaults to `num_cpus::get().max(2)`.
///
/// # Errors
/// Returns an error if any shard fails. If `data` is empty, it returns `Ok(0)`.
///
/// # Feature
/// Requires both `io-mongodb` and `parallel-io`.
#[cfg(all(feature = "io-mongodb", feature = "parallel-io"))]
pub fn write_mongodb_par<T>(
    uri: &str,
    database: &str,
    collection: &str,
    data: &[T],
    shards: Option<usize>,
) -> Result<usize>
where
    T: serde::Serialize + Send + Sync,
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
        .map(|chunk_data| write_mongodb_vec(uri, database, collection, chunk_data))
        .try_fold(|| 0usize, |acc, r| r.map(|n| acc + n))
        .try_reduce(|| 0usize, |a, b| Ok(a + b))
}

// ── Streaming sharding (feature-gated, no stub) ──────────────────────────────

/// Build [`MongoShards`] by counting documents matching `filter` and slicing into
/// `docs_per_shard`.
///
/// Requires a network round-trip to count documents. For very large collections with many
/// shards, choose a `docs_per_shard` that keeps the shard count small to avoid many
/// skip-heavy scans.
///
/// # Errors
/// Returns an error if the connection fails or the count command fails.
///
/// # Feature
/// Requires `io-mongodb`. This function's signature needs `bson::Document`, so unlike
/// [`crate::io::sql::build_sql_shards`] it receives **no stub** when the feature is off.
#[cfg(feature = "io-mongodb")]
pub fn build_mongodb_shards(
    uri: &str,
    database: &str,
    collection: &str,
    filter: bson::Document,
    docs_per_shard: usize,
) -> Result<MongoShards> {
    let uri_owned = uri.to_owned();
    let database_owned = database.to_owned();
    let collection_owned = collection.to_owned();
    let filter_bson = filter
        .to_vec()
        .map_err(|e| anyhow::anyhow!("build_mongodb_shards: serialize filter: {e}"))?;
    MONGO_RUNTIME.block_on(async move {
        let client = mongo_client(&uri_owned).await?;
        let coll = mongo_collection::<bson::Document>(&client, &database_owned, &collection_owned);
        let total = coll
            .count_documents(filter)
            .await
            .map_err(|e| anyhow::anyhow!("build_mongodb_shards count_documents: {e}"))?;
        Ok(make_mongo_shards(
            uri_owned,
            database_owned,
            collection_owned,
            filter_bson,
            total,
            docs_per_shard,
        ))
    })
}

// ── Range reader (feature-gated, no stub) ────────────────────────────────────

/// Read the shard described by `(skip, limit)` from the collection referenced by `shards`.
///
/// A `limit` of `0` returns an empty vector without contacting the server: MongoDB's wire
/// protocol treats `limit(0)` as "no limit" rather than "zero documents", so this case is
/// special-cased to preserve the expected "zero documents in, zero documents out" contract.
///
/// # Errors
/// Returns an error if the connection or query fails.
///
/// # Feature
/// Requires `io-mongodb`.
#[cfg(feature = "io-mongodb")]
pub fn read_mongodb_range<T>(shards: &MongoShards, skip: u64, limit: u64) -> Result<Vec<T>>
where
    T: serde::de::DeserializeOwned + Send + Sync + Unpin,
{
    if limit == 0 {
        return Ok(Vec::new());
    }
    let uri_owned = shards.uri.clone();
    let database_owned = shards.database.clone();
    let collection_owned = shards.collection.clone();
    let filter = bson::Document::from_reader(shards.filter_bson.as_slice())
        .map_err(|e| anyhow::anyhow!("read_mongodb_range: decode filter: {e}"))?;
    let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
    MONGO_RUNTIME.block_on(async move {
        let client = mongo_client(&uri_owned).await?;
        let coll = mongo_collection::<T>(&client, &database_owned, &collection_owned);
        let mut cursor = coll
            .find(filter)
            .skip(skip)
            .limit(limit_i64)
            .await
            .map_err(|e| anyhow::anyhow!("read_mongodb_range find: {e}"))?;
        let mut out = Vec::new();
        while cursor
            .advance()
            .await
            .map_err(|e| anyhow::anyhow!("read_mongodb_range advance: {e}"))?
        {
            out.push(
                cursor
                    .deserialize_current()
                    .map_err(|e| anyhow::anyhow!("read_mongodb_range deserialize: {e}"))?,
            );
        }
        Ok(out)
    })
}

// ── VecOps adapter ────────────────────────────────────────────────────────────

/// `VecOps` adapter for streaming MongoDB reads via [`MongoShards`].
///
/// The struct and its constructor always compile. The [`VecOps`] implementation is
/// gated by `#[cfg(feature = "io-mongodb")]` — a disabled source can never be
/// constructed in practice, so the runner will never attempt to call these methods
/// when the feature is off.
pub struct MongoVecOps<T>(PhantomData<T>);

impl<T> MongoVecOps<T> {
    /// Construct an `Arc`-wrapped adapter.
    #[must_use]
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self(PhantomData))
    }
}

#[cfg(feature = "io-mongodb")]
impl<T> VecOps for MongoVecOps<T>
where
    T: serde::de::DeserializeOwned + Send + Sync + Clone + Unpin + 'static,
{
    fn len(&self, data: &dyn Any) -> Option<usize> {
        data.downcast_ref::<MongoShards>()
            .and_then(|s| usize::try_from(s.total_docs).ok())
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let s = data.downcast_ref::<MongoShards>()?;
        s.ranges
            .iter()
            .map(|&(skip, limit)| {
                let v: Vec<T> = read_mongodb_range(s, skip, limit).ok()?;
                Some(Box::new(v) as Partition)
            })
            .collect()
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let s = data.downcast_ref::<MongoShards>()?;
        let filter = bson::Document::from_reader(s.filter_bson.as_slice()).ok()?;
        let v: Vec<T> = read_mongodb_vec::<T>(&s.uri, &s.database, &s.collection, filter).ok()?;
        Some(Box::new(v) as Partition)
    }
}
