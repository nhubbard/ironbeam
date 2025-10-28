//! Parquet helpers (feature `io-parquet`).
//!
//! These helpers let you **write** a typed `PCollection<T>` to a Parquet file and
//! **read** a Parquet file as a *streaming* source that shards by **row groups**.
//!
//! ### Notes
//! - Requires the `io-parquet` feature (Arrow/Parquet + serde-arrow integration).
//! - Schemas are inferred from `T` via `serde` + `serde-arrow`. Your `T` should be
//!   `Serialize` for writing and `Deserialize` for reading.
//! - The streaming reader divides the file by **row groups** (not by bytes/rows).
//!   Each partition reads its assigned row-group range and deserializes into `Vec<T>`.
//! - Writing collects results **sequentially** first (deterministic order), then
//!   writes a single Parquet file.
//!
//! ### When to use
//! - Use `write_parquet` to export final results in a columnar, analytics-friendly format.
//! - Use `read_parquet_streaming` for large datasets where loading the entire file
//!   would be too expensive; processing happens partition-by-partition.

use crate::io::parquet::{build_parquet_shards, write_parquet_vec, ParquetShards, ParquetVecOps};
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{PCollection, Pipeline, RFBound};
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

#[cfg(feature = "io-parquet")]
impl<T: RFBound + DeserializeOwned + Serialize> PCollection<T> {
    /// Execute the pipeline, collect results, and write them to a **single Parquet file**.
    ///
    /// The Arrow schema is inferred from `T` (via `serde-arrow`). The entire collection
    /// is first collected into memory (sequentially) to preserve deterministic ordering,
    /// and then written as one Parquet file.
    ///
    /// Returns the number of rows written.
    ///
    /// ### Example
    /// ```ignore
    /// use rustflow::*;
    /// # fn main() -> anyhow::Result<()> {
    /// #[cfg(feature = "io-parquet")]
    /// {
    ///     #[derive(serde::Serialize, serde::Deserialize, Clone)]
    ///     struct Row { k: String, v: u64 }
    ///
    ///     let p = Pipeline::default();
    ///     let out = from_vec(&p, vec![
    ///         Row { k: "a".into(), v: 1 },
    ///         Row { k: "b".into(), v: 2 },
    ///     ]);
    ///
    ///     let n = out.write_parquet("data/out.parquet")?;
    ///     assert_eq!(n, 2);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn write_parquet(self, path: impl AsRef<Path>) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        write_parquet_vec(path, &rows)
    }
}

/// Read a Parquet file as a **streaming** source partitioned by row groups.
///
/// Each partition reads a contiguous range of **row groups** and deserializes
/// the rows into `Vec<T>`. This avoids loading the entire file into memory at once.
///
/// - `groups_per_shard`: how many row groups each shard/partition should read (minimum 1).
/// - The returned `PCollection<T>` can be processed with the usual stateless / keyed ops.
///
/// ### Example
/// ```ignore
/// use rustflow::*;
/// # fn main() -> anyhow::Result<()> {
/// #[cfg(feature = "io-parquet")]
/// {
///     #[derive(serde::Serialize, serde::Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
///     struct Rec { k: String, v: u64 }
///
///     let p = Pipeline::default();
///     let stream = read_parquet_streaming::<Rec>(&p, "data/in.parquet", 1)?;
///
///     // You can collect (and sort if Rec: Ord) to make results deterministic for testing:
///     let rows = stream.collect_seq_sorted()?;
///     println!("rows = {}", rows.len());
/// }
/// # Ok(()) }
/// ```
#[cfg(feature = "io-parquet")]
pub fn read_parquet_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    groups_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: ParquetShards = build_parquet_shards(path, groups_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: ParquetVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}
