//! Parquet helpers (feature `io-parquet`).
//!
//! These helpers let you **read** and **write** a typed `PCollection<T>` to/from
//! Parquet files.
//!
//! ## Available operations
//! - [`read_parquet`] - Eager glob-aware source (loads all matching files into memory)
//! - [`read_parquet_streaming`] - Read Parquet file(s) as a streaming source
//! - [`PCollection::write_parquet`](PCollection::write_parquet) - Write a collection to a Parquet file
//! - [`PCollection::write_parquet_par`](PCollection::write_parquet_par) - Write in parallel (feature: `parallel-io`)
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
//! - Use `read_parquet` to eagerly load one or more Parquet files (glob support).
//! - Use `write_parquet` to export final results in a columnar, analytics-friendly format.
//! - Use `read_parquet_streaming` for large datasets where loading the entire file
//!   would be too expensive; processing happens partition-by-partition.
//! - Use `write_parquet_par` for faster multi-shard writes when ordering within
//!   a shard is acceptable (element order is preserved across shards).

use crate::io::glob::expand_glob;
use crate::io::parquet::{
    ParquetShards, ParquetVecOps, build_parquet_shards, read_parquet_vec, write_parquet_vec,
};
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{Element, PCollection, Pipeline, from_vec};
use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

/// Read one or more Parquet files into a typed `PCollection<T>` (eager mode).
///
/// This eagerly loads the entire file(s) into memory using `serde_arrow` and
/// returns a source collection. For very large files, prefer
/// [`read_parquet_streaming`].
///
/// ### Glob Pattern Support
///
/// The `path` parameter can be either:
/// - A single file path: `"data/input.parquet"`
/// - A glob pattern: `"data/*.parquet"` or `"data/year=2024/**/*.parquet"`
///
/// When a glob pattern is provided, all matching files are read and concatenated
/// in sorted (lexicographic) order for deterministic results.
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `path`: File path or glob pattern to read.
///
/// # Errors
/// Returns an error if `path` contains invalid UTF-8, if a glob pattern does not
/// match any files, or if any matched file cannot be read or deserialized.
///
/// # Panics
/// Panics if the internal glob-detection regex cannot be compiled — not reachable
/// in practice because the pattern is a compile-time constant.
///
/// # Examples
///
/// Single file:
/// ```no_run
/// use ironbeam::*;
/// use serde::{Deserialize, Serialize};
/// use anyhow::Result;
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct Row { k: String, v: u64 }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let rows = read_parquet::<Row>(&p, "data/input.parquet")?;
/// let out = rows.collect_seq()?;
/// # Ok(())
/// # }
/// ```
///
/// Glob pattern:
/// ```no_run
/// use ironbeam::*;
/// use serde::{Deserialize, Serialize};
/// use anyhow::Result;
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct Row { k: String, v: u64 }
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let rows = read_parquet::<Row>(&p, "data/*.parquet")?;
/// let out = rows.collect_seq()?;
/// # Ok(())
/// # }
/// ```
pub fn read_parquet<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
where
    T: Element + DeserializeOwned,
{
    let path_str = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow!("path contains invalid UTF-8"))?;

    let glob_regex = Regex::new(r"[*?\[]").expect("valid glob regex");
    if glob_regex.is_match(path_str) {
        let files =
            expand_glob(path_str).with_context(|| format!("expanding glob pattern: {path_str}"))?;

        if files.is_empty() {
            bail!("no files found matching pattern: {path_str}");
        }

        let mut all_data = Vec::new();
        for file in files {
            let data: Vec<T> =
                read_parquet_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        let v = read_parquet_vec::<T>(path)?;
        Ok(from_vec(p, v))
    }
}

impl<T: Element + DeserializeOwned + Serialize> PCollection<T> {
    /// Execute the pipeline, collect results, and write them to a **single Parquet file**.
    ///
    /// The Arrow schema is inferred from `T` (via `serde-arrow`). The entire collection
    /// is first collected into memory (sequentially) to preserve deterministic ordering
    /// and then written as one Parquet file.
    ///
    /// Returns the number of rows written.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    /// # fn main() -> Result<()> {
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
    ///
    /// # Errors
    ///
    /// If an error is encountered while writing the Parquet file, a [`Result`] is returned.
    pub fn write_parquet(self, path: impl AsRef<Path>) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        write_parquet_vec(path, &rows)
    }
}

#[cfg_attr(docsrs, doc(cfg(feature = "parallel-io")))]
#[cfg(feature = "parallel-io")]
impl<T: Element + DeserializeOwned + Serialize + Send + Sync> PCollection<T> {
    /// Execute the collection and write it to a **single Parquet file** using parallel
    /// shard writers.
    ///
    /// The collection is first collected into memory (sequentially) to establish a
    /// deterministic element order. The data is then split into `shards` temporary
    /// Parquet files written concurrently via Rayon. The temp files are merged back
    /// into one final Parquet file in shard-index order, preserving the original order.
    ///
    /// Returns the number of rows written.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    /// # fn main() -> Result<()> {
    /// #[cfg(all(feature = "io-parquet", feature = "parallel-io"))]
    /// {
    ///     #[derive(serde::Serialize, serde::Deserialize, Clone)]
    ///     struct Row { k: String, v: u64 }
    ///
    ///     let p = Pipeline::default();
    ///     let out = ironbeam::from_vec(&p, vec![
    ///         Row { k: "a".into(), v: 1 },
    ///         Row { k: "b".into(), v: 2 },
    ///     ]);
    ///
    ///     let n = out.write_parquet_par("data/out.parquet", Some(2))?;
    ///     assert_eq!(n, 2);
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Propagates I/O and serialization errors from shard writing or the merge step.
    pub fn write_parquet_par(self, path: impl AsRef<Path>, shards: Option<usize>) -> Result<usize> {
        let data = self.collect_seq()?;
        crate::io::parquet::write_parquet_par(path, &data, shards)
    }
}

/// Read Parquet file(s) as a **streaming** source partitioned by row groups.
///
/// Each partition reads a contiguous range of **row groups** and deserializes
/// the rows into `Vec<T>`. This avoids loading the entire file into memory at once.
///
/// ### Glob Pattern Support
///
/// The `path` parameter can be either:
/// - A single file path: `"data/input.parquet"`
/// - A glob pattern: `"data/*.parquet"` or `"data/year=2024/month=*/day=*/*.parquet"`
///
/// When a glob pattern is provided, all matching files are read and concatenated
/// in sorted (lexicographic) order for deterministic results. Note: For glob patterns,
/// the function uses eager loading (all files read into memory) rather than streaming.
///
/// - `groups_per_shard`: how many row groups each shard/partition should read (minimum 1).
/// - The returned `PCollection<T>` can be processed with the usual stateless / keyed ops.
///
/// ### Examples
///
/// Single file (streaming):
/// ```no_run
/// use ironbeam::*;
/// use anyhow::Result;
/// # fn main() -> Result<()> {
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
///
/// Glob pattern:
/// ```no_run
/// use ironbeam::*;
/// use anyhow::Result;
/// # fn main() -> Result<()> {
/// #[cfg(feature = "io-parquet")]
/// {
///     #[derive(serde::Serialize, serde::Deserialize, Clone)]
///     struct Rec { k: String, v: u64 }
///
///     let p = Pipeline::default();
///     // Read all Parquet files in date partitions
///     let stream = read_parquet_streaming::<Rec>(&p, "data/year=2024/month=*/day=*/*.parquet", 1)?;
///     let rows = stream.collect_seq()?;
/// }
/// # Ok(()) }
/// ```
///
/// # Errors
///
/// If an error occurs while streaming the Parquet input data, then a [`Result`] is returned.
///
/// # Panics
///
/// Panics if the regex engine fails.
pub fn read_parquet_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    groups_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: Element + DeserializeOwned,
{
    let path_str = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow!("path contains invalid UTF-8"))?;

    let glob_regex = Regex::new(r"[*?\[]").expect("valid glob regex");
    if glob_regex.is_match(path_str) {
        let files =
            expand_glob(path_str).with_context(|| format!("expanding glob pattern: {path_str}"))?;

        if files.is_empty() {
            bail!("no files found matching pattern: {path_str}");
        }

        let mut all_data = Vec::new();
        for file in files {
            let data: Vec<T> =
                read_parquet_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        let shards: ParquetShards = build_parquet_shards(path, groups_per_shard)?;
        let id = p.insert_node(Node::Source {
            payload: Arc::new(shards),
            vec_ops: ParquetVecOps::<T>::new(),
            elem_tag: TypeTag::of::<T>(),
        });
        p.set_coder::<T>(id);
        Ok(PCollection {
            pipeline: p.clone(),
            id,
            _t: PhantomData,
        })
    }
}
