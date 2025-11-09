//! CSV sources and sinks for `PCollection`.
//!
//! This module provides typed, serde-backed CSV I/O that integrates with the
//! Rustflow pipeline. You can either:
//!
//! - **Vector I/O** -- read the whole file into memory or write an in-memory collection:
//!   - [`read_csv`] -> `PCollection<T>`
//!   - [`PCollection::write_csv`] / [`PCollection::write_csv_par`]
//!
//! - **Streaming I/O** -- build a source that shards a CSV file by row count and
//!   parses each shard lazily in the runner:
//!   - [`read_csv_streaming`] -> `PCollection<T>`
//!
//! All functions are serde-driven: your record type `T` should `#[derive(serde::Deserialize)]`
//! for reads and `#[derive(serde::Serialize)]` for writes.
//!
//! ## Feature flags
//! - `io-csv`: enables CSV helpers.
//! - `parallel-io`: enables the parallel writer (`write_csv_par`).
//!
//! ## Examples
//! Read a CSV file into a typed collection and write it back out (vector I/O):
//! ```no_run
//! use rustflow::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
//! struct Row { k: String, v: u64 }
//!
//! # fn main() -> anyhow::Result<()> {
//! let p = Pipeline::default();
//!
//! // Vector read -> PCollection<Row>
//! let rows = read_csv::<Row>(&p, "input.csv", true)?;
//!
//! // Transform and write
//! let doubled = rows.map(|r: &Row| Row { k: r.k.clone(), v: r.v * 2 });
//! doubled.write_csv("output.csv", true)?;
//! # Ok(()) }
//! ```
//!
//! Streaming read shard-by-shard (useful for large files):
//! ```no_run
//! use rustflow::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Row { k: String, v: u64 }
//!
//! # fn main() -> anyhow::Result<()> {
//! let p = Pipeline::default();
//! let stream = read_csv_streaming::<Row>(&p, "input.csv", true, 50_000)?;
//! let out = stream.collect_seq()?; // materialize after transforms
//! # Ok(()) }
//! ```

use crate::io::csv::{build_csv_shards, read_csv_vec, write_csv_vec, CsvShards, CsvVecOps};
use crate::io::glob::expand_glob;
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{from_vec, PCollection, Pipeline, RFBound};
use anyhow::{Context, Result};
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

/// Read CSV file(s) into a typed `PCollection<T>` (vector mode).
///
/// This eagerly parses the entire file(s) into memory using `serde` and returns
/// a source collection. For very large files, prefer [`read_csv_streaming`].
///
/// ### Glob Pattern Support
///
/// The `path` parameter can be either:
/// - A single file path: `"data/input.csv"`
/// - A glob pattern: `"data/*.csv"` or `"data/year=2024/month=*/day=*/*.csv"`
///
/// When a glob pattern is provided, all matching files are read and concatenated
/// in sorted (lexicographic) order for deterministic results.
///
/// *Enabled when the `io-csv` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `path`: File path or glob pattern to read.
/// - `has_headers`: Whether the input CSV includes a header row.
///
/// # Errors
/// An error is returned if the file cannot be opened or if any row fails to deserialize.
///
/// # Examples
///
/// Single file:
/// ```no_run
/// use rustflow::*;
/// use serde::Deserialize;
///
/// #[derive(Clone, Deserialize)]
/// struct Row { k: String, v: u64 }
///
/// # fn main() -> anyhow::Result<()> {
/// let p = Pipeline::default();
/// let rows = read_csv::<Row>(&p, "data.csv", true)?;
/// let out = rows.collect_seq()?;
/// # Ok(()) }
/// ```
///
/// Glob pattern:
/// ```no_run
/// use rustflow::*;
/// use serde::Deserialize;
///
/// #[derive(Clone, Deserialize)]
/// struct Row { k: String, v: u64 }
///
/// # fn main() -> anyhow::Result<()> {
/// let p = Pipeline::default();
/// // Read all CSV files in the data directory
/// let rows = read_csv::<Row>(&p, "data/*.csv", true)?;
/// let out = rows.collect_seq()?;
/// # Ok(()) }
/// ```
#[cfg(feature = "io-csv")]
pub fn read_csv<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    has_headers: bool,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let path_str = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("path contains invalid UTF-8"))?;

    // Check if path contains glob patterns
    let glob_regex = Regex::new(r"[*?\[]").expect("valid glob regex");
    if glob_regex.is_match(path_str) {
        // Glob pattern - expand and read all matching files
        let files =
            expand_glob(path_str).with_context(|| format!("expanding glob pattern: {path_str}"))?;

        if files.is_empty() {
            anyhow::bail!("no files found matching pattern: {path_str}");
        }

        let mut all_data = Vec::new();
        for file in files {
            let data: Vec<T> = read_csv_vec(&file, has_headers)
                .with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        // Single file path
        let v = read_csv_vec::<T>(path, has_headers)?;
        Ok(from_vec(p, v))
    }
}

#[cfg(feature = "io-csv")]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the pipeline sequentially and write the result as CSV (vector mode).
    ///
    /// This collects the entire result into memory and writes it to `path` using `serde`.
    /// For parallel writing, see [`PCollection::write_csv_par`] (requires `parallel-io`).
    ///
    /// # Arguments
    /// - `path`: Destination path.
    /// - `has_headers`: Whether to write a header row.
    ///
    /// # Errors
    /// An error is returned if writing/serialization fails.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    /// use serde::Serialize;
    ///
    /// #[derive(Clone, Serialize)]
    /// struct Row { k: String, v: u64 }
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let rows = from_vec(&p, vec![Row { k: "a".into(), v: 1 }]);
    /// rows.write_csv("out.csv", true)?;
    /// # Ok(()) }
    /// ```
    pub fn write_csv(self, path: impl AsRef<Path>, has_headers: bool) -> Result<usize> {
        let v = self.collect_seq()?;
        write_csv_vec(path, has_headers, &v)
    }
}

#[cfg_attr(docsrs, doc(cfg(all(feature = "io-csv", feature = "parallel-io"))))]
#[cfg(all(feature = "io-csv", feature = "parallel-io"))]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the pipeline in parallel and write the result as CSV.
    ///
    /// This collects the result in parallel (respecting the runner's partition settings),
    /// then writes a single CSV file in deterministic order.
    ///
    /// # Arguments
    /// - `path`: Destination path.
    /// - `shards`: Optional parallelism hint for collection (partitions); `None` lets the runner decide.
    /// - `has_headers`: Whether to write a header row.
    ///
    /// # Errors
    /// An error is returned if collection or CSV serialization fails.
    ///
    /// # Example
    /// ```no_run
    /// use rustflow::*;
    /// use serde::Serialize;
    ///
    /// #[derive(Clone, Serialize)]
    /// struct Row { k: String, v: u64 }
    ///
    /// # fn main() -> anyhow::Result<()> {
    /// let p = Pipeline::default();
    /// let rows = from_vec(&p, vec![Row { k: "a".into(), v: 1 }]);
    /// rows.write_csv_par("out.csv", Some(4), true)?;
    /// # Ok(()) }
    /// ```
    pub fn write_csv_par(
        self,
        path: impl AsRef<Path>,
        shards: Option<usize>,
        has_headers: bool,
    ) -> Result<usize> {
        let data = self.collect_par(shards, None)?;
        write_csv_vec(path, has_headers, &data)
    }
}

/// Create a **streaming** CSV source, sharded by a fixed number of rows per shard.
///
/// This builds a `CsvShards` descriptor (counting rows up front) and inserts a `Source`
/// node that reads and deserializes only its shard when executed by the runner. Useful
/// for large files that don't fit comfortably in system memory.
///
/// *Enabled when the `io-csv` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `path`: CSV file path.
/// - `has_headers`: Whether the input CSV includes a header row.
/// - `rows_per_shard`: Target number of rows per shard (minimum 1).
///
/// # Errors
/// Returns an error if the file cannot be scanned or opened by the CSV reader.
///
/// # Example
/// ```no_run
/// use rustflow::*;
/// use serde::Deserialize;
///
/// #[derive(Clone, Deserialize)]
/// struct Row { k: String, v: u64 }
///
/// # fn main() -> anyhow::Result<()> {
/// let p = Pipeline::default();
/// let stream = read_csv_streaming::<Row>(&p, "big.csv", true, 50_000)?;
/// let out = stream.collect_seq()?; // materialize after transforms
/// # Ok(()) }
/// ```
#[cfg(feature = "io-csv")]
pub fn read_csv_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    has_headers: bool,
    rows_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: CsvShards = build_csv_shards(path, has_headers, rows_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: CsvVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}
