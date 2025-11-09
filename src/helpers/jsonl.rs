//! JSON Lines (JSONL) sources and sinks.
//!
//! This module provides both **eager** (vector) and **streaming** JSONL sources,
//! plus sequential and parallel writers. All APIs are serde-backed and type-safe.
//!
//! - `read_jsonl<T>()` reads the whole file into memory and returns a typed
//!   `PCollection<T>`.
//! - `read_jsonl_streaming<T>()` builds a streaming source by pre-scanning the
//!   file into `[start,end)` line ranges; each partition parses only its own range.
//! - `PCollection<T>::write_jsonl()` executes the collection and writes sequentially.
//! - `PCollection<T>::write_jsonl_par()` (feature `parallel-io`) executes
//!   sequentially for deterministic element order, then writes the file in
//!   parallel **while preserving global order**.
//!
//! ### Feature gates
//! - All functions in this module require `io-jsonl`.
//! - The parallel writer additionally requires `parallel-io`.
//!
//! ### Examples
//! Read and write (sequential):
//! ```no_run
//! use rustflow::*;
//! use serde::{Deserialize, Serialize};
//! # fn main() -> anyhow::Result<()> {
//! #[derive(Serialize, Deserialize, Clone)]
//! struct Row { k: String, v: u64 }
//!
//! let p = Pipeline::default();
//! let pc: PCollection<Row> = read_jsonl(&p, "data/input.jsonl")?;
//! let cnt = pc.clone().write_jsonl("data/out.jsonl")?;
//! assert!(cnt > 0);
//! # Ok(()) }
//! ```
//!
//! Streaming read:
//! ```no_run
//! use rustflow::*;
//! use serde::{Deserialize, Serialize};
//! # fn main() -> anyhow::Result<()> {
//! #[derive(Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd)]
//! struct Row { k: String, v: u64 }
//!
//! let p = Pipeline::default();
//! let pc: PCollection<Row> = read_jsonl_streaming(&p, "data/input.jsonl", 32)?;
//! // process, then collect deterministically for tests
//! let out = pc.collect_seq_sorted()?;
//! # Ok(()) }
//! ```
//!
//! Parallel write with stable order:
//! ```no_run
//! use rustflow::*;
//! use serde::{Deserialize, Serialize};
//! # fn main() -> anyhow::Result<()> {
//! #[derive(Serialize, Deserialize, Clone)]
//! struct Row { k: String, v: u64 }
//!
//! let p = Pipeline::default();
//! let pc = from_vec(&p, vec![Row { k: "a".into(), v: 1 }]);
//! // choose shard count or pass None to auto-size
//! let written = pc.write_jsonl_par("data/out.jsonl", Some(4))?;
//! assert_eq!(written, 1);
//! # Ok(()) }
//! ```

use crate::io::glob::expand_glob;
pub use crate::io::jsonl::{build_jsonl_shards, write_jsonl_vec, JsonlShards, JsonlVecOps};
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{from_vec, read_jsonl_vec, write_jsonl_par, PCollection, Pipeline, RFBound};
use anyhow::{Context, Result};
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

/// Read JSONL file(s) into a typed `PCollection<T>`.
///
/// This is an *eager* source: the file(s) are fully read and parsed into memory
/// before being wrapped as a `PCollection`.
///
/// # Glob Pattern Support
///
/// The `path` parameter can be either:
/// - A single file path: `"data/input.jsonl"`
/// - A glob pattern: `"logs/*.jsonl"` or `"data/year=2024/month=*/day=*/*.jsonl"`
///
/// When a glob pattern is provided, all matching files are read and concatenated
/// in sorted (lexicographic) order for deterministic results.
///
/// # Errors
/// Propagates I/O and JSON parsing errors with line context.
///
/// # Panics
/// Propagates I/O and JSON parsing errors with line context.
///
/// # Examples
///
/// Single file:
/// ```no_run
/// use rustflow::*;
/// use serde::{Deserialize, Serialize};
/// # fn main() -> anyhow::Result<()> {
/// #[derive(Serialize, Deserialize, Clone)]
/// struct Row { k: String, v: u64 }
///
/// let p = Pipeline::default();
/// let pc: PCollection<Row> = read_jsonl(&p, "data/input.jsonl")?;
/// let v = pc.collect_seq()?;
/// # Ok(()) }
/// ```
///
/// Glob pattern:
/// ```no_run
/// use rustflow::*;
/// use serde::{Deserialize, Serialize};
/// # fn main() -> anyhow::Result<()> {
/// #[derive(Serialize, Deserialize, Clone)]
/// struct Row { k: String, v: u64 }
///
/// let p = Pipeline::default();
/// // Read all JSONL files in the logs directory
/// let pc: PCollection<Row> = read_jsonl(&p, "logs/*.jsonl")?;
/// let v = pc.collect_seq()?;
/// # Ok(()) }
/// ```
///
/// Glob pattern:
/// ```no_run
/// use rustflow::*;
/// use serde::{Deserialize, Serialize};
/// # fn main() -> anyhow::Result<()> {
/// #[derive(Serialize, Deserialize, Clone)]
/// struct Row { k: String, v: u64 }
///
/// let p = Pipeline::default();
/// // Read all JSONL files in the logs directory
/// let pc: PCollection<Row> = read_jsonl(&p, "logs/*.jsonl")?;
/// let v = pc.collect_seq()?;
/// # Ok(()) }
/// ```
#[cfg(feature = "io-jsonl")]
pub fn read_jsonl<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
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
            let data: Vec<T> =
                read_jsonl_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        // Single file path
        let data: Vec<T> = read_jsonl_vec(path)?;
        Ok(from_vec(p, data))
    }
}

#[cfg(feature = "io-jsonl")]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the collection and write it to a JSONL file (sequential).
    ///
    /// Returns the number of records written. The output order matches the
    /// collection's sequential execution order.
    ///
    /// ### Errors
    /// Propagates I/O and serialization errors.
    pub fn write_jsonl(self, path: impl AsRef<Path>) -> Result<usize> {
        let data = self.collect_seq()?;
        write_jsonl_vec(path, &data)
    }
}

/// Create a **streaming** JSONL source that shards by line ranges.
///
/// The file is pre-scanned to compute `[start, end)` ranges, which are then
/// used by the `VecOps` to parse only the lines for each partition.
///
/// `lines_per_shard` controls shard granularity (minimum 1).
///
/// ### Errors
/// Propagates I/O and parsing errors during pre-scan or per-partition reads.
///
/// ### Example
/// ```no_run
/// use rustflow::*;
/// use serde::{Deserialize, Serialize};
/// # fn main() -> anyhow::Result<()> {
/// #[derive(Serialize, Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd)]
/// struct Row { k: String, v: u64 }
///
/// let p = Pipeline::default();
/// let pc: PCollection<Row> = read_jsonl_streaming(&p, "data/input.jsonl", 64)?;
/// let out = pc.collect_par_sorted(None, None)?;
/// # Ok(()) }
/// ```
#[cfg(feature = "io-jsonl")]
pub fn read_jsonl_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    lines_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: JsonlShards = build_jsonl_shards(path, lines_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: JsonlVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}

#[cfg_attr(docsrs, doc(cfg(all(feature = "io-jsonl", feature = "parallel-io"))))]
#[cfg(all(feature = "io-jsonl", feature = "parallel-io"))]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the collection sequentially (to lock in a deterministic order),
    /// then write JSONL **in parallel** while preserving that order.
    ///
    /// `shards = Some(n)` sets the number of writer shards; `None` uses a sensible default.
    ///
    /// Returns the number of records written.
    ///
    /// ### Errors
    /// Propagates I/O and serialization errors.
    pub fn write_jsonl_par(self, path: impl AsRef<Path>, shards: Option<usize>) -> Result<usize> {
        let data = self.collect_seq()?; // deterministic order of elements
        write_jsonl_par(path, &data, shards)
    }
}
