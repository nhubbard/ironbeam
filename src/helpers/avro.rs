//! Avro sources and sinks for [`crate::collection::PCollection`].
//!
//! This module provides typed, serde-backed Avro I/O that integrates with the
//! Ironbeam pipeline. You can either:
//!
//! - **Vector I/O** -- read the whole file into memory or write an in-memory collection:
//!   - [`read_avro`] -> `PCollection<T>`
//!   - [`PCollection::write_avro_with_schema`]
//!
//! - **Streaming I/O** -- build a source that shards an Avro file by record count and
//!   parses each shard lazily in the runner:
//!   - [`read_avro_streaming`] -> `PCollection<T>`
//!
//! All functions are serde-driven: your record type `T` should `#[derive(serde::Deserialize)]`
//! for reads and additionally `#[derive(serde::Serialize)]` for writes.
//!
//! ## Feature flags
//! - `io-avro`: enables Avro helpers.
//! - `parallel-io`: enables the parallel writer (`write_avro_par`).
//!
//! ## Examples
//! Read an Avro file into a typed collection and write it back out (vector I/O):
//! ```no_run
//! use ironbeam::*;
//! use serde::{Deserialize, Serialize};
//! use anyhow::Result;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Row { k: String, v: u64 }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//!
//! // Vector read -> PCollection<Row>
//! let rows = read_avro::<Row>(&p, "data/input.avro")?;
//!
//! // Transform and write
//! let doubled = rows.map(|r: &Row| Row { k: r.k.clone(), v: r.v * 2 });
//! doubled.write_avro_with_schema("data/out.avro", r#"{"type":"record","name":"Row","fields":[{"name":"k","type":"string"},{"name":"v","type":"long"}]}"#)?;
//! # Ok(())
//! # }
//! ```
//!
//! Streaming read shard-by-shard (useful for large files):
//! ```no_run
//! use ironbeam::*;
//! use serde::{Deserialize, Serialize};
//! use anyhow::Result;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Row { k: String, v: u64 }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let stream = read_avro_streaming::<Row>(&p, "data/input.avro", 100_000)?;
//! let out = stream.collect_seq()?; // materialize after transforms
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use crate::io::avro::{AvroShards, AvroVecOps, build_avro_shards, read_avro_vec, write_avro_vec};
use crate::io::glob::expand_glob;
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{PCollection, Pipeline, RFBound, from_vec};
use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::path::Path;

#[cfg(feature = "io-avro")]
impl<T: RFBound + DeserializeOwned + Serialize> PCollection<T> {
    /// Execute the pipeline, collect results, and write them to a **single Avro file**.
    ///
    /// The Avro schema is inferred automatically from `T` using Serde's derive feature.
    /// If you haven't derived the [`Schema`](apache_avro::Schema) for your type,
    /// use [`write_avro_vec`] with an explicit schema string instead.
    ///
    /// The entire collection is first collected into memory (sequentially) to preserve
    /// deterministic ordering and then written as one Avro file.
    ///
    /// Returns the number of rows written.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    ///
    /// #[derive(Clone, Serialize, Deserialize)]
    /// struct Row { k: String, v: u64 }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let rows = from_vec(&p, vec![Row { k: "a".into(), v: 1 }]);
    /// // For types without schema derive, provide a schema string:
    /// let n = rows.write_avro_with_schema("data/out.avro", r#"{"type":"record","name":"Row","fields":[{"name":"k","type":"string"},{"name":"v","type":"long"}]}"#)?;
    /// assert_eq!(n, 1);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// If an error is encountered while writing the Avro file, a [`Result`] is returned.
    pub fn write_avro_with_schema(
        self,
        path: impl AsRef<Path>,
        schema: impl AsRef<str>,
    ) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        write_avro_vec(path, &rows, schema)
    }
}

#[cfg_attr(docsrs, doc(cfg(all(feature = "io-avro", feature = "parallel-io"))))]
#[cfg(all(feature = "io-avro", feature = "parallel-io"))]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the pipeline in parallel and write the result as Avro.
    ///
    /// This collects the result in parallel (respecting the runner's partition settings),
    /// then writes a single Avro file in deterministic order.
    ///
    /// `shards = Some(n)` sets the number of collection shards; `None` lets the runner decide.
    /// The schema is required and should be provided as a string.
    ///
    /// Returns the number of rows written.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use serde::{Serialize, Deserialize};
    /// use anyhow::Result;
    ///
    /// #[derive(Clone, Serialize, Deserialize)]
    /// struct Row { k: String, v: u64 }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let rows = from_vec(&p, vec![Row { k: "a".into(), v: 1 }]);
    /// // For types without schema derive, provide a schema string:
    /// rows.write_avro_par("data/out.avro", Some(4), r#"{"type":"record","name":"Row","fields":[{"name":"k","type":"string"},{"name":"v","type":"long"}]}"#)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// If an error is encountered while writing the Avro file, a [`Result`] is returned.
    pub fn write_avro_par(
        self,
        path: impl AsRef<Path>,
        shards: Option<usize>,
        schema: impl AsRef<str>,
    ) -> Result<usize> {
        let data = self.collect_par(shards, None)?;
        write_avro_vec(path, &data, schema)
    }
}

/// Read an Avro file(s) into a typed `PCollection<T>` (vector mode).
///
/// This eagerly parses the entire file(s) into memory using `serde` and returns
/// a source collection. For very large files, prefer [`read_avro_streaming`].
///
/// ### Glob Pattern Support
///
/// The `path` parameter can be either:
/// - A single file path: `"data/input.avro"`
/// - A glob pattern: `"data/*.avro"` or `"data/year=2024/month=*/day=*/*.avro"`
///
/// When a glob pattern is provided, all matching files are read and concatenated
/// in sorted (lexicographic) order for deterministic results.
///
/// *Enabled when the `io-avro` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `path`: File path or glob pattern to read.
///
/// # Errors
/// An error is returned if the file cannot be opened or if any record fails to deserialize.
///
/// # Panics
/// The code panics if the `path` parameter is invalid UTF-8 or the regex engine fails.
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
/// let rows = read_avro::<Row>(&p, "data/input.avro")?;
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
/// // Read all Avro files in the data directory
/// let rows = read_avro::<Row>(&p, "data/*.avro")?;
/// let out = rows.collect_seq()?;
/// # Ok(())
/// # }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "io-avro")))]
#[cfg(feature = "io-avro")]
pub fn read_avro<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let path_str = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow!("path contains invalid UTF-8"))?;

    // Check if the path contains glob patterns
    let glob_regex = Regex::new(r"[*?\[]").expect("valid glob regex");
    if glob_regex.is_match(path_str) {
        // Glob pattern - expand and read all matching files
        let files =
            expand_glob(path_str).with_context(|| format!("expanding glob pattern: {path_str}"))?;

        if files.is_empty() {
            bail!("no files found matching pattern: {path_str}");
        }

        let mut all_data = Vec::new();
        for file in files {
            let data: Vec<T> =
                read_avro_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        // Single file path
        let v = read_avro_vec::<T>(path)?;
        Ok(from_vec(p, v))
    }
}

/// Create a **streaming** Avro source, sharded by a fixed number of records per shard.
///
/// This builds an `AvroShards` descriptor (counting records up front) and inserts a `Source`
/// node that reads and deserializes only its shard when executed by the runner. Useful
/// for large files that don't fit comfortably in system memory.
///
/// *Enabled when the `io-avro` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `path`: Avro file path.
/// - `records_per_shard`: Target number of records per shard (minimum 1).
///
/// # Errors
/// Returns an error if the file cannot be scanned or opened by the Avro reader.
///
/// # Example
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
/// let stream = read_avro_streaming::<Row>(&p, "data/input.avro", 100_000)?;
/// let out = stream.collect_seq()?; // materialize after transforms
/// # Ok(())
/// # }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "io-avro")))]
#[cfg(feature = "io-avro")]
pub fn read_avro_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: AvroShards = build_avro_shards(path, records_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: AvroVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}
