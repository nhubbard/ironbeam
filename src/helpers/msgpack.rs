//! `MessagePack` sources and sinks for [`PCollection`].
//!
//! `MessagePack` is a compact binary serialization format common in cache and
//! streaming systems. This module provides typed, serde-backed `MessagePack` I/O
//! that integrates with the Ironbeam pipeline. You can either:
//!
//! - **Vector I/O** -- read the whole file into memory or write an in-memory collection:
//!   - [`read_msgpack`] -> `PCollection<T>`
//!   - [`PCollection::write_msgpack`]
//!   - [`PCollection::write_msgpack_par`] (feature: `parallel-io`)
//!
//! - **Streaming I/O** -- build a source that shards a `MessagePack` file by record
//!   count and parses each shard lazily in the runner:
//!   - [`read_msgpack_streaming`] -> `PCollection<T>`
//!
//! All functions are serde-driven: your record type `T` should
//! `#[derive(serde::Deserialize)]` for reads and additionally
//! `#[derive(serde::Serialize)]` for writes.
//!
//! ## Feature flags
//! - `io-msgpack`: enables `MessagePack` helpers. This connector is **not** part of
//!   the default feature set; opt in explicitly to avoid pulling in `rmp-serde`.
//! - `parallel-io`: enables the parallel writer ([`PCollection::write_msgpack_par`]).
//!
//! ## Examples
//! Read a `MessagePack` file into a typed collection and write it back out:
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
//! let rows = read_msgpack::<Row>(&p, "data/input.msgpack")?;
//! let doubled = rows.map(|r: &Row| Row { k: r.k.clone(), v: r.v * 2 });
//! doubled.write_msgpack("data/out.msgpack")?;
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
//! let stream = read_msgpack_streaming::<Row>(&p, "data/input.msgpack", 100_000)?;
//! let out = stream.collect_seq()?; // materialize after transforms
//! # Ok(())
//! # }
//! ```

#![cfg_attr(docsrs, doc(cfg(feature = "io-msgpack")))]
#![cfg(feature = "io-msgpack")]

use crate::io::glob::expand_glob;
pub use crate::io::msgpack::{
    MsgpackShards, MsgpackVecOps, build_msgpack_shards, read_msgpack_vec, write_msgpack_vec,
};
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{PCollection, Pipeline, RFBound, from_vec};
use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

/// Read one or more `MessagePack` files into a typed `PCollection<T>` (vector mode).
///
/// This eagerly parses the entire file(s) into memory using `serde` and returns a
/// source collection. For very large files, prefer [`read_msgpack_streaming`].
///
/// ### Glob Pattern Support
///
/// The `path` parameter can be either:
/// - A single file path: `"data/input.msgpack"`
/// - A glob pattern: `"data/*.msgpack"` or `"data/year=2024/month=*/day=*/*.msgpack"`
///
/// When a glob pattern is provided, all matching files are read and concatenated
/// in sorted (lexicographic) order for deterministic results.
///
/// *Enabled when the `io-msgpack` feature is on.*
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
/// let rows = read_msgpack::<Row>(&p, "data/input.msgpack")?;
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
/// // Read all MessagePack files in the data directory
/// let rows = read_msgpack::<Row>(&p, "data/*.msgpack")?;
/// let out = rows.collect_seq()?;
/// # Ok(())
/// # }
/// ```
pub fn read_msgpack<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
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
                read_msgpack_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        let v = read_msgpack_vec::<T>(path)?;
        Ok(from_vec(p, v))
    }
}

/// Create a **streaming** `MessagePack` source, sharded by a fixed number of records.
///
/// This builds a [`MsgpackShards`] descriptor (counting records up front) and
/// inserts a `Source` node that reads and deserializes only its shard when
/// executed by the runner. Useful for large files that don't fit comfortably in
/// system memory.
///
/// *Enabled when the `io-msgpack` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `path`: `MessagePack` file path.
/// - `records_per_shard`: Target number of records per shard (minimum 1).
///
/// # Errors
/// Returns an error if the file cannot be scanned or opened.
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
/// let stream = read_msgpack_streaming::<Row>(&p, "data/input.msgpack", 100_000)?;
/// let out = stream.collect_seq()?; // materialize after transforms
/// # Ok(())
/// # }
/// ```
#[cfg_attr(docsrs, doc(cfg(feature = "io-msgpack")))]
#[cfg(feature = "io-msgpack")]
pub fn read_msgpack_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: RFBound + DeserializeOwned,
{
    let shards: MsgpackShards = build_msgpack_shards(path, records_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: MsgpackVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}

#[cfg(feature = "io-msgpack")]
impl<T: RFBound + Serialize> PCollection<T> {
    /// Execute the collection and write it to a `MessagePack` file (sequential).
    ///
    /// The entire collection is first collected into memory (sequentially) to
    /// preserve deterministic ordering, then written as one `MessagePack` file.
    ///
    /// Returns the number of records written.
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
    /// let n = rows.write_msgpack("data/out.msgpack")?;
    /// assert_eq!(n, 1);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Propagates I/O and serialization errors.
    pub fn write_msgpack(self, path: impl AsRef<Path>) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        write_msgpack_vec(path, &rows)
    }
}

#[cfg_attr(docsrs, doc(cfg(all(feature = "io-msgpack", feature = "parallel-io"))))]
#[cfg(all(feature = "io-msgpack", feature = "parallel-io"))]
impl<T: RFBound + Serialize + Send + Sync> PCollection<T> {
    /// Execute the collection sequentially (to lock in a deterministic order),
    /// then write `MessagePack` **in parallel** while preserving that order.
    ///
    /// `shards = Some(n)` sets the number of writer shards; `None` uses a sensible
    /// default.
    ///
    /// Returns the number of records written.
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
    /// rows.write_msgpack_par("data/out.msgpack", Some(4))?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    /// Propagates I/O and serialization errors.
    pub fn write_msgpack_par(self, path: impl AsRef<Path>, shards: Option<usize>) -> Result<usize> {
        let data = self.collect_seq()?; // deterministic order of elements
        crate::io::msgpack::write_msgpack_par(path, &data, shards)
    }
}
