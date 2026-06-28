//! Protocol Buffers sources and sinks for `PCollection`.
//!
//! Protocol Buffers is a strongly-typed binary serialization format. This module
//! provides `prost`-backed protobuf I/O that integrates with the Ironbeam pipeline.
//! You can either:
//!
//! - **Vector I/O** — read the whole file into memory or write an in-memory collection:
//!   - `read_proto` -> `PCollection<T>`
//!   - `PCollection::write_proto`
//!   - `PCollection::write_proto_par` (feature: `parallel-io`)
//!
//! - **Streaming I/O** — build a source that shards a protobuf file by record count
//!   and parses each shard lazily in the runner:
//!   - `read_proto_streaming` -> `PCollection<T>`
//!
//! All functions are prost-driven: your record type `T` must implement
//! `prost::Message`. For reads, `T` also needs `Default`.
//!
//! ## Feature flags
//! - `io-protobuf`: enables protobuf helpers. This connector is **not** part of the
//!   default feature set; opt in explicitly to avoid pulling in `prost`.
//! - `parallel-io`: enables the parallel writer (`PCollection::write_proto_par`).
//!
//! ## Examples
//! Read a protobuf file into a typed collection and write it back out:
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//! use serde::{Serialize, Deserialize};
//!
//! // prost::Message auto-derives Default; do not also derive Default.
//! #[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
//! struct Row {
//!     #[prost(string, tag = "1")]
//!     pub name: String,
//!     #[prost(int64, tag = "2")]
//!     pub value: i64,
//! }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let rows = read_proto::<Row>(&p, "data/input.proto")?;
//! let doubled = rows.map(|r: &Row| Row { name: r.name.clone(), value: r.value * 2 });
//! doubled.write_proto("data/out.proto")?;
//! # Ok(())
//! # }
//! ```
//!
//! Streaming read shard-by-shard (useful for large files):
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//! use serde::{Serialize, Deserialize};
//!
//! // prost::Message auto-derives Default; do not also derive Default.
//! #[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
//! struct Row {
//!     #[prost(string, tag = "1")]
//!     pub name: String,
//! }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let stream = read_proto_streaming::<Row>(&p, "data/input.proto", 100_000)?;
//! let out = stream.collect_seq()?;
//! # Ok(())
//! # }
//! ```

// Always-available re-exports: ProtoShards and ProtoVecOps have no prost types,
// and build_proto_shards has a stub for the disabled case.
pub use crate::io::protobuf::{ProtoShards, ProtoVecOps, build_proto_shards};

// Feature-gated re-exports: these reference prost types.
#[cfg(feature = "io-protobuf")]
pub use crate::io::protobuf::{read_proto_vec, write_proto_vec};

#[cfg(feature = "io-protobuf")]
use crate::io::glob::expand_glob;
#[cfg(feature = "io-protobuf")]
use crate::node::Node;
#[cfg(feature = "io-protobuf")]
use crate::type_token::TypeTag;
#[cfg(feature = "io-protobuf")]
use crate::{Element, PCollection, Pipeline, from_vec};
#[cfg(feature = "io-protobuf")]
use anyhow::{Context, Result, anyhow, bail};
#[cfg(feature = "io-protobuf")]
use regex::Regex;
#[cfg(feature = "io-protobuf")]
use std::marker::PhantomData;
#[cfg(feature = "io-protobuf")]
use std::path::Path;
#[cfg(feature = "io-protobuf")]
use std::sync::Arc;

/// Read one or more length-delimited protobuf files into a typed `PCollection<T>`
/// (vector mode).
///
/// This eagerly decodes the entire file(s) into memory using `prost` and returns a
/// source collection. For very large files, prefer [`read_proto_streaming`].
///
/// ### Glob Pattern Support
///
/// The `path` parameter can be either:
/// - A single file path: `"data/input.proto"`
/// - A glob pattern: `"data/*.proto"` or `"shards/part-*.proto"`
///
/// When a glob pattern is provided, all matching files are read and concatenated
/// in sorted (lexicographic) order for deterministic results.
///
/// *Enabled when the `io-protobuf` feature is on.*
///
/// # Errors
/// Returns an error if `path` contains invalid UTF-8, if a glob pattern does not
/// match any files, or if any matched file cannot be read or decoded.
///
/// # Panics
/// Panics if the internal glob-detection regex cannot be compiled — not reachable
/// in practice because the pattern is a compile-time constant.
#[cfg(feature = "io-protobuf")]
pub fn read_proto<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
where
    T: Element + prost::Message + Default,
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
                read_proto_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        let v = read_proto_vec::<T>(path)?;
        Ok(from_vec(p, v))
    }
}

/// Create a **streaming** protobuf source, sharded by a fixed number of records.
///
/// This builds a [`ProtoShards`] descriptor (counting records up front) and inserts a
/// `Source` node that reads and decodes only its shard when executed by the runner.
/// Useful for large files that don't fit comfortably in system memory.
///
/// *Enabled when the `io-protobuf` feature is on.*
///
/// # Arguments
/// - `p`: Pipeline to attach the source to.
/// - `path`: Protobuf file path.
/// - `records_per_shard`: Target number of records per shard (minimum 1).
///
/// # Errors
/// Returns an error if the file cannot be scanned or opened.
#[cfg(feature = "io-protobuf")]
pub fn read_proto_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: Element + prost::Message + Default,
{
    let shards: ProtoShards = build_proto_shards(path, records_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: ProtoVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    p.set_coder::<T>(id);
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}

#[cfg(feature = "io-protobuf")]
impl<T: Element + prost::Message> PCollection<T> {
    /// Execute the collection and write it to a length-delimited protobuf file
    /// (sequential).
    ///
    /// The entire collection is first collected into memory, then written as one file.
    ///
    /// Returns the number of records written.
    ///
    /// # Errors
    /// Propagates I/O and encoding errors.
    pub fn write_proto(self, path: impl AsRef<Path>) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        write_proto_vec(path, &rows)
    }
}

#[cfg(all(feature = "parallel-io", feature = "io-protobuf"))]
impl<T: Element + prost::Message + Send + Sync> PCollection<T> {
    /// Execute the collection sequentially (to lock in a deterministic order), then
    /// write protobuf records **in parallel** while preserving that order.
    ///
    /// `shards = Some(n)` sets the number of writer shards; `None` uses a sensible
    /// default.
    ///
    /// Returns the number of records written.
    ///
    /// # Errors
    /// Propagates I/O and encoding errors.
    pub fn write_proto_par(self, path: impl AsRef<Path>, shards: Option<usize>) -> Result<usize> {
        let data = self.collect_seq()?;
        crate::io::protobuf::write_proto_par(path, &data, shards)
    }
}
