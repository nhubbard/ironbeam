//! Arrow IPC sources and sinks for `PCollection`.
//!
//! Arrow IPC is a binary column-store format. This module exposes two parallel
//! APIs:
//!
//! - **Row-level** — element type is `T: Serialize + DeserializeOwned` (via
//!   `serde_arrow`):
//!   - `read_arrow_ipc::<T>(p, path)` → `PCollection<T>`
//!   - `read_arrow_ipc_streaming::<T>(p, path, bps)` → `PCollection<T>`
//!   - `PCollection::<T>::write_arrow_ipc_rows(path)` → `Result<usize>`
//!   - `PCollection::<T>::write_arrow_ipc_rows_par(path, shards)` (parallel-io)
//!
//! - **Batch-level** — element type is [`ArrowBatch`] (serde wrapper around
//!   `RecordBatch`):
//!   - `read_arrow_ipc_batches(p, path)` → `PCollection<ArrowBatch>`
//!   - `PCollection::<ArrowBatch>::write_arrow_ipc_batches(path)` → `Result<usize>`
//!   - `PCollection::<ArrowBatch>::write_arrow_ipc_batches_par(path, shards)` (parallel-io)
//!
//! ## Feature flags
//! - `io-arrow`: enables Arrow IPC helpers. **Not** part of the default feature
//!   set; shares `arrow` and `serde_arrow` with `io-parquet`.
//! - `parallel-io`: enables the parallel write variants.
//!
//! ## Examples
//! Write typed rows as Arrow IPC then read them back:
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Row { id: u32, name: String }
//!
//! # fn main() -> Result<()> {
//! let rows = vec![Row { id: 1, name: "alice".into() }];
//! write_arrow_ipc_rows_vec("data/out.arrow", &rows)?;
//!
//! let p = Pipeline::default();
//! let pc = read_arrow_ipc::<Row>(&p, "data/out.arrow")?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "io-arrow")]
use crate::io::arrow_ipc::{
    ArrowBatch, ArrowBatchVecOps, ArrowRowVecOps, ArrowShards, build_arrow_shards,
    read_arrow_ipc_rows_vec, read_arrow_ipc_vec, write_arrow_ipc_rows_vec, write_arrow_ipc_vec,
};
#[cfg(feature = "io-arrow")]
use crate::io::glob::expand_glob;
#[cfg(feature = "io-arrow")]
use crate::node::Node;
#[cfg(feature = "io-arrow")]
use crate::type_token::TypeTag;
#[cfg(feature = "io-arrow")]
use crate::{Element, PCollection, Pipeline, from_vec};
#[cfg(feature = "io-arrow")]
use anyhow::{Context, Result, anyhow, bail};
#[cfg(feature = "io-arrow")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "io-arrow")]
use regex::Regex;
#[cfg(feature = "io-arrow")]
use serde::Serialize;
#[cfg(feature = "io-arrow")]
use serde::de::DeserializeOwned;
#[cfg(feature = "io-arrow")]
use std::marker::PhantomData;
#[cfg(feature = "io-arrow")]
use std::path::Path;
#[cfg(feature = "io-arrow")]
use std::sync::Arc;

// ── Row-level API ─────────────────────────────────────────────────────────────

/// Read one or more Arrow IPC files into a `PCollection<T>` (eager, row-level).
///
/// Each row is deserialized via `serde_arrow::from_record_batch`. Glob patterns
/// (`*`, `?`, `[...]`) are detected and expanded; matching files are read and
/// concatenated in sorted order.
///
/// # Errors
/// Returns an error if the path is invalid UTF-8, the glob does not match any
/// files, or any file cannot be read or deserialized.
///
/// # Panics
/// Panics if the internal glob-detection regex cannot be compiled.
#[cfg(feature = "io-arrow")]
pub fn read_arrow_ipc<T>(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<T>>
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
        let mut all: Vec<T> = Vec::new();
        for file in files {
            let rows = read_arrow_ipc_rows_vec::<T>(&file)
                .with_context(|| format!("reading {}", file.display()))?;
            all.extend(rows);
        }
        Ok(from_vec(p, all))
    } else {
        let v = read_arrow_ipc_rows_vec(path)?;
        Ok(from_vec(p, v))
    }
}

/// Create a **streaming** Arrow IPC row source, sharded by `RecordBatch` count.
///
/// Builds [`ArrowShards`] (counting batches and rows up front) and inserts a
/// `Source` node that reads only its shard when executed.
///
/// # Errors
/// Returns an error if the file cannot be scanned.
#[cfg(feature = "io-arrow")]
pub fn read_arrow_ipc_streaming<T>(
    p: &Pipeline,
    path: impl AsRef<Path>,
    batches_per_shard: usize,
) -> Result<PCollection<T>>
where
    T: Element + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let shards: ArrowShards = build_arrow_shards(path, batches_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: ArrowRowVecOps::<T>::new(),
        elem_tag: TypeTag::of::<T>(),
    });
    p.set_coder::<T>(id);
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}

// ── Batch-level API ───────────────────────────────────────────────────────────

/// Read one or more Arrow IPC files into a `PCollection<ArrowBatch>`.
///
/// Glob patterns are supported and matched files are concatenated in sorted
/// order.
///
/// # Errors
/// Returns an error if the path is invalid, the glob fails, or any file cannot
/// be read.
///
/// # Panics
/// Panics if the internal glob-detection regex cannot be compiled.
#[cfg(feature = "io-arrow")]
pub fn read_arrow_ipc_batches(
    p: &Pipeline,
    path: impl AsRef<Path>,
) -> Result<PCollection<ArrowBatch>> {
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
        let mut all: Vec<ArrowBatch> = Vec::new();
        for file in files {
            let batches =
                read_arrow_ipc_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all.extend(batches.into_iter().map(ArrowBatch));
        }
        Ok(from_vec(p, all))
    } else {
        let batches = read_arrow_ipc_vec(path)?;
        let wrapped: Vec<ArrowBatch> = batches.into_iter().map(ArrowBatch).collect();
        Ok(from_vec(p, wrapped))
    }
}

// ── PCollection impls ─────────────────────────────────────────────────────────

#[cfg(feature = "io-arrow")]
impl<T: Element + Serialize + DeserializeOwned + 'static> PCollection<T> {
    /// Execute the collection and write rows to an Arrow IPC file (sequential).
    ///
    /// Returns the number of rows written.
    ///
    /// # Errors
    /// Propagates I/O or serialization errors.
    pub fn write_arrow_ipc_rows(self, path: impl AsRef<Path>) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        write_arrow_ipc_rows_vec(path, &rows)
    }
}

#[cfg(feature = "io-arrow")]
impl PCollection<ArrowBatch> {
    /// Execute the collection and write `RecordBatch`es to an Arrow IPC file.
    ///
    /// Returns the total number of rows written.
    ///
    /// # Errors
    /// Propagates I/O errors.
    pub fn write_arrow_ipc_batches(self, path: impl AsRef<Path>) -> Result<usize> {
        let wrapped: Vec<ArrowBatch> = self.collect_seq()?;
        let batches: Vec<RecordBatch> = wrapped.into_iter().map(|b| b.0).collect();
        write_arrow_ipc_vec(path, &batches)
    }
}

#[cfg(all(feature = "io-arrow", feature = "parallel-io"))]
impl<T: Element + Serialize + DeserializeOwned + Send + Sync + 'static> PCollection<T> {
    /// Execute the collection sequentially (locking in order), then write rows
    /// to an Arrow IPC file **in parallel**.
    ///
    /// `shards = Some(n)` sets the shard count; `None` uses a sensible default.
    /// Returns the number of rows written.
    ///
    /// # Errors
    /// Propagates I/O or serialization errors.
    pub fn write_arrow_ipc_rows_par(
        self,
        path: impl AsRef<Path>,
        shards: Option<usize>,
    ) -> Result<usize> {
        let rows: Vec<T> = self.collect_seq()?;
        crate::io::arrow_ipc::write_arrow_ipc_rows_par(path, &rows, shards)
    }
}

#[cfg(all(feature = "io-arrow", feature = "parallel-io"))]
impl PCollection<ArrowBatch> {
    /// Execute the collection sequentially, then write `RecordBatch`es to an
    /// Arrow IPC file **in parallel**.
    ///
    /// Returns the total number of rows written.
    ///
    /// # Errors
    /// Propagates I/O errors.
    pub fn write_arrow_ipc_batches_par(
        self,
        path: impl AsRef<Path>,
        shards: Option<usize>,
    ) -> Result<usize> {
        let wrapped: Vec<ArrowBatch> = self.collect_seq()?;
        let batches: Vec<RecordBatch> = wrapped.into_iter().map(|b| b.0).collect();
        crate::io::arrow_ipc::write_arrow_ipc_par(path, &batches, shards)
    }
}
