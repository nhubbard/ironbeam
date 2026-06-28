//! `TFRecord` sources and sinks for `PCollection`.
//!
//! `TFRecord` is a simple length-prefixed binary container with masked CRC-32C
//! checksums. It does not require TensorFlow. Each record is a raw byte buffer
//! (`Vec<u8>`), which you can encode/decode as any schema you like. For the
//! common case of TensorFlow's `tf.Example` protobuf format, see
//! `read_tfrecord_examples` (requires both `io-tfrecord` and `io-protobuf`).
//!
//! You can either:
//!
//! - **Vector I/O** — read the whole file into memory or write an in-memory collection:
//!   - `read_tfrecord` -> `PCollection<Vec<u8>>`
//!   - `PCollection::<Vec<u8>>::write_tfrecord`
//!   - `PCollection::<Vec<u8>>::write_tfrecord_par` (feature: `parallel-io`)
//!
//! - **Streaming I/O** — build a source that shards a `TFRecord` file by record
//!   count and reads each shard lazily in the runner:
//!   - `read_tfrecord_streaming` -> `PCollection<Vec<u8>>`
//!
//! ## Feature flags
//! - `io-tfrecord`: enables `TFRecord` helpers. **Not** part of the default feature
//!   set; opt in explicitly to avoid pulling in `crc32c`.
//! - `parallel-io`: enables the parallel writer.
//! - `io-protobuf`: combined with `io-tfrecord`, enables `read_tfrecord_examples`
//!   and `read_tfrecord_examples_vec` for `tf.Example` decoding.
//!
//! ## Examples
//! Write raw bytes as `TFRecord` then read them back:
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let records: Vec<Vec<u8>> = vec![b"hello".to_vec(), b"world".to_vec()];
//! write_tfrecord_vec("data/out.tfrecord", &records)?;
//!
//! let p = Pipeline::default();
//! let pc = read_tfrecord(&p, "data/out.tfrecord")?;
//! let back = pc.collect_seq()?;
//! assert_eq!(back, records);
//! # Ok(())
//! # }
//! ```

use crate::io::glob::expand_glob;
pub use crate::io::tfrecord::{
    TFRecordShards, TFRecordVecOps, build_tfrecord_shards, read_tfrecord_vec, write_tfrecord_vec,
};
use crate::node::Node;
use crate::type_token::TypeTag;
use crate::{Element, PCollection, Pipeline, from_vec};
use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

/// Read one or more `TFRecord` files into a `PCollection<Vec<u8>>` (vector mode).
///
/// Each record is returned as a raw `Vec<u8>`; no decoding is performed.
/// For `tf.Example` decoding, use `read_tfrecord_examples` instead.
///
/// ### Glob Pattern Support
///
/// The `path` parameter can be either:
/// - A single file path: `"data/input.tfrecord"`
/// - A glob pattern: `"data/*.tfrecord"`
///
/// When a glob pattern is provided, all matching files are read and concatenated
/// in sorted (lexicographic) order for deterministic results.
///
/// # Errors
/// Returns an error if the path contains invalid UTF-8, if a glob pattern does not
/// match any files, or if any file cannot be read or its CRC checks fail.
///
/// # Panics
/// Panics if the internal glob-detection regex cannot be compiled.
pub fn read_tfrecord(p: &Pipeline, path: impl AsRef<Path>) -> Result<PCollection<Vec<u8>>> {
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
            let data =
                read_tfrecord_vec(&file).with_context(|| format!("reading {}", file.display()))?;
            all_data.extend(data);
        }
        Ok(from_vec(p, all_data))
    } else {
        let v = read_tfrecord_vec(path)?;
        Ok(from_vec(p, v))
    }
}

/// Create a **streaming** `TFRecord` source, sharded by a fixed number of records.
///
/// Builds a [`TFRecordShards`] descriptor (counting records up front) and
/// inserts a `Source` node that reads only its shard when executed by the runner.
///
/// # Errors
/// Returns an error if the file cannot be scanned or opened.
pub fn read_tfrecord_streaming(
    p: &Pipeline,
    path: impl AsRef<Path>,
    records_per_shard: usize,
) -> Result<PCollection<Vec<u8>>> {
    let shards: TFRecordShards = build_tfrecord_shards(path, records_per_shard)?;
    let id = p.insert_node(Node::Source {
        payload: Arc::new(shards),
        vec_ops: TFRecordVecOps::new(),
        elem_tag: TypeTag::of::<Vec<u8>>(),
    });
    p.set_coder::<Vec<u8>>(id);
    Ok(PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    })
}

/// Read a `TFRecord` file and decode every record as a `tf.Example` protobuf,
/// returning a `PCollection<Example>`.
///
/// Requires **both** the `io-tfrecord` and `io-protobuf` features.
///
/// # Errors
/// Returns an error if the file cannot be opened, any CRC check fails, or any
/// record cannot be decoded as a `tf.Example`.
#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
pub fn read_tfrecord_examples(
    p: &Pipeline,
    path: impl AsRef<Path>,
) -> Result<PCollection<crate::io::tfrecord_proto::Example>> {
    let examples = crate::io::tfrecord::read_tfrecord_examples_vec(path)?;
    Ok(from_vec(p, examples))
}

impl PCollection<Vec<u8>> {
    /// Execute the collection and write it to a `TFRecord` file (sequential).
    ///
    /// Returns the number of records written.
    ///
    /// # Errors
    /// Propagates I/O errors.
    pub fn write_tfrecord(self, path: impl AsRef<Path>) -> Result<usize> {
        let rows: Vec<Vec<u8>> = self.collect_seq()?;
        write_tfrecord_vec(path, &rows)
    }
}

#[cfg(feature = "parallel-io")]
impl PCollection<Vec<u8>> {
    /// Execute the collection sequentially (to lock in a deterministic order),
    /// then write `TFRecord` **in parallel** while preserving that order.
    ///
    /// `shards = Some(n)` sets the number of writer shards; `None` uses a
    /// sensible default.
    ///
    /// Returns the number of records written.
    ///
    /// # Errors
    /// Propagates I/O errors.
    pub fn write_tfrecord_par(
        self,
        path: impl AsRef<Path>,
        shards: Option<usize>,
    ) -> Result<usize> {
        let data = self.collect_seq()?;
        crate::io::tfrecord::write_tfrecord_par(path, &data, shards)
    }
}

// Re-export tf.Example types so callers don't need to reach into io internals.
#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
pub use crate::io::tfrecord_proto::{BytesList, Example, Feature, Features, FloatList, Int64List};

// Ensure Element is available for type inference in the streaming helper above.
// (The bound appears in read_tfrecord_streaming via TypeTag and set_coder.)
const _: fn() = || {
    const fn assert_element<T: Element>() {}
    assert_element::<Vec<u8>>();
};
