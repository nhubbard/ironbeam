//! Standard library helpers for constructing `PCollection`s.
//!
//! These helpers create in-memory sources for pipelines directly from
//! native Rust data structures like `Vec<T>` or iterators. They're ideal for
//! tests, demos, or pipelines where data is small and self-contained -- avoiding
//! external I/O layers such as JSONL, CSV, or Parquet.
//!
//! ### Overview
//! - [`from_vec`] -- Converts a `Vec<T>` into a `PCollection<T>` source node.
//! - [`from_iter`] -- Builds a `PCollection<T>` from any `IntoIterator<Item = T>`.
//! - [`from_custom_source`] -- Create a `PCollection<T>` from a custom data source.
//!
//! These utilities insert a [`Node::Source`] into the [`Pipeline`] graph using
//! a type-aware vector operations handler derived from `vec_ops_for::<T>()`.
//!
//! ### Example
//! ```no_run
//! use ironbeam::*;
//!
//! let p = Pipeline::default();
//!
//! // Create a PCollection directly from a Vec
//! let words = from_vec(&p, vec!["alpha", "beta", "gamma"]);
//!
//! // Or from any iterable sequence
//! let numbers = from_iter(&p, 1..=5);
//!
//! // Basic transform
//! let squared = numbers.map(|n| n * n);
//! assert_eq!(squared.collect_seq().unwrap(), vec![1, 4, 9, 16, 25]);
//! ```

use crate::node::Node;
use crate::type_token::{TypeTag, VecOps, vec_ops_for};
use crate::{PCollection, Pipeline, RFBound};
use std::marker::PhantomData;
use std::sync::Arc;

/// Create a [`PCollection<T>`] from a pre-existing [`Vec<T>`].
///
/// This function inserts a [`Node::Source`] node into the provided [`Pipeline`],
/// wrapping the given vector in an `Arc` and recording its type metadata.
///
/// The resulting `PCollection` acts as a root source for later transforms.
///
/// ### Arguments
/// - `p` -- The pipeline to attach the source node to.
/// - `data` -- The in-memory vector to use as the data source.
///
/// ### Returns
/// A [`PCollection<T>`] representing the vector as a stream of elements.
///
/// ### Example
/// ```no_run
/// use ironbeam::*;
///
/// let p = Pipeline::default();
/// let numbers = vec![10, 20, 30];
/// let pc = from_vec(&p, numbers);
/// assert_eq!(pc.collect_seq().unwrap(), vec![10, 20, 30]);
/// ```
#[must_use]
pub fn from_vec<T>(p: &Pipeline, data: Vec<T>) -> PCollection<T>
where
    T: RFBound,
{
    let id = p.insert_node(Node::Source {
        payload: Arc::new(data),
        vec_ops: vec_ops_for::<T>(),
        elem_tag: TypeTag::of::<T>(),
    });
    PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    }
}

/// Create a [`PCollection<T>`] from any iterator or collection implementing [`IntoIterator`].
///
/// Internally collects the iterator into a [`Vec<T>`] and delegates to [`from_vec`].
///
/// ### Arguments
/// - `p` -- The pipeline to attach the source node to.
/// - `iter` -- Any `IntoIterator<Item = T>` -- e.g., a range, vector, or array.
///
/// ### Example
/// ```no_run
/// use ironbeam::*;
///
/// let p = Pipeline::default();
///
/// // Build a PCollection from a range
/// let nums = from_iter(&p, 1..=4);
/// assert_eq!(nums.collect_seq().unwrap(), vec![1, 2, 3, 4]);
///
/// // Build from a vector of strings
/// let strs = from_iter(&p, vec!["x", "y", "z"]);
/// assert_eq!(strs.collect_seq().unwrap(), vec!["x", "y", "z"]);
/// ```
pub fn from_iter<T, I>(p: &Pipeline, iter: I) -> PCollection<T>
where
    T: RFBound,
    I: IntoIterator<Item = T>,
{
    from_vec(p, iter.into_iter().collect::<Vec<T>>())
}

/// Create a [`PCollection<T>`] from a custom data source.
///
/// This is the primary extension point for integrating custom I/O formats or data sources.
/// Your custom source must provide a [`VecOps`] implementation that knows how to split,
/// count, and clone the source data.
///
/// ### Type Safety
/// The payload is type-erased at runtime via `Arc<dyn Any>`, so your `VecOps` implementation
/// must correctly downcast to the expected type. Mismatches will cause runtime panics.
///
/// ### Use Cases
/// - Custom file formats (Avro, Protocol Buffers, Message Pack, etc.)
/// - Database connections with custom sharding
/// - Streaming data sources with buffering
/// - External data APIs with pagination
///
/// ### Arguments
/// - `p` -- The pipeline to attach the source node to
/// - `payload` -- Your custom data source (e.g., connection handle, file metadata, and shard info)
/// - `vec_ops` -- Implementation of [`VecOps`] that knows how to work with your payload type
///
/// ### Example
///
/// See `tests/extensions.rs` in the source code for a complete working example
/// of implementing a custom data source with `VecOps`.
///
/// ```
/// use ironbeam::*;
/// use ironbeam::type_token::VecOps;
/// use std::any::Any;
/// use std::sync::Arc;
/// use anyhow::Result;
/// # fn main() -> Result<()> {
/// // Simple example: source from a pre-computed Vec
/// let p = Pipeline::default();
/// let data = vec![1, 2, 3, 4, 5];
///
/// // Use vec_ops_for helper for simple cases
/// let collection: PCollection<i32> = from_custom_source(
///     &p,
///     data,
///     type_token::vec_ops_for::<i32>()
/// );
///
/// let results = collection.collect_seq()?;
/// assert_eq!(results, vec![1, 2, 3, 4, 5]);
/// # Ok(())
/// # }
/// ```
pub fn from_custom_source<T, P>(
    p: &Pipeline,
    payload: P,
    vec_ops: Arc<dyn VecOps>,
) -> PCollection<T>
where
    T: RFBound,
    P: 'static + Send + Sync,
{
    let id = p.insert_node(Node::Source {
        payload: Arc::new(payload),
        vec_ops,
        elem_tag: TypeTag::of::<T>(),
    });
    PCollection {
        pipeline: p.clone(),
        id,
        _t: PhantomData,
    }
}
