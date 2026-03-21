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

use crate::collection::FlatMapOp;
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

impl<T: RFBound> PCollection<T> {
    /// Filter and transform elements in one step using an `Option`-returning function.
    ///
    /// This is a convenience method that combines [`filter`](PCollection::filter) and
    /// [`map`](PCollection::map) into a single operation. Elements are kept when the
    /// function returns `Some(value)` and discarded when it returns `None`.
    ///
    /// This is particularly useful for:
    /// - Extracting specific variants from enums (see multi-output pattern example)
    /// - Parsing strings that may fail
    /// - Transforming data while filtering invalid entries
    ///
    /// ### Arguments
    /// - `f` -- Function that returns `Some(O)` to keep and transform, or `None` to discard
    ///
    /// ### Returns
    /// A new `PCollection<O>` containing only the transformed values from `Some` results.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// # use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec!["1", "2", "not a number", "3"]);
    ///
    /// let parsed = data.filter_map(|s: &&str| s.parse::<i32>().ok());
    ///
    /// assert_eq!(parsed.collect_seq()?, vec![1, 2, 3]);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ### Multi-Output Pattern
    /// This is especially useful for implementing the side outputs/multi-output pattern
    /// by filtering enum variants:
    /// ```no_run
    /// use ironbeam::*;
    /// # use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// #[derive(Clone)]
    /// enum RecordOutput {
    ///     Good(String),
    ///     Bad(String),
    /// }
    ///
    /// let p = Pipeline::default();
    /// let outputs = from_vec(&p, vec![
    ///     RecordOutput::Good("valid".into()),
    ///     RecordOutput::Bad("invalid".into()),
    ///     RecordOutput::Good("ok".into()),
    /// ]);
    ///
    /// // Extract only the Good variants
    /// let good = outputs.filter_map(|out: &RecordOutput| match out {
    ///     RecordOutput::Good(s) => Some(s.clone()),
    ///     RecordOutput::Bad(_) => None,
    /// });
    ///
    /// assert_eq!(good.collect_seq()?, vec!["valid", "ok"]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter_map<O, F>(&self, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: Fn(&T) -> Option<O> + Send + Sync + 'static,
    {
        // Use flat_map to implement filter_map
        self.apply_transform(Arc::new(FlatMapOp(
            move |elem: &T| f(elem).map_or_else(Vec::new, |o| vec![o]),
            PhantomData::<(T, O)>,
        )))
    }
}
