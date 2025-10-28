//! Batched transform helpers for [`PCollection`].
//!
//! Provides two batching operators for expensive transformations where
//! per-element closures are inefficient:
//!
//! - [`PCollection::map_batches`] — applies a function over fixed-size slices of
//!   elements (`&[T]`) and concatenates their results.
//! - [`PCollection::map_values_batches`] — same concept, but operates only on
//!   the *values* in a keyed collection `(K, V)`.
//!
//! Batching allows CPU-intensive or I/O-heavy transforms to amortize setup
//! costs, vectorize operations, or reuse buffers while preserving deterministic
//! ordering within partitions.

use crate::collection::{BatchMapOp, BatchMapValuesOp};
use crate::node::{DynOp, Node};
use crate::{PCollection, RFBound};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

impl<T: RFBound> PCollection<T> {
    /// Apply a **batched map** over elements of this collection.
    ///
    /// Instead of applying a function `f: T -> O` per element, this groups the
    /// data into consecutive slices of at most `batch_size` elements, passes
    /// each slice as `&[T]` to the function, and concatenates all returned
    /// `Vec<O>` results.
    ///
    /// This is ideal for expensive transforms where per-element function calls
    /// are too granular — for example, model inference, regex parsing, or
    /// external API lookups.
    ///
    /// # Type parameters
    /// - `O`: Output element type.
    /// - `F`: Function type implementing `Fn(&[T]) -> Vec<O>`.
    ///
    /// # Arguments
    /// - `batch_size`: Maximum number of elements in each slice passed to `f`.
    /// - `f`: The batched transform function.
    ///
    /// # Returns
    /// A new [`PCollection<O>`] representing the concatenated results of all
    /// batch outputs.
    ///
    /// # Example
    /// ```
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, (0..100).collect::<Vec<_>>());
    /// let doubled = data.map_batches(10, |chunk| {
    ///     chunk.iter().map(|x| x * 2).collect::<Vec<_>>()
    /// });
    /// ```
    pub fn map_batches<O, F>(self, batch_size: usize, f: F) -> PCollection<O>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&[T]) -> Vec<O>,
    {
        let op: Arc<dyn DynOp> = Arc::new(BatchMapOp::<T, O, F>(batch_size, f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
    /// Apply a **batched map** over the *values* of a keyed collection.
    ///
    /// Similar to [`map_batches`](PCollection::map_batches), but operates only
    /// on the value slices (`&[V]`) of each partition, preserving keys and the
    /// order of records.
    ///
    /// The provided function `f` receives a contiguous slice of values and must
    /// return a vector of outputs of the same length.
    ///
    /// # Contract
    /// `f(chunk).len()` **must equal** `chunk.len()`.
    /// Violations will trigger an `assert!` panic at runtime.
    ///
    /// # Arguments
    /// - `batch_size`: Number of values per batch.
    /// - `f`: Function to apply to each contiguous batch of values.
    ///
    /// # Returns
    /// A new [`PCollection<(K, O)>`] where each key maps to its transformed
    /// values.
    ///
    /// # Example
    /// ```
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let kvs = from_vec(&p, vec![("a".to_string(), 1), ("a".to_string(), 2)]);
    /// let squared = kvs.map_values_batches(2, |vals| {
    ///     vals.iter().map(|v| v * v).collect::<Vec<_>>()
    /// });
    /// ```
    pub fn map_values_batches<O, F>(self, batch_size: usize, f: F) -> PCollection<(K, O)>
    where
        O: RFBound,
        F: 'static + Send + Sync + Fn(&[V]) -> Vec<O>,
    {
        let op: Arc<dyn DynOp> = Arc::new(BatchMapValuesOp::<K, V, O, F>(
            batch_size,
            f,
            PhantomData,
        ));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}
