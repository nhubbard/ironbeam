//! Batched transform helpers for [`PCollection`].
//!
//! Provides batching operators for expensive transformations where
//! per-element closures are inefficient, plus a per-key batch grouper:
//!
//! - [`PCollection::map_batches`] -- applies a function over fixed-size slices of
//!   elements (`&[T]`) and concatenates their results.
//! - [`PCollection::map_values_batches`] -- same concept, but operates only on
//!   the *values* in a keyed collection `(K, V)`.
//! - [`PCollection::group_into_batches`] -- groups per-key values into fixed-size
//!   `Vec` batches of at most `N` elements per key.
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
    /// are too granular -- for example, model inference, regex parsing, or
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
    /// ```no_run
    /// use ironbeam::*;
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
    /// return an output vector of the same length.
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
    /// ```no_run
    /// use ironbeam::*;
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
        let op: Arc<dyn DynOp> =
            Arc::new(BatchMapValuesOp::<K, V, O, F>(batch_size, f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    /// Group per-key values into fixed-size `Vec` batches of at most `batch_size`
    /// elements.
    ///
    /// Each input pair `(K, V)` is grouped by key, and then the per-key value
    /// list is split into consecutive chunks of at most `batch_size` items.
    /// Every chunk is emitted as a separate `(K, Vec<V>)` pair, sharing the same
    /// key. The final batch per key may be smaller than `batch_size`. Keys with
    /// no values are not emitted at all.
    ///
    /// This is the Ironbeam (batch-runner) equivalent of Apache Beam's
    /// `GroupIntoBatches.of(N)`. Beam's streaming variant emits partial batches
    /// when a timer fires; in a batch runner there are no timers, so batches are
    /// always emitted strictly at chunk boundaries within each key's full value
    /// set.
    ///
    /// # Arguments
    /// - `batch_size`: Maximum number of values per emitted batch. Must be > 0.
    ///
    /// # Memory
    /// This transform materializes each key's full value list via
    /// [`group_by_key`](Self::group_by_key) before chunking. For very wide keys
    /// where the full list won't fit in memory, prefer a `combine_values`-based
    /// summary instead.
    ///
    /// # Panics
    /// Panics if `batch_size == 0`.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// // Key "a" has 5 values, key "b" has 3 — with batch_size = 2 we get
    /// // ("a", [v, v]), ("a", [v, v]), ("a", [v]), ("b", [v, v]), ("b", [v]).
    /// let pairs = from_vec(
    ///     &p,
    ///     vec![
    ///         ("a", 1u32), ("a", 2), ("a", 3), ("a", 4), ("a", 5),
    ///         ("b", 10), ("b", 20), ("b", 30),
    ///     ],
    /// );
    /// let batched = pairs.group_into_batches(2);
    /// let out = batched.collect_seq().unwrap();
    /// // 5 emitted (k, batch) pairs total — batch sizes may be 2, 2, 1 for "a"
    /// // and 2, 1 for "b" (the order of keys is unspecified).
    /// assert_eq!(out.len(), 5);
    /// ```
    #[must_use]
    pub fn group_into_batches(self, batch_size: usize) -> PCollection<(K, Vec<V>)> {
        assert!(batch_size > 0, "group_into_batches requires batch_size > 0");
        self.group_by_key().flat_map(move |(k, vs): &(K, Vec<V>)| {
            vs.chunks(batch_size)
                .map(|chunk| (k.clone(), chunk.to_vec()))
                .collect()
        })
    }
}
