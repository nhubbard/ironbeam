//! Batched transform helpers for [`PCollection`].
//!
//! Provides batching operators for expensive transformations where
//! per-element closures are inefficient, plus per-key and per-partition batch
//! grouping helpers:
//!
//! - [`PCollection::map_batches`] -- applies a function over fixed-size slices of
//!   elements (`&[T]`) and concatenates their results.
//! - [`PCollection::map_values_batches`] -- same concept, but operates only on
//!   the *values* in a keyed collection `(K, V)`.
//! - [`PCollection::group_into_batches`] -- groups per-key values into fixed-size
//!   `Vec` batches of at most `N` elements per key.
//! - [`PCollection::batch_elements`] -- groups consecutive elements within each
//!   partition into `Vec<T>` batches of at most `N` elements (not per-key).
//! - [`PCollection::batch_by_size`] -- groups consecutive elements within each
//!   partition into `Vec<T>` batches whose caller-estimated total byte size
//!   does not exceed a limit.
//!
//! Batching allows CPU-intensive or I/O-heavy transforms to amortize setup
//! costs, vectorize operations, or reuse buffers while preserving deterministic
//! ordering within partitions.

use crate::collection::{BatchBySizeOp, BatchElementsOp, BatchMapOp, BatchMapValuesOp};
use crate::node::{DynOp, Node};
use crate::{Element, PCollection};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

impl<T: Element> PCollection<T> {
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
        O: Element,
        F: 'static + Send + Sync + Fn(&[T]) -> Vec<O>,
    {
        let op: Arc<dyn DynOp> = Arc::new(BatchMapOp::<T, O, F>(batch_size, f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        self.pipeline.set_coder::<O>(id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    /// Group consecutive elements within each partition into `Vec<T>` batches
    /// of at most `batch_size` elements.
    ///
    /// Unlike [`group_into_batches`](Self::group_into_batches), this transform
    /// is **not per-key** — it operates on the entire element stream of each
    /// partition. Unlike [`map_batches`](Self::map_batches), which adapts to
    /// the runner's internal chunking and immediately reconstructs a flat
    /// `PCollection`, this transform **emits** the batches as the output
    /// element type, so downstream stages observe `Vec<T>` values directly.
    ///
    /// This is the Ironbeam equivalent of Apache Beam's
    /// `BatchElements(min_batch_size=N, max_batch_size=N)` for a fixed count.
    ///
    /// # Batching boundary semantics
    ///
    /// Batches are formed within each partition independently. A batch never
    /// crosses partition boundaries, so the total number of emitted batches is
    /// `sum_p(ceil(|partition_p| / batch_size))` across all partitions `p`.
    /// Under parallel execution with multiple partitions, this can yield more
    /// (smaller) terminal batches than a fully sequential run. For a strict
    /// global batching guarantee, call `collect_seq` or apply a `Reshuffle(1)`
    /// barrier upstream.
    ///
    /// # Arguments
    /// - `batch_size`: Maximum number of elements in each emitted batch.
    ///   `0` is silently clamped to `1`.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, (0u32..10).collect::<Vec<_>>());
    /// let batches = data.batch_elements(3).collect_seq().unwrap();
    /// // Within a single sequential partition: [[0,1,2], [3,4,5], [6,7,8], [9]]
    /// assert_eq!(batches.len(), 4);
    /// assert_eq!(batches[0], vec![0u32, 1, 2]);
    /// assert_eq!(batches[3], vec![9u32]);
    /// ```
    #[must_use]
    pub fn batch_elements(self, batch_size: usize) -> PCollection<Vec<T>> {
        let op: Arc<dyn DynOp> = Arc::new(BatchElementsOp::<T>(batch_size, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        self.pipeline.set_coder::<Vec<T>>(id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }

    /// Group consecutive elements within each partition into `Vec<T>` batches
    /// whose caller-estimated total byte size does not exceed `max_bytes`.
    ///
    /// Each element's contribution is computed by `size_fn(&T) -> usize`. A
    /// batch is closed and a new one started when adding the next element
    /// would make the running total exceed `max_bytes`. A single element whose
    /// own estimated size already exceeds `max_bytes` is emitted alone in its
    /// own batch (rather than being silently dropped).
    ///
    /// This is the size-bounded variant of [`batch_elements`](Self::batch_elements),
    /// matching Apache Beam's `BatchElements` size-bound flavor.
    ///
    /// # Why a `size_fn` callback?
    ///
    /// Rust has no universal "estimated serialized size" trait, and
    /// `std::mem::size_of_val` only returns the stack footprint of `T` — for
    /// types like `String` or `Vec<U>`, the heap-allocated payload is not
    /// counted. Requiring an explicit callback lets callers pick whatever
    /// estimation makes sense (e.g. `|s: &String| s.len()` for raw byte
    /// length, `|t| serde_json::to_string(t).map(|s| s.len()).unwrap_or(0)`
    /// for serialized size, or a domain-specific heuristic).
    ///
    /// # Batching boundary semantics
    ///
    /// Same as [`batch_elements`](Self::batch_elements): batches never cross
    /// partition boundaries. The total number of emitted batches under
    /// parallel execution can be larger than under sequential execution.
    ///
    /// # Arguments
    /// - `max_bytes`: Maximum estimated total size per batch. `0` is silently
    ///   clamped to `1`.
    /// - `size_fn`: Caller-supplied size estimator. Must be `Send + Sync`.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let data = from_vec(
    ///     &p,
    ///     vec!["aa".to_string(), "bb".into(), "cccc".into(), "d".into()],
    /// );
    /// // Bytes per element: 2, 2, 4, 1. With max_bytes=4 we get:
    /// // [["aa", "bb"], ["cccc"], ["d"]]
    /// let batches = data
    ///     .batch_by_size(4, |s: &String| s.len())
    ///     .collect_seq()
    ///     .unwrap();
    /// assert_eq!(batches.len(), 3);
    /// ```
    #[must_use]
    pub fn batch_by_size<F>(self, max_bytes: usize, size_fn: F) -> PCollection<Vec<T>>
    where
        F: 'static + Send + Sync + Fn(&T) -> usize,
    {
        let op: Arc<dyn DynOp> = Arc::new(BatchBySizeOp::<T, F>(max_bytes, size_fn, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        self.pipeline.set_coder::<Vec<T>>(id);
        PCollection {
            pipeline: self.pipeline,
            id,
            _t: PhantomData,
        }
    }
}

impl<K: Element + Eq + Hash, V: Element> PCollection<(K, V)> {
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
        O: Element,
        F: 'static + Send + Sync + Fn(&[V]) -> Vec<O>,
    {
        let op: Arc<dyn DynOp> =
            Arc::new(BatchMapValuesOp::<K, V, O, F>(batch_size, f, PhantomData));
        let id = self.pipeline.insert_node(Node::Stateless(vec![op]));
        self.pipeline.connect(self.id, id);
        self.pipeline.set_coder::<(K, O)>(id);
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
    ///
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
    ///         ("a".to_string(), 1u32), ("a".to_string(), 2), ("a".to_string(), 3), ("a".to_string(), 4), ("a".to_string(), 5),
    ///         ("b".to_string(), 10), ("b".to_string(), 20), ("b".to_string(), 30),
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
