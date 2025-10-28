//! Sorted collection helpers for [`PCollection`].
//!
//! This module provides convenience methods to **collect and sort** a
//! [`PCollection`] either sequentially or in parallel.
//!
//! - [`PCollection::collect_seq_sorted`] — collects results on a single thread and sorts them.
//! - [`PCollection::collect_par_sorted`] — collects results in parallel (via partitioned execution) and sorts them.
//! - [`PCollection::collect_par_sorted_by_key`] — collects keyed data `(K, V)` and sorts by `K` only.
//!
//! These helpers are typically used in tests or final sinks where deterministic
//! output ordering is desired for validation or snapshot comparison.

use crate::{PCollection, RFBound};
use anyhow::Result;

impl<T: RFBound + Ord> PCollection<T> {
    /// Collect all elements **sequentially** and return a **sorted** `Vec<T>`.
    ///
    /// Internally calls [`PCollection::collect_seq`] to materialize the entire
    /// collection, then sorts it using [`Vec::sort`] (total ordering via `Ord`).
    ///
    /// # Type bounds
    /// - `T: Ord` — Elements must implement total ordering.
    ///
    /// # Returns
    /// A sorted vector of all collected elements.
    ///
    /// # Errors
    /// Propagates any error from `collect_seq()`, such as upstream I/O or
    /// deserialization failures.
    ///
    /// # Example
    /// ```
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![3, 1, 2]);
    /// let sorted = data.collect_seq_sorted().unwrap();
    /// assert_eq!(sorted, vec![1, 2, 3]);
    /// ```
    pub fn collect_seq_sorted(self) -> Result<Vec<T>> {
        let mut v = self.collect_seq()?;
        v.sort();
        Ok(v)
    }

    /// Collect all elements **in parallel** and return a **sorted** `Vec<T>`.
    ///
    /// Internally calls [`PCollection::collect_par`] with optional partition
    /// and chunk sizing, then sorts the aggregated output via [`Vec::sort`].
    ///
    /// # Arguments
    /// - `parts`: Optional number of parallel partitions (defaults to pipeline policy).
    /// - `chunk`: Optional chunk size per partition.
    ///
    /// # Returns
    /// A globally sorted vector of all collected elements.
    ///
    /// # Errors
    /// Propagates any error from `collect_par()`, such as partition errors,
    /// deserialization failures, or operator errors.
    ///
    /// # Example
    /// ```
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![5, 3, 4]);
    /// let sorted = data.collect_par_sorted(None, None).unwrap();
    /// assert_eq!(sorted, vec![3, 4, 5]);
    /// ```
    pub fn collect_par_sorted(self, parts: Option<usize>, chunk: Option<usize>) -> Result<Vec<T>> {
        let mut v = self.collect_par(parts, chunk)?;
        v.sort();
        Ok(v)
    }
}

impl<K: RFBound + Ord, V: RFBound> PCollection<(K, V)> {
    /// Collect all `(K, V)` pairs **in parallel** and return a vector sorted by **key**.
    ///
    /// Sorting is stable across partitions and uses [`Vec::sort_by`] with `K`’s
    /// total order (`Ord`).
    /// Values are not compared or grouped — only key order is enforced.
    ///
    /// # Arguments
    /// - `parts`: Optional number of partitions for parallel collection.
    /// - `chunk`: Optional chunk size for each partition.
    ///
    /// # Returns
    /// A vector of `(K, V)` tuples sorted by `K`.
    ///
    /// # Errors
    /// Propagates any error from `collect_par()`.
    ///
    /// # Example
    /// ```
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let kvs = from_vec(&p, vec![
    ///     ("b".to_string(), 2),
    ///     ("a".to_string(), 1),
    /// ]);
    /// let sorted = kvs.collect_par_sorted_by_key(None, None).unwrap();
    /// assert_eq!(sorted, vec![
    ///     ("a".to_string(), 1),
    ///     ("b".to_string(), 2),
    /// ]);
    /// ```
    pub fn collect_par_sorted_by_key(
        self,
        parts: Option<usize>,
        chunk: Option<usize>,
    ) -> Result<Vec<(K, V)>> {
        let mut v = self.collect_par(parts, chunk)?;
        v.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(v)
    }
}
