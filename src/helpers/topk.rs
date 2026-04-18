//! Top-K and Bottom-K convenience API for selecting the largest or smallest values globally or per key.
//!
//! This module provides ergonomic helpers for selecting the top-K or bottom-K values
//! without needing to explicitly instantiate combiner structs.
//!
//! ## Available operations
//! - [`PCollection::top_k_globally`] - Select the top-K largest elements (global)
//! - [`PCollection::top_k_per_key`](crate::PCollection::top_k_per_key) - Select the top-K largest values per key
//! - [`PCollection::bottom_k_globally`] - Select the bottom-K smallest elements (global)
//! - [`PCollection::bottom_k_per_key`](crate::PCollection::bottom_k_per_key) - Select the bottom-K smallest values per key
//!
//! ## Example
//! ```no_run
//! use ironbeam::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let scores = from_vec(&p, vec![
//!     ("alice", 95),
//!     ("alice", 87),
//!     ("alice", 92),
//!     ("bob", 78),
//!     ("bob", 88),
//! ]);
//!
//! // Get top 2 scores per person
//! let top_scores = scores.clone().top_k_per_key(2).collect_seq_sorted()?;
//! // Result: [("alice", vec![95, 92]), ("bob", vec![88, 78])]
//!
//! // Get bottom 2 scores per person
//! let bottom_scores = scores.bottom_k_per_key(2).collect_seq_sorted()?;
//! // Result: [("alice", vec![87, 92]), ("bob", vec![78, 88])]
//! # Ok(())
//! # }
//! ```

use crate::combiners::{BottomK, TopK};
use crate::{PCollection, RFBound};
use std::cmp::Ord;
use std::hash::Hash;

impl<T: RFBound + Ord> PCollection<T> {
    /// Select the top-K largest elements globally.
    ///
    /// Returns a single `Vec<T>` containing at most `k` elements sorted in
    /// descending order (largest first). If `k == 0`, produces an empty `Vec`.
    /// If the collection has fewer than `k` elements, all elements are returned.
    ///
    /// # Parameters
    /// - `k`: the number of largest elements to retain.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let top3 = from_vec(&p, vec![5i32, 2, 8, 1, 9, 3])
    ///     .top_k_globally(3)
    ///     .collect_seq()?;
    /// assert_eq!(top3[0], vec![9i32, 8, 5]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn top_k_globally(self, k: usize) -> PCollection<Vec<T>> {
        self.combine_globally(TopK::new(k), None)
    }
}

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound + Ord,
{
    /// Select the top-K largest values per key.
    ///
    /// This is a convenience method that wraps [`combine_values`](PCollection::combine_values)
    /// with a [`TopK`] combiner. It returns a `PCollection<(K, Vec<V>)>` where each vector
    /// contains at most `k` elements sorted in descending order (largest first).
    ///
    /// # Parameters
    /// - `k`: The number of largest values to keep per key. If `k == 0`, all keys will
    ///   produce empty vectors.
    ///
    /// # Returns
    /// A `PCollection<(K, Vec<V>)>` with the top K values per key, sorted descending.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// # use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![
    ///     ("a", 5),
    ///     ("a", 2),
    ///     ("a", 8),
    ///     ("a", 1),
    ///     ("b", 10),
    ///     ("b", 7),
    /// ]);
    ///
    /// let top3 = data.top_k_per_key(3).collect_seq_sorted()?;
    /// // Expected: [("a", vec![8, 5, 2]), ("b", vec![10, 7])]
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    /// This operation uses a min-heap internally with size ≤ k, so memory usage per key
    /// is O(k). The combiner is associative and supports parallel execution efficiently.
    ///
    /// # See Also
    /// - [`TopK`] - The underlying combiner implementation
    /// - [`combine_values`](PCollection::combine_values) - General combiner API
    #[must_use]
    pub fn top_k_per_key(self, k: usize) -> PCollection<(K, Vec<V>)> {
        self.combine_values(TopK::new(k))
    }
}

impl<T: RFBound + Ord> PCollection<T> {
    /// Select the bottom-K smallest elements globally.
    ///
    /// Returns a single `Vec<T>` containing at most `k` elements sorted in
    /// ascending order (smallest first). If `k == 0`, produces an empty `Vec`.
    /// If the collection has fewer than `k` elements, all elements are returned.
    ///
    /// # Parameters
    /// - `k`: the number of smallest elements to retain.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let bottom3 = from_vec(&p, vec![5i32, 2, 8, 1, 9, 3])
    ///     .bottom_k_globally(3)
    ///     .collect_seq()?;
    /// assert_eq!(bottom3[0], vec![1i32, 2, 3]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn bottom_k_globally(self, k: usize) -> PCollection<Vec<T>> {
        self.combine_globally(BottomK::new(k), None)
    }
}

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound + Ord,
{
    /// Select the bottom-K smallest values per key.
    ///
    /// Returns a `PCollection<(K, Vec<V>)>` where each vector contains at most
    /// `k` elements sorted in ascending order (smallest first).
    ///
    /// # Parameters
    /// - `k`: The number of smallest values to keep per key. If `k == 0`, all keys will
    ///   produce empty vectors.
    ///
    /// # Returns
    /// A `PCollection<(K, Vec<V>)>` with the bottom K values per key, sorted ascending.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// # use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![
    ///     ("a", 5),
    ///     ("a", 2),
    ///     ("a", 8),
    ///     ("a", 1),
    ///     ("b", 10),
    ///     ("b", 7),
    /// ]);
    ///
    /// let bottom3 = data.bottom_k_per_key(3).collect_seq_sorted()?;
    /// // Expected: [("a", vec![1, 2, 5]), ("b", vec![7, 10])]
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    /// This operation uses a max-heap internally with size ≤ k, so memory usage per key
    /// is O(k). The combiner is associative and supports parallel execution efficiently.
    ///
    /// # See Also
    /// - [`BottomK`] - The underlying combiner implementation
    /// - [`combine_values`](PCollection::combine_values) - General combiner API
    #[must_use]
    pub fn bottom_k_per_key(self, k: usize) -> PCollection<(K, Vec<V>)> {
        self.combine_values(BottomK::new(k))
    }
}
