//! Top-K convenience API for selecting the largest values per key.
//!
//! This module provides ergonomic helpers for selecting the top-K largest values
//! from keyed collections without needing to explicitly instantiate combiners.
//!
//! ## Available operations
//! - [`PCollection::top_k_per_key`](crate::PCollection::top_k_per_key) - Select the top-K largest values per key
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
//! let top_scores = scores.top_k_per_key(2).collect_seq_sorted()?;
//! // Result: [("alice", vec![95, 92]), ("bob", vec![88, 78])]
//! # Ok(())
//! # }
//! ```

use crate::combiners::TopK;
use crate::{PCollection, RFBound};
use std::cmp::Ord;
use std::hash::Hash;

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
