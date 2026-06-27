//! Distinct helpers: exact (dedupe) and distinct counts for unkeyed and keyed collections.
//!
//! # Overview
//! - [`PCollection::distinct`](PCollection::distinct) - Remove duplicates globally (exact)
//! - [`PCollection::distinct_by`](PCollection::distinct_by) - Remove duplicates by a computed projection
//! - [`PCollection::distinct_per_key`](crate::PCollection::distinct_per_key) - Remove duplicate values per key (exact)
//! - [`PCollection::distinct_count_globally`] - Exact count of distinct elements (global)
//! - [`PCollection::distinct_count_per_key`] - Exact count of distinct values per key
//! - [`PCollection::approx_distinct_count`](PCollection::approx_distinct_count) - Approximate global cardinality (KMV, f64)
//! - [`PCollection::approx_distinct_count_per_key`](crate::PCollection::approx_distinct_count_per_key) - Approximate cardinality per key (KMV, f64)
//! - [`PCollection::approx_count_distinct`](PCollection::approx_count_distinct) - Approximate global cardinality (`HyperLogLog`++, u64)
//! - [`PCollection::approx_count_distinct_with_error`](PCollection::approx_count_distinct_with_error) - HLL global cardinality with custom relative error
//! - [`PCollection::approx_count_distinct_per_key`](crate::PCollection::approx_count_distinct_per_key) - HLL per-key cardinality
//! - [`PCollection::approx_count_distinct_per_key_with_error`](crate::PCollection::approx_count_distinct_per_key_with_error) - HLL per-key cardinality with custom relative error
//!
//! Exact distinct is implemented with the `DistinctSet<T>` combiner and then
//! expanded back into an element stream via `flat_map`. Exact distinct counts use
//! `DistinctCount<T>` (returns `u64`). Two approximate-count families are
//! available: KMV-based (returns `f64`, exact when cardinality `< k`) and
//! HyperLogLog++-based (returns `u64`, bounded relative error at all scales).

use crate::combiners::{
    DistinctCount, DistinctSet, HllApproxDistinctCount, KMVApproxDistinctCount,
};
use crate::{Element, PCollection};
use std::hash::Hash;

impl<T: Element + Eq + Hash> PCollection<T> {
    /// Count the exact number of distinct elements globally.
    ///
    /// Unlike [`approx_distinct_count`](Self::approx_distinct_count) (which uses a KMV
    /// estimator), this method is exact: it collects a `HashSet<T>` over the whole
    /// collection and returns its length as `u64`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let count = from_vec(&p, vec![1u32, 1, 2, 3, 3, 3])
    ///     .distinct_count_globally()
    ///     .collect_seq()?;
    /// assert_eq!(count, vec![3u64]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn distinct_count_globally(self) -> PCollection<u64> {
        self.combine_globally(DistinctCount::<T>::new(), None)
    }

    /// Exact global distinct. Removes duplicates across the entire collection.
    ///
    /// Returns a `PCollection<T>` containing each unique element once (order unspecified).
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// let p = Pipeline::default();
    /// let out = from_vec(&p, vec![1,1,2,3,3,3]).distinct();
    /// let v = out.collect_seq().unwrap();
    /// // v contains 1,2,3 in some order
    /// assert_eq!(v.len(), 3);
    /// ```
    #[must_use]
    pub fn distinct(self) -> Self {
        // CombineGlobally produces Vec<T> (single element in the stream), then expand.
        let vecs = self.combine_globally(DistinctSet::<T>::default(), None);
        vecs.flat_map(|vs: &Vec<T>| vs.clone())
    }

    /// Approximate global distinct count using KMV with `k` retained minima.
    ///
    /// * For small cardinalities (`< k`), returns the exact count.
    /// * For large sets, estimator variance decreases with a larger `k`.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// let p = Pipeline::default();
    /// let out = from_vec(&p, (0..10_000u64).map(|n| n % 1234).collect::<Vec<_>>())
    ///     .approx_distinct_count(256);
    /// let est = out.collect_seq().unwrap()[0];
    /// assert!(est > 1200.0 && est < 1300.0); // rough bound
    /// ```
    #[must_use]
    pub fn approx_distinct_count(self, k: usize) -> PCollection<f64> {
        self.combine_globally(KMVApproxDistinctCount::<T>::new(k), None)
    }

    /// Approximate global distinct count via the `HyperLogLog`++ algorithm.
    ///
    /// Returns a `u64` cardinality estimate computed by
    /// [`HllApproxDistinctCount`] with the default precision (`12`, ~1.6%
    /// relative standard error). Memory usage is constant in the input
    /// size (a few kilobytes), so this is well-suited for streams whose
    /// cardinality is too large for the exact
    /// [`distinct_count_globally`](Self::distinct_count_globally).
    ///
    /// For small cardinalities the HLL++ sparse representation gives
    /// essentially exact counts; the bounded error only kicks in once the
    /// register array saturates.
    ///
    /// Use [`approx_count_distinct_with_error`](Self::approx_count_distinct_with_error)
    /// when you need a tighter (or looser) error bound.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let est = from_vec(&p, (0u32..10_000).map(|n| n % 1234).collect::<Vec<_>>())
    ///     .approx_count_distinct()
    ///     .collect_seq()?;
    /// assert!((1200..=1260).contains(&est[0])); // ~1234 ± a few %
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn approx_count_distinct(self) -> PCollection<u64> {
        self.combine_globally(HllApproxDistinctCount::<T>::new(), None)
    }

    /// Approximate global distinct count via `HyperLogLog`++ with an explicit
    /// relative-standard-error target.
    ///
    /// `error` must lie in `(0, 1)` (typical values: `0.02` for the
    /// default, `0.01` for tighter bounds, `0.005` for ~0.5 %). The
    /// resulting sketch uses ~`2^p` registers where `p` is the smallest
    /// precision in `[4, 18]` such that
    /// `1.04 / sqrt(2^p) <= error`. See
    /// [`HllApproxDistinctCount::with_error`] for the precision table.
    ///
    /// # Panics
    ///
    /// Panics if `error` is not finite or is outside `(0, 1)`.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let est = from_vec(&p, (0u32..100_000).collect::<Vec<_>>())
    ///     .approx_count_distinct_with_error(0.01)
    ///     .collect_seq()?;
    /// assert!((99_000..=101_000).contains(&est[0]));
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn approx_count_distinct_with_error(self, error: f64) -> PCollection<u64> {
        self.combine_globally(HllApproxDistinctCount::<T>::with_error(error), None)
    }
}

impl<T: Element> PCollection<T> {
    /// Deduplicate elements by a computed projection, keeping one arbitrary element per
    /// distinct key value.
    ///
    /// This differs from [`distinct`](Self::distinct), which requires `T: Eq + Hash` and
    /// deduplicates the element itself. `distinct_by` lets you project to any hashable key
    /// and retains one full element for each unique projected value.
    ///
    /// The element retained per key is **arbitrary** (determined by processing order).
    /// If a specific element must be selected, sort or filter before calling this method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    /// struct Event { user_id: u32, payload: String }
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Event { user_id: 1, payload: "first".into() },
    ///     Event { user_id: 1, payload: "second".into() },
    ///     Event { user_id: 2, payload: "only".into() },
    /// ]);
    ///
    /// // Keep one event per user_id
    /// let one_per_user = events.distinct_by(|e| e.user_id);
    /// let mut results = one_per_user.collect_seq()?;
    /// results.sort_by_key(|e| e.user_id);
    /// assert_eq!(results.len(), 2);
    /// assert_eq!(results[0].user_id, 1);
    /// assert_eq!(results[1].user_id, 2);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn distinct_by<K, F>(self, key_fn: F) -> Self
    where
        K: Element + Eq + Hash,
        F: 'static + Send + Sync + Fn(&T) -> K,
    {
        self.key_by(key_fn)
            .group_by_key()
            .flat_map(|kv: &(K, Vec<T>)| kv.1.iter().take(1).cloned().collect::<Vec<_>>())
    }
}

impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash,
    V: Element + Eq + Hash,
{
    /// Count the exact number of distinct values per key.
    ///
    /// Returns `(K, u64)` where the value is the number of unique values seen for
    /// that key. This is different from
    /// [`distinct_per_key`](Self::distinct_per_key), which returns the actual
    /// distinct elements; and from
    /// [`approx_distinct_count_per_key`](Self::approx_distinct_count_per_key),
    /// which uses an approximate estimator.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let counts = from_vec(&p, vec![
    ///     ("a".to_string(), 1u32), ("a".to_string(), 1), ("a".to_string(), 2),
    ///     ("b".to_string(), 7u32), ("b".to_string(), 7),
    /// ])
    /// .distinct_count_per_key()
    /// .collect_seq_sorted()?;
    /// assert_eq!(counts, vec![("a".to_string(), 2u64), ("b".to_string(), 1u64)]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn distinct_count_per_key(self) -> PCollection<(K, u64)> {
        self.combine_values(DistinctCount::<V>::new())
    }

    /// Exact per-key distinct of values: removes duplicate values for each key.
    ///
    /// Output is a `(K, V)` stream containing only unique `(key, value)` pairs,
    /// at most one per distinct value within each key.
    ///
    /// # Example
    /// ```no_run
    /// use ironbeam::*;
    /// let p = Pipeline::default();
    /// let kv = vec![
    ///   ("a".to_string(), 1), ("a".to_string(), 1), ("a".to_string(), 2),
    ///   ("b".to_string(), 7), ("b".to_string(), 7),
    /// ];
    /// let out = from_vec(&p, kv).distinct_per_key();
    /// let mut v = out.collect_seq().unwrap();
    /// v.sort(); // deterministic for test
    /// assert_eq!(v, vec![
    ///   ("a".to_string(), 1),
    ///   ("a".to_string(), 2),
    ///   ("b".to_string(), 7),
    /// ]);
    /// ```
    #[must_use]
    pub fn distinct_per_key(self) -> Self {
        // (K, V) -> group_by_key -> (K, Vec<V>) -> combine_values_lifted(DistinctSet) -> (K, Vec<V>)
        // -> flat_map to (K, V)
        self.group_by_key()
            .combine_values_lifted(DistinctSet::<V>::default())
            .flat_map(|kv: &(K, Vec<V>)| {
                kv.1.iter()
                    .cloned()
                    .map(|v| (kv.0.clone(), v))
                    .collect::<Vec<_>>()
            })
    }

    /// Approximate per-key distinct value count using KMV with `k` retained minima.
    ///
    /// Produces `(K, f64)` where the value is the estimated number of distinct
    /// values observed for that key. For keys with fewer than `k` unique values,
    /// the estimate is exact (equal to the true count).
    #[must_use]
    pub fn approx_distinct_count_per_key(self, k: usize) -> PCollection<(K, f64)> {
        self.combine_values(KMVApproxDistinctCount::<V>::new(k))
    }

    /// Approximate per-key distinct value count via `HyperLogLog`++.
    ///
    /// Produces `(K, u64)` where the value is the estimated cardinality of
    /// the per-key value set. Uses the default precision (`12`, ~1.6%
    /// relative standard error). For keys with small cardinality the
    /// HLL++ sparse representation makes the estimate essentially exact.
    ///
    /// Use [`approx_count_distinct_per_key_with_error`](Self::approx_count_distinct_per_key_with_error)
    /// to tune the error bound.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let counts = from_vec(&p, vec![
    ///     ("a".to_string(), 1u32), ("a".to_string(), 1), ("a".to_string(), 2),
    ///     ("b".to_string(), 7u32), ("b".to_string(), 7),
    /// ])
    /// .approx_count_distinct_per_key()
    /// .collect_seq_sorted()?;
    /// assert_eq!(counts, vec![("a".to_string(), 2u64), ("b".to_string(), 1u64)]);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn approx_count_distinct_per_key(self) -> PCollection<(K, u64)> {
        self.combine_values(HllApproxDistinctCount::<V>::new())
    }

    /// Approximate per-key distinct value count via `HyperLogLog`++ with an
    /// explicit relative-standard-error target.
    ///
    /// `error` must lie in `(0, 1)`. See
    /// [`HllApproxDistinctCount::with_error`] for the precision table.
    ///
    /// # Panics
    ///
    /// Panics if `error` is not finite or is outside `(0, 1)`.
    #[must_use]
    pub fn approx_count_distinct_per_key_with_error(self, error: f64) -> PCollection<(K, u64)> {
        self.combine_values(HllApproxDistinctCount::<V>::with_error(error))
    }
}
