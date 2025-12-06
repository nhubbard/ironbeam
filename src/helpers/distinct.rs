//! Distinct helpers: exact (dedupe) and approximate distinct counts (KMV) for
//! unkeyed and keyed collections.
//!
//! # Overview
//! - [`PCollection::distinct`](PCollection::distinct) - Remove duplicates globally (exact)
//! - [`PCollection::distinct_per_key`](crate::PCollection::distinct_per_key) - Remove duplicate values per key (exact)
//! - [`PCollection::approx_distinct_count`](PCollection::approx_distinct_count) - Approximate global cardinality (f64)
//! - [`PCollection::approx_distinct_count_per_key`](crate::PCollection::approx_distinct_count_per_key) - Approximate cardinality per key
//!
//! Exact distinct is implemented with the `DistinctSet<T>` combiner and then
//! expanded back into an element stream via `flat_map`. Approximate counts use
//! a KMV estimator (`KMVApproxDistinctCount<T>`).

use crate::combiners::{DistinctSet, KMVApproxDistinctCount};
use crate::{PCollection, RFBound};
use std::hash::Hash;

impl<T: RFBound + Eq + Hash> PCollection<T> {
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
    /// * For large sets, estimator variance decreases with larger `k`.
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
}

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound + Eq + Hash,
{
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
        // (K,V) -> group_by_key -> (K, Vec<V>) -> combine_values_lifted(DistinctSet) -> (K, Vec<V>)
        // -> flat_map to (K,V)
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
}
