//! # Sampling helpers
//!
//! Deterministic, mergeable **reservoir sampling** helpers built on top of
//! the `PriorityReservoir` combiner (Efraimidis-Spirakis priority sampling,
//! unit weights). These are stable across sequential vs. parallel execution
//! and across partitions of the same input.
//!
//! ## APIs
//! - Global (unkeyed):
//!   - [`PCollection<T>::sample_reservoir_vec`](#method.sample_reservoir_vec)
//!   - [`PCollection<T>::sample_reservoir`](#method.sample_reservoir)
//! - Per-key (keyed):
//!   - [`PCollection<(K,V)>::sample_values_reservoir_vec`](#method.sample_values_reservoir_vec)
//!   - [`PCollection<(K,V)>::sample_values_reservoir`](#method.sample_values_reservoir)
//!
//! All helpers accept a `k` (sample size) and a `seed` (u64).

use crate::combiners::PriorityReservoir;
use crate::{PCollection, RFBound};

impl<T: RFBound> PCollection<T> {
    /// Sample **k** elements globally using a priority reservoir and return a single `Vec<T>`.
    ///
    /// Deterministic across seq/par for a given `seed` and input multiset.
    #[must_use]
    pub fn sample_reservoir_vec(self, k: usize, seed: u64) -> PCollection<Vec<T>> {
        // CombineGlobally over T -> Vec<T>
        self.combine_globally(PriorityReservoir::<T>::new(k, seed), None)
    }

    /// Sample **k** elements globally and **flatten** the resulting `Vec<T>` back into a stream.
    ///
    /// Useful when you want to continue processing the sampled elements as a normal collection.
    #[must_use]
    pub fn sample_reservoir(self, k: usize, seed: u64) -> PCollection<T> {
        self.sample_reservoir_vec(k, seed)
            .flat_map(|v: &Vec<T>| v.clone())
    }
}

impl<K: RFBound + Eq + core::hash::Hash, V: RFBound> PCollection<(K, V)> {
    /// Per-key reservoir sample of values (size **k** per key), returns `(K, Vec<V>)`.
    ///
    /// Implemented via **lifted** combine so it can skip an explicit `group_by_key`
    /// barrier when the planner detects adjacency.
    #[must_use]
    pub fn sample_values_reservoir_vec(self, k: usize, seed: u64) -> PCollection<(K, Vec<V>)> {
        // Lifted combine over (K, Vec<V>) produces (K, Vec<V>)
        self.group_by_key()
            .combine_values_lifted(PriorityReservoir::<V>::new(k, seed))
    }

    /// Per-key reservoir sample of values and **flatten** back to `(K, V)`.
    #[must_use]
    pub fn sample_values_reservoir(self, k: usize, seed: u64) -> PCollection<(K, V)> {
        self.sample_values_reservoir_vec(k, seed)
            .flat_map(|kv: &(K, Vec<V>)| {
                let (k, vs) = kv;
                vs.iter()
                    .cloned()
                    .map(|v| (k.clone(), v))
                    .collect::<Vec<_>>()
            })
    }
}
