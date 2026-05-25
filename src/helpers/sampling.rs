//! # Sampling helpers
//!
//! Deterministic, mergeable **reservoir sampling** helpers built on top of
//! the `PriorityReservoir` combiner (Efraimidis-Spirakis priority sampling,
//! unit weights). These are stable across sequential vs. parallel execution
//! and across partitions of the same input.
//!
//! ## APIs
//! - Global (unkeyed), seeded:
//!   - [`PCollection<T>::sample_reservoir_vec`](#method.sample_reservoir_vec)
//!   - [`PCollection<T>::sample_reservoir`](#method.sample_reservoir)
//! - Global (unkeyed), Beam-compatible (default seed):
//!   - [`PCollection<T>::sample_globally`](#method.sample_globally)
//!   - [`PCollection<T>::sample_globally_with_seed`](#method.sample_globally_with_seed)
//! - Per-key (keyed), seeded:
//!   - [`PCollection<(K,V)>::sample_values_reservoir_vec`](#method.sample_values_reservoir_vec)
//!   - [`PCollection<(K,V)>::sample_values_reservoir`](#method.sample_values_reservoir)
//! - Per-key (keyed), Beam-compatible (default seed):
//!   - [`PCollection<(K,V)>::sample_per_key`](#method.sample_per_key)
//!   - [`PCollection<(K,V)>::sample_per_key_with_seed`](#method.sample_per_key_with_seed)
//!
//! The Beam-style `sample_globally` / `sample_per_key` helpers use a fixed
//! default seed so two runs over the same input produce the same sample;
//! pass an explicit seed via the `_with_seed` variants to vary the choice.

use crate::combiners::PriorityReservoir;
use crate::{PCollection, RFBound};
use core::hash::Hash;

/// Default seed used by [`PCollection::sample_globally`] and
/// [`PCollection::sample_per_key`]. The constant is the `SplitMix64` golden
/// ratio (`(sqrt(5) - 1) / 2 * 2^64`); it scrambles well under the
/// combiner's seed-mixing multiply, giving a high-entropy starting state
/// for the internal PRNG. Being fixed means two runs in the same
/// execution mode pick the same sample.
const DEFAULT_SAMPLE_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

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
    pub fn sample_reservoir(self, k: usize, seed: u64) -> Self {
        self.sample_reservoir_vec(k, seed)
            .flat_map(|v: &Vec<T>| v.clone())
    }

    /// Beam-compatible global fixed-size sample: returns a single `Vec<T>`
    /// of at most `n` elements drawn from `self` using reservoir sampling
    /// without replacement.
    ///
    /// This is the Ironbeam equivalent of Beam's
    /// `Sample.FixedSizeGlobally(n)`. The combiner uses a fixed default
    /// seed, so two runs in the **same execution mode** (both sequential,
    /// or both parallel with the same partition count) over the same
    /// input multiset return the same sample.
    /// Sequential and parallel runs may pick different elements — every
    /// such sample is still a valid uniform draw of size `n` — because
    /// per-partition PRNG streams diverge under parallelism. Use
    /// [`sample_globally_with_seed`](Self::sample_globally_with_seed) to
    /// vary the seed (for stratified sampling, A/B-test splits, …).
    ///
    /// If `self` contains fewer than `n` elements, the result is a
    /// `Vec<T>` of every element (still wrapped in a singleton stream).
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let sample = from_vec(&p, (0u32..1_000).collect::<Vec<_>>())
    ///     .sample_globally(5)
    ///     .collect_seq()?;
    /// assert_eq!(sample.len(), 1);
    /// assert_eq!(sample[0].len(), 5);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn sample_globally(self, n: usize) -> PCollection<Vec<T>> {
        self.sample_reservoir_vec(n, DEFAULT_SAMPLE_SEED)
    }

    /// Beam-compatible global fixed-size sample with a user-supplied seed.
    ///
    /// Identical to [`sample_globally`](Self::sample_globally) except the
    /// internal PRNG is seeded with `seed`, so different seeds yield
    /// different deterministic samples from the same input.
    #[must_use]
    pub fn sample_globally_with_seed(self, n: usize, seed: u64) -> PCollection<Vec<T>> {
        self.sample_reservoir_vec(n, seed)
    }
}

impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, V)> {
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
    pub fn sample_values_reservoir(self, k: usize, seed: u64) -> Self {
        self.sample_values_reservoir_vec(k, seed)
            .flat_map(|kv: &(K, Vec<V>)| {
                let (k, vs) = kv;
                vs.iter()
                    .cloned()
                    .map(|v| (k.clone(), v))
                    .collect::<Vec<_>>()
            })
    }

    /// Beam-compatible per-key fixed-size sample: for each key, returns a
    /// `Vec<V>` of at most `n` values drawn from that key's value stream
    /// using reservoir sampling without replacement.
    ///
    /// This is the Ironbeam equivalent of Beam's
    /// `Sample.FixedSizePerKey(n)`. See
    /// [`sample_globally`](PCollection::sample_globally) for the
    /// determinism contract (same execution mode ⇒ identical samples);
    /// vary the choice with
    /// [`sample_per_key_with_seed`](Self::sample_per_key_with_seed).
    ///
    /// Per-key samples are emitted as `(K, Vec<V>)` pairs. Keys whose
    /// value stream has fewer than `n` values produce a `Vec<V>`
    /// containing every value for that key.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let mut data: Vec<(&str, u32)> = Vec::new();
    /// for i in 0..100 { data.push(("a", i)); data.push(("b", i + 1_000)); }
    ///
    /// let samples = from_vec(&p, data)
    ///     .sample_per_key(5)
    ///     .collect_seq_sorted()?;
    /// // Two keys, each with a 5-element sample.
    /// assert_eq!(samples.len(), 2);
    /// for (_k, vs) in &samples { assert_eq!(vs.len(), 5); }
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn sample_per_key(self, n: usize) -> PCollection<(K, Vec<V>)> {
        self.sample_values_reservoir_vec(n, DEFAULT_SAMPLE_SEED)
    }

    /// Beam-compatible per-key fixed-size sample with a user-supplied seed.
    ///
    /// Identical to [`sample_per_key`](Self::sample_per_key) except the
    /// internal PRNG is seeded with `seed`.
    #[must_use]
    pub fn sample_per_key_with_seed(self, n: usize, seed: u64) -> PCollection<(K, Vec<V>)> {
        self.sample_values_reservoir_vec(n, seed)
    }
}
