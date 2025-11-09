//! Distinct value combiners: `DistinctCount`, `DistinctSet`, `KMVApproxDistinctCount`

use crate::collection::{CombineFn, LiftableCombiner};
use crate::RFBound;
use ordered_float::NotNan;
use std::collections::{BinaryHeap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::marker::PhantomData;

/* ===================== DistinctCount<T> ===================== */

/// Count of **distinct** values per key.
///
/// - Accumulator: `HashSet<T>`
/// - Output: `u64`
///
/// Requires `T: Eq + Hash`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DistinctCount<T>(pub PhantomData<T>);
impl<T> DistinctCount<T> {
    /// Convenience constructor (same as `Default`).
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<T, HashSet<T>, u64> for DistinctCount<T>
where
    T: RFBound + Eq + Hash,
{
    fn create(&self) -> HashSet<T> {
        HashSet::new()
    }

    fn add_input(&self, acc: &mut HashSet<T>, v: T) {
        acc.insert(v);
    }

    fn merge(&self, acc: &mut HashSet<T>, other: HashSet<T>) {
        if acc.is_empty() {
            *acc = other;
        } else {
            acc.extend(other);
        }
    }

    fn finish(&self, acc: HashSet<T>) -> u64 {
        acc.len() as u64
    }
}

impl<T> LiftableCombiner<T, HashSet<T>, u64> for DistinctCount<T>
where
    T: RFBound + Eq + Hash,
{
    fn build_from_group(&self, values: &[T]) -> HashSet<T> {
        values.iter().cloned().collect()
    }
}

/* ===================== DistinctSet<T> (exact set) ===================== */

/// Get the distinct elements over a stream: accumulates a `HashSet<T>` and outputs a `Vec<T>`.
///
/// This is intended for use through helpers (e.g., `PCollection<T>::distinct()` or
/// `PCollection<(K,V)>::distinct_per_key()`), which expand the final `Vec<T>` back
/// into an element stream via `flat_map`.
#[derive(Clone, Copy, Debug)]
pub struct DistinctSet<T>(pub PhantomData<T>);
impl<T> DistinctSet<T> {
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}
impl<T> Default for DistinctSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CombineFn<T, HashSet<T>, Vec<T>> for DistinctSet<T>
where
    T: RFBound + Eq + Hash,
{
    fn create(&self) -> HashSet<T> {
        HashSet::new()
    }
    fn add_input(&self, acc: &mut HashSet<T>, v: T) {
        acc.insert(v);
    }
    fn merge(&self, acc: &mut HashSet<T>, other: HashSet<T>) {
        if acc.is_empty() {
            *acc = other;
        } else {
            acc.extend(other);
        }
    }
    fn finish(&self, acc: HashSet<T>) -> Vec<T> {
        acc.into_iter().collect()
    }
}

impl<T> LiftableCombiner<T, HashSet<T>, Vec<T>> for DistinctSet<T>
where
    T: RFBound + Eq + Hash,
{
    fn build_from_group(&self, values: &[T]) -> HashSet<T> {
        values.iter().cloned().collect()
    }
}

/* ===================== KMVApproxDistinctCount<T> (approximate count) ===================== */

/// Approximate distinct count via the KMV (K-Minimum Values) estimator.
///
/// Keeps the smallest `k` hash ranks in a max-heap; estimate is `(k-1)/r_k` where
/// `r_k` is the largest (i.e., k-th smallest) retained rank. For small `n < k`,
/// returns the exact count (`heap.len()`).
#[derive(Clone, Debug)]
pub struct KMVApproxDistinctCount<T> {
    pub k: usize,
    _m: PhantomData<T>,
}
impl<T> KMVApproxDistinctCount<T> {
    #[must_use]
    pub fn new(k: usize) -> Self {
        Self {
            k: k.max(4),
            _m: PhantomData,
        }
    }
}

/// Accumulator keeps k smallest **distinct** ranks.
#[derive(Default)]
pub struct KMVAcc {
    heap: BinaryHeap<NotNan<f64>>, // max-heap of the kept k smallest
    set: HashSet<NotNan<f64>>,     // membership test to prevent duplicates
    k: usize,
}

#[inline]
#[allow(clippy::cast_precision_loss)]
fn rank_from_value<T: Hash>(v: &T) -> NotNan<f64> {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    let u = h.finish();
    // Uniform in [0,1). NotNan is safe (never NaN).
    NotNan::new((u as f64) / ((u64::MAX as f64) + 1.0)).unwrap()
}

impl KMVAcc {
    #[inline]
    fn try_insert(&mut self, r: NotNan<f64>) {
        // Skip exact-duplicate ranks (same element hash)
        if !self.set.insert(r) {
            return;
        }
        if self.heap.len() < self.k {
            self.heap.push(r);
        } else if let Some(&rk) = self.heap.peek() {
            if r < rk {
                // Remove the current threshold from both heap and set
                let old = self.heap.pop().unwrap();
                self.set.remove(&old);
                // Insert the better (smaller) rank
                self.heap.push(r);
            } else {
                // New rank worse than the threshold; forget it from the set
                self.set.remove(&r);
            }
        }
        debug_assert!(self.heap.len() <= self.k && self.set.len() >= self.heap.len());
    }

    #[inline]
    fn merge_from(&mut self, mut other: Self) {
        // Move 'better' (smaller) ranks across, deduping
        while let Some(r) = other.heap.pop() {
            // Remove from other's set as we pop; not strictly needed, but neat
            other.set.remove(&r);
            self.try_insert(r);
        }
        // Any leftovers in other's set that weren't in heap (rare) can be ignored:
        // their ranks are >= other's threshold, and we've already considered the k
        // best from `other`.
    }

    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn finish(self) -> f64 {
        let m = self.set.len();
        if m == 0 {
            return 0.0;
        }
        if m < self.k {
            // Fewer than k uniques: exact count.
            return m as f64;
        }
        // m >= k: estimator based on the k-th smallest rank (heap root).
        let rk = *self.heap.peek().expect("heap non-empty when m>=k");
        // Classic KMV estimator: (k-1)/R_k. Using (k-1) reduces small-sample bias slightly.
        ((self.k as f64) - 1.0) / rk.into_inner()
    }
}

impl<T> CombineFn<T, KMVAcc, f64> for KMVApproxDistinctCount<T>
where
    T: RFBound + Hash,
{
    fn create(&self) -> KMVAcc {
        KMVAcc {
            heap: BinaryHeap::new(),
            set: HashSet::new(),
            k: self.k,
        }
    }

    fn add_input(&self, acc: &mut KMVAcc, v: T) {
        let r = rank_from_value(&v);
        acc.try_insert(r);
    }

    fn merge(&self, acc: &mut KMVAcc, other: KMVAcc) {
        acc.merge_from(other);
    }

    fn finish(&self, acc: KMVAcc) -> f64 {
        acc.finish()
    }
}

impl<T> LiftableCombiner<T, KMVAcc, f64> for KMVApproxDistinctCount<T>
where
    T: RFBound + Hash,
{
    fn build_from_group(&self, values: &[T]) -> KMVAcc {
        let mut acc = self.create();
        for v in values {
            acc.try_insert(rank_from_value(v));
        }
        acc
    }
}
