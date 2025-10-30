//! Built-in combiners for `combine_values` and `combine_values_lifted`.
//!
//! These are reusable implementations of [`CombineFn`] (and many also implement
//! [`LiftableCombiner`]) that operate over per-key value streams:
//!
//! - [`Sum<T>`] -- sum of values.
//! - [`Min<T>`] -- minimum value.
//! - [`Max<T>`] -- maximum value.
//! - [`AverageF64`] -- average as `f64` (values convertible to `f64`).
//! - [`DistinctCount<T>`] -- count of distinct values.
//! - [`TopK<T>`] -- the top-K largest values.
//!
//! Each combiner specifies its accumulator type (`A`) and output type (`O`).
//! Many provide a `build_from_group` optimization via [`LiftableCombiner`],
//! enabling efficient `group_by_key().combine_values_lifted(...)` plans.
//!
//! # Examples
//! ```ignore
//! use rustflow::*;
//! use rustflow::combiners::{Sum, Min, Max, AverageF64, DistinctCount, TopK};
//!
//! let p = Pipeline::default();
//!
//! // Sum
//! let s = from_vec(&p, vec![("a", 1u64), ("a", 2), ("b", 10)])
//!     .combine_values(Sum::<u64>::default())
//!     .collect_seq_sorted()?;
//!
//! // Min / Max (require Ord)
//! let mn = from_vec(&p, vec![("a", 3u64), ("a", 2), ("a", 5)])
//!     .combine_values(Min::<u64>::default())
//!     .collect_seq()?;
//! let mx = from_vec(&p, vec![("a", 3u64), ("a", 2), ("a", 5)])
//!     .combine_values(Max::<u64>::default())
//!     .collect_seq()?;
//!
//! // AverageF64 (values must be Into<f64>)
//! let avg = from_vec(&p, vec![("a", 1u32), ("a", 2), ("a", 3)])
//!     .combine_values(AverageF64::default())
//!     .collect_seq()?;
//!
//! // DistinctCount (values must be Eq + Hash)
//! let dc = from_vec(&p, vec![("a", 1u32), ("a", 1), ("a", 2)])
//!     .combine_values(DistinctCount::<u32>::default())
//!     .collect_seq()?;
//!
//! // TopK (values must be Ord)
//! let top = from_vec(&p, vec![("a", 3u32), ("a", 7), ("a", 5)])
//!     .combine_values(TopK::<u32>::new(2))
//!     .collect_seq()?;
//!
//! # anyhow::Result::<()>::Ok(())
//! ```

use crate::collection::{CombineFn, LiftableCombiner};
use crate::RFBound;
use ordered_float::NotNan;
use std::cmp::{Ord, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::mem::take;
use std::ops::Add;

/* ===================== Sum<T> ===================== */

/// Sum of values per key.
///
/// - Accumulator: `T`
/// - Output: `T`
///
/// Requires `T: Add<Output=T> + Default`.
#[derive(Clone, Copy, Debug, Default)]
pub struct Sum<T>(pub PhantomData<T>);
impl<T> Sum<T> {
    /// Convenience constructor (same as `Default`).
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<T, T, T> for Sum<T>
where
    T: RFBound + Add<Output = T> + Default,
{
    fn create(&self) -> T {
        T::default()
    }

    fn add_input(&self, acc: &mut T, v: T) {
        *acc = take(acc) + v;
    }

    fn merge(&self, acc: &mut T, other: T) {
        *acc = take(acc) + other;
    }

    fn finish(&self, acc: T) -> T {
        acc
    }
}

impl<T> LiftableCombiner<T, T, T> for Sum<T>
where
    T: RFBound + Add<Output = T> + Default,
{
    fn build_from_group(&self, values: &[T]) -> T {
        values.iter().cloned().fold(T::default(), |a, v| a + v)
    }
}

/* ===================== Min<T> ===================== */

/// Minimum value per key (requires `Ord`).
///
/// - Accumulator: `Option<T>`
/// - Output: `T`
#[derive(Clone, Copy, Debug, Default)]
pub struct Min<T>(pub PhantomData<T>);
impl<T> Min<T> {
    /// Convenience constructor (same as `Default`).
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<T, Option<T>, T> for Min<T>
where
    T: RFBound + Ord,
{
    fn create(&self) -> Option<T> {
        None
    }

    fn add_input(&self, acc: &mut Option<T>, v: T) {
        match acc {
            Some(cur) => {
                if v < *cur {
                    *cur = v
                }
            }
            None => *acc = Some(v),
        }
    }

    fn merge(&self, acc: &mut Option<T>, other: Option<T>) {
        if let Some(b) = other {
            match acc {
                Some(a) => {
                    if b < *a {
                        *a = b
                    }
                }
                None => *acc = Some(b),
            }
        }
    }

    fn finish(&self, acc: Option<T>) -> T {
        acc.expect("Min::finish called on empty group")
    }
}

impl<T> LiftableCombiner<T, Option<T>, T> for Min<T>
where
    T: RFBound + Ord,
{
    fn build_from_group(&self, values: &[T]) -> Option<T> {
        values.iter().cloned().min()
    }
}

/* ===================== Max<T> ===================== */

/// Maximum value per key (requires `Ord`).
///
/// - Accumulator: `Option<T>`
/// - Output: `T`
#[derive(Clone, Copy, Debug, Default)]
pub struct Max<T>(pub PhantomData<T>);
impl<T> Max<T> {
    /// Convenience constructor (same as `Default`).
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<T, Option<T>, T> for Max<T>
where
    T: RFBound + Ord,
{
    fn create(&self) -> Option<T> {
        None
    }

    fn add_input(&self, acc: &mut Option<T>, v: T) {
        match acc {
            Some(cur) => {
                if v > *cur {
                    *cur = v
                }
            }
            None => *acc = Some(v),
        }
    }

    fn merge(&self, acc: &mut Option<T>, other: Option<T>) {
        if let Some(b) = other {
            match acc {
                Some(a) => {
                    if b > *a {
                        *a = b
                    }
                }
                None => *acc = Some(b),
            }
        }
    }

    fn finish(&self, acc: Option<T>) -> T {
        acc.expect("Max::finish called on empty group")
    }
}

impl<T> LiftableCombiner<T, Option<T>, T> for Max<T>
where
    T: RFBound + Ord,
{
    fn build_from_group(&self, values: &[T]) -> Option<T> {
        values.iter().cloned().max()
    }
}

/* ===================== AverageF64 ===================== */

/// Average of values per key as `f64`.
///
/// Values must be convertible into `f64` via `Into<f64>`.
///
/// - Accumulator: `(sum_f64, count_u64)`
/// - Output: `f64`
///
/// Empty groups produce `0.0`.
#[derive(Clone, Copy, Debug, Default)]
pub struct AverageF64;

impl<V> CombineFn<V, (f64, u64), f64> for AverageF64
where
    V: RFBound + Into<f64>,
{
    fn create(&self) -> (f64, u64) {
        (0.0, 0)
    }

    fn add_input(&self, acc: &mut (f64, u64), v: V) {
        acc.0 += v.into();
        acc.1 += 1;
    }

    fn merge(&self, acc: &mut (f64, u64), other: (f64, u64)) {
        acc.0 += other.0;
        acc.1 += other.1;
    }

    fn finish(&self, acc: (f64, u64)) -> f64 {
        if acc.1 == 0 {
            0.0
        } else {
            acc.0 / (acc.1 as f64)
        }
    }
}

impl<V> LiftableCombiner<V, (f64, u64), f64> for AverageF64
where
    V: RFBound + Into<f64>,
{
    fn build_from_group(&self, values: &[V]) -> (f64, u64) {
        let sum: f64 = values.iter().map(|v| v.clone().into()).sum();
        (sum, values.len() as u64)
    }
}

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
    pub fn new() -> Self {
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

/* ===================== TopK<T> ===================== */

/// The largest top-**K** values per key (requires `Ord`).
///
/// The accumulator maintains a **min-heap** (via `BinaryHeap<Reverse<T>>`) of
/// size ≤ `k`, so memory is bounded by `k`.
///
/// - Accumulator: `BinaryHeap<Reverse<T>>`
/// - Output: `Vec<T>` sorted descending.
///
/// # Notes
/// - `k == 0` will always produce an empty vector.
/// - Merge is implemented by pushing elements and trimming back to `k`.
#[derive(Clone, Debug)]
pub struct TopK<T> {
    /// Number of largest elements to keep.
    pub k: usize,
    _m: PhantomData<T>,
}
impl<T> TopK<T> {
    /// Create a new `TopK` with the given `k`.
    pub fn new(k: usize) -> Self {
        Self { k, _m: PhantomData }
    }
}

// We store a min-heap of size ≤ k using Reverse to keep the largest k elements.
impl<T> CombineFn<T, BinaryHeap<Reverse<T>>, Vec<T>> for TopK<T>
where
    T: RFBound + Ord,
{
    fn create(&self) -> BinaryHeap<Reverse<T>> {
        BinaryHeap::new()
    }

    fn add_input(&self, acc: &mut BinaryHeap<Reverse<T>>, v: T) {
        acc.push(Reverse(v));
        if acc.len() > self.k {
            acc.pop();
        } // drop smallest
    }

    fn merge(&self, acc: &mut BinaryHeap<Reverse<T>>, mut other: BinaryHeap<Reverse<T>>) {
        // Merge by pushing and trimming
        while let Some(x) = other.pop() {
            acc.push(x);
            if acc.len() > self.k {
                acc.pop();
            }
        }
    }

    fn finish(&self, mut acc: BinaryHeap<Reverse<T>>) -> Vec<T> {
        // Drain to a Vec in descending order
        let mut v = Vec::with_capacity(acc.len());
        while let Some(Reverse(x)) = acc.pop() {
            v.push(x);
        }
        v.reverse(); // largest first
        v
    }
}

impl<T> LiftableCombiner<T, BinaryHeap<Reverse<T>>, Vec<T>> for TopK<T>
where
    T: RFBound + Ord,
{
    fn build_from_group(&self, values: &[T]) -> BinaryHeap<Reverse<T>> {
        let mut heap: BinaryHeap<Reverse<T>> = BinaryHeap::new();
        for v in values.iter().cloned() {
            heap.push(Reverse(v));
            if heap.len() > self.k {
                heap.pop();
            }
        }
        heap
    }
}

/* ===================== DistinctSet<T> (exact set) ===================== */

/// Exact distinct over a stream: accumulates a `HashSet<T>` and finishes to a `Vec<T>`.
///
/// This is intended for use through helpers (e.g., `PCollection<T>::distinct()` or
/// `PCollection<(K,V)>::distinct_per_key()`), which expand the final `Vec<T>` back
/// into an element stream via `flat_map`.
#[derive(Clone, Copy, Debug)]
pub struct DistinctSet<T>(pub PhantomData<T>);
impl<T> DistinctSet<T> {
    pub fn new() -> Self {
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
/// Keeps the smallest `k` hash ranks in a max-heap; estimate is `(k-1)/r_k` where
/// `r_k` is the largest (i.e., k-th smallest) retained rank. For small `n < k`,
/// returns the exact count (`heap.len()`).
#[derive(Clone, Debug)]
pub struct KMVApproxDistinctCount<T> {
    pub k: usize,
    _m: PhantomData<T>,
}
impl<T> KMVApproxDistinctCount<T> {
    pub fn new(k: usize) -> Self {
        Self { k: k.max(4), _m: PhantomData }
    }
}

/// Accumulator keeps k smallest **distinct** ranks.
#[derive(Default)]
pub(crate) struct KMVAcc {
    heap: BinaryHeap<NotNan<f64>>,   // max-heap of the kept k smallest
    set: HashSet<NotNan<f64>>,       // membership test to prevent duplicates
    k: usize,
}

#[inline]
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
                // New rank worse than threshold; forget it from the set
                self.set.remove(&r);
            }
        }
        debug_assert!(self.heap.len() <= self.k && self.set.len() >= self.heap.len());
    }

    #[inline]
    fn merge_from(&mut self, mut other: KMVAcc) {
        // Move 'better' (smaller) ranks across, deduping
        while let Some(r) = other.heap.pop() {
            // Remove from other's set as we pop; not strictly needed, but neat
            other.set.remove(&r);
            self.try_insert(r);
        }
        // Any leftovers in other's set that weren't in heap (rare) can be ignored:
        // their ranks are >= other's threshold, and we’ve already considered the k best from other.
    }

    #[inline]
    fn finish(self) -> f64 {
        let m = self.set.len();
        if m == 0 {
            return 0.0;
        }
        if m < self.k {
            // Fewer than k uniques: exact count.
            return m as f64;
        }
        // m >= k: estimator based on k-th smallest rank (heap root).
        let rk = *self.heap.peek().expect("heap non-empty when m>=k");
        // Classic KMV estimator: (k-1)/R_k . Using (k-1) reduces small-sample bias slightly.
        ((self.k as f64) - 1.0) / rk.into_inner()
    }
}

impl<T> CombineFn<T, KMVAcc, f64> for KMVApproxDistinctCount<T>
where
    T: RFBound + Hash,
{
    fn create(&self) -> KMVAcc {
        KMVAcc { heap: BinaryHeap::new(), set: HashSet::new(), k: self.k }
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