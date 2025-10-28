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
//! - [`TopK<T>`] -- top-K largest values.
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
use std::cmp::{Ord, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::hash::Hash;
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

/// Top-**K** largest values per key (requires `Ord`).
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
