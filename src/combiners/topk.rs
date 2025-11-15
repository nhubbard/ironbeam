//! Top-K combiner for selecting the largest values

use crate::RFBound;
use crate::collection::{CombineFn, LiftableCombiner};
use std::cmp::{Ord, Reverse};
use std::collections::BinaryHeap;
use std::marker::PhantomData;

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
    #[must_use]
    pub const fn new(k: usize) -> Self {
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

    fn merge(&self, acc: &mut BinaryHeap<Reverse<T>>, other: BinaryHeap<Reverse<T>>) {
        // Optimized partial-order merge:
        // If we can fit everything, just extend
        if acc.len() + other.len() <= self.k {
            acc.extend(other);
            return;
        }

        // Otherwise, merge via sorted order for better cache locality
        // Convert both heaps to sorted vecs (largest first)
        let mut v1: Vec<T> = Vec::with_capacity(acc.len());
        let mut v2: Vec<T> = Vec::with_capacity(other.len());

        while let Some(Reverse(x)) = acc.pop() {
            v1.push(x);
        }
        v1.reverse(); // now largest first

        for Reverse(x) in other {
            v2.push(x);
        }
        v2.sort_unstable();
        v2.reverse(); // now largest first

        // Merge the two sorted vectors, keeping only top k
        let mut i = 0;
        let mut j = 0;
        let mut result = BinaryHeap::with_capacity(self.k);

        while result.len() < self.k && (i < v1.len() || j < v2.len()) {
            let val = if i >= v1.len() {
                j += 1;
                v2[j - 1].clone()
            } else if j >= v2.len() || v1[i] >= v2[j] {
                i += 1;
                v1[i - 1].clone()
            } else {
                j += 1;
                v2[j - 1].clone()
            };
            result.push(Reverse(val));
        }

        *acc = result;
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
