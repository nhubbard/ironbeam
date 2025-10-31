//! Top-K combiner for selecting the largest values

use crate::collection::{CombineFn, LiftableCombiner};
use crate::RFBound;
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
