use std::cmp::Ord;
use std::collections::{BinaryHeap, HashSet};
use std::hash::Hash;
use std::ops::Add;

use crate::collection::{CombineFn, LiftableCombiner};
use crate::RFBound;

/* ===================== Sum<T> ===================== */

#[derive(Clone, Copy, Debug, Default)]
pub struct Sum<T>(pub std::marker::PhantomData<T>);
impl<T> Sum<T> { pub fn new() -> Self { Self(std::marker::PhantomData) } }

impl<T> CombineFn<T, T, T> for Sum<T>
where
    T: RFBound + Add<Output = T> + Default,
{
    fn create(&self) -> T { T::default() }

    fn add_input(&self, acc: &mut T, v: T) {
        *acc = std::mem::take(acc) + v;
    }

    fn merge(&self, acc: &mut T, other: T) {
        *acc = std::mem::take(acc) + other;
    }

    fn finish(&self, acc: T) -> T { acc }
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

#[derive(Clone, Copy, Debug, Default)]
pub struct Min<T>(pub std::marker::PhantomData<T>);
impl<T> Min<T> { pub fn new() -> Self { Self(std::marker::PhantomData) } }

impl<T> CombineFn<T, Option<T>, T> for Min<T>
where
    T: RFBound + Ord,
{
    fn create(&self) -> Option<T> { None }

    fn add_input(&self, acc: &mut Option<T>, v: T) {
        match acc {
            Some(cur) => if v < *cur { *cur = v },
            None => *acc = Some(v),
        }
    }

    fn merge(&self, acc: &mut Option<T>, other: Option<T>) {
        if let Some(b) = other {
            match acc {
                Some(a) => { if b < *a { *a = b } }
                None => { *acc = Some(b) }
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

#[derive(Clone, Copy, Debug, Default)]
pub struct Max<T>(pub std::marker::PhantomData<T>);
impl<T> Max<T> { pub fn new() -> Self { Self(std::marker::PhantomData) } }

impl<T> CombineFn<T, Option<T>, T> for Max<T>
where
    T: RFBound + Ord,
{
    fn create(&self) -> Option<T> { None }

    fn add_input(&self, acc: &mut Option<T>, v: T) {
        match acc {
            Some(cur) => if v > *cur { *cur = v },
            None => *acc = Some(v),
        }
    }

    fn merge(&self, acc: &mut Option<T>, other: Option<T>) {
        if let Some(b) = other {
            match acc {
                Some(a) => { if b > *a { *a = b } }
                None => { *acc = Some(b) }
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

#[derive(Clone, Copy, Debug, Default)]
pub struct AverageF64;

impl<V> CombineFn<V, (f64, u64), f64> for AverageF64
where
    V: RFBound + Into<f64>,
{
    fn create(&self) -> (f64, u64) { (0.0, 0) }

    fn add_input(&self, acc: &mut (f64, u64), v: V) {
        acc.0 += v.into();
        acc.1 += 1;
    }

    fn merge(&self, acc: &mut (f64, u64), other: (f64, u64)) {
        acc.0 += other.0;
        acc.1 += other.1;
    }

    fn finish(&self, acc: (f64, u64)) -> f64 {
        if acc.1 == 0 { 0.0 } else { acc.0 / (acc.1 as f64) }
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

#[derive(Clone, Copy, Debug, Default)]
pub struct DistinctCount<T>(pub std::marker::PhantomData<T>);
impl<T> DistinctCount<T> { pub fn new() -> Self { Self(std::marker::PhantomData) } }

impl<T> CombineFn<T, HashSet<T>, u64> for DistinctCount<T>
where
    T: RFBound + Eq + Hash,
{
    fn create(&self) -> HashSet<T> { HashSet::new() }

    fn add_input(&self, acc: &mut HashSet<T>, v: T) { acc.insert(v); }

    fn merge(&self, acc: &mut HashSet<T>, other: HashSet<T>) {
        if acc.is_empty() { *acc = other; }
        else { acc.extend(other); }
    }

    fn finish(&self, acc: HashSet<T>) -> u64 { acc.len() as u64 }
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

#[derive(Clone, Debug)]
pub struct TopK<T> { pub k: usize, _m: std::marker::PhantomData<T> }
impl<T> TopK<T> { pub fn new(k: usize) -> Self { Self { k, _m: std::marker::PhantomData } } }

// We store a min-heap of size ≤ k using Reverse to keep the largest k elements.
impl<T> CombineFn<T, BinaryHeap<std::cmp::Reverse<T>>, Vec<T>> for TopK<T>
where
    T: RFBound + Ord,
{
    fn create(&self) -> BinaryHeap<std::cmp::Reverse<T>> { BinaryHeap::new() }

    fn add_input(&self, acc: &mut BinaryHeap<std::cmp::Reverse<T>>, v: T) {
        acc.push(std::cmp::Reverse(v));
        if acc.len() > self.k { acc.pop(); } // drop smallest
    }

    fn merge(&self, acc: &mut BinaryHeap<std::cmp::Reverse<T>>, mut other: BinaryHeap<std::cmp::Reverse<T>>) {
        // Merge by pushing and trimming
        while let Some(x) = other.pop() {
            acc.push(x);
            if acc.len() > self.k { acc.pop(); }
        }
    }

    fn finish(&self, mut acc: BinaryHeap<std::cmp::Reverse<T>>) -> Vec<T> {
        // Drain to a Vec in descending order
        let mut v = Vec::with_capacity(acc.len());
        while let Some(std::cmp::Reverse(x)) = acc.pop() {
            v.push(x);
        }
        v.reverse(); // largest first
        v
    }
}

impl<T> LiftableCombiner<T, BinaryHeap<std::cmp::Reverse<T>>, Vec<T>> for TopK<T>
where
    T: RFBound + Ord,
{
    fn build_from_group(&self, values: &[T]) -> BinaryHeap<std::cmp::Reverse<T>> {
        let mut heap: BinaryHeap<std::cmp::Reverse<T>> = BinaryHeap::new();
        for v in values.iter().cloned() {
            heap.push(std::cmp::Reverse(v));
            if heap.len() > self.k { heap.pop(); }
        }
        heap
    }
}