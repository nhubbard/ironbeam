use std::cmp::Ord;
use std::ops::Add;

use crate::{RFBound};
use crate::collection::CombineFn; // wherever CombineFn lives
use crate::collection::LiftableCombiner; // your new trait

/* ---------- Sum<T> ---------- */

#[derive(Clone, Copy, Debug, Default)]
pub struct Sum<T>(pub std::marker::PhantomData<T>);

impl<T> Sum<T> { pub fn new() -> Self { Sum(std::marker::PhantomData) } }

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

/* ---------- Min<T> ---------- */

#[derive(Clone, Copy, Debug, Default)]
pub struct Min<T>(pub std::marker::PhantomData<T>);

impl<T> Min<T> { pub fn new() -> Self { Min(std::marker::PhantomData) } }

// Accumulator is Option<T> to avoid requiring a sentinel default value.
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
                Some(a) => { if b < *a { *a = b; } }
                None => { *acc = Some(b); }
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

/* ---------- Max<T> ---------- */

#[derive(Clone, Copy, Debug, Default)]
pub struct Max<T>(pub std::marker::PhantomData<T>);

impl<T> Max<T> { pub fn new() -> Self { Max(std::marker::PhantomData) } }

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
                Some(a) => { if b > *a { *a = b; } }
                None => { *acc = Some(b); }
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

/* ---------- (Optional) AverageF64 ---------- */

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
        (values.iter().map(|v| (*v).clone().into()).sum(), values.len() as u64)
    }
}