//! Basic arithmetic combiners: Sum, Min, Max

use crate::collection::{CombineFn, LiftableCombiner};
use crate::RFBound;
use std::cmp::Ord;
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
