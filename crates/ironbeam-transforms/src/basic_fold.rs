//! Small lifted accumulators for exact bounded aggregations.
use crate::MeanOutput;
use ironbeam_core::{CombineFn, Element, OrderedFoldFn};
use std::collections::HashSet;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Add;

pub struct Count;
impl<T: Element> CombineFn<T, u64, u64> for Count {
    fn create(&self) -> u64 {
        0
    }
    fn add_input(&self, count: &mut u64, _: T) {
        *count = count.saturating_add(1);
    }
    fn merge(&self, count: &mut u64, other: u64) {
        *count = count.saturating_add(other);
    }
    fn finish(&self, count: u64) -> u64 {
        count
    }
}

pub struct Sum;
impl<T: Element + Default + Add<Output = T>> CombineFn<T, T, T> for Sum {
    fn create(&self) -> T {
        T::default()
    }
    fn add_input(&self, total: &mut T, value: T) {
        *total = std::mem::take(total) + value;
    }
    fn merge(&self, total: &mut T, other: T) {
        self.add_input(total, other);
    }
    fn finish(&self, total: T) -> T {
        total
    }
}

pub struct Minimum;
impl<T: Element + Ord> CombineFn<T, Option<T>, Vec<T>> for Minimum {
    fn create(&self) -> Option<T> {
        None
    }
    fn add_input(&self, minimum: &mut Option<T>, value: T) {
        if minimum.as_ref().is_none_or(|current| value < *current) {
            *minimum = Some(value);
        }
    }
    fn merge(&self, minimum: &mut Option<T>, other: Option<T>) {
        if let Some(value) = other {
            self.add_input(minimum, value);
        }
    }
    fn finish(&self, minimum: Option<T>) -> Vec<T> {
        minimum.into_iter().collect()
    }
}

pub struct Maximum;
impl<T: Element + Ord> CombineFn<T, Option<T>, Vec<T>> for Maximum {
    fn create(&self) -> Option<T> {
        None
    }
    fn add_input(&self, maximum: &mut Option<T>, value: T) {
        if maximum.as_ref().is_none_or(|current| value > *current) {
            *maximum = Some(value);
        }
    }
    fn merge(&self, maximum: &mut Option<T>, other: Option<T>) {
        if let Some(value) = other {
            self.add_input(maximum, value);
        }
    }
    fn finish(&self, maximum: Option<T>) -> Vec<T> {
        maximum.into_iter().collect()
    }
}

pub struct Average;
impl<T: Element + Into<f64>> CombineFn<T, (f64, usize), f64> for Average {
    fn create(&self) -> (f64, usize) {
        (0.0, 0)
    }
    fn add_input(&self, acc: &mut (f64, usize), value: T) {
        acc.0 += value.into();
        acc.1 += 1;
    }
    fn merge(&self, acc: &mut (f64, usize), other: (f64, usize)) {
        acc.0 += other.0;
        acc.1 += other.1;
    }
    fn finish(&self, acc: (f64, usize)) -> f64 {
        if acc.1 == 0 {
            0.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            {
                acc.0 / acc.1 as f64
            }
        }
    }
}

pub struct Mean<O>(pub PhantomData<fn() -> O>);
impl<T: Element + Into<O>, O: MeanOutput> CombineFn<T, (O, usize), O> for Mean<O> {
    fn create(&self) -> (O, usize) {
        (O::default(), 0)
    }
    fn add_input(&self, acc: &mut (O, usize), value: T) {
        acc.0 = std::mem::take(&mut acc.0) + value.into();
        acc.1 += 1;
    }
    fn merge(&self, acc: &mut (O, usize), other: (O, usize)) {
        acc.0 = std::mem::take(&mut acc.0) + other.0;
        acc.1 += other.1;
    }
    fn finish(&self, acc: (O, usize)) -> O {
        if acc.1 == 0 {
            O::default()
        } else {
            O::divide_count(acc.0, acc.1)
        }
    }
}

pub struct DistinctCount;
impl<T: Element + Eq + Hash> CombineFn<T, HashSet<T>, u64> for DistinctCount {
    fn create(&self) -> HashSet<T> {
        HashSet::new()
    }
    fn add_input(&self, values: &mut HashSet<T>, value: T) {
        values.insert(value);
    }
    fn merge(&self, values: &mut HashSet<T>, other: HashSet<T>) {
        values.extend(other);
    }
    fn finish(&self, values: HashSet<T>) -> u64 {
        u64::try_from(values.len()).unwrap_or(u64::MAX)
    }
}

pub struct Take(pub usize);
impl<T: Element> OrderedFoldFn<T, Vec<T>, Vec<T>> for Take {
    fn create(&self) -> Vec<T> {
        Vec::new()
    }
    fn add_input(&self, values: &mut Vec<T>, value: T) {
        if values.len() < self.0 {
            values.push(value);
        }
    }
    fn finish(&self, values: Vec<T>) -> Vec<T> {
        values
    }
}

pub struct Distinct;
impl<T: Element + Eq + Hash + Clone> OrderedFoldFn<T, (HashSet<T>, Vec<T>), Vec<T>> for Distinct {
    fn create(&self) -> (HashSet<T>, Vec<T>) {
        (HashSet::new(), Vec::new())
    }
    fn add_input(&self, acc: &mut (HashSet<T>, Vec<T>), value: T) {
        if acc.0.insert(value.clone()) {
            acc.1.push(value);
        }
    }
    fn finish(&self, acc: (HashSet<T>, Vec<T>)) -> Vec<T> {
        acc.1
    }
}

pub struct DistinctBy<F>(pub F);
impl<T: Element, K: Eq + Hash + Send + 'static, F: Fn(&T) -> K + Send + Sync + 'static>
    OrderedFoldFn<T, (HashSet<K>, Vec<T>), Vec<T>> for DistinctBy<F>
{
    fn create(&self) -> (HashSet<K>, Vec<T>) {
        (HashSet::new(), Vec::new())
    }
    fn add_input(&self, acc: &mut (HashSet<K>, Vec<T>), value: T) {
        if acc.0.insert((self.0)(&value)) {
            acc.1.push(value);
        }
    }
    fn finish(&self, acc: (HashSet<K>, Vec<T>)) -> Vec<T> {
        acc.1
    }
}
