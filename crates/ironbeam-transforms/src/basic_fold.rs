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

#[cfg(test)]
mod tests {
    use super::{Average, Count, DistinctCount, Maximum, Mean, Minimum, Sum};
    use ironbeam_core::CombineFn;
    use std::collections::HashSet;
    use std::marker::PhantomData;

    fn merge_two<V, A, O, C: CombineFn<V, A, O>>(
        combiner: &C,
        left: impl IntoIterator<Item = V>,
        right: impl IntoIterator<Item = V>,
    ) -> O {
        let mut first = combiner.create();
        let mut second = combiner.create();
        for value in left {
            combiner.add_input(&mut first, value);
        }
        for value in right {
            combiner.add_input(&mut second, value);
        }
        combiner.merge(&mut first, second);
        combiner.finish(first)
    }

    #[test]
    fn exact_accumulators_merge_partial_results_and_keep_empty_identity() {
        assert_eq!(merge_two(&Count, [1_i32, 2], [3]), 3);
        assert_eq!(merge_two(&Sum, [1_i32, 2], [3]), 6);
        assert_eq!(merge_two(&Minimum, [3_i32, 1], [2]), [1]);
        assert_eq!(merge_two(&Maximum, [3_i32, 1], [2]), [3]);
        assert_eq!(merge_two(&Average, [2_i32, 4], [6]), 4.0);
        assert_eq!(
            merge_two(&Mean::<f64>(PhantomData), [2_f64, 4.0], [6.0]),
            4.0
        );
        assert_eq!(
            merge_two::<i32, _, _, _>(&Minimum, [], []),
            Vec::<i32>::new()
        );
        assert_eq!(
            merge_two::<i32, _, _, _>(&Maximum, [], []),
            Vec::<i32>::new()
        );
        assert_eq!(
            merge_two::<f64, _, _, _>(&Mean::<f64>(PhantomData), [], []),
            0.0
        );
    }

    #[test]
    fn count_saturates_and_distinct_count_merges_without_duplicates() {
        let mut saturated = u64::MAX;
        <Count as CombineFn<i32, u64, u64>>::merge(&Count, &mut saturated, 1);
        assert_eq!(saturated, u64::MAX);
        let distinct: u64 = merge_two(&DistinctCount, [1_i32, 2], [2, 3]);
        assert_eq!(distinct, 3);
        let empty: u64 = merge_two::<i32, HashSet<i32>, u64, _>(&DistinctCount, [], []);
        assert_eq!(empty, 0);
    }
}
