//! Bounded-memory top/bottom selection for linear Direct inputs.
use ironbeam_core::{CombineFn, Element, Error, PCollection};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::hash::Hash;

struct TopK {
    k: usize,
}
impl<T: Element + Ord> CombineFn<T, BinaryHeap<Reverse<T>>, Vec<T>> for TopK {
    fn create(&self) -> BinaryHeap<Reverse<T>> {
        BinaryHeap::new()
    }
    fn add_input(&self, heap: &mut BinaryHeap<Reverse<T>>, value: T) {
        if self.k == 0 {
            return;
        }
        heap.push(Reverse(value));
        if heap.len() > self.k {
            heap.pop();
        }
    }
    fn merge(&self, heap: &mut BinaryHeap<Reverse<T>>, other: BinaryHeap<Reverse<T>>) {
        for Reverse(value) in other {
            self.add_input(heap, value);
        }
    }
    fn finish(&self, heap: BinaryHeap<Reverse<T>>) -> Vec<T> {
        let mut values = heap
            .into_iter()
            .map(|Reverse(value)| value)
            .collect::<Vec<_>>();
        values.sort_unstable_by(|left, right| right.cmp(left));
        values
    }
}

struct BottomK {
    k: usize,
}
impl<T: Element + Ord> CombineFn<T, BinaryHeap<T>, Vec<T>> for BottomK {
    fn create(&self) -> BinaryHeap<T> {
        BinaryHeap::new()
    }
    fn add_input(&self, heap: &mut BinaryHeap<T>, value: T) {
        if self.k == 0 {
            return;
        }
        heap.push(value);
        if heap.len() > self.k {
            heap.pop();
        }
    }
    fn merge(&self, heap: &mut BinaryHeap<T>, other: BinaryHeap<T>) {
        for value in other {
            self.add_input(heap, value);
        }
    }
    fn finish(&self, heap: BinaryHeap<T>) -> Vec<T> {
        let mut values = heap.into_vec();
        values.sort_unstable();
        values
    }
}

pub trait SelectionExt<T: Element + Ord> {
    fn top_k_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error>;
    fn bottom_k_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error>;
}
impl<T: Element + Ord> SelectionExt<T> for PCollection<T> {
    fn top_k_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error> {
        self.combine_globally_lifted_named(key, TopK { k })
    }
    fn bottom_k_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error> {
        self.combine_globally_lifted_named(key, BottomK { k })
    }
}

pub trait KeyedSelectionExt<K: Element + Eq + Hash + Clone, V: Element + Ord> {
    fn top_k_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error>;
    fn bottom_k_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error>;
}
impl<K: Element + Eq + Hash + Clone, V: Element + Ord> KeyedSelectionExt<K, V>
    for PCollection<(K, V)>
{
    fn top_k_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        self.combine_values_lifted_named(group_key, TopK { k })?
            .map_named(key, |row| row)
    }
    fn bottom_k_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        self.combine_values_lifted_named(group_key, BottomK { k })?
            .map_named(key, |row| row)
    }
}
