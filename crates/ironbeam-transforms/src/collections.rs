//! Finite collection and dictionary conveniences.
use ironbeam_core::{CombineFn, Element, Error, OrderedFoldFn, PCollection};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub struct CollectVec;
impl<T: Element> OrderedFoldFn<T, Vec<T>, Vec<T>> for CollectVec {
    fn create(&self) -> Vec<T> {
        Vec::new()
    }
    fn add_input(&self, values: &mut Vec<T>, value: T) {
        values.push(value);
    }
    fn finish(&self, values: Vec<T>) -> Vec<T> {
        values
    }
}

struct CollectSet;
impl<T: Element + Eq + Hash> CombineFn<T, HashSet<T>, HashSet<T>> for CollectSet {
    fn create(&self) -> HashSet<T> {
        HashSet::new()
    }
    fn add_input(&self, values: &mut HashSet<T>, value: T) {
        values.insert(value);
    }
    fn merge(&self, values: &mut HashSet<T>, other: HashSet<T>) {
        values.extend(other);
    }
    fn finish(&self, values: HashSet<T>) -> HashSet<T> {
        values
    }
}

struct CollectMap;
impl<K: Element + Eq + Hash, V: Element> OrderedFoldFn<(K, V), HashMap<K, V>, HashMap<K, V>>
    for CollectMap
{
    fn create(&self) -> HashMap<K, V> {
        HashMap::new()
    }
    fn add_input(&self, values: &mut HashMap<K, V>, (key, value): (K, V)) {
        values.insert(key, value);
    }
    fn finish(&self, values: HashMap<K, V>) -> HashMap<K, V> {
        values
    }
}

pub trait CollectionExt<T: Element + Eq + Hash> {
    fn to_set_globally_named(&self, key: &str) -> Result<PCollection<HashSet<T>>, Error>;
}
impl<T: Element + Eq + Hash> CollectionExt<T> for PCollection<T> {
    fn to_set_globally_named(&self, key: &str) -> Result<PCollection<HashSet<T>>, Error> {
        self.combine_globally_lifted_named(key, CollectSet)
    }
}

pub trait KeyedCollectionExt<K: Element + Eq + Hash + Clone, V: Element> {
    fn to_list_per_key_named(&self, key: &str) -> Result<PCollection<(K, Vec<V>)>, Error>;
    fn to_set_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, HashSet<V>)>, Error>
    where
        V: Eq + Hash;
    fn to_dict_named(&self, key: &str) -> Result<PCollection<HashMap<K, V>>, Error>;
}
impl<K: Element + Eq + Hash + Clone, V: Element> KeyedCollectionExt<K, V> for PCollection<(K, V)> {
    fn to_list_per_key_named(&self, key: &str) -> Result<PCollection<(K, Vec<V>)>, Error> {
        self.fold_values_ordered_named(key, CollectVec)
    }
    fn to_set_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, HashSet<V>)>, Error>
    where
        V: Eq + Hash,
    {
        self.combine_values_lifted_named(group_key, CollectSet)?
            .map_named(key, |row| row)
    }
    fn to_dict_named(&self, key: &str) -> Result<PCollection<HashMap<K, V>>, Error> {
        self.fold_globally_ordered_named(key, CollectMap)
    }
}

#[cfg(test)]
#[path = "../tests/unit/collections__merge_tests.rs"]
mod merge_tests;
