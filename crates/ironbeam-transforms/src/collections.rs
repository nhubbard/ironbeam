//! Finite collection and dictionary conveniences.
use ironbeam_core::{Element, Error, PCollection};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

pub trait CollectionExt<T: Element + Eq + Hash> {
    fn to_set_globally_named(&self, key: &str) -> Result<PCollection<HashSet<T>>, Error>;
}
impl<T: Element + Eq + Hash> CollectionExt<T> for PCollection<T> {
    fn to_set_globally_named(&self, key: &str) -> Result<PCollection<HashSet<T>>, Error> {
        self.combine_globally_named(key, |values| values.into_iter().collect())
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
        self.group_by_key_named(key)
    }
    fn to_set_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, HashSet<V>)>, Error>
    where
        V: Eq + Hash,
    {
        self.group_by_key_named(group_key)?
            .map_named(key, |(group, values)| (group, values.into_iter().collect()))
    }
    fn to_dict_named(&self, key: &str) -> Result<PCollection<HashMap<K, V>>, Error> {
        self.combine_globally_named(key, |values| values.into_iter().collect())
    }
}
