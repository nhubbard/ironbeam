//! Finite top/bottom selection composed over global combine and keyed group.
use ironbeam_core::{Element, Error, PCollection};
use std::hash::Hash;

pub trait SelectionExt<T: Element + Ord> {
    fn top_k_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error>;
    fn bottom_k_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error>;
}
impl<T: Element + Ord> SelectionExt<T> for PCollection<T> {
    fn top_k_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error> {
        self.combine_globally_named(key, move |mut values| {
            values.sort_unstable_by(|left, right| right.cmp(left));
            values.truncate(k);
            values
        })
    }
    fn bottom_k_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error> {
        self.combine_globally_named(key, move |mut values| {
            values.sort_unstable();
            values.truncate(k);
            values
        })
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
        self.group_by_key_named(group_key)?
            .map_named(key, move |(group, mut values)| {
                values.sort_unstable_by(|left, right| right.cmp(left));
                values.truncate(k);
                (group, values)
            })
    }
    fn bottom_k_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        self.group_by_key_named(group_key)?
            .map_named(key, move |(group, mut values)| {
                values.sort_unstable();
                values.truncate(k);
                (group, values)
            })
    }
}
