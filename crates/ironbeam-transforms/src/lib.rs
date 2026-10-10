#![allow(clippy::missing_errors_doc)]
//! Composed convenience transforms over runner-neutral Ironbeam graph primitives.
use ironbeam_core::{Element, Error, PCollection};
use std::hash::Hash;

pub trait ElementwiseExt<T: Element> {
    fn key_by_named<K: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<(K, T)>, Error>;
    fn filter_map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> Option<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    fn count_globally_named(&self, key: &str) -> Result<PCollection<u64>, Error>;
}
impl<T: Element> ElementwiseExt<T> for PCollection<T> {
    fn key_by_named<K: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<(K, T)>, Error> {
        self.map_named(key, move |value| {
            let group = f(&value);
            (group, value)
        })
    }
    fn filter_map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> Option<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.flat_map_named(key, move |value| f(value).into_iter().collect())
    }
    fn count_globally_named(&self, key: &str) -> Result<PCollection<u64>, Error> {
        self.combine_globally_named(key, |values| {
            u64::try_from(values.len()).unwrap_or(u64::MAX)
        })
    }
}

pub trait KeyedExt<K: Element, V: Element> {
    fn keys_named(&self, key: &str) -> Result<PCollection<K>, Error>;
    fn values_named(&self, key: &str) -> Result<PCollection<V>, Error>;
    fn map_values_named<W: Element>(
        &self,
        key: &str,
        f: impl Fn(V) -> W + Send + Sync + 'static,
    ) -> Result<PCollection<(K, W)>, Error>;
    fn filter_values_named(
        &self,
        key: &str,
        f: impl Fn(&V) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<(K, V)>, Error>;
    fn count_per_key_named(
        &self,
        group_key: &str,
        count_key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>
    where
        K: Eq + Hash + Clone;
}
impl<K: Element, V: Element> KeyedExt<K, V> for PCollection<(K, V)> {
    fn keys_named(&self, key: &str) -> Result<PCollection<K>, Error> {
        self.map_named(key, |(key, _)| key)
    }
    fn values_named(&self, key: &str) -> Result<PCollection<V>, Error> {
        self.map_named(key, |(_, value)| value)
    }
    fn map_values_named<W: Element>(
        &self,
        key: &str,
        f: impl Fn(V) -> W + Send + Sync + 'static,
    ) -> Result<PCollection<(K, W)>, Error> {
        self.map_named(key, move |(group, value)| (group, f(value)))
    }
    fn filter_values_named(
        &self,
        key: &str,
        f: impl Fn(&V) -> bool + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.filter_named(key, move |(_, value)| f(value))
    }
    fn count_per_key_named(
        &self,
        group_key: &str,
        count_key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>
    where
        K: Eq + Hash + Clone,
    {
        self.group_by_key_named(group_key)?
            .map_named(count_key, |(key, values)| {
                (key, u64::try_from(values.len()).unwrap_or(u64::MAX))
            })
    }
}
