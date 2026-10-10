#![allow(clippy::missing_errors_doc)]
//! Composed convenience transforms over runner-neutral Ironbeam graph primitives.
use ironbeam_core::{Element, Error, PCollection};
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Add;

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
    fn with_constant_key_named<K: Element + Clone + Sync>(
        &self,
        key: &str,
        constant: K,
    ) -> Result<PCollection<(K, T)>, Error>;
    fn to_display_string_named(&self, key: &str) -> Result<PCollection<String>, Error>
    where
        T: Display;
    fn to_list_globally_named(&self, key: &str) -> Result<PCollection<Vec<T>>, Error>;
    fn take_named(&self, key: &str, limit: usize) -> Result<PCollection<T>, Error>;
    fn distinct_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Eq + Hash + Clone;
    fn count_per_element_named(&self, key: &str) -> Result<PCollection<(T, u64)>, Error>
    where
        T: Eq + Hash + Clone;
    fn sum_globally_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Add<Output = T> + Default;
    fn min_globally_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Ord;
    fn max_globally_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Ord;
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
    fn with_constant_key_named<K: Element + Clone + Sync>(
        &self,
        key: &str,
        constant: K,
    ) -> Result<PCollection<(K, T)>, Error> {
        self.map_named(key, move |value| (constant.clone(), value))
    }
    fn to_display_string_named(&self, key: &str) -> Result<PCollection<String>, Error>
    where
        T: Display,
    {
        self.map_named(key, |value| value.to_string())
    }
    fn to_list_globally_named(&self, key: &str) -> Result<PCollection<Vec<T>>, Error> {
        self.combine_globally_named(key, |values| values)
    }
    fn take_named(&self, key: &str, limit: usize) -> Result<Self, Error> {
        self.combine_globally_named(&format!("{key}/collect"), move |mut values| {
            values.truncate(limit);
            values
        })?
        .flat_map_named(key, |values| values)
    }
    fn distinct_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Eq + Hash + Clone,
    {
        self.combine_globally_named(&format!("{key}/collect"), |values| {
            let mut seen = HashSet::new();
            values
                .into_iter()
                .filter(|value| seen.insert(value.clone()))
                .collect::<Vec<_>>()
        })?
        .flat_map_named(key, |values| values)
    }
    fn count_per_element_named(&self, key: &str) -> Result<PCollection<(T, u64)>, Error>
    where
        T: Eq + Hash + Clone,
    {
        self.map_named(&format!("{key}/key"), |value| (value, 1_u64))?
            .group_by_key_named(&format!("{key}/group"))?
            .map_named(key, |(value, occurrences)| {
                (value, u64::try_from(occurrences.len()).unwrap_or(u64::MAX))
            })
    }
    fn sum_globally_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Add<Output = T> + Default,
    {
        self.combine_globally_named(key, |values| {
            values.into_iter().fold(T::default(), Add::add)
        })
    }
    fn min_globally_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Ord,
    {
        self.combine_globally_named(&format!("{key}/collect"), |values| {
            values.into_iter().min().into_iter().collect::<Vec<_>>()
        })?
        .flat_map_named(key, |values| values)
    }
    fn max_globally_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Ord,
    {
        self.combine_globally_named(&format!("{key}/collect"), |values| {
            values.into_iter().max().into_iter().collect::<Vec<_>>()
        })?
        .flat_map_named(key, |values| values)
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
    fn swap_named(&self, key: &str) -> Result<PCollection<(V, K)>, Error>;
    fn sum_per_key_named(&self, key: &str) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Add<Output = V> + Default;
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
    fn swap_named(&self, key: &str) -> Result<PCollection<(V, K)>, Error> {
        self.map_named(key, |(group, value)| (value, group))
    }
    fn sum_per_key_named(&self, key: &str) -> Result<Self, Error>
    where
        K: Eq + Hash + Clone,
        V: Add<Output = V> + Default,
    {
        self.group_by_key_named(&format!("{key}/group"))?
            .map_named(key, |(group, values)| {
                (group, values.into_iter().fold(V::default(), Add::add))
            })
    }
}
