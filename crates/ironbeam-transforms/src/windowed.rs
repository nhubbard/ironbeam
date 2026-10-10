//! Bounded fixed-window conveniences using envelope event time.
use crate::basic_fold::{Average, Count, Maximum, Minimum, Sum};
use ironbeam_core::{Element, Error, KeyedWindowed, PCollection, WindowInterval};
use std::hash::Hash;
use std::ops::Add;

pub trait WindowedExt<T: Element> {
    fn combine_per_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<T>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<(WindowInterval, U)>, Error>;
    fn sum_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Default + Add<Output = T>;
    fn count_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, u64)>, Error>;
    fn min_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Ord;
    fn max_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Ord;
    fn average_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, f64)>, Error>
    where
        T: Into<f64>;
}

impl<T: Element> WindowedExt<T> for PCollection<T> {
    fn combine_per_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<T>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<(WindowInterval, U)>, Error> {
        PCollection::combine_per_window_named(self, key, size_ns, offset_ns, f)
    }
    fn sum_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Default + Add<Output = T>,
    {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_globally_lifted_named(&format!("{key}/combine"), Sum)?
            .with_window_named(key)
    }
    fn count_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, u64)>, Error> {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_globally_lifted_named(&format!("{key}/combine"), Count)?
            .with_window_named(key)
    }
    fn min_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Ord,
    {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_globally_lifted_named(&format!("{key}/combine"), Minimum)?
            .with_window_named(&format!("{key}/tag"))?
            .flat_map_named(key, |(window, values)| {
                values.into_iter().map(|value| (window, value)).collect()
            })
    }
    fn max_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Ord,
    {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_globally_lifted_named(&format!("{key}/combine"), Maximum)?
            .with_window_named(&format!("{key}/tag"))?
            .flat_map_named(key, |(window, values)| {
                values.into_iter().map(|value| (window, value)).collect()
            })
    }
    fn average_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, f64)>, Error>
    where
        T: Into<f64>,
    {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_globally_lifted_named(&format!("{key}/combine"), Average)?
            .with_window_named(key)
    }
}

pub trait KeyedWindowedExt<K: Element, V: Element> {
    fn combine_per_key_and_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<KeyedWindowed<K, U>>, Error>;
    fn sum_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Default + Add<Output = V>;
    fn count_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, u64>>, Error>;
    fn min_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Ord;
    fn max_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Ord;
    fn average_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, f64>>, Error>
    where
        V: Into<f64>;
}

impl<K: Element + Eq + Hash + Clone, V: Element> KeyedWindowedExt<K, V> for PCollection<(K, V)> {
    fn combine_per_key_and_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<KeyedWindowed<K, U>>, Error> {
        PCollection::combine_per_key_and_window_named(self, key, size_ns, offset_ns, f)
    }
    fn sum_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Default + Add<Output = V>,
    {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_values_lifted_named(&format!("{key}/combine"), Sum)?
            .with_window_named(&format!("{key}/tag"))?
            .map_named(key, |(window, (group, value))| ((group, window), value))
    }
    fn count_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, u64>>, Error> {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_values_lifted_named(&format!("{key}/combine"), Count)?
            .with_window_named(&format!("{key}/tag"))?
            .map_named(key, |(window, (group, value))| ((group, window), value))
    }
    fn min_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Ord,
    {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_values_lifted_named(&format!("{key}/combine"), Minimum)?
            .with_window_named(&format!("{key}/tag"))?
            .flat_map_named(key, |(window, (group, values))| {
                values
                    .into_iter()
                    .map(|value| ((group.clone(), window), value))
                    .collect()
            })
    }
    fn max_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Ord,
    {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_values_lifted_named(&format!("{key}/combine"), Maximum)?
            .with_window_named(&format!("{key}/tag"))?
            .flat_map_named(key, |(window, (group, values))| {
                values
                    .into_iter()
                    .map(|value| ((group.clone(), window), value))
                    .collect()
            })
    }
    fn average_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, f64>>, Error>
    where
        V: Into<f64>,
    {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .combine_values_lifted_named(&format!("{key}/combine"), Average)?
            .with_window_named(&format!("{key}/tag"))?
            .map_named(key, |(window, (group, value))| ((group, window), value))
    }
}
