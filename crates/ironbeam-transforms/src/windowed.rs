//! Bounded fixed-window conveniences using envelope event time.
use crate::basic_fold::{Average, Count, Maximum, Minimum, Sum};
use ironbeam_core::{Element, Error, KeyedWindowed, PCollection, WindowInterval};
use std::hash::Hash;
use std::ops::Add;

pub type KeyedWindowedGroups<K, V> = ((K, WindowInterval), Vec<V>);

pub trait WindowedExt<T: Element> {
    /// Pair each value with its assigned fixed-window interval.
    fn key_by_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>;
    /// Collect values by fixed-window interval.
    fn group_by_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, Vec<T>)>, Error>;
    /// Apply `f` to values grouped in each fixed window.
    fn combine_per_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<T>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<(WindowInterval, U)>, Error>;
    /// Sum values in each fixed window.
    fn sum_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Default + Add<Output = T>;
    /// Count values in each fixed window.
    fn count_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, u64)>, Error>;
    /// Return the least value in each fixed window.
    fn min_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Ord;
    /// Return the greatest value in each fixed window.
    fn max_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error>
    where
        T: Ord;
    /// Return the arithmetic mean in each fixed window as `f64`.
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
    fn key_by_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, T)>, Error> {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .with_window_named(key)
    }
    fn group_by_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, Vec<T>)>, Error> {
        self.key_by_window_named(&format!("{key}/tag"), size_ns, offset_ns)?
            .group_by_key_named(key)
    }
    fn combine_per_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<T>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<(WindowInterval, U)>, Error> {
        self.group_by_window_named(&format!("{key}/group"), size_ns, offset_ns)?
            .map_named(key, move |(window, values)| (window, f(values)))
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
    /// Pair each keyed value with its assigned fixed-window interval.
    fn key_by_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>;
    /// Collect values by key and fixed-window interval.
    fn group_by_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowedGroups<K, V>>, Error>;
    /// Apply `f` to values grouped by key and fixed window.
    fn combine_per_key_and_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<KeyedWindowed<K, U>>, Error>;
    /// Sum values by key and fixed window.
    fn sum_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Default + Add<Output = V>;
    /// Count values by key and fixed window.
    fn count_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, u64>>, Error>;
    /// Return the least value by key and fixed window.
    fn min_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Ord;
    /// Return the greatest value by key and fixed window.
    fn max_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error>
    where
        V: Ord;
    /// Return the arithmetic mean by key and fixed window as `f64`.
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
    fn key_by_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, V>>, Error> {
        self.fixed_windows_with_offset(&format!("{key}/window"), size_ns, offset_ns)?
            .with_window_named(&format!("{key}/tag"))?
            .map_named(key, |(window, (group, value))| ((group, window), value))
    }
    fn group_by_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowedGroups<K, V>>, Error> {
        self.key_by_key_and_window_named(&format!("{key}/tagged"), size_ns, offset_ns)?
            .group_by_key_named(key)
    }
    fn combine_per_key_and_window_named<U: Element>(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
        f: impl Fn(Vec<V>) -> U + Send + Sync + 'static,
    ) -> Result<PCollection<KeyedWindowed<K, U>>, Error> {
        self.group_by_key_and_window_named(&format!("{key}/group"), size_ns, offset_ns)?
            .map_named(key, move |(key_and_window, values)| {
                (key_and_window, f(values))
            })
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
