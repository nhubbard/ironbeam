//! Bounded fixed-window conveniences using envelope event time.
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
        self.combine_per_window_named(key, size_ns, offset_ns, |values| {
            values
                .into_iter()
                .fold(T::default(), |sum, value| sum + value)
        })
    }
    fn count_per_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<(WindowInterval, u64)>, Error> {
        self.combine_per_window_named(key, size_ns, offset_ns, |values| {
            u64::try_from(values.len()).unwrap_or(u64::MAX)
        })
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
        self.combine_per_window_named(key, size_ns, offset_ns, |values| {
            values.into_iter().min().expect("non-empty fixed window")
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
        self.combine_per_window_named(key, size_ns, offset_ns, |values| {
            values.into_iter().max().expect("non-empty fixed window")
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
        self.combine_per_window_named(key, size_ns, offset_ns, |values| {
            let count = values.len();
            let total: f64 = values.into_iter().map(Into::into).sum();
            #[allow(clippy::cast_precision_loss)]
            {
                total / count as f64
            }
        })
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
        self.combine_per_key_and_window_named(key, size_ns, offset_ns, |values| {
            values
                .into_iter()
                .fold(V::default(), |sum, value| sum + value)
        })
    }
    fn count_per_key_and_window_named(
        &self,
        key: &str,
        size_ns: i64,
        offset_ns: i64,
    ) -> Result<PCollection<KeyedWindowed<K, u64>>, Error> {
        self.combine_per_key_and_window_named(key, size_ns, offset_ns, |values| {
            u64::try_from(values.len()).unwrap_or(u64::MAX)
        })
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
        self.combine_per_key_and_window_named(key, size_ns, offset_ns, |values| {
            values.into_iter().min().expect("non-empty fixed window")
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
        self.combine_per_key_and_window_named(key, size_ns, offset_ns, |values| {
            values.into_iter().max().expect("non-empty fixed window")
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
        self.combine_per_key_and_window_named(key, size_ns, offset_ns, |values| {
            let count = values.len();
            let total: f64 = values.into_iter().map(Into::into).sum();
            #[allow(clippy::cast_precision_loss)]
            {
                total / count as f64
            }
        })
    }
}
