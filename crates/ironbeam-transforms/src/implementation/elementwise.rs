use ironbeam_core::{Boundedness, Element, Error, PCollection};
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Add;

use crate::basic_fold::{
    Average, Count, Distinct, DistinctBy, DistinctCount, Maximum, Mean, Minimum, Sum, Take,
};
use crate::collections::CollectVec;
pub trait MeanOutput: Element + Default + Add<Output = Self> {
    fn divide_count(total: Self, count: usize) -> Self;
}
impl MeanOutput for f32 {
    fn divide_count(total: Self, count: usize) -> Self {
        #[allow(clippy::cast_precision_loss)]
        {
            total / count as Self
        }
    }
}
impl MeanOutput for f64 {
    fn divide_count(total: Self, count: usize) -> Self {
        #[allow(clippy::cast_precision_loss)]
        {
            total / count as Self
        }
    }
}

pub trait ElementwiseExt<T: Element> {
    fn key_by_named<K: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<(K, T)>, Error>;
    fn with_keys_named<K: Element>(
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
    fn first_named(&self, key: &str) -> Result<PCollection<T>, Error>;
    fn distinct_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Eq + Hash + Clone;
    /// Emit one value per key and window using a portable keyed shuffle.
    /// An unbounded global input requires an early trigger before this call.
    fn distinct_per_window_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Eq + Hash + Clone;
    fn count_per_element_named(&self, key: &str) -> Result<PCollection<(T, u64)>, Error>
    where
        T: Eq + Hash + Clone;
    fn distinct_count_globally_named(&self, key: &str) -> Result<PCollection<u64>, Error>
    where
        T: Eq + Hash;
    fn distinct_by_named<K: Eq + Hash + Send + 'static>(
        &self,
        key: &str,
        by: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    /// Emit one representative per derived key and window through a portable
    /// shuffle. When several values share a key, choose the value with the
    /// smallest encoded bytes, independent of bundle arrival order. The
    /// value coder must be deterministic. An unbounded global input requires
    /// an early trigger.
    fn distinct_by_per_window_named<K: Element + Eq + Hash + Clone>(
        &self,
        key: &str,
        by: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    fn sum_globally_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Add<Output = T> + Default;
    fn average_globally_named(&self, key: &str) -> Result<PCollection<f64>, Error>
    where
        T: Into<f64>;
    fn mean_globally_named<O: MeanOutput>(&self, key: &str) -> Result<PCollection<O>, Error>
    where
        T: Into<O>;
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
    fn with_keys_named<K: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<(K, T)>, Error> {
        self.key_by_named(key, f)
    }
    fn mean_globally_named<O: MeanOutput>(&self, key: &str) -> Result<PCollection<O>, Error>
    where
        T: Into<O>,
    {
        self.combine_globally_lifted_named(key, Mean::<O>(PhantomData))
    }
    fn filter_map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> Option<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error> {
        self.flat_map_named(key, move |value| f(value).into_iter().collect())
    }
    fn count_globally_named(&self, key: &str) -> Result<PCollection<u64>, Error> {
        self.combine_globally_lifted_named(key, Count)
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
        self.fold_globally_ordered_named(key, CollectVec)
    }
    fn take_named(&self, key: &str, limit: usize) -> Result<Self, Error> {
        self.fold_globally_ordered_named(&format!("{key}/collect"), Take(limit))?
            .flat_map_named(key, |values| values)
    }
    fn first_named(&self, key: &str) -> Result<Self, Error> {
        self.take_named(key, 1)
    }
    fn distinct_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Eq + Hash + Clone,
    {
        self.fold_globally_ordered_named(&format!("{key}/collect"), Distinct)?
            .flat_map_named(key, |values| values)
    }
    fn distinct_per_window_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Eq + Hash + Clone,
    {
        self.map_named(&format!("{key}/key"), |value| (value, 0_u8))?
            .group_by_key_named(&format!("{key}/group"))?
            .map_named(key, |(value, _)| value)
    }
    fn count_per_element_named(&self, key: &str) -> Result<PCollection<(T, u64)>, Error>
    where
        T: Eq + Hash + Clone,
    {
        let keyed = self.map_named(&format!("{key}/key"), |value| (value, 1_u8))?;
        if self.boundedness()? == Boundedness::Unbounded {
            keyed
                .group_by_key_named(&format!("{key}/group"))?
                .map_named(key, |(value, occurrences)| {
                    (value, u64::try_from(occurrences.len()).unwrap_or(u64::MAX))
                })
        } else {
            keyed.combine_values_lifted_named(key, Count)
        }
    }
    fn distinct_count_globally_named(&self, key: &str) -> Result<PCollection<u64>, Error>
    where
        T: Eq + Hash,
    {
        self.combine_globally_lifted_named(key, DistinctCount)
    }
    fn distinct_by_named<K: Eq + Hash + Send + 'static>(
        &self,
        key: &str,
        by: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.fold_globally_ordered_named(&format!("{key}/collect"), DistinctBy(by))?
            .flat_map_named(key, |values| values)
    }
    fn distinct_by_per_window_named<K: Element + Eq + Hash + Clone>(
        &self,
        key: &str,
        by: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        if !T::supports_coder() || !K::supports_coder() {
            return Err(Error::graph(
                key,
                "portable representative distinct requires coded values and keys",
            ));
        }
        self.key_by_named(&format!("{key}/key"), by)?
            .group_by_key_named(&format!("{key}/group"))?
            .flat_map_fallible_named(key, |(_, values)| {
                let mut chosen = None::<(Vec<u8>, T)>;
                for value in values {
                    let bytes = value.encode()?;
                    if chosen.as_ref().is_none_or(|(prior, _)| bytes < *prior) {
                        chosen = Some((bytes, value));
                    }
                }
                Ok(chosen.into_iter().map(|(_, value)| value).collect())
            })
    }
    fn sum_globally_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Add<Output = T> + Default,
    {
        self.combine_globally_lifted_named(key, Sum)
    }
    fn average_globally_named(&self, key: &str) -> Result<PCollection<f64>, Error>
    where
        T: Into<f64>,
    {
        self.combine_globally_lifted_named(key, Average)
    }
    fn min_globally_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Ord,
    {
        self.combine_globally_lifted_named(&format!("{key}/collect"), Minimum)?
            .flat_map_named(key, |values| values)
    }
    fn max_globally_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Ord,
    {
        self.combine_globally_lifted_named(&format!("{key}/collect"), Maximum)?
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
    fn distinct_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash + Clone;
    fn distinct_count_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash;
    fn swap_named(&self, key: &str) -> Result<PCollection<(V, K)>, Error>;
    fn sum_per_key_named(&self, key: &str) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Add<Output = V> + Default;
    fn min_per_key_named(&self, group_key: &str, key: &str) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Ord;
    fn max_per_key_named(&self, group_key: &str, key: &str) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Ord;
    fn average_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, f64)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Into<f64>;
    fn mean_per_key_named<O: MeanOutput>(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Into<O>;
}
