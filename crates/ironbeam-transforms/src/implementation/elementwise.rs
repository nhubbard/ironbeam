use ironbeam_core::{Boundedness, Element, Error, PCollection};
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Add;

use crate::basic_fold::{
    Average, Count, Distinct, DistinctBy, DistinctCount, Maximum, Mean, Minimum, Sum, Take,
};
use crate::collections::CollectVec;
/// Numeric output that can divide an accumulated sum by its count.
pub trait MeanOutput: Element + Default + Add<Output = Self> {
    /// Divide an accumulated total by its input count.
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

/// Elementwise selection, keying, finite collection, and aggregate helpers.
pub trait ElementwiseExt<T: Element> {
    /// Pair each input with a key derived from it.
    fn key_by_named<K: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<(K, T)>, Error>;
    /// Alias for `key_by_named`, using the same key derivation.
    fn with_keys_named<K: Element>(
        &self,
        key: &str,
        f: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<(K, T)>, Error>;
    /// Drop `None` results and emit the mapped values from `Some`.
    fn filter_map_named<U: Element>(
        &self,
        key: &str,
        f: impl Fn(T) -> Option<U> + Send + Sync + 'static,
    ) -> Result<PCollection<U>, Error>;
    /// Count all inputs through a global combine.
    fn count_globally_named(&self, key: &str) -> Result<PCollection<u64>, Error>;
    /// Pair each input with a clone of `constant`.
    fn with_constant_key_named<K: Element + Clone + Sync>(
        &self,
        key: &str,
        constant: K,
    ) -> Result<PCollection<(K, T)>, Error>;
    /// Format each input with its `Display` implementation.
    fn to_display_string_named(&self, key: &str) -> Result<PCollection<String>, Error>
    where
        T: Display;
    /// Collect all values into a vector in Direct encounter order.
    fn to_list_globally_named(&self, key: &str) -> Result<PCollection<Vec<T>>, Error>;
    /// Collect each window's values in ascending order of `order_key`.
    ///
    /// Each nonempty pane produces one vector. A bounded, empty global
    /// window produces an empty vector. Duplicate order keys in a pane
    /// fail the job. The output timestamp comes from the smallest order key.
    /// Unbounded input requires a finite window or an early trigger.
    fn to_list_ordered_by_named<Q: Ord + Send + 'static>(
        &self,
        key: &str,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<PCollection<Vec<T>>, Error>;
    /// Emit at most `limit` values in Direct encounter order.
    fn take_named(&self, key: &str, limit: usize) -> Result<PCollection<T>, Error>;
    /// Emit at most `limit` values per pane, in ascending order of `order_key`.
    ///
    /// Order keys must be unique within each pane, including values
    /// beyond `limit`. Unbounded input requires a finite window or an early
    /// trigger.
    fn take_ordered_by_named<Q: Ord + Send + 'static>(
        &self,
        key: &str,
        limit: usize,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    /// Emit the first value in Direct encounter order, if any.
    fn first_named(&self, key: &str) -> Result<PCollection<T>, Error>;
    /// Emit the value with the smallest `order_key` in each nonempty pane.
    ///
    /// Duplicate order keys fail the job. Unbounded input requires a finite
    /// window or an early trigger.
    fn first_ordered_by_named<Q: Ord + Send + 'static>(
        &self,
        key: &str,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    /// Keep the first occurrence of each value in Direct encounter order.
    fn distinct_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Eq + Hash + Clone;
    /// Emit one value per key and window using a portable keyed shuffle.
    /// An unbounded global input requires an early trigger before this call.
    fn distinct_per_window_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Eq + Hash + Clone;
    /// Count occurrences of each distinct input value.
    fn count_per_element_named(&self, key: &str) -> Result<PCollection<(T, u64)>, Error>
    where
        T: Eq + Hash + Clone;
    /// Count distinct values through a global combine.
    fn distinct_count_globally_named(&self, key: &str) -> Result<PCollection<u64>, Error>
    where
        T: Eq + Hash;
    /// Keep the first value for each derived key in Direct encounter order.
    fn distinct_by_named<K: Eq + Hash + Send + 'static>(
        &self,
        key: &str,
        by: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<PCollection<T>, Error>;
    /// Keep the first value for each derived key in ascending `order_key` order.
    ///
    /// Selection is independent within each pane. Order keys must be unique
    /// within that pane, including discarded values. Unbounded input
    /// requires a finite window or an early trigger.
    fn distinct_by_ordered_by_named<K: Eq + Hash + Send + 'static, Q: Ord + Send + 'static>(
        &self,
        key: &str,
        by: impl Fn(&T) -> K + Send + Sync + 'static,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
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
    /// Sum all values, using `Default` as the empty-input identity.
    fn sum_globally_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Add<Output = T> + Default;
    /// Return the arithmetic mean of all values as `f64`.
    fn average_globally_named(&self, key: &str) -> Result<PCollection<f64>, Error>
    where
        T: Into<f64>;
    /// Return the arithmetic mean using the requested output type.
    fn mean_globally_named<O: MeanOutput>(&self, key: &str) -> Result<PCollection<O>, Error>
    where
        T: Into<O>;
    /// Return the least value under `Ord`.
    fn min_globally_named(&self, key: &str) -> Result<PCollection<T>, Error>
    where
        T: Ord;
    /// Return the greatest value under `Ord`.
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
    fn to_list_ordered_by_named<Q: Ord + Send + 'static>(
        &self,
        key: &str,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<PCollection<Vec<T>>, Error> {
        self.fold_globally_ordered_by_named(key, order_key, CollectVec)
    }
    fn take_named(&self, key: &str, limit: usize) -> Result<Self, Error> {
        self.fold_globally_ordered_named(&format!("{key}/collect"), Take(limit))?
            .flat_map_named(key, |values| values)
    }
    fn take_ordered_by_named<Q: Ord + Send + 'static>(
        &self,
        key: &str,
        limit: usize,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.fold_globally_ordered_by_named(&format!("{key}/collect"), order_key, Take(limit))?
            .flat_map_named(key, |values| values)
    }
    fn first_named(&self, key: &str) -> Result<Self, Error> {
        self.take_named(key, 1)
    }
    fn first_ordered_by_named<Q: Ord + Send + 'static>(
        &self,
        key: &str,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.take_ordered_by_named(key, 1, order_key)
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
    fn distinct_by_ordered_by_named<K: Eq + Hash + Send + 'static, Q: Ord + Send + 'static>(
        &self,
        key: &str,
        by: impl Fn(&T) -> K + Send + Sync + 'static,
        order_key: impl Fn(&T) -> Q + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.fold_globally_ordered_by_named(&format!("{key}/collect"), order_key, DistinctBy(by))?
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

/// Keyed mapping, selection, distinctness, and aggregate helpers.
pub trait KeyedExt<K: Element, V: Element> {
    /// Drop values and retain each pair's key.
    fn keys_named(&self, key: &str) -> Result<PCollection<K>, Error>;
    /// Drop keys and retain each pair's value.
    fn values_named(&self, key: &str) -> Result<PCollection<V>, Error>;
    /// Map each value while keeping its key.
    fn map_values_named<W: Element>(
        &self,
        key: &str,
        f: impl Fn(V) -> W + Send + Sync + 'static,
    ) -> Result<PCollection<(K, W)>, Error>;
    /// Keep pairs whose value satisfies the predicate.
    fn filter_values_named(
        &self,
        key: &str,
        f: impl Fn(&V) -> bool + Send + Sync + 'static,
    ) -> Result<PCollection<(K, V)>, Error>;
    /// Count input pairs per key.
    fn count_per_key_named(
        &self,
        group_key: &str,
        count_key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>
    where
        K: Eq + Hash + Clone;
    /// Keep one occurrence of each value per key and emitted window pane.
    ///
    /// Unbounded input requires a closing window or a trigger. A value can
    /// appear again in a later pane when the trigger discards prior contents.
    /// Output order after grouping is unspecified.
    fn distinct_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash + Clone;
    /// Count distinct values per key.
    fn distinct_count_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash;
    /// Exchange the key and value in each pair.
    fn swap_named(&self, key: &str) -> Result<PCollection<(V, K)>, Error>;
    /// Sum values per key, using `Default` as the identity.
    fn sum_per_key_named(&self, key: &str) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Add<Output = V> + Default;
    /// Return the least value per key under `Ord`.
    fn min_per_key_named(&self, group_key: &str, key: &str) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Ord;
    /// Return the greatest value per key under `Ord`.
    fn max_per_key_named(&self, group_key: &str, key: &str) -> Result<PCollection<(K, V)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Ord;
    /// Return the arithmetic mean of values per key as `f64`.
    fn average_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, f64)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Into<f64>;
    /// Return the arithmetic mean of values per key in output type `O`.
    fn mean_per_key_named<O: MeanOutput>(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Into<O>;
}
