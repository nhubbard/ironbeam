#![allow(clippy::missing_errors_doc)]
//! Composed convenience transforms over runner-neutral Ironbeam graph primitives.
use ironbeam_core::{Element, Error, PCollection};
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Add;

pub trait MeanOutput: Element + Default + Add<Output = Self> {
    fn divide_count(total: Self, count: usize) -> Self;
}
impl MeanOutput for f32 {
    fn divide_count(total: Self, count: usize) -> Self {
        #[allow(clippy::cast_precision_loss)]
        {
            total / count as f32
        }
    }
}
impl MeanOutput for f64 {
    fn divide_count(total: Self, count: usize) -> Self {
        #[allow(clippy::cast_precision_loss)]
        {
            total / count as f64
        }
    }
}

mod predicates;
pub use predicates::PredicateExt;
mod selection;
pub use selection::{KeyedSelectionExt, SelectionExt};
mod collections;
pub use collections::{CollectionExt, KeyedCollectionExt};
mod regex_helpers;
pub use regex_helpers::RegexExt;
mod fallible;
pub use fallible::TryProcessExt;
mod sampling;
pub use sampling::{KeyedSamplingExt, SamplingExt};
mod statistical;
pub use statistical::{KeyedStatisticalExt, StatisticalExt, TDigest};
mod approx_distinct;
pub use approx_distinct::{ApproxDistinctExt, KeyedApproxDistinctExt};
mod side_inputs;
pub use side_inputs::{
    ConstantSideInputExt, SideMap, SideMultimap, SideSingleton, SideVec, side_hashmap,
    side_multimap, side_singleton, side_vec,
};
mod log_elements;
pub use log_elements::LogElementsExt;
mod validation;
pub use validation::{
    ErrorCollector, KeyedValidationExt, RecordError, Validate, ValidationError, ValidationExt,
    ValidationMode, ValidationResult,
};
mod dead_letter;
pub use dead_letter::{Classified, DeadLetter, DeadLetterExt};

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
        self.combine_globally_named(key, |values| {
            if values.is_empty() {
                O::default()
            } else {
                let count = values.len();
                let total = values
                    .into_iter()
                    .map(Into::into)
                    .fold(O::default(), Add::add);
                O::divide_count(total, count)
            }
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
    fn first_named(&self, key: &str) -> Result<Self, Error> {
        self.take_named(key, 1)
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
    fn distinct_count_globally_named(&self, key: &str) -> Result<PCollection<u64>, Error>
    where
        T: Eq + Hash,
    {
        self.combine_globally_named(key, |values| {
            u64::try_from(values.into_iter().collect::<HashSet<_>>().len()).unwrap_or(u64::MAX)
        })
    }
    fn distinct_by_named<K: Eq + Hash + Send + 'static>(
        &self,
        key: &str,
        by: impl Fn(&T) -> K + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        self.combine_globally_named(&format!("{key}/collect"), move |values| {
            let mut seen = HashSet::new();
            values
                .into_iter()
                .filter(|value| seen.insert(by(value)))
                .collect::<Vec<_>>()
        })?
        .flat_map_named(key, |values| values)
    }
    fn sum_globally_named(&self, key: &str) -> Result<Self, Error>
    where
        T: Add<Output = T> + Default,
    {
        self.combine_globally_named(key, |values| {
            values.into_iter().fold(T::default(), Add::add)
        })
    }
    fn average_globally_named(&self, key: &str) -> Result<PCollection<f64>, Error>
    where
        T: Into<f64>,
    {
        self.combine_globally_named(key, |values| {
            if values.is_empty() {
                0.0
            } else {
                #[allow(clippy::cast_precision_loss)]
                let count = values.len() as f64;
                values.into_iter().map(Into::into).sum::<f64>() / count
            }
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
    fn distinct_per_key_named(&self, group_key: &str, key: &str) -> Result<Self, Error>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash + Clone,
    {
        self.group_by_key_named(group_key)?
            .flat_map_named(key, |(group, values)| {
                let mut seen = HashSet::new();
                values
                    .into_iter()
                    .filter(|value| seen.insert(value.clone()))
                    .map(|value| (group.clone(), value))
                    .collect()
            })
    }
    fn distinct_count_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Eq + Hash,
    {
        self.group_by_key_named(group_key)?
            .map_named(key, |(group, values)| {
                (
                    group,
                    u64::try_from(values.into_iter().collect::<HashSet<_>>().len())
                        .unwrap_or(u64::MAX),
                )
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
    fn min_per_key_named(&self, group_key: &str, key: &str) -> Result<Self, Error>
    where
        K: Eq + Hash + Clone,
        V: Ord,
    {
        self.group_by_key_named(group_key)?
            .flat_map_named(key, |(group, values)| {
                values
                    .into_iter()
                    .min()
                    .map(|value| (group, value))
                    .into_iter()
                    .collect()
            })
    }
    fn max_per_key_named(&self, group_key: &str, key: &str) -> Result<Self, Error>
    where
        K: Eq + Hash + Clone,
        V: Ord,
    {
        self.group_by_key_named(group_key)?
            .flat_map_named(key, |(group, values)| {
                values
                    .into_iter()
                    .max()
                    .map(|value| (group, value))
                    .into_iter()
                    .collect()
            })
    }
    fn average_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, f64)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Into<f64>,
    {
        self.group_by_key_named(group_key)?
            .map_named(key, |(group, values)| {
                #[allow(clippy::cast_precision_loss)]
                let count = values.len() as f64;
                let average = if values.is_empty() {
                    0.0
                } else {
                    values.into_iter().map(Into::into).sum::<f64>() / count
                };
                (group, average)
            })
    }
    fn mean_per_key_named<O: MeanOutput>(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, O)>, Error>
    where
        K: Eq + Hash + Clone,
        V: Into<O>,
    {
        self.group_by_key_named(group_key)?
            .map_named(key, |(group, values)| {
                let count = values.len();
                let total = values
                    .into_iter()
                    .map(Into::into)
                    .fold(O::default(), Add::add);
                (
                    group,
                    if count == 0 {
                        O::default()
                    } else {
                        O::divide_count(total, count)
                    },
                )
            })
    }
}
