//! Direct-runner terminal conveniences for bounded collections.
use crate::{DirectRunner, JobConfig};
use ironbeam_core::{Element, Error, PCollection};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::panic::{AssertUnwindSafe, catch_unwind};

pub trait DirectSortedExt<T: Element + Ord> {
    fn collect_sorted_named(&self, key: &str, config: &JobConfig) -> Result<Vec<T>, Error>;
    fn collect_seq_sorted_named(&self, key: &str) -> Result<Vec<T>, Error> {
        self.collect_sorted_named(key, &JobConfig::default())
    }
    fn collect_par_sorted_named(&self, key: &str, parallelism: usize) -> Result<Vec<T>, Error> {
        self.collect_sorted_named(
            key,
            &JobConfig {
                parallelism,
                ..JobConfig::default()
            },
        )
    }
}
impl<T: Element + Ord> DirectSortedExt<T> for PCollection<T> {
    fn collect_sorted_named(&self, key: &str, config: &JobConfig) -> Result<Vec<T>, Error> {
        let output = self.output(key)?;
        let mut values = DirectRunner.run_collect(&output, config)?.0;
        values.sort_unstable();
        Ok(values)
    }
}

pub trait DirectKeyedCollectExt<K: Element + Eq + Hash, V: Element> {
    fn to_hashmap_named(&self, key: &str, config: &JobConfig) -> Result<HashMap<K, V>, Error>;
}
impl<K: Element + Eq + Hash, V: Element> DirectKeyedCollectExt<K, V> for PCollection<(K, V)> {
    fn to_hashmap_named(&self, key: &str, config: &JobConfig) -> Result<HashMap<K, V>, Error> {
        let output = self.output(key)?;
        Ok(DirectRunner
            .run_collect(&output, config)?
            .0
            .into_iter()
            .collect())
    }
}

pub trait DirectKeySortedExt<K: Element + Ord, V: Element> {
    fn collect_sorted_by_key_named(
        &self,
        key: &str,
        config: &JobConfig,
    ) -> Result<Vec<(K, V)>, Error>;

    fn collect_seq_sorted_by_key_named(&self, key: &str) -> Result<Vec<(K, V)>, Error> {
        self.collect_sorted_by_key_named(key, &JobConfig::default())
    }

    fn collect_par_sorted_by_key_named(
        &self,
        key: &str,
        parallelism: usize,
    ) -> Result<Vec<(K, V)>, Error> {
        self.collect_sorted_by_key_named(
            key,
            &JobConfig {
                parallelism,
                ..JobConfig::default()
            },
        )
    }
}
impl<K: Element + Ord, V: Element> DirectKeySortedExt<K, V> for PCollection<(K, V)> {
    fn collect_sorted_by_key_named(
        &self,
        key: &str,
        config: &JobConfig,
    ) -> Result<Vec<(K, V)>, Error> {
        let output = self.output(key)?;
        let mut values = DirectRunner.run_collect(&output, config)?.0;
        values.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(values)
    }
}

pub trait DirectFallibleExt<T: Element, E: Element + Display> {
    fn collect_fail_fast_named(&self, key: &str, config: &JobConfig) -> Result<Vec<T>, Error>;
}

pub trait DirectPartitionExt<T: Element, E: Element> {
    fn collect_partitioned_named(
        &self,
        key: &str,
        config: &JobConfig,
    ) -> Result<(Vec<T>, Vec<E>), Error>;
}

/// One of three typed buckets produced after a bounded Direct collection.
pub enum Partition3<A, B, C> {
    First(A),
    Second(B),
    Third(C),
}

/// One of four typed buckets produced after a bounded Direct collection.
pub enum Partition4<A, B, C, D> {
    First(A),
    Second(B),
    Third(C),
    Fourth(D),
}

pub type Partitioned3<A, B, C> = (Vec<A>, Vec<B>, Vec<C>);
pub type Partitioned4<A, B, C, D> = (Vec<A>, Vec<B>, Vec<C>, Vec<D>);

/// Collects one classified output once, then separates its values locally.
///
/// The returned buckets can hold different types. This terminal does not
/// execute downstream graph branches on each bucket.
pub trait DirectPartitionByExt<T: Element> {
    fn collect_partitioned3_named<A, B, C>(
        &self,
        key: &str,
        config: &JobConfig,
        classify: impl Fn(T) -> Partition3<A, B, C>,
    ) -> Result<Partitioned3<A, B, C>, Error>;

    fn collect_partitioned4_named<A, B, C, D>(
        &self,
        key: &str,
        config: &JobConfig,
        classify: impl Fn(T) -> Partition4<A, B, C, D>,
    ) -> Result<Partitioned4<A, B, C, D>, Error>;
}

impl<T: Element> DirectPartitionByExt<T> for PCollection<T> {
    fn collect_partitioned3_named<A, B, C>(
        &self,
        key: &str,
        config: &JobConfig,
        classify: impl Fn(T) -> Partition3<A, B, C>,
    ) -> Result<Partitioned3<A, B, C>, Error> {
        let values = DirectRunner.run_collect(&self.output(key)?, config)?.0;
        let (mut first, mut second, mut third) = (Vec::new(), Vec::new(), Vec::new());
        for value in values {
            let bucket = catch_unwind(AssertUnwindSafe(|| classify(value)))
                .map_err(|_| Error::execution(key, "direct", "partition classifier panicked"))?;
            match bucket {
                Partition3::First(value) => first.push(value),
                Partition3::Second(value) => second.push(value),
                Partition3::Third(value) => third.push(value),
            }
        }
        Ok((first, second, third))
    }

    fn collect_partitioned4_named<A, B, C, D>(
        &self,
        key: &str,
        config: &JobConfig,
        classify: impl Fn(T) -> Partition4<A, B, C, D>,
    ) -> Result<Partitioned4<A, B, C, D>, Error> {
        let values = DirectRunner.run_collect(&self.output(key)?, config)?.0;
        let (mut first, mut second, mut third, mut fourth) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for value in values {
            let bucket = catch_unwind(AssertUnwindSafe(|| classify(value)))
                .map_err(|_| Error::execution(key, "direct", "partition classifier panicked"))?;
            match bucket {
                Partition4::First(value) => first.push(value),
                Partition4::Second(value) => second.push(value),
                Partition4::Third(value) => third.push(value),
                Partition4::Fourth(value) => fourth.push(value),
            }
        }
        Ok((first, second, third, fourth))
    }
}

impl<T: Element, E: Element> DirectPartitionExt<T, E> for PCollection<Result<T, E>> {
    fn collect_partitioned_named(
        &self,
        key: &str,
        config: &JobConfig,
    ) -> Result<(Vec<T>, Vec<E>), Error> {
        let output = self.output(key)?;
        let values = DirectRunner.run_collect(&output, config)?.0;
        let mut good = Vec::new();
        let mut errors = Vec::new();
        for value in values {
            match value {
                Ok(value) => good.push(value),
                Err(error) => errors.push(error),
            }
        }
        Ok((good, errors))
    }
}

pub trait DirectFlatPartitionExt<T: Element, E: Element> {
    fn collect_flat_partitioned_named(
        &self,
        key: &str,
        config: &JobConfig,
    ) -> Result<(Vec<T>, Vec<E>), Error>;
}
impl<T: Element, E: Element> DirectFlatPartitionExt<T, E> for PCollection<Result<Vec<T>, E>> {
    fn collect_flat_partitioned_named(
        &self,
        key: &str,
        config: &JobConfig,
    ) -> Result<(Vec<T>, Vec<E>), Error> {
        let output = self.output(key)?;
        let values = DirectRunner.run_collect(&output, config)?.0;
        let mut good = Vec::new();
        let mut errors = Vec::new();
        for value in values {
            match value {
                Ok(values) => good.extend(values),
                Err(error) => errors.push(error),
            }
        }
        Ok((good, errors))
    }
}
impl<T: Element, E: Element + Display> DirectFallibleExt<T, E> for PCollection<Result<T, E>> {
    fn collect_fail_fast_named(&self, key: &str, config: &JobConfig) -> Result<Vec<T>, Error> {
        let output = self.output(key)?;
        DirectRunner
            .run_collect(&output, config)?
            .0
            .into_iter()
            .map(|item| {
                item.map_err(|error| {
                    Error::execution(key, "direct", format!("element failed: {error}"))
                })
            })
            .collect()
    }
}
