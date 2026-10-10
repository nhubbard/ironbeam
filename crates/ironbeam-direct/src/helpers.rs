//! Direct-runner terminal conveniences for bounded collections.
use crate::{DirectRunner, JobConfig};
use ironbeam_core::{Element, Error, PCollection};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;

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

pub trait DirectFallibleExt<T: Element, E: Element + Display> {
    fn collect_fail_fast_named(&self, key: &str, config: &JobConfig) -> Result<Vec<T>, Error>;
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
