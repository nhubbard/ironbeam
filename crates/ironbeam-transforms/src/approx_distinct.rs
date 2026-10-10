//! V4-compatible KMV approximate distinct counting.
use ironbeam_core::{Element, Error, PCollection};
use std::collections::{BTreeSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

fn kmv<T: Hash>(values: Vec<T>, requested_k: usize) -> f64 {
    let k = requested_k.max(4);
    let mut ranks = BTreeSet::new();
    for value in values {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        #[allow(clippy::cast_precision_loss)]
        let rank = ((hasher.finish() as f64) / ((u64::MAX as f64) + 1.0)).to_bits();
        ranks.insert(rank);
        if ranks.len() > k {
            ranks.pop_last();
        }
    }
    if ranks.len() < k {
        #[allow(clippy::cast_precision_loss)]
        return ranks.len() as f64;
    }
    #[allow(clippy::cast_precision_loss)]
    let estimate = ((k as f64) - 1.0) / f64::from_bits(*ranks.last().unwrap());
    estimate
}

pub trait ApproxDistinctExt<T: Element + Hash> {
    fn approx_distinct_count_named(&self, key: &str, k: usize) -> Result<PCollection<f64>, Error>;
}
impl<T: Element + Hash> ApproxDistinctExt<T> for PCollection<T> {
    fn approx_distinct_count_named(&self, key: &str, k: usize) -> Result<PCollection<f64>, Error> {
        self.combine_globally_named(key, move |values| kmv(values, k))
    }
}

pub trait KeyedApproxDistinctExt<K: Element + Eq + Hash + Clone, V: Element + Hash> {
    fn approx_distinct_count_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, f64)>, Error>;
}
impl<K: Element + Eq + Hash + Clone, V: Element + Hash> KeyedApproxDistinctExt<K, V>
    for PCollection<(K, V)>
{
    fn approx_distinct_count_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, f64)>, Error> {
        self.group_by_key_named(group_key)?
            .map_named(key, move |(group, values)| (group, kmv(values, k)))
    }
}
