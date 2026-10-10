//! V4-compatible KMV approximate distinct counting.
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use ironbeam_core::{Element, Error, PCollection};
use std::collections::{BTreeSet, hash_map::DefaultHasher};
use std::hash::{BuildHasherDefault, Hash, Hasher};

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

fn precision_for_error(key: &str, error: f64) -> Result<u8, Error> {
    if !error.is_finite() || !(0.0..1.0).contains(&error) {
        return Err(Error::graph(
            key,
            "relative HLL error must be finite and in (0, 1)",
        ));
    }
    let raw = (2.0 * (1.04_f64 / error).log2()).ceil();
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let precision = (raw as i32).clamp(4, 18) as u8;
    Ok(precision)
}

fn hll<T: Hash>(values: Vec<T>, precision: u8) -> u64 {
    let mut sketch = HyperLogLogPlus::<T, BuildHasherDefault<DefaultHasher>>::new(
        precision,
        BuildHasherDefault::default(),
    )
    .expect("HLL precision validated during graph construction");
    for value in values {
        sketch.insert(&value);
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let count = sketch.count().round() as u64;
    count
}

pub trait ApproxDistinctExt<T: Element + Hash> {
    fn approx_distinct_count_named(&self, key: &str, k: usize) -> Result<PCollection<f64>, Error>;
    fn approx_count_distinct_named(&self, key: &str) -> Result<PCollection<u64>, Error>;
    fn approx_count_distinct_with_error_named(
        &self,
        key: &str,
        error: f64,
    ) -> Result<PCollection<u64>, Error>;
}
impl<T: Element + Hash> ApproxDistinctExt<T> for PCollection<T> {
    fn approx_distinct_count_named(&self, key: &str, k: usize) -> Result<PCollection<f64>, Error> {
        self.combine_globally_named(key, move |values| kmv(values, k))
    }
    fn approx_count_distinct_named(&self, key: &str) -> Result<PCollection<u64>, Error> {
        self.combine_globally_named(key, |values| hll(values, 12))
    }
    fn approx_count_distinct_with_error_named(
        &self,
        key: &str,
        error: f64,
    ) -> Result<PCollection<u64>, Error> {
        let precision = precision_for_error(key, error)?;
        self.combine_globally_named(key, move |values| hll(values, precision))
    }
}

pub trait KeyedApproxDistinctExt<K: Element + Eq + Hash + Clone, V: Element + Hash> {
    fn approx_distinct_count_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, f64)>, Error>;
    fn approx_count_distinct_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>;
    fn approx_count_distinct_per_key_with_error_named(
        &self,
        group_key: &str,
        key: &str,
        error: f64,
    ) -> Result<PCollection<(K, u64)>, Error>;
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
    fn approx_count_distinct_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, u64)>, Error> {
        self.group_by_key_named(group_key)?
            .map_named(key, |(group, values)| (group, hll(values, 12)))
    }
    fn approx_count_distinct_per_key_with_error_named(
        &self,
        group_key: &str,
        key: &str,
        error: f64,
    ) -> Result<PCollection<(K, u64)>, Error> {
        let precision = precision_for_error(key, error)?;
        self.group_by_key_named(group_key)?
            .map_named(key, move |(group, values)| (group, hll(values, precision)))
    }
}
