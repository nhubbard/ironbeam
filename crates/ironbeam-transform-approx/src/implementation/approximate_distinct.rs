use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use ironbeam_core::{CombineFn, Element, Error, PCollection};
use std::collections::{BTreeSet, hash_map::DefaultHasher};
use std::hash::{BuildHasherDefault, Hash, Hasher};

struct Kmv {
    k: usize,
}
impl<T: Element + Hash> CombineFn<T, BTreeSet<u64>, f64> for Kmv {
    fn create(&self) -> BTreeSet<u64> {
        BTreeSet::new()
    }
    fn add_input(&self, ranks: &mut BTreeSet<u64>, value: T) {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        #[allow(clippy::cast_precision_loss)]
        let rank = ((hasher.finish() as f64) / ((u64::MAX as f64) + 1.0)).to_bits();
        ranks.insert(rank);
        if ranks.len() > self.k {
            ranks.pop_last();
        }
    }
    fn merge(&self, ranks: &mut BTreeSet<u64>, other: BTreeSet<u64>) {
        for rank in other {
            ranks.insert(rank);
            if ranks.len() > self.k {
                ranks.pop_last();
            }
        }
    }
    fn finish(&self, ranks: BTreeSet<u64>) -> f64 {
        if ranks.len() < self.k {
            #[allow(clippy::cast_precision_loss)]
            return ranks.len() as f64;
        }
        #[allow(clippy::cast_precision_loss)]
        let estimate = ((self.k as f64) - 1.0) / f64::from_bits(*ranks.last().unwrap());
        estimate
    }
}

fn precision_for_error(key: &str, error: f64) -> Result<u8, Error> {
    if !error.is_finite() || error <= 0.0 || error >= 1.0 {
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

struct Hll {
    precision: u8,
}
type HllSketch<T> = HyperLogLogPlus<T, BuildHasherDefault<DefaultHasher>>;
impl<T: Element + Hash> CombineFn<T, HllSketch<T>, u64> for Hll {
    fn create(&self) -> HllSketch<T> {
        HllSketch::new(self.precision, BuildHasherDefault::default())
            .expect("HLL precision validated during graph construction")
    }
    fn add_input(&self, sketch: &mut HllSketch<T>, value: T) {
        sketch.insert(&value);
    }
    fn merge(&self, sketch: &mut HllSketch<T>, other: HllSketch<T>) {
        sketch
            .merge(&other)
            .expect("HLL precision fixed for one graph node");
    }
    fn finish(&self, mut sketch: HllSketch<T>) -> u64 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let count = sketch.count().round() as u64;
        count
    }
}

/// Approximate global distinct counts using mergeable sketches.
pub trait ApproxDistinctExt<T: Element + Hash> {
    /// Estimate global distinct count with a KMV sketch of at least four samples.
    fn approx_distinct_count_named(&self, key: &str, k: usize) -> Result<PCollection<f64>, Error>;
    /// Estimate global distinct count with the default `HyperLogLog` precision.
    fn approx_count_distinct_named(&self, key: &str) -> Result<PCollection<u64>, Error>;
    /// Estimate global distinct count with a target relative error.
    fn approx_count_distinct_with_error_named(
        &self,
        key: &str,
        error: f64,
    ) -> Result<PCollection<u64>, Error>;
}
impl<T: Element + Hash> ApproxDistinctExt<T> for PCollection<T> {
    fn approx_distinct_count_named(&self, key: &str, k: usize) -> Result<PCollection<f64>, Error> {
        self.combine_globally_lifted_named(key, Kmv { k: k.max(4) })
    }
    fn approx_count_distinct_named(&self, key: &str) -> Result<PCollection<u64>, Error> {
        self.combine_globally_lifted_named(key, Hll { precision: 12 })
    }
    fn approx_count_distinct_with_error_named(
        &self,
        key: &str,
        error: f64,
    ) -> Result<PCollection<u64>, Error> {
        let precision = precision_for_error(key, error)?;
        self.combine_globally_lifted_named(key, Hll { precision })
    }
}

/// Approximate distinct value counts within each key.
pub trait KeyedApproxDistinctExt<K: Element + Eq + Hash + Clone, V: Element + Hash> {
    /// Estimate distinct values per key with a KMV sketch of at least four samples.
    fn approx_distinct_count_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, f64)>, Error>;
    /// Estimate distinct values per key with default `HyperLogLog` precision.
    fn approx_count_distinct_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, u64)>, Error>;
    /// Estimate distinct values per key with a target relative error.
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
        self.combine_values_lifted_named(group_key, Kmv { k: k.max(4) })?
            .map_named(key, |row| row)
    }
    fn approx_count_distinct_per_key_named(
        &self,
        group_key: &str,
        key: &str,
    ) -> Result<PCollection<(K, u64)>, Error> {
        self.combine_values_lifted_named(group_key, Hll { precision: 12 })?
            .map_named(key, |row| row)
    }
    fn approx_count_distinct_per_key_with_error_named(
        &self,
        group_key: &str,
        key: &str,
        error: f64,
    ) -> Result<PCollection<(K, u64)>, Error> {
        let precision = precision_for_error(key, error)?;
        self.combine_values_lifted_named(group_key, Hll { precision })?
            .map_named(key, |row| row)
    }
}

#[cfg(test)]
#[path = "../../tests/unit/lib__tests.rs"]
mod tests;
