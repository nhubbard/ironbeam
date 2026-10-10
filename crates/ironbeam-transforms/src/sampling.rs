//! Seeded priority reservoir sampling with v4's SplitMix64 priority sequence.
use ironbeam_core::{Element, Error, PCollection};
use std::hash::Hash;

const DEFAULT_SAMPLE_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

fn sample<T>(values: Vec<T>, k: usize, seed: u64) -> Vec<T> {
    if k == 0 {
        return Vec::new();
    }
    let mut state = seed.wrapping_mul(0xA24B_AED4_0B9C_497C);
    let mut ranked: Vec<(f64, usize, T)> = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            let random = z ^ (z >> 31);
            #[allow(clippy::cast_precision_loss)]
            let priority = ((random >> 11) as f64) * (1.0 / ((1_u64 << 53) as f64));
            (priority, index, value)
        })
        .collect();
    ranked.sort_unstable_by(|left, right| {
        right
            .0
            .total_cmp(&left.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    ranked.truncate(k);
    ranked.into_iter().map(|(_, _, value)| value).collect()
}

pub trait SamplingExt<T: Element> {
    fn sample_reservoir_vec_named(
        &self,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<Vec<T>>, Error>;
    fn sample_reservoir_named(
        &self,
        sample_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<T>, Error>;
    fn sample_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error>;
    fn sample_globally_with_seed_named(
        &self,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<Vec<T>>, Error>;
}
impl<T: Element> SamplingExt<T> for PCollection<T> {
    fn sample_reservoir_vec_named(
        &self,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<Vec<T>>, Error> {
        self.combine_globally_named(key, move |values| sample(values, k, seed))
    }
    fn sample_reservoir_named(
        &self,
        sample_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<Self, Error> {
        self.sample_reservoir_vec_named(sample_key, k, seed)?
            .flat_map_named(key, |values| values)
    }
    fn sample_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error> {
        self.sample_reservoir_vec_named(key, k, DEFAULT_SAMPLE_SEED)
    }
    fn sample_globally_with_seed_named(
        &self,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<Vec<T>>, Error> {
        self.sample_reservoir_vec_named(key, k, seed)
    }
}

pub trait KeyedSamplingExt<K: Element + Eq + Hash + Clone, V: Element> {
    fn sample_values_reservoir_vec_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<(K, Vec<V>)>, Error>;
    fn sample_values_reservoir_named(
        &self,
        group_key: &str,
        sample_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<(K, V)>, Error>;
    fn sample_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error>;
    fn sample_per_key_with_seed_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<(K, Vec<V>)>, Error>;
}
impl<K: Element + Eq + Hash + Clone, V: Element> KeyedSamplingExt<K, V> for PCollection<(K, V)> {
    fn sample_values_reservoir_vec_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        self.group_by_key_named(group_key)?
            .map_named(key, move |(group, values)| (group, sample(values, k, seed)))
    }
    fn sample_values_reservoir_named(
        &self,
        group_key: &str,
        sample_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<Self, Error> {
        self.sample_values_reservoir_vec_named(group_key, sample_key, k, seed)?
            .flat_map_named(key, |(group, values)| {
                values
                    .into_iter()
                    .map(|value| (group.clone(), value))
                    .collect()
            })
    }
    fn sample_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        self.sample_values_reservoir_vec_named(group_key, key, k, DEFAULT_SAMPLE_SEED)
    }
    fn sample_per_key_with_seed_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<(K, Vec<V>)>, Error> {
        self.sample_values_reservoir_vec_named(group_key, key, k, seed)
    }
}
