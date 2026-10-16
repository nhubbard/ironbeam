//! Seeded priority reservoir sampling with v4's `SplitMix64` priority sequence.
use ironbeam_core::{Element, Error, OrderedFoldFn, PCollection};
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::hash::Hash;

const DEFAULT_SAMPLE_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

struct Ranked<T> {
    priority_bits: u64,
    index: usize,
    value: T,
}
impl<T> PartialEq for Ranked<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority_bits == other.priority_bits && self.index == other.index
    }
}
impl<T> Eq for Ranked<T> {}
impl<T> PartialOrd for Ranked<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Ranked<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority_bits
            .cmp(&other.priority_bits)
            .then_with(|| other.index.cmp(&self.index))
    }
}

struct Reservoir<T> {
    state: u64,
    index: usize,
    heap: BinaryHeap<Reverse<Ranked<T>>>,
}
struct PriorityReservoir {
    k: usize,
    seed: u64,
}
impl<T: Element> OrderedFoldFn<T, Reservoir<T>, Vec<T>> for PriorityReservoir {
    fn create(&self) -> Reservoir<T> {
        Reservoir {
            state: self.seed.wrapping_mul(0xA24B_AED4_0B9C_497C),
            index: 0,
            heap: BinaryHeap::new(),
        }
    }
    fn add_input(&self, reservoir: &mut Reservoir<T>, value: T) {
        if self.k == 0 {
            return;
        }
        reservoir.state = reservoir.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = reservoir.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        let random = z ^ (z >> 31);
        #[allow(clippy::cast_precision_loss)]
        let mut priority = ((random >> 11) as f64) * (1.0 / ((1_u64 << 53) as f64));
        if priority == 0.0 {
            priority = f64::from_bits(1);
        }
        reservoir.heap.push(Reverse(Ranked {
            priority_bits: priority.to_bits(),
            index: reservoir.index,
            value,
        }));
        reservoir.index += 1;
        if reservoir.heap.len() > self.k {
            reservoir.heap.pop();
        }
    }
    fn finish(&self, reservoir: Reservoir<T>) -> Vec<T> {
        let mut ranked = reservoir
            .heap
            .into_iter()
            .map(|Reverse(value)| value)
            .collect::<Vec<_>>();
        ranked.sort_unstable_by(|left, right| right.cmp(left));
        ranked.into_iter().map(|item| item.value).collect()
    }
}

pub trait SamplingExt<T: Element> {
    /// Collect up to `k` seeded reservoir samples in one vector.
    fn sample_reservoir_vec_named(
        &self,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<Vec<T>>, Error>;
    /// Emit the seeded reservoir samples as individual values.
    fn sample_reservoir_named(
        &self,
        sample_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<T>, Error>;
    /// Sample up to `k` values using the default seed.
    fn sample_globally_named(&self, key: &str, k: usize) -> Result<PCollection<Vec<T>>, Error>;
    /// Sample up to `k` values using a caller-provided seed.
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
        self.fold_globally_ordered_named(key, PriorityReservoir { k, seed })
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
    /// Collect up to `k` seeded reservoir samples per key.
    fn sample_values_reservoir_vec_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<(K, Vec<V>)>, Error>;
    /// Emit seeded per-key samples as individual key-value pairs.
    fn sample_values_reservoir_named(
        &self,
        group_key: &str,
        sample_key: &str,
        key: &str,
        k: usize,
        seed: u64,
    ) -> Result<PCollection<(K, V)>, Error>;
    /// Sample up to `k` values per key using the default seed.
    fn sample_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        k: usize,
    ) -> Result<PCollection<(K, Vec<V>)>, Error>;
    /// Sample up to `k` values per key using a caller-provided seed.
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
        self.fold_values_ordered_named(group_key, PriorityReservoir { k, seed })?
            .map_named(key, |row| row)
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

#[cfg(test)]
#[path = "../tests/unit/sampling__rank_tests.rs"]
mod rank_tests;
