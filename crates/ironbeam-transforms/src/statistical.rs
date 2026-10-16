//! Approximate quantiles using a t-digest with monotone interpolation.
use ironbeam_core::{CombineFn, Element, Error, PCollection};
use std::hash::Hash;

#[derive(Clone, Debug)]
struct Centroid {
    mean: f64,
    weight: f64,
}

/// V4-compatible centroid construction. Non-finite observations are ignored.
#[derive(Clone, Debug)]
pub struct TDigest {
    compression: f64,
    centroids: Vec<Centroid>,
    total_weight: f64,
    min: f64,
    max: f64,
}

impl TDigest {
    /// Create an empty digest with the requested centroid compression.
    ///
    /// Larger values retain more centroids. The value is not validated here.
    #[must_use]
    pub const fn new(compression: f64) -> Self {
        Self {
            compression,
            centroids: Vec::new(),
            total_weight: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Return the total weight of retained observations.
    #[must_use]
    pub const fn count(&self) -> f64 {
        self.total_weight
    }

    /// Return whether no observation has contributed weight.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_weight == 0.0
    }

    /// Add one observation with unit weight.
    pub fn add(&mut self, value: f64) {
        self.add_weighted(value, 1.0);
    }

    /// Add an observation with caller-supplied weight.
    ///
    /// Non-finite values are ignored. Weight is not validated by this method.
    pub fn add_weighted(&mut self, value: f64, weight: f64) {
        if !value.is_finite() {
            return;
        }
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.centroids.push(Centroid {
            mean: value,
            weight,
        });
        self.total_weight += weight;
        #[allow(clippy::cast_precision_loss)]
        if (self.centroids.len() as f64) > self.compression * 2.0 {
            self.compress();
        }
    }

    /// Merge another digest's centroids into this digest.
    pub fn merge(&mut self, other: &Self) {
        if other.is_empty() {
            return;
        }
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.centroids.extend(other.centroids.iter().cloned());
        self.total_weight += other.total_weight;
        self.compress();
    }

    /// Merge nearby centroids to bound the digest's size.
    pub fn compress(&mut self) {
        if self.centroids.is_empty() {
            return;
        }
        self.centroids.sort_by(|a, b| a.mean.total_cmp(&b.mean));
        let mut compressed = Vec::new();
        let mut cumulative = 0.0;
        let mut current = self.centroids[0].clone();
        for centroid in self.centroids.iter().skip(1) {
            let proposed = current.weight + centroid.weight;
            let q0 = cumulative / self.total_weight;
            let q1 = (cumulative + proposed) / self.total_weight;
            if proposed <= self.k_size(q0).min(self.k_size(q1)) {
                current.mean = current
                    .mean
                    .mul_add(current.weight, centroid.mean * centroid.weight)
                    / proposed;
                current.weight = proposed;
            } else {
                cumulative += current.weight;
                compressed.push(current);
                current = centroid.clone();
            }
        }
        compressed.push(current);
        self.centroids = compressed;
    }

    fn k_size(&self, q: f64) -> f64 {
        let q = q.clamp(0.0, 1.0);
        (self.compression * q * (1.0 - q) / 2.0).max(1.0)
    }

    /// Estimate a quantile in `[0, 1]`; return `NaN` for an empty digest.
    ///
    /// The requested quantile is clamped to this interval.
    #[must_use]
    pub fn quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() {
            return f64::NAN;
        }
        let q = q.clamp(0.0, 1.0);
        if q.abs() <= f64::EPSILON {
            return self.min;
        }
        if (q - 1.0).abs() <= f64::EPSILON {
            return self.max;
        }
        if self.centroids.len() == 1 {
            return q.mul_add(self.max - self.min, self.min);
        }
        // Centroid midpoints are ordered in both rank and value. Interpolating
        // between adjacent midpoints cannot reverse the percentile sequence.
        let ordered = if self
            .centroids
            .windows(2)
            .all(|pair| pair[0].mean.total_cmp(&pair[1].mean).is_le())
        {
            None
        } else {
            let mut centroids = self.centroids.clone();
            centroids.sort_by(|left, right| left.mean.total_cmp(&right.mean));
            Some(centroids)
        };
        let centroids = ordered.as_deref().unwrap_or(&self.centroids);
        let target = q * self.total_weight;
        let mut cumulative = 0.0;
        let mut previous_rank = 0.0;
        let mut previous_value = self.min;
        for centroid in centroids {
            let rank = cumulative + centroid.weight / 2.0;
            if target <= rank {
                let span = rank - previous_rank;
                if span <= f64::EPSILON {
                    return centroid.mean;
                }
                let fraction = ((target - previous_rank) / span).clamp(0.0, 1.0);
                return fraction.mul_add(centroid.mean - previous_value, previous_value);
            }
            previous_rank = rank;
            previous_value = centroid.mean;
            cumulative += centroid.weight;
        }
        let span = self.total_weight - previous_rank;
        if span <= f64::EPSILON {
            return self.max;
        }
        let fraction = ((target - previous_rank) / span).clamp(0.0, 1.0);
        fraction.mul_add(self.max - previous_value, previous_value)
    }

    /// Estimate several quantiles in the order requested.
    #[must_use]
    pub fn quantiles(&self, qs: &[f64]) -> Vec<f64> {
        qs.iter().map(|&q| self.quantile(q)).collect()
    }

    /// Estimate the cumulative fraction at or below `value`.
    #[must_use]
    pub fn cdf(&self, value: f64) -> f64 {
        if self.centroids.is_empty() || value < self.min {
            return 0.0;
        }
        if value >= self.max {
            return 1.0;
        }
        let mut cumulative = 0.0;
        let mut previous = self.min;
        for centroid in &self.centroids {
            if value < centroid.mean {
                let fraction = (value - previous) / (centroid.mean - previous).max(f64::EPSILON);
                return fraction.mul_add(centroid.weight, cumulative) / self.total_weight;
            }
            cumulative += centroid.weight;
            previous = centroid.mean;
        }
        cumulative / self.total_weight
    }
}

fn validate(key: &str, quantiles: &[f64], compression: f64) -> Result<(), Error> {
    if !compression.is_finite() || compression <= 0.0 {
        return Err(Error::graph(
            key,
            "t-digest compression must be finite and positive",
        ));
    }
    if quantiles
        .iter()
        .any(|q| !q.is_finite() || !(0.0..=1.0).contains(q))
    {
        return Err(Error::graph(
            key,
            "quantiles must be finite values in [0, 1]",
        ));
    }
    Ok(())
}

struct Quantiles {
    quantiles: Vec<f64>,
    compression: f64,
}
impl<T: Element + Into<f64>> CombineFn<T, TDigest, Vec<f64>> for Quantiles {
    fn create(&self) -> TDigest {
        TDigest::new(self.compression)
    }
    fn add_input(&self, digest: &mut TDigest, value: T) {
        digest.add(value.into());
    }
    fn merge(&self, digest: &mut TDigest, other: TDigest) {
        digest.merge(&other);
    }
    fn finish(&self, mut digest: TDigest) -> Vec<f64> {
        digest.compress();
        digest.quantiles(&self.quantiles)
    }
}
struct Median {
    compression: f64,
}
impl<T: Element + Into<f64>> CombineFn<T, TDigest, f64> for Median {
    fn create(&self) -> TDigest {
        TDigest::new(self.compression)
    }
    fn add_input(&self, digest: &mut TDigest, value: T) {
        digest.add(value.into());
    }
    fn merge(&self, digest: &mut TDigest, other: TDigest) {
        digest.merge(&other);
    }
    fn finish(&self, mut digest: TDigest) -> f64 {
        digest.compress();
        digest.quantile(0.5)
    }
}

pub trait StatisticalExt<T: Element + Into<f64>> {
    /// Estimate selected global quantiles with a t-digest.
    fn approx_quantiles_named(
        &self,
        key: &str,
        quantiles: Vec<f64>,
        compression: f64,
    ) -> Result<PCollection<Vec<f64>>, Error>;
    /// Estimate the global median with a t-digest.
    fn approx_median_named(&self, key: &str, compression: f64) -> Result<PCollection<f64>, Error>;
}

impl<T: Element + Into<f64>> StatisticalExt<T> for PCollection<T> {
    fn approx_quantiles_named(
        &self,
        key: &str,
        quantiles: Vec<f64>,
        compression: f64,
    ) -> Result<PCollection<Vec<f64>>, Error> {
        validate(key, &quantiles, compression)?;
        self.combine_globally_lifted_named(
            key,
            Quantiles {
                quantiles,
                compression,
            },
        )
    }

    fn approx_median_named(&self, key: &str, compression: f64) -> Result<PCollection<f64>, Error> {
        validate(key, &[0.5], compression)?;
        self.combine_globally_lifted_named(key, Median { compression })
    }
}

pub trait KeyedStatisticalExt<K: Element + Eq + Hash + Clone, V: Element + Into<f64>> {
    /// Estimate selected value quantiles per key with a t-digest.
    fn approx_quantiles_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        quantiles: Vec<f64>,
        compression: f64,
    ) -> Result<PCollection<(K, Vec<f64>)>, Error>;
    /// Estimate the value median per key with a t-digest.
    fn approx_median_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        compression: f64,
    ) -> Result<PCollection<(K, f64)>, Error>;
}

impl<K: Element + Eq + Hash + Clone, V: Element + Into<f64>> KeyedStatisticalExt<K, V>
    for PCollection<(K, V)>
{
    fn approx_quantiles_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        quantiles: Vec<f64>,
        compression: f64,
    ) -> Result<PCollection<(K, Vec<f64>)>, Error> {
        validate(key, &quantiles, compression)?;
        self.combine_values_lifted_named(
            group_key,
            Quantiles {
                quantiles,
                compression,
            },
        )?
        .map_named(key, |row| row)
    }

    fn approx_median_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        compression: f64,
    ) -> Result<PCollection<(K, f64)>, Error> {
        validate(key, &[0.5], compression)?;
        self.combine_values_lifted_named(group_key, Median { compression })?
            .map_named(key, |row| row)
    }
}

#[cfg(test)]
#[path = "../tests/unit/statistical__merge_tests.rs"]
mod merge_tests;
