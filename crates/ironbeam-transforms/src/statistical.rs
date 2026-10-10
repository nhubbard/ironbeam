//! Approximate quantiles using the v4 t-digest calculation.
use ironbeam_core::{Element, Error, PCollection};
use std::hash::Hash;

#[derive(Clone, Debug)]
struct Centroid {
    mean: f64,
    weight: f64,
}

/// V4-compatible t-digest. Non-finite observations are ignored.
#[derive(Clone, Debug)]
pub struct TDigest {
    compression: f64,
    centroids: Vec<Centroid>,
    total_weight: f64,
    min: f64,
    max: f64,
}

impl TDigest {
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

    #[must_use]
    pub const fn count(&self) -> f64 {
        self.total_weight
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_weight == 0.0
    }

    pub fn add(&mut self, value: f64) {
        self.add_weighted(value, 1.0);
    }

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

    #[must_use]
    pub fn quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() {
            return f64::NAN;
        }
        let q = q.clamp(0.0, 1.0);
        if q.abs() <= f64::EPSILON || self.centroids.len() == 1 {
            return self.min;
        }
        if (q - 1.0).abs() <= f64::EPSILON {
            return self.max;
        }
        let target = q * self.total_weight;
        let mut cumulative = 0.0;
        for (index, centroid) in self.centroids.iter().enumerate() {
            let next = cumulative + centroid.weight;
            if next >= target {
                if (next - cumulative).abs() < f64::EPSILON {
                    return centroid.mean;
                }
                let fraction = (target - cumulative) / centroid.weight;
                let left = if index == 0 {
                    self.min
                } else {
                    self.centroids[index - 1].mean
                };
                let right = if index + 1 == self.centroids.len() {
                    self.max
                } else {
                    self.centroids[index + 1].mean
                };
                return fraction.mul_add(right - left, left);
            }
            cumulative = next;
        }
        self.max
    }

    #[must_use]
    pub fn quantiles(&self, qs: &[f64]) -> Vec<f64> {
        qs.iter().map(|&q| self.quantile(q)).collect()
    }

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

fn digest<T: Into<f64>>(values: Vec<T>, compression: f64) -> TDigest {
    let mut result = TDigest::new(compression);
    for value in values {
        result.add(value.into());
    }
    result.compress();
    result
}

pub trait StatisticalExt<T: Element + Into<f64>> {
    fn approx_quantiles_named(
        &self,
        key: &str,
        quantiles: Vec<f64>,
        compression: f64,
    ) -> Result<PCollection<Vec<f64>>, Error>;
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
        self.combine_globally_named(key, move |values| {
            digest(values, compression).quantiles(&quantiles)
        })
    }

    fn approx_median_named(&self, key: &str, compression: f64) -> Result<PCollection<f64>, Error> {
        validate(key, &[0.5], compression)?;
        self.combine_globally_named(key, move |values| digest(values, compression).quantile(0.5))
    }
}

pub trait KeyedStatisticalExt<K: Element + Eq + Hash + Clone, V: Element + Into<f64>> {
    fn approx_quantiles_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        quantiles: Vec<f64>,
        compression: f64,
    ) -> Result<PCollection<(K, Vec<f64>)>, Error>;
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
        self.group_by_key_named(group_key)?
            .map_named(key, move |(group, values)| {
                (group, digest(values, compression).quantiles(&quantiles))
            })
    }

    fn approx_median_per_key_named(
        &self,
        group_key: &str,
        key: &str,
        compression: f64,
    ) -> Result<PCollection<(K, f64)>, Error> {
        validate(key, &[0.5], compression)?;
        self.group_by_key_named(group_key)?
            .map_named(key, move |(group, values)| {
                (group, digest(values, compression).quantile(0.5))
            })
    }
}
