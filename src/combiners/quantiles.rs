//! Approximate quantiles combiners using t-digest algorithm.
//!
//! Provides memory-efficient approximate quantile estimation suitable for large-scale
//! distributed streaming data processing.

use crate::RFBound;
use crate::collection::{CombineFn, LiftableCombiner};
use std::cmp::Ordering;
use std::marker::PhantomData;
/* ===================== TDigest ===================== */

/// A centroid in the t-digest: a weighted point representing a cluster of values.
#[derive(Clone, Debug)]
struct Centroid {
    mean: f64,
    weight: f64,
}

impl Centroid {
    #[must_use]
    const fn new(mean: f64, weight: f64) -> Self {
        Self { mean, weight }
    }
}

/// T-Digest data structure for approximate quantile estimation.
///
/// The t-digest is a probabilistic data structure that maintains an approximate
/// distribution of values with bounded memory usage. It provides more accurate
/// estimates at the extreme quantiles (0.0 and 1.0) than at the median.
///
/// Based on "Computing Extremely Accurate Quantiles Using t-Digests" by Ted Dunning.
#[derive(Clone, Debug)]
pub struct TDigest {
    /// Compression parameter: controls accuracy vs. memory tradeoff.
    /// Higher values = more accuracy but more memory.
    /// Typical range: 20-1000, default: 100.
    compression: f64,
    /// Centroids sorted by mean.
    centroids: Vec<Centroid>,
    /// Total weight (count) of all values added.
    total_weight: f64,
    /// Minimum value seen.
    min: f64,
    /// Maximum value seen.
    max: f64,
}

impl TDigest {
    /// Create a new t-digest with the specified compression parameter.
    ///
    /// # Arguments
    /// * `compression` - Controls accuracy vs. memory. Higher = more accurate but more memory.
    ///   Typical values: 20-1000. Default recommendation: 100.
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

    /// Add a single value with weight 1.
    pub fn add(&mut self, value: f64) {
        self.add_weighted(value, 1.0);
    }

    /// Add a value with a specified weight.
    #[allow(clippy::cast_precision_loss)]
    pub fn add_weighted(&mut self, value: f64, weight: f64) {
        if !value.is_finite() {
            return; // Skip NaN and infinity
        }

        self.min = self.min.min(value);
        self.max = self.max.max(value);

        self.centroids.push(Centroid::new(value, weight));
        self.total_weight += weight;

        // Compress when we have too many centroids
        if self.centroids.len() as f64 > self.compression * 2.0 {
            self.compress();
        }
    }

    /// Merge another t-digest into this one.
    pub fn merge(&mut self, other: &Self) {
        if other.total_weight == 0.0 {
            return;
        }

        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);

        // Add all centroids from other
        self.centroids.extend(other.centroids.iter().cloned());
        self.total_weight += other.total_weight;

        // Compress the merged result
        self.compress();
    }

    /// Compress the digest by merging nearby centroids.
    fn compress(&mut self) {
        if self.centroids.is_empty() {
            return;
        }

        // Sort centroids by mean
        self.centroids
            .sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(Ordering::Equal));

        let mut compressed = Vec::new();
        let mut cumulative_weight = 0.0;

        let mut current = self.centroids[0].clone();

        for centroid in self.centroids.iter().skip(1) {
            let proposed_weight = current.weight + centroid.weight;
            let q0 = cumulative_weight / self.total_weight;
            let q1 = (cumulative_weight + proposed_weight) / self.total_weight;

            // K-size limit based on compression and quantile
            let k_limit = self.k_size(q0).min(self.k_size(q1));

            if proposed_weight <= k_limit {
                // Merge centroid into current
                current.mean = current
                    .mean
                    .mul_add(current.weight, centroid.mean * centroid.weight)
                    / proposed_weight;
                current.weight = proposed_weight;
            } else {
                // Push current and start a new one
                cumulative_weight += current.weight;
                compressed.push(current);
                current = centroid.clone();
            }
        }

        // Remember the last centroid
        compressed.push(current);

        self.centroids = compressed;
    }

    /// Compute the k-size limit for a given quantile.
    /// This implements the scaling function that gives better accuracy at extremes.
    fn k_size(&self, q: f64) -> f64 {
        let q = q.clamp(0.0, 1.0);
        // Use the standard scaling function: k(q, δ) = δ * q * (1 - q) / 2
        (self.compression * q * (1.0 - q) / 2.0).max(1.0)
    }

    /// Estimate the quantile for a given rank (0.0 to 1.0).
    ///
    /// # Arguments
    /// * `q` - The desired quantile, where 0.0 = minimum, 0.5 = median, 1.0 = maximum.
    ///
    /// # Returns
    /// The estimated value at the given quantile, or `f64::NAN` if the digest is empty.
    ///
    /// # Examples
    /// ```
    /// use ironbeam::combiners::TDigest;
    /// let mut digest = TDigest::new(100.0);
    /// for i in 1..=100 {
    ///     digest.add(i as f64);
    /// }
    /// let median = digest.quantile(0.5);  // ~50.0
    /// let p95 = digest.quantile(0.95);    // ~95.0
    /// assert!((median - 50.0).abs() < 5.0);  // Approximate
    /// assert!((p95 - 95.0).abs() < 5.0);
    /// ```
    #[must_use]
    pub fn quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() {
            return f64::NAN;
        }

        let q = q.clamp(0.0, 1.0);

        // Handle edge cases
        if (q - 0.0).abs() <= f64::EPSILON || self.centroids.len() == 1 {
            return self.min;
        }
        if (q - 1.0).abs() <= f64::EPSILON {
            return self.max;
        }

        let target = q * self.total_weight;
        let mut cumulative = 0.0;

        for i in 0..self.centroids.len() {
            let c = &self.centroids[i];
            let next_cumulative = cumulative + c.weight;

            if next_cumulative >= target {
                // Interpolate within this centroid
                if (next_cumulative - cumulative).abs() < f64::EPSILON {
                    return c.mean;
                }

                let fraction = (target - cumulative) / c.weight;

                // Use linear interpolation between adjacent centroids
                let left = if i == 0 {
                    self.min
                } else {
                    self.centroids[i - 1].mean
                };

                let right = if i == self.centroids.len() - 1 {
                    self.max
                } else {
                    self.centroids[i + 1].mean
                };

                return left + fraction * (right - left);
            }

            cumulative = next_cumulative;
        }

        self.max
    }

    /// Get multiple quantiles at once (more efficient than calling `quantile()` multiple times).
    #[must_use]
    pub fn quantiles(&self, qs: &[f64]) -> Vec<f64> {
        qs.iter().map(|&q| self.quantile(q)).collect()
    }

    /// Estimate the cumulative distribution function (CDF) at a given value.
    ///
    /// Returns the estimated fraction of values less than or equal to the given value.
    #[must_use]
    pub fn cdf(&self, value: f64) -> f64 {
        if self.centroids.is_empty() || value < self.min {
            return 0.0;
        }
        if value >= self.max {
            return 1.0;
        }

        let mut cumulative = 0.0;
        let mut prev_mean = self.min;

        for c in &self.centroids {
            if value < c.mean {
                // Interpolate between prev_mean and c.mean
                let fraction = (value - prev_mean) / (c.mean - prev_mean).max(f64::EPSILON);
                return fraction.mul_add(c.weight, cumulative) / self.total_weight;
            }
            cumulative += c.weight;
            prev_mean = c.mean;
        }

        cumulative / self.total_weight
    }

    /// Get the total count of values added.
    #[must_use]
    pub const fn count(&self) -> f64 {
        self.total_weight
    }

    /// Check if the digest is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_weight == 0.0
    }
}

/* ===================== ApproxQuantiles ===================== */

/// Approximate quantiles combiner using t-digest.
///
/// Computes approximate quantiles (percentiles) efficiently with bounded memory.
/// The accuracy is typically within 1-2% for most quantiles, with better accuracy
/// at the extremes (0.0 and 1.0).
///
/// - Accumulator: `TDigest`
/// - Output: `Vec<f64>` containing the requested quantiles
///
/// # Type Parameters
/// * `V` - Value type that can be converted to `f64`
///
/// # Examples
/// ```
/// # use anyhow::Result;
/// use ironbeam::*;
/// use ironbeam::combiners::ApproxQuantiles;
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
///
/// // Compute median and quartiles per key
/// let quantiles = from_vec(&p, vec![
///     ("a", 1.0), ("a", 2.0), ("a", 3.0), ("a", 4.0), ("a", 5.0),
///     ("b", 10.0), ("b", 20.0), ("b", 30.0),
/// ])
///     .combine_values(ApproxQuantiles::new(vec![0.25, 0.5, 0.75], 100.0))
///     .collect_seq()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ApproxQuantiles<V> {
    /// The quantiles to compute (each in range 0.0-1.0)
    quantiles: Vec<f64>,
    /// Compression parameter for t-digest
    compression: f64,
    _phantom: PhantomData<V>,
}

impl<V> ApproxQuantiles<V> {
    /// Create a new approximate quantiles combiner.
    ///
    /// # Arguments
    /// * `quantiles` - The quantiles to compute (e.g., `vec![0.5]` for median,
    ///   `vec![0.25, 0.5, 0.75]` for quartiles)
    /// * `compression` - T-digest compression parameter (typical: 20-1000, recommended: 100)
    #[must_use]
    pub const fn new(quantiles: Vec<f64>, compression: f64) -> Self {
        Self {
            quantiles,
            compression,
            _phantom: PhantomData,
        }
    }

    /// Create a combiner for common quantiles: min, 25th, median, 75th, max.
    #[must_use]
    pub fn five_number_summary(compression: f64) -> Self {
        Self::new(vec![0.0, 0.25, 0.5, 0.75, 1.0], compression)
    }

    /// Create a combiner for percentiles (1st, 5th, 10th, ..., 90th, 95th, 99th).
    #[must_use]
    pub fn percentiles(compression: f64) -> Self {
        Self::new(
            vec![0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99],
            compression,
        )
    }

    /// Create a combiner for just the median (50th percentile).
    #[must_use]
    pub fn median(compression: f64) -> Self {
        Self::new(vec![0.5], compression)
    }
}

impl<V> CombineFn<V, TDigest, Vec<f64>> for ApproxQuantiles<V>
where
    V: RFBound + Into<f64>,
{
    fn create(&self) -> TDigest {
        TDigest::new(self.compression)
    }

    fn add_input(&self, acc: &mut TDigest, v: V) {
        acc.add(v.into());
    }

    fn merge(&self, acc: &mut TDigest, other: TDigest) {
        acc.merge(&other);
    }

    fn finish(&self, mut acc: TDigest) -> Vec<f64> {
        if acc.is_empty() {
            return vec![f64::NAN; self.quantiles.len()];
        }
        // Compress one final time for the best accuracy
        acc.compress();
        acc.quantiles(&self.quantiles)
    }
}

impl<V> LiftableCombiner<V, TDigest, Vec<f64>> for ApproxQuantiles<V>
where
    V: RFBound + Into<f64>,
{
    fn build_from_group(&self, values: &[V]) -> TDigest {
        let mut digest = TDigest::new(self.compression);
        for v in values {
            digest.add(v.clone().into());
        }
        digest.compress();
        digest
    }
}

/* ===================== ApproxMedian ===================== */

/// Approximate median combiner using t-digest.
///
/// A convenience wrapper around `ApproxQuantiles` for computing just the median (50th percentile).
///
/// - Accumulator: `TDigest`
/// - Output: `f64` (the median value)
#[derive(Clone, Debug)]
pub struct ApproxMedian<V> {
    compression: f64,
    _phantom: PhantomData<V>,
}

impl<V> ApproxMedian<V> {
    /// Create a new approximate median combiner.
    ///
    /// # Arguments
    /// * `compression` - T-digest compression parameter (typical: 20-1000, recommended: 100)
    #[must_use]
    pub const fn new(compression: f64) -> Self {
        Self {
            compression,
            _phantom: PhantomData,
        }
    }
}

impl<V> Default for ApproxMedian<V> {
    fn default() -> Self {
        Self::new(100.0)
    }
}

impl<V> CombineFn<V, TDigest, f64> for ApproxMedian<V>
where
    V: RFBound + Into<f64>,
{
    fn create(&self) -> TDigest {
        TDigest::new(self.compression)
    }

    fn add_input(&self, acc: &mut TDigest, v: V) {
        acc.add(v.into());
    }

    fn merge(&self, acc: &mut TDigest, other: TDigest) {
        acc.merge(&other);
    }

    fn finish(&self, mut acc: TDigest) -> f64 {
        if acc.is_empty() {
            return f64::NAN;
        }
        acc.compress();
        acc.quantile(0.5)
    }
}

impl<V> LiftableCombiner<V, TDigest, f64> for ApproxMedian<V>
where
    V: RFBound + Into<f64>,
{
    fn build_from_group(&self, values: &[V]) -> TDigest {
        let mut digest = TDigest::new(self.compression);
        for v in values {
            digest.add(v.clone().into());
        }
        digest.compress();
        digest
    }
}
