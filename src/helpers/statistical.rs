//! Statistical aggregate convenience methods for [`PCollection`].
//!
//! Provides thin wrappers around [`combine_globally`](PCollection::combine_globally) and
//! [`combine_values`](PCollection::combine_values) for approximate-quantile operations,
//! so callers never need to import or construct combiner structs directly.
//!
//! All methods are backed by the t-digest algorithm via [`ApproxMedian`] and
//! [`ApproxQuantiles`]. The `compression` parameter controls the accuracy/memory
//! trade-off (typical range: 20–1000; recommended default: `100.0`).
//!
//! ## Unkeyed (global) operations — `PCollection<T>`
//! - [`PCollection::approx_median_globally`] — approximate median → `PCollection<f64>`
//! - [`PCollection::approx_quantiles_globally`] — approximate quantile set → `PCollection<Vec<f64>>`
//!
//! ## Per-key operations — `PCollection<(K, V)>`
//! - [`PCollection::approx_median_per_key`] — approximate median per key → `PCollection<(K, f64)>`
//! - [`PCollection::approx_quantiles_per_key`] — approximate quantile set per key → `PCollection<(K, Vec<f64>)>`

use crate::combiners::{ApproxMedian, ApproxQuantiles};
use crate::{PCollection, RFBound};
use std::hash::Hash;

/* ─────────────────────────────── Unkeyed (global) ─────────────────────────────── */

impl<T> PCollection<T>
where
    T: RFBound + Into<f64>,
{
    /// Compute the approximate median (50th percentile) of all elements globally.
    ///
    /// Returns a single `f64`. Returns `f64::NAN` if the collection is empty.
    ///
    /// # Parameters
    /// - `compression` — t-digest compression parameter. Higher values produce more
    ///   accurate results at the cost of more memory. Typical range: 20–1000;
    ///   recommended default: `100.0`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let median = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
    ///     .approx_median_globally(100.0)
    ///     .collect_seq()?;
    /// assert!((median[0] - 50.0).abs() < 5.0);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn approx_median_globally(self, compression: f64) -> PCollection<f64> {
        self.combine_globally(ApproxMedian::<T>::new(compression), None)
    }

    /// Compute approximate quantiles of all elements globally.
    ///
    /// Returns a single `Vec<f64>` whose length equals `quantiles.len()`. Each
    /// value is the estimated value at the corresponding quantile rank (0.0 = min,
    /// 1.0 = max). Returns a vector of `f64::NAN` if the collection is empty.
    ///
    /// # Parameters
    /// - `quantiles` — ranks to compute, each in `[0.0, 1.0]`.
    ///   E.g. `vec![0.25, 0.5, 0.75]` for quartiles.
    /// - `compression` — t-digest compression parameter (recommended: `100.0`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let qs = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
    ///     .approx_quantiles_globally(vec![0.25, 0.5, 0.75], 100.0)
    ///     .collect_seq()?;
    /// // qs[0] is a Vec<f64> with three values
    /// assert_eq!(qs[0].len(), 3);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn approx_quantiles_globally(
        self,
        quantiles: Vec<f64>,
        compression: f64,
    ) -> PCollection<Vec<f64>> {
        self.combine_globally(ApproxQuantiles::<T>::new(quantiles, compression), None)
    }
}

/* ─────────────────────────────── Per-key ─────────────────────────────── */

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Eq + Hash,
    V: RFBound + Into<f64>,
{
    /// Compute the approximate median of values per key.
    ///
    /// Returns `(K, f64)`. Returns `f64::NAN` for keys with no values
    /// (cannot occur in practice).
    ///
    /// # Parameters
    /// - `compression` — t-digest compression parameter (recommended: `100.0`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let medians = from_vec(&p, vec![("a", 1.0f64), ("a", 3.0), ("b", 10.0)])
    ///     .approx_median_per_key(100.0)
    ///     .collect_seq()?;
    /// assert_eq!(medians[0].0, "a");
    /// assert!((medians[0].1 - 2.0).abs() < 1.0);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn approx_median_per_key(self, compression: f64) -> PCollection<(K, f64)> {
        self.combine_values(ApproxMedian::<V>::new(compression))
    }

    /// Compute approximate quantiles of values per key.
    ///
    /// Returns `(K, Vec<f64>)` where the vector length equals `quantiles.len()`.
    ///
    /// # Parameters
    /// - `quantiles` — ranks to compute, each in `[0.0, 1.0]`.
    /// - `compression` — t-digest compression parameter (recommended: `100.0`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let qs = from_vec(&p, vec![
    ///     ("a", 1.0f64), ("a", 2.0), ("a", 3.0), ("a", 4.0), ("a", 5.0),
    /// ])
    /// .approx_quantiles_per_key(vec![0.25, 0.5, 0.75], 100.0)
    /// .collect_seq()?;
    /// assert_eq!(qs[0].1.len(), 3);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn approx_quantiles_per_key(
        self,
        quantiles: Vec<f64>,
        compression: f64,
    ) -> PCollection<(K, Vec<f64>)> {
        self.combine_values(ApproxQuantiles::<V>::new(quantiles, compression))
    }
}
