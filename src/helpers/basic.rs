//! Arithmetic aggregate convenience methods for [`PCollection`].
//!
//! Provides thin wrappers around [`combine_globally`](PCollection::combine_globally) and
//! [`combine_values`](PCollection::combine_values) for the most common numeric reductions,
//! so callers never need to import or construct combiner structs directly.
//!
//! ## Unkeyed (global) operations — `PCollection<T>`
//! - [`PCollection::sum_globally`] — sum of all elements → `PCollection<T>`
//! - [`PCollection::min_globally`] — minimum element → `PCollection<T>`
//! - [`PCollection::max_globally`] — maximum element → `PCollection<T>`
//! - [`PCollection::average_globally`] — arithmetic mean → `PCollection<f64>`
//! - [`PCollection::mean_globally`] — arithmetic mean with a caller-chosen output type → `PCollection<O>`
//!
//! ## Per-key operations — `PCollection<(K, V)>`
//! - [`PCollection::sum_per_key`] — sum of values per key → `PCollection<(K, V)>`
//! - [`PCollection::min_per_key`] — minimum value per key → `PCollection<(K, V)>`
//! - [`PCollection::max_per_key`] — maximum value per key → `PCollection<(K, V)>`
//! - [`PCollection::average_per_key`] — arithmetic mean per key → `PCollection<(K, f64)>`
//! - [`PCollection::mean_per_key`] — arithmetic mean per key with caller-chosen output → `PCollection<(K, O)>`

use crate::collection::CombineFn;
use crate::combiners::{AverageF64, Max, Mean, Min, Sum};
use crate::{Element, PCollection};
use std::hash::Hash;
use std::ops::Add;

/* ─────────────────────────────── Unkeyed (global) ─────────────────────────────── */

impl<T> PCollection<T>
where
    T: Element + Add<Output = T> + Default,
{
    /// Sum all elements globally into a single value.
    ///
    /// Requires `T: Add<Output = T> + Default`. The identity value (`T::default()`)
    /// is used as the initial accumulator, so an empty collection produces one
    /// element equal to `T::default()`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let total = from_vec(&p, vec![1u64, 2, 3, 4]).sum_globally().collect_seq()?;
    /// assert_eq!(total, vec![10u64]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn sum_globally(self) -> Self {
        self.combine_globally(Sum::<T>::new(), None)
    }
}

impl<T> PCollection<T>
where
    T: Element + Ord,
{
    /// Return the minimum element globally as a single-element collection.
    ///
    /// # Panics
    ///
    /// Panics if the input collection is empty (there is no minimum of an empty set).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let min = from_vec(&p, vec![3i32, 1, 4, 1, 5]).min_globally().collect_seq()?;
    /// assert_eq!(min, vec![1i32]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn min_globally(self) -> Self {
        self.combine_globally(Min::<T>::new(), None)
    }

    /// Return the maximum element globally as a single-element collection.
    ///
    /// # Panics
    ///
    /// Panics if the input collection is empty (there is no maximum of an empty set).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let max = from_vec(&p, vec![3i32, 1, 4, 1, 5]).max_globally().collect_seq()?;
    /// assert_eq!(max, vec![5i32]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn max_globally(self) -> Self {
        self.combine_globally(Max::<T>::new(), None)
    }
}

impl<T> PCollection<T>
where
    T: Element + Into<f64>,
{
    /// Compute the arithmetic mean of all elements globally.
    ///
    /// Produces a single `f64`. Returns `0.0` if the collection is empty.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let avg = from_vec(&p, vec![1u32, 2, 3, 4, 5]).average_globally().collect_seq()?;
    /// assert!((avg[0] - 3.0).abs() < 1e-12);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn average_globally(self) -> PCollection<f64> {
        self.combine_globally(AverageF64, None)
    }
}

impl<T> PCollection<T>
where
    T: Element,
{
    /// Compute the arithmetic mean of all elements globally with a caller-chosen
    /// floating-point output type.
    ///
    /// Generic over the output type `O`, which must be `f32` or `f64`. The input
    /// element type `T` must be convertible to `O` via `Into<O>`. The
    /// [`Mean`] combiner is used internally. Empty
    /// collections produce `0.0` cast to `O`.
    ///
    /// This complements [`average_globally`](Self::average_globally), which is
    /// fixed to `f64` output. Use `mean_globally::<f32>()` when memory
    /// footprint matters more than precision.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let avg = from_vec(&p, vec![1u32, 2, 3, 4, 5])
    ///     .mean_globally::<f64>()
    ///     .collect_seq()?;
    /// assert!((avg[0] - 3.0).abs() < 1e-12);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn mean_globally<O>(self) -> PCollection<O>
    where
        O: Element,
        Mean<O>: CombineFn<T, (O, u64), O> + 'static,
    {
        self.combine_globally(Mean::<O>::new(), None)
    }
}

/* ─────────────────────────────── Per-key ─────────────────────────────── */

impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash,
    V: Element + Add<Output = V> + Default,
{
    /// Sum values per key.
    ///
    /// Requires `V: Add<Output = V> + Default`. Returns `V::default()` for
    /// keys with no values (cannot occur in practice because `combine_values`
    /// only emits entries for observed keys).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let sums = from_vec(&p, vec![("a".to_string(), 1u64), ("a".to_string(), 2), ("b".to_string(), 10)])
    ///     .sum_per_key()
    ///     .collect_seq_sorted()?;
    /// assert_eq!(sums, vec![("a".to_string(), 3u64), ("b".to_string(), 10u64)]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn sum_per_key(self) -> Self {
        self.combine_values(Sum::<V>::new())
    }
}

impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash,
    V: Element + Ord,
{
    /// Select the minimum value per key.
    ///
    /// # Panics
    ///
    /// Panics if any key has no associated values (cannot occur when using
    /// `combine_values` over a non-empty source).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let mins = from_vec(&p, vec![("a".to_string(), 5i32), ("a".to_string(), 2), ("b".to_string(), 8)])
    ///     .min_per_key()
    ///     .collect_seq_sorted()?;
    /// assert_eq!(mins, vec![("a".to_string(), 2i32), ("b".to_string(), 8i32)]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn min_per_key(self) -> Self {
        self.combine_values(Min::<V>::new())
    }

    /// Select the maximum value per key.
    ///
    /// # Panics
    ///
    /// Panics if any key has no associated values (cannot occur when using
    /// `combine_values` over a non-empty source).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let maxs = from_vec(&p, vec![("a".to_string(), 5i32), ("a".to_string(), 2), ("b".to_string(), 8)])
    ///     .max_per_key()
    ///     .collect_seq_sorted()?;
    /// assert_eq!(maxs, vec![("a".to_string(), 5i32), ("b".to_string(), 8i32)]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn max_per_key(self) -> Self {
        self.combine_values(Max::<V>::new())
    }
}

impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash,
    V: Element + Into<f64>,
{
    /// Compute the arithmetic mean of values per key, producing `(K, f64)`.
    ///
    /// Returns `0.0` for keys with zero values (cannot occur in practice).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let avgs = from_vec(&p, vec![("a".to_string(), 1u32), ("a".to_string(), 3), ("b".to_string(), 10)])
    ///     .average_per_key()
    ///     .collect_seq()?;
    /// assert_eq!(avgs[0].0, "a");
    /// assert!((avgs[0].1 - 2.0).abs() < 1e-12);
    /// assert_eq!(avgs[1].0, "b");
    /// assert!((avgs[1].1 - 10.0).abs() < 1e-12);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn average_per_key(self) -> PCollection<(K, f64)> {
        self.combine_values(AverageF64)
    }
}

impl<K, V> PCollection<(K, V)>
where
    K: Element + Eq + Hash,
    V: Element,
{
    /// Compute the arithmetic mean of values per key with a caller-chosen
    /// floating-point output type, producing `(K, O)`.
    ///
    /// Generic over the output type `O`, which must be `f32` or `f64`. The
    /// input value type `V` must be convertible to `O` via `Into<O>`. The
    /// [`Mean`] combiner is used internally. Returns
    /// `0.0` cast to `O` for keys with zero values (cannot occur in practice
    /// because `combine_values` only emits entries for observed keys).
    ///
    /// This complements [`average_per_key`](Self::average_per_key), which is
    /// fixed to `f64` output. Use `mean_per_key::<f32>()` when memory footprint
    /// matters more than precision.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let avgs = from_vec(&p, vec![("a".to_string(), 1u32), ("a".to_string(), 3), ("b".to_string(), 10)])
    ///     .mean_per_key::<f64>()
    ///     .collect_seq()?;
    /// // "a" -> (1 + 3) / 2 = 2.0, "b" -> 10.0
    /// # let _ = avgs;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn mean_per_key<O>(self) -> PCollection<(K, O)>
    where
        O: Element,
        Mean<O>: CombineFn<V, (O, u64), O> + 'static,
    {
        self.combine_values(Mean::<O>::new())
    }
}
