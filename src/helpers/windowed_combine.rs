//! Windowed combining helpers for timestamped and keyed streams.
//!
//! This module provides one-call convenience methods for the most common pattern in
//! tumbling-window pipelines: assign → group → aggregate. Without these helpers the
//! caller must chain three explicit steps:
//!
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let readings = from_vec(&p, vec![
//!     Timestamped::new(5_000u64, 1.0f64),
//!     Timestamped::new(15_000u64, 2.0f64),
//! ]);
//!
//! // Without helpers: three explicit steps
//! let windowed = readings
//!     .key_by_window(10_000, 0)
//!     .group_by_key()
//!     .combine_values_lifted(Sum::<f64>::new());
//! # let _ = windowed.collect_seq()?;
//! # Ok(())
//! # }
//! ```
//!
//! With the helpers the same pipeline collapses to a single call:
//!
//! ```no_run
//! use ironbeam::*;
//! use anyhow::Result;
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let readings = from_vec(&p, vec![
//!     Timestamped::new(5_000u64, 1.0f64),
//!     Timestamped::new(15_000u64, 2.0f64),
//! ]);
//!
//! let windowed = readings.sum_per_window(10_000, 0); // PCollection<(Window, f64)>
//! # let _ = windowed.collect_seq()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Unkeyed stream — `PCollection<Timestamped<T>>`
//!
//! | Method | Output type | Constraint |
//! |---|---|---|
//! | [`combine_per_window`](PCollection::combine_per_window) | `(Window, O)` | custom combiner |
//! | [`sum_per_window`](PCollection::sum_per_window) | `(Window, T)` | `T: Add + Default` |
//! | [`count_per_window`](PCollection::count_per_window) | `(Window, u64)` | — |
//! | [`min_per_window`](PCollection::min_per_window) | `(Window, T)` | `T: Ord` |
//! | [`max_per_window`](PCollection::max_per_window) | `(Window, T)` | `T: Ord` |
//! | [`average_per_window`](PCollection::average_per_window) | `(Window, f64)` | `T: Into<f64>` |
//!
//! ## Keyed stream — `PCollection<(K, Timestamped<V>)>`
//!
//! | Method | Output type | Constraint |
//! |---|---|---|
//! | [`combine_per_key_and_window`](PCollection::combine_per_key_and_window) | `((K, Window), O)` | custom combiner |
//! | [`sum_per_key_and_window`](PCollection::sum_per_key_and_window) | `((K, Window), V)` | `V: Add + Default` |
//! | [`count_per_key_and_window`](PCollection::count_per_key_and_window) | `((K, Window), u64)` | — |
//! | [`min_per_key_and_window`](PCollection::min_per_key_and_window) | `((K, Window), V)` | `V: Ord` |
//! | [`max_per_key_and_window`](PCollection::max_per_key_and_window) | `((K, Window), V)` | `V: Ord` |
//! | [`average_per_key_and_window`](PCollection::average_per_key_and_window) | `((K, Window), f64)` | `V: Into<f64>` |

use crate::collection::LiftableCombiner;
use crate::combiners::{AverageF64, Count, Max, Min, Sum};
use crate::{CombineFn, PCollection, RFBound, Timestamped, Window};
use std::hash::Hash;
use std::ops::Add;

/* ─────────────────────────── Unkeyed: PCollection<Timestamped<T>> ─────────────────────────── */

impl<T: RFBound> PCollection<Timestamped<T>> {
    /// Aggregate elements into tumbling windows using a custom [`CombineFn`].
    ///
    /// This is the generic building block. It is equivalent to:
    /// ```text
    /// self.group_by_window(size_ms, offset_ms)  // (Window, Vec<T>)
    ///     .combine_values_lifted(comb)           // (Window, O)
    /// ```
    ///
    /// The combiner must also implement [`LiftableCombiner`] so that it can be
    /// applied efficiently to the grouped `Vec<T>` without a redundant per-element
    /// loop. All built-in combiners (`Sum`, `Min`, `Max`, `AverageF64`, `Count`) satisfy
    /// this requirement.
    ///
    /// # Type Parameters
    /// - `C`: combiner implementing both `CombineFn<T, A, O>` and `LiftableCombiner<T, A, O>`.
    /// - `A`: accumulator type (must be `Send + Sync + 'static`).
    /// - `O`: output value per window.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_000u64, 10u32),
    ///     Timestamped::new(5_000u64, 20u32),
    ///     Timestamped::new(12_000u64, 5u32),
    /// ]);
    ///
    /// // Sum values inside each 10-second window
    /// let sums = events.combine_per_window(10_000, 0, Sum::<u32>::new());
    /// let mut result = sums.collect_seq()?;
    /// result.sort_by_key(|(w, _)| w.start);
    /// assert_eq!(result, vec![
    ///     (Window::new(0, 10_000), 30u32),
    ///     (Window::new(10_000, 20_000), 5u32),
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn combine_per_window<C, A, O>(
        self,
        size_ms: u64,
        offset_ms: u64,
        comb: C,
    ) -> PCollection<(Window, O)>
    where
        C: CombineFn<T, A, O> + LiftableCombiner<T, A, O> + 'static,
        A: Send + Sync + 'static,
        O: RFBound,
    {
        self.group_by_window(size_ms, offset_ms)
            .combine_values_lifted(comb)
    }

    /// Sum elements per tumbling window.
    ///
    /// Requires `T: Add<Output = T> + Default`. The identity value (`T::default()`) is used
    /// as the initial accumulator, so an empty window is not possible (windows only appear in
    /// the output when they contain at least one element).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_000u64, 1u32),
    ///     Timestamped::new(5_000u64, 2u32),
    ///     Timestamped::new(11_000u64, 4u32),
    /// ]);
    ///
    /// let sums = events.sum_per_window(10_000, 0);
    /// let mut result = sums.collect_seq()?;
    /// result.sort_by_key(|(w, _)| w.start);
    /// assert_eq!(result, vec![
    ///     (Window::new(0, 10_000), 3u32),
    ///     (Window::new(10_000, 20_000), 4u32),
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn sum_per_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, T)>
    where
        T: Add<Output = T> + Default,
    {
        self.combine_per_window(size_ms, offset_ms, Sum::<T>::new())
    }

    /// Count elements per tumbling window, producing `(Window, u64)`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_000u64, "a"),
    ///     Timestamped::new(5_000u64, "b"),
    ///     Timestamped::new(11_000u64, "c"),
    ///     Timestamped::new(14_000u64, "d"),
    /// ]);
    ///
    /// let counts = events.count_per_window(10_000, 0);
    /// let mut result = counts.collect_seq()?;
    /// result.sort_by_key(|(w, _)| w.start);
    /// assert_eq!(result, vec![
    ///     (Window::new(0, 10_000), 2u64),
    ///     (Window::new(10_000, 20_000), 2u64),
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn count_per_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, u64)> {
        self.combine_per_window(size_ms, offset_ms, Count::<T>::new())
    }

    /// Select the minimum element per tumbling window.
    ///
    /// # Panics
    ///
    /// Panics if any window contains zero elements (cannot occur in practice because windows
    /// only appear when they have at least one element).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_000u64, 5u32),
    ///     Timestamped::new(5_000u64, 2u32),
    ///     Timestamped::new(11_000u64, 8u32),
    /// ]);
    ///
    /// let mins = events.min_per_window(10_000, 0);
    /// let mut result = mins.collect_seq()?;
    /// result.sort_by_key(|(w, _)| w.start);
    /// assert_eq!(result, vec![
    ///     (Window::new(0, 10_000), 2u32),
    ///     (Window::new(10_000, 20_000), 8u32),
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn min_per_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, T)>
    where
        T: Ord,
    {
        self.combine_per_window(size_ms, offset_ms, Min::<T>::new())
    }

    /// Select the maximum element per tumbling window.
    ///
    /// # Panics
    ///
    /// Panics if any window contains zero elements (cannot occur in practice).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_000u64, 5u32),
    ///     Timestamped::new(5_000u64, 2u32),
    ///     Timestamped::new(11_000u64, 8u32),
    /// ]);
    ///
    /// let maxs = events.max_per_window(10_000, 0);
    /// let mut result = maxs.collect_seq()?;
    /// result.sort_by_key(|(w, _)| w.start);
    /// assert_eq!(result, vec![
    ///     (Window::new(0, 10_000), 5u32),
    ///     (Window::new(10_000, 20_000), 8u32),
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn max_per_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, T)>
    where
        T: Ord,
    {
        self.combine_per_window(size_ms, offset_ms, Max::<T>::new())
    }

    /// Compute the arithmetic mean of elements per tumbling window, producing `(Window, f64)`.
    ///
    /// Returns `0.0` for windows with no elements (cannot occur in practice).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_000u64, 1u32),
    ///     Timestamped::new(5_000u64, 3u32),
    ///     Timestamped::new(11_000u64, 10u32),
    /// ]);
    ///
    /// let avgs = events.average_per_window(10_000, 0);
    /// let mut result = avgs.collect_seq()?;
    /// result.sort_by_key(|(w, _)| w.start);
    /// assert!((result[0].1 - 2.0).abs() < 1e-12); // window [0, 10_000): (1+3)/2
    /// assert!((result[1].1 - 10.0).abs() < 1e-12); // window [10_000, 20_000): 10/1
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn average_per_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, f64)>
    where
        T: Into<f64>,
    {
        self.combine_per_window(size_ms, offset_ms, AverageF64)
    }
}

/* ─────────── Keyed: PCollection<(K, Timestamped<V>)> ─────────── */

impl<K, V> PCollection<(K, Timestamped<V>)>
where
    K: RFBound + Eq + Hash,
    V: RFBound,
{
    /// Aggregate keyed elements into tumbling windows using a custom [`CombineFn`].
    ///
    /// This is the generic keyed building block. It is equivalent to:
    /// ```text
    /// self.group_by_key_and_window(size_ms, offset_ms)  // ((K, Window), Vec<V>)
    ///     .combine_values_lifted(comb)                   // ((K, Window), O)
    /// ```
    ///
    /// The combiner must also implement [`LiftableCombiner`].
    ///
    /// # Type Parameters
    /// - `C`: combiner implementing both `CombineFn<V, A, O>` and `LiftableCombiner<V, A, O>`.
    /// - `A`: accumulator type (must be `Send + Sync + 'static`).
    /// - `O`: output value per `(key, window)` pair.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     ("sensor_a".to_string(), Timestamped::new(1_000u64, 10u32)),
    ///     ("sensor_a".to_string(), Timestamped::new(5_000u64, 20u32)),
    ///     ("sensor_b".to_string(), Timestamped::new(3_000u64, 5u32)),
    ///     ("sensor_a".to_string(), Timestamped::new(12_000u64, 3u32)),
    /// ]);
    ///
    /// let sums = events.combine_per_key_and_window(10_000, 0, Sum::<u32>::new());
    /// let mut result = sums.collect_seq()?;
    /// result.sort();
    /// // sensor_a: window 0 = 10+20=30, window 1 = 3; sensor_b: window 0 = 5
    /// assert_eq!(result[0], (("sensor_a".to_string(), Window::new(0, 10_000)), 30u32));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn combine_per_key_and_window<C, A, O>(
        self,
        size_ms: u64,
        offset_ms: u64,
        comb: C,
    ) -> PCollection<((K, Window), O)>
    where
        C: CombineFn<V, A, O> + LiftableCombiner<V, A, O> + 'static,
        A: Send + Sync + 'static,
        O: RFBound,
    {
        self.group_by_key_and_window(size_ms, offset_ms)
            .combine_values_lifted(comb)
    }

    /// Sum values per key and tumbling window, producing `((K, Window), V)`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     ("k1".to_string(), Timestamped::new(1_000u64, 1u32)),
    ///     ("k1".to_string(), Timestamped::new(5_000u64, 2u32)),
    ///     ("k2".to_string(), Timestamped::new(3_000u64, 10u32)),
    /// ]);
    ///
    /// let sums = events.sum_per_key_and_window(10_000, 0);
    /// let mut result = sums.collect_seq()?;
    /// result.sort();
    /// assert_eq!(result[0], (("k1".to_string(), Window::new(0, 10_000)), 3u32));
    /// assert_eq!(result[1], (("k2".to_string(), Window::new(0, 10_000)), 10u32));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn sum_per_key_and_window(
        self,
        size_ms: u64,
        offset_ms: u64,
    ) -> PCollection<((K, Window), V)>
    where
        V: Add<Output = V> + Default,
    {
        self.combine_per_key_and_window(size_ms, offset_ms, Sum::<V>::new())
    }

    /// Count values per key and tumbling window, producing `((K, Window), u64)`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     ("k1".to_string(), Timestamped::new(1_000u64, "a")),
    ///     ("k1".to_string(), Timestamped::new(5_000u64, "b")),
    ///     ("k2".to_string(), Timestamped::new(3_000u64, "c")),
    /// ]);
    ///
    /// let counts = events.count_per_key_and_window(10_000, 0);
    /// let mut result = counts.collect_seq()?;
    /// result.sort();
    /// assert_eq!(result[0], (("k1".to_string(), Window::new(0, 10_000)), 2u64));
    /// assert_eq!(result[1], (("k2".to_string(), Window::new(0, 10_000)), 1u64));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn count_per_key_and_window(
        self,
        size_ms: u64,
        offset_ms: u64,
    ) -> PCollection<((K, Window), u64)> {
        self.combine_per_key_and_window(size_ms, offset_ms, Count::<V>::new())
    }

    /// Select the minimum value per key and tumbling window.
    ///
    /// # Panics
    ///
    /// Panics if any `(key, window)` pair has zero associated values (cannot occur in practice).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     ("k1".to_string(), Timestamped::new(1_000u64, 5u32)),
    ///     ("k1".to_string(), Timestamped::new(5_000u64, 2u32)),
    ///     ("k1".to_string(), Timestamped::new(11_000u64, 8u32)),
    /// ]);
    ///
    /// let mins = events.min_per_key_and_window(10_000, 0);
    /// let mut result = mins.collect_seq()?;
    /// result.sort();
    /// assert_eq!(result[0], (("k1".to_string(), Window::new(0, 10_000)), 2u32));
    /// assert_eq!(result[1], (("k1".to_string(), Window::new(10_000, 20_000)), 8u32));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn min_per_key_and_window(
        self,
        size_ms: u64,
        offset_ms: u64,
    ) -> PCollection<((K, Window), V)>
    where
        V: Ord,
    {
        self.combine_per_key_and_window(size_ms, offset_ms, Min::<V>::new())
    }

    /// Select the maximum value per key and tumbling window.
    ///
    /// # Panics
    ///
    /// Panics if any `(key, window)` pair has zero associated values (cannot occur in practice).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     ("k1".to_string(), Timestamped::new(1_000u64, 5u32)),
    ///     ("k1".to_string(), Timestamped::new(5_000u64, 2u32)),
    ///     ("k1".to_string(), Timestamped::new(11_000u64, 8u32)),
    /// ]);
    ///
    /// let maxs = events.max_per_key_and_window(10_000, 0);
    /// let mut result = maxs.collect_seq()?;
    /// result.sort();
    /// assert_eq!(result[0], (("k1".to_string(), Window::new(0, 10_000)), 5u32));
    /// assert_eq!(result[1], (("k1".to_string(), Window::new(10_000, 20_000)), 8u32));
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn max_per_key_and_window(
        self,
        size_ms: u64,
        offset_ms: u64,
    ) -> PCollection<((K, Window), V)>
    where
        V: Ord,
    {
        self.combine_per_key_and_window(size_ms, offset_ms, Max::<V>::new())
    }

    /// Compute the arithmetic mean of values per key and tumbling window, producing
    /// `((K, Window), f64)`.
    ///
    /// Returns `0.0` for any `(key, window)` pair with no values (cannot occur in practice).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use ironbeam::*;
    /// use anyhow::Result;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     ("k1".to_string(), Timestamped::new(1_000u64, 1u32)),
    ///     ("k1".to_string(), Timestamped::new(5_000u64, 3u32)),
    ///     ("k1".to_string(), Timestamped::new(11_000u64, 10u32)),
    /// ]);
    ///
    /// let avgs = events.average_per_key_and_window(10_000, 0);
    /// let mut result = avgs.collect_seq()?;
    /// result.sort_by(|a, b| a.0.cmp(&b.0));
    /// assert!((result[0].1 - 2.0).abs() < 1e-12); // (1+3)/2
    /// assert!((result[1].1 - 10.0).abs() < 1e-12); // 10/1
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn average_per_key_and_window(
        self,
        size_ms: u64,
        offset_ms: u64,
    ) -> PCollection<((K, Window), f64)>
    where
        V: Into<f64>,
    {
        self.combine_per_key_and_window(size_ms, offset_ms, AverageF64)
    }
}
