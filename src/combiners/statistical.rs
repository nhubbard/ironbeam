//! Statistical combiners: `AverageF64`, `Mean<O>`.

use crate::RFBound;
use crate::collection::CombineFn;
use std::marker::PhantomData;

/* ===================== AverageF64 ===================== */

/// Average of values per key as `f64`.
///
/// Values must be convertible into `f64` via `Into<f64>`.
///
/// - Accumulator: `(sum_f64, count_u64)`
/// - Output: `f64`
///
/// Empty groups produce `0.0`.
#[derive(Clone, Copy, Debug, Default)]
pub struct AverageF64;

impl<V> CombineFn<V, (f64, u64), f64> for AverageF64
where
    V: RFBound + Into<f64>,
{
    fn create(&self) -> (f64, u64) {
        (0.0, 0)
    }

    fn add_input(&self, acc: &mut (f64, u64), v: V) {
        acc.0 += v.into();
        acc.1 += 1;
    }

    fn merge(&self, acc: &mut (f64, u64), other: (f64, u64)) {
        acc.0 += other.0;
        acc.1 += other.1;
    }

    #[allow(clippy::cast_precision_loss)]
    fn finish(&self, acc: (f64, u64)) -> f64 {
        if acc.1 == 0 {
            0.0
        } else {
            acc.0 / (acc.1 as f64)
        }
    }
}

/* ===================== Mean<O> ===================== */

/// Arithmetic mean of input values producing an output of type `O`.
///
/// Generic over both the input value type `V` (via `Into<O>`) and the output
/// type `O`, which must be a floating-point type (`f32` or `f64`). This is the
/// Ironbeam equivalent of Apache Beam's `Mean.Globally()` / `Mean.PerKey()`
/// transforms and complements [`AverageF64`] by allowing the caller to choose
/// the floating-point precision of the result.
///
/// - Accumulator: `(O, u64)` — running sum and count.
/// - Output: `O`.
///
/// Empty groups produce `0.0` cast to `O`.
///
/// The combiner can be used directly with [`combine_globally`] or
/// [`combine_values`], or via the convenience helpers
/// [`mean_globally`](crate::PCollection::mean_globally) and
/// [`mean_per_key`](crate::PCollection::mean_per_key).
///
/// [`combine_globally`]: crate::PCollection::combine_globally
/// [`combine_values`]: crate::PCollection::combine_values
///
/// ## Example
/// ```no_run
/// # use anyhow::Result;
/// use ironbeam::*;
/// use ironbeam::combiners::Mean;
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
/// let out = from_vec(&p, vec![1u32, 2, 3, 4, 5])
///     .combine_globally(Mean::<f64>::new(), None)
///     .collect_seq()?;
/// assert!((out[0] - 3.0).abs() < 1e-12);
/// # Ok(()) }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct Mean<O>(PhantomData<O>);

impl<O> Mean<O> {
    /// Convenience constructor (same as `Default`).
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

macro_rules! impl_mean_for_float {
    ($float:ty) => {
        impl<V> CombineFn<V, ($float, u64), $float> for Mean<$float>
        where
            V: RFBound + Into<$float>,
        {
            fn create(&self) -> ($float, u64) {
                (0.0, 0)
            }

            fn add_input(&self, acc: &mut ($float, u64), v: V) {
                acc.0 += v.into();
                acc.1 += 1;
            }

            fn merge(&self, acc: &mut ($float, u64), other: ($float, u64)) {
                acc.0 += other.0;
                acc.1 += other.1;
            }

            #[allow(clippy::cast_precision_loss)]
            fn finish(&self, acc: ($float, u64)) -> $float {
                if acc.1 == 0 {
                    0.0
                } else {
                    acc.0 / (acc.1 as $float)
                }
            }
        }

    };
}

impl_mean_for_float!(f64);
impl_mean_for_float!(f32);
