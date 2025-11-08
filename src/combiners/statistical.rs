//! Statistical combiners: `AverageF64`

use crate::collection::{CombineFn, LiftableCombiner};
use crate::RFBound;

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

impl<V> LiftableCombiner<V, (f64, u64), f64> for AverageF64
where
    V: RFBound + Into<f64>,
{
    fn build_from_group(&self, values: &[V]) -> (f64, u64) {
        let sum: f64 = values.iter().map(|v| v.clone().into()).sum();
        (sum, values.len() as u64)
    }
}
