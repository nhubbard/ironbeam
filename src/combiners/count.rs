//! Count combiner for counting elements per key or globally.

use crate::RFBound;
use crate::collection::{CombineFn, LiftableCombiner};
use std::marker::PhantomData;

/* ===================== Count<T> ===================== */

/// Count of values per key.
///
/// This combiner counts the number of values for each key, regardless of the actual
/// value content. It's more efficient than collecting all values and then counting them.
///
/// - Accumulator: `u64`
/// - Output: `u64`
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// use ironbeam::*;
/// use ironbeam::combiners::Count;
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
///
/// // Count values per key
/// let counts = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3), ("a", 4)])
///     .combine_values(Count::new())
///     .collect_seq_sorted()?;
/// assert_eq!(counts, vec![("a", 3), ("b", 1)]);
///
/// // Count globally
/// let total = from_vec(&p, vec![1, 2, 3, 4, 5])
///     .combine_globally(Count::new(), None)
///     .collect_seq()?;
/// assert_eq!(total, vec![5]);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct Count<T>(PhantomData<T>);

impl<T> Count<T> {
    /// Creates a new `Count` combiner.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<T, u64, u64> for Count<T>
where
    T: RFBound,
{
    fn create(&self) -> u64 {
        0
    }

    fn add_input(&self, acc: &mut u64, _v: T) {
        *acc += 1;
    }

    fn merge(&self, acc: &mut u64, other: u64) {
        *acc += other;
    }

    fn finish(&self, acc: u64) -> u64 {
        acc
    }
}

impl<T> LiftableCombiner<T, u64, u64> for Count<T>
where
    T: RFBound,
{
    fn build_from_group(&self, values: &[T]) -> u64 {
        values.len() as u64
    }
}
