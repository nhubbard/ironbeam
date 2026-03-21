//! Collection combiners: `ToList`, `ToSet` for gathering values.

use crate::RFBound;
use crate::collection::{CombineFn, LiftableCombiner};
use std::collections::HashSet;
use std::hash::Hash;
use std::marker::PhantomData;

/* ===================== ToList<T> ===================== */

/// Collects all values into a `Vec<T>`.
///
/// This combiner gathers all values for a key into a single vector. Unlike collecting
/// all values and then grouping, this combiner efficiently merges partial results.
///
/// - Accumulator: `Vec<T>`
/// - Output: `Vec<T>`
///
/// # Performance Note
///
/// This combiner clones values during accumulation. For large values, consider
/// whether you truly need all values or if a sampling or aggregation combiner
/// would be more appropriate.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// use ironbeam::*;
/// use ironbeam::combiners::ToList;
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
///
/// // Collect all values per key into a list
/// let lists = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3), ("a", 4)])
///     .combine_values(ToList::new())
///     .collect_seq_sorted()?;
///
/// // Order within each list is not guaranteed
/// assert_eq!(lists.len(), 2);
/// assert_eq!(lists[0].0, "a");
/// assert_eq!(lists[0].1.len(), 3);
/// assert_eq!(lists[1].0, "b");
/// assert_eq!(lists[1].1, vec![3]);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct ToList<T>(PhantomData<T>);

impl<T> ToList<T> {
    /// Creates a new `ToList` combiner.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<T, Vec<T>, Vec<T>> for ToList<T>
where
    T: RFBound,
{
    fn create(&self) -> Vec<T> {
        Vec::new()
    }

    fn add_input(&self, acc: &mut Vec<T>, v: T) {
        acc.push(v);
    }

    fn merge(&self, acc: &mut Vec<T>, mut other: Vec<T>) {
        acc.append(&mut other);
    }

    fn finish(&self, acc: Vec<T>) -> Vec<T> {
        acc
    }
}

impl<T> LiftableCombiner<T, Vec<T>, Vec<T>> for ToList<T>
where
    T: RFBound,
{
    fn build_from_group(&self, values: &[T]) -> Vec<T> {
        values.to_vec()
    }
}

/* ===================== ToSet<T> ===================== */

/// Collects all unique values into a `HashSet<T>`.
///
/// This combiner gathers all distinct values for a key into a single hash set.
/// Duplicate values within the same key are automatically deduplicated.
///
/// - Accumulator: `HashSet<T>`
/// - Output: `HashSet<T>`
///
/// # Requirements
///
/// Values must implement `Hash + Eq` for deduplication.
///
/// # Examples
///
/// ```no_run
/// # use anyhow::Result;
/// use ironbeam::*;
/// use ironbeam::combiners::ToSet;
/// use std::collections::HashSet;
///
/// # fn main() -> Result<()> {
/// let p = Pipeline::default();
///
/// // Collect unique values per key into a set
/// let mut sets = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3), ("a", 1)])
///     .combine_values(ToSet::new())
///     .collect_seq()?;
/// sets.sort_by_key(|x| x.0);
///
/// assert_eq!(sets.len(), 2);
/// assert_eq!(sets[0].0, "a");
/// assert_eq!(sets[0].1.len(), 2); // Only unique values: 1 and 2
/// assert_eq!(sets[1].0, "b");
/// assert_eq!(sets[1].1, vec![3].into_iter().collect::<HashSet<_>>());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct ToSet<T>(PhantomData<T>);

impl<T> ToSet<T> {
    /// Creates a new `ToSet` combiner.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> CombineFn<T, HashSet<T>, HashSet<T>> for ToSet<T>
where
    T: RFBound + Hash + Eq,
{
    fn create(&self) -> HashSet<T> {
        HashSet::new()
    }

    fn add_input(&self, acc: &mut HashSet<T>, v: T) {
        acc.insert(v);
    }

    fn merge(&self, acc: &mut HashSet<T>, other: HashSet<T>) {
        acc.extend(other);
    }

    fn finish(&self, acc: HashSet<T>) -> HashSet<T> {
        acc
    }
}

impl<T> LiftableCombiner<T, HashSet<T>, HashSet<T>> for ToSet<T>
where
    T: RFBound + Hash + Eq,
{
    fn build_from_group(&self, values: &[T]) -> HashSet<T> {
        values.iter().cloned().collect()
    }
}
