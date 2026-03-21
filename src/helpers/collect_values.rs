//! Collection convenience methods for `PCollection`.
//!
//! This module provides ergonomic helpers for collecting values:
//! - `to_list_per_key()` - Collect all values per key into a Vec
//! - `to_set_per_key()` - Collect unique values per key into a `HashSet`

use crate::combiners::{ToList, ToSet};
use crate::{PCollection, RFBound};
use std::collections::HashSet;
use std::hash::Hash;

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Hash + Eq,
    V: RFBound,
{
    /// Collect all values per key into a `Vec<V>`.
    ///
    /// This gathers all values for each key into a single vector. The order of
    /// values within each vector is not guaranteed.
    ///
    /// # Performance Note
    ///
    /// This combiner clones values during accumulation and materializes all values
    /// in memory. For large value sets, consider using a combiner that summarizes
    /// or samples the data instead.
    ///
    /// # Returns
    ///
    /// A `PCollection<(K, Vec<V>)>` where each tuple contains a key and all
    /// values associated with that key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![
    ///     ("a", 1), ("a", 2), ("b", 3), ("a", 4)
    /// ]);
    ///
    /// let lists = data.to_list_per_key().collect_seq_sorted()?;
    /// assert_eq!(lists[0].0, "a");
    /// assert_eq!(lists[0].1.len(), 3);
    /// assert_eq!(lists[1].0, "b");
    /// assert_eq!(lists[1].1, vec![3]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn to_list_per_key(self) -> PCollection<(K, Vec<V>)> {
        self.combine_values(ToList::new())
    }

    /// Collect all unique values per key into a `HashSet<V>`.
    ///
    /// This gathers all distinct values for each key into a single hash set.
    /// Duplicate values within the same key are automatically deduplicated.
    ///
    /// # Requirements
    ///
    /// Values must implement `Hash + Eq` for deduplication.
    ///
    /// # Returns
    ///
    /// A `PCollection<(K, HashSet<V>)>` where each tuple contains a key and
    /// all unique values associated with that key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    /// use std::collections::HashSet;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![
    ///     ("a", 1), ("a", 2), ("b", 3), ("a", 1)
    /// ]);
    ///
    /// let mut sets = data.to_set_per_key().collect_seq()?;
/// sets.sort_by_key(|x| x.0);
    /// assert_eq!(sets[0].0, "a");
    /// assert_eq!(sets[0].1.len(), 2); // Only 1 and 2
    /// assert_eq!(sets[1].0, "b");
    /// assert_eq!(sets[1].1, vec![3].into_iter().collect::<HashSet<_>>());
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn to_set_per_key(self) -> PCollection<(K, HashSet<V>)>
    where
        V: Hash + Eq,
    {
        self.combine_values(ToSet::new())
    }
}
