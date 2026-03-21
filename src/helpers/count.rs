//! Count convenience methods for `PCollection`.
//!
//! This module provides ergonomic helpers for counting elements:
//! - `count_globally()` - Count all elements in the collection
//! - `count_per_key()` - Count values per key
//! - `count_per_element()` - Count occurrences of each distinct element

use crate::combiners::Count;
use crate::{PCollection, RFBound};
use std::hash::Hash;

impl<T: RFBound> PCollection<T> {
    /// Count all elements globally, producing a single count.
    ///
    /// This is more efficient than collecting all elements and then counting them,
    /// as it aggregates counts in parallel.
    ///
    /// # Returns
    ///
    /// A `PCollection<u64>` containing a single element: the total count.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec![1, 2, 3, 4, 5]);
    ///
    /// let count = data.count_globally().collect_seq()?;
    /// assert_eq!(count, vec![5]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn count_globally(self) -> PCollection<u64> {
        self.combine_globally(Count::new(), None)
    }

    /// Count occurrences of each distinct element.
    ///
    /// This transforms each element into a key and counts how many times
    /// each distinct element appears in the collection.
    ///
    /// # Requirements
    ///
    /// Elements must implement `Hash + Eq` for grouping.
    ///
    /// # Returns
    ///
    /// A `PCollection<(T, u64)>` where each tuple contains an element
    /// and its count.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let data = from_vec(&p, vec!["a", "b", "a", "c", "a", "b"]);
    ///
    /// let counts = data.count_per_element().collect_seq_sorted()?;
    /// assert_eq!(counts, vec![("a", 3), ("b", 2), ("c", 1)]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn count_per_element(self) -> PCollection<(T, u64)>
    where
        T: Hash + Eq,
    {
        self.key_by(std::clone::Clone::clone)
            .map_values(|_| ())
            .combine_values(Count::new())
    }
}

impl<K, V> PCollection<(K, V)>
where
    K: RFBound + Hash + Eq,
    V: RFBound,
{
    /// Count values per key.
    ///
    /// This aggregates all values for each key and returns the count of values
    /// per key. The actual value content is ignored; only the number of values
    /// matters.
    ///
    /// # Returns
    ///
    /// A `PCollection<(K, u64)>` where each tuple contains a key and the
    /// count of values associated with that key.
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
    ///     ("a", 1), ("a", 2), ("b", 3), ("a", 4), ("c", 5)
    /// ]);
    ///
    /// let counts = data.count_per_key().collect_seq_sorted()?;
    /// assert_eq!(counts, vec![("a", 3), ("b", 1), ("c", 1)]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn count_per_key(self) -> PCollection<(K, u64)> {
        self.combine_values(Count::new())
    }
}
