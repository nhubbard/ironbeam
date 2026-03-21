//! Latest convenience methods for `PCollection`.
//!
//! This module provides ergonomic helpers for selecting the most recent
//! timestamped values:
//! - `latest_per_key()` - Select latest value per key
//! - `latest_globally()` - Select latest value globally

use crate::combiners::Latest;
use crate::window::Timestamped;
use crate::{PCollection, RFBound};
use std::hash::Hash;

impl<T: RFBound> PCollection<Timestamped<T>> {
    /// Select the value with the latest (most recent) timestamp globally.
    ///
    /// This aggregates all timestamped elements and returns the single element
    /// with the maximum timestamp. The timestamp is discarded in the output,
    /// returning only the unwrapped value.
    ///
    /// # Returns
    ///
    /// A `PCollection<T>` containing a single element: the value with the
    /// latest timestamp.
    ///
    /// # Panics
    ///
    /// Panics if the collection is empty.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    /// use ironbeam::window::Timestamped;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(100, "event1"),
    ///     Timestamped::new(300, "event2"),
    ///     Timestamped::new(200, "event3"),
    /// ]);
    ///
    /// let latest = events.latest_globally().collect_seq()?;
    /// assert_eq!(latest, vec!["event2"]); // timestamp 300 is latest
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn latest_globally(self) -> PCollection<T> {
        self.combine_globally(Latest::new(), None)
    }
}

impl<K, T> PCollection<(K, Timestamped<T>)>
where
    K: RFBound + Hash + Eq,
    T: RFBound,
{
    /// Select the value with the latest (most recent) timestamp per key.
    ///
    /// This groups by key and returns the value with the maximum timestamp
    /// for each key. The timestamp is discarded in the output, returning
    /// only the unwrapped values.
    ///
    /// # Returns
    ///
    /// A `PCollection<(K, T)>` where each tuple contains a key and the
    /// value with the latest timestamp for that key.
    ///
    /// # Panics
    ///
    /// Panics if any key has no values.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    /// use ironbeam::window::Timestamped;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     ("user1", Timestamped::new(100, "login")),
    ///     ("user1", Timestamped::new(200, "click")),
    ///     ("user2", Timestamped::new(150, "purchase")),
    ///     ("user1", Timestamped::new(180, "view")),
    /// ]);
    ///
    /// let latest = events.latest_per_key().collect_seq_sorted()?;
    /// assert_eq!(latest, vec![
    ///     ("user1", "click"),  // timestamp 200 is latest
    ///     ("user2", "purchase")
    /// ]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn latest_per_key(self) -> PCollection<(K, T)> {
        self.combine_values(Latest::new())
    }
}
