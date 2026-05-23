//! `Display`-based string conversion helpers for [`PCollection`].
//!
//! Provides a single transform — [`PCollection::to_display_string`] — that
//! formats each element via its [`Display`] implementation and emits
//! the resulting `String`. The Ironbeam equivalent of Apache Beam's
//! `ToString.elements()`.
//!
//! The method is named `to_display_string` (rather than `to_string`) to avoid
//! collision with the inherent `ToString::to_string` trait method that ships
//! on every `Display` type. Calling `pcoll.to_string()` would otherwise be
//! ambiguous in trait resolution and surprising at call sites.

use crate::{PCollection, RFBound};
use std::fmt::Display;

impl<T: RFBound + Display> PCollection<T> {
    /// Convert each element to its [`Display`] representation as a `String`.
    ///
    /// This is the Ironbeam equivalent of Apache Beam's `ToString.elements()`.
    /// It's a thin wrapper over `map` that calls `format!("{t}")` on each
    /// element. The output is `PCollection<String>`.
    ///
    /// The method is named `to_display_string` to avoid colliding with the
    /// inherent `ToString::to_string` method that ships on every `Display`
    /// type — calling `.to_string()` directly on a `PCollection<T>` would be
    /// confusing because `PCollection<T>` itself isn't `Display`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let strings = from_vec(&p, vec![1u32, 2, 3])
    ///     .to_display_string()
    ///     .collect_seq()?;
    /// assert_eq!(strings, vec!["1".to_string(), "2".into(), "3".into()]);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn to_display_string(self) -> PCollection<String> {
        self.map(|t| format!("{t}"))
    }
}
