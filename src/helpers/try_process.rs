//! Fallible element transforms (`try_map`, `try_flat_map`) and a fail-fast terminal.
//!
//! These helpers let you express transformations that may fail on a per-element
//! basis while keeping the pipeline type-safe. Results are wrapped in
//! `Result<_, E>` and can be surfaced or short-circuited at the end with
//! [`collect_fail_fast`].
//!
//! ## When to use
//! - You have parsing/validation/IO-lite logic that can fail per record, and you
//!   want to keep going until the terminal sink is reached.
//! - You want easy error surfacing during tests or interactive runs.
//!
//! ## Pattern
//! 1) turn `PCollection<T>` into `PCollection<Result<O,E>>` (or `Result<Vec<O>,E>`)
//! 2) at the end, call `collect_fail_fast()` to bail out on the first error.
//!
//! ```no_run
//! use rustflow::*;
//! use anyhow::Result;
//!
//! let p = Pipeline::default();
//! let lines = from_vec(&p, vec!["1","x","3","4"]);
//!
//! // Parse each line -> Result<u64, String>
//! let parsed = lines
//!     .try_map::<u64, String, _>(|s| s.parse::<u64>().map_err(|e| e.to_string()));
//!
//! // Fail on first bad element (here: "x").
//! let res: Result<Vec<u64>> = parsed.collect_fail_fast();
//! assert!(res.is_err());
//! ```

use crate::{PCollection, RFBound};
use anyhow::{anyhow, Result};
use std::fmt::Display;

impl<T: RFBound> PCollection<T> {
    /// Fallible 1->1 transform: `T -> Result<O, E>`.
    ///
    /// Converts a `PCollection<T>` into a `PCollection<Result<O, E>>` by
    /// applying a function that may fail per element. Errors are not raised
    /// immediately; they flow downstream as values until you explicitly collect
    /// them or use a terminal like [`PCollection<T>::collect_fail_fast`].
    ///
    /// ### Type bounds
    /// - `E: Clone + Display`: cloning allows the `Result<_,E>` to satisfy the library's
    ///   internal trait constraints; `Display` is used for human-readable errors.
    ///
    /// ### Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let raw = from_vec(&p, vec!["10", "oops", "42"]);
    ///
    /// let maybe_nums = raw.try_map::<u64, String, _>(|s| {
    ///     s.parse::<u64>().map_err(|e| e.to_string())
    /// });
    ///
    /// // maybe_nums: PCollection<Result<u64, String>>
    /// ```
    pub fn try_map<O, E, F>(self, f: F) -> PCollection<Result<O, E>>
    where
        O: RFBound,
        E: 'static + Send + Sync + Clone + Display,
        F: 'static + Send + Sync + Fn(&T) -> Result<O, E>,
    {
        // Result<O,E> now satisfies RFBound because E: Clone
        self.map(move |t| f(t))
    }

    /// Fallible 1->N transform: `T -> Result<Vec<O>, E>`.
    ///
    /// Like [`PCollection<T>::try_map`], but your function expands each input into zero or more
    /// outputs, still with fallible behavior. This yields
    /// `PCollection<Result<Vec<O>, E>>`. You typically combine this with
    /// a later `flat_map` or just collect and inspect failures explicitly.
    ///
    /// ### Example
    /// ```no_run
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let raw = from_vec(&p, vec!["1,2,3", "bad", "4,5"]);
    ///
    /// let maybe_lists = raw.try_flat_map::<u32, String, _>(|s| {
    ///     s.split(',')
    ///         .map(|tok| tok.parse::<u32>().map_err(|e| e.to_string()))
    ///         .collect::<Result<Vec<_>, _>>() // If any token fails, whole element fails
    /// });
    ///
    /// // maybe_lists: PCollection<Result<Vec<u32>, String>>
    /// ```
    pub fn try_flat_map<O, E, F>(self, f: F) -> PCollection<Result<Vec<O>, E>>
    where
        O: RFBound,
        E: 'static + Send + Sync + Clone + Display,
        F: 'static + Send + Sync + Fn(&T) -> Result<Vec<O>, E>,
    {
        self.map(move |t| f(t))
    }
}

// Fail-fast terminal (keeps errors ergonomic)
impl<T: RFBound, E> PCollection<Result<T, E>>
where
    E: 'static + Send + Sync + Clone + Display,
{
    /// Collect all `Ok` values or return the first `Err` encountered.
    ///
    /// This terminal runs the pipeline and materializes results. If every item
    /// is `Ok(T)`, it returns `Ok<Vec<T>>`. As soon as an `Err(E)` is seen, it
    /// returns `anyhow::Error` with your `E: Display` rendered in the message.
    ///
    /// ### Example
    /// ```no_run
    /// use rustflow::*;
    /// use anyhow::Result;
    ///
    /// let p = Pipeline::default();
    /// let raw = from_vec(&p, vec!["1","x","3"]);
    ///
    /// let parsed = raw.try_map::<u64, String, _>(|s| {
    ///     s.parse::<u64>().map_err(|e| format!("bad int: {e}"))
    /// });
    ///
    /// let res: Result<Vec<u64>> = parsed.collect_fail_fast();
    /// assert!(res.is_err()); // fails at "x"
    /// ```
    ///
    /// # Errors
    ///
    /// If an error occurs, it returns a [`Result`] type.
    pub fn collect_fail_fast(self) -> Result<Vec<T>> {
        let mut ok = Vec::new();
        for r in self.collect_seq()? {
            match r {
                Ok(v) => ok.push(v),
                Err(e) => return Err(anyhow!("element failed: {e}")),
            }
        }
        Ok(ok)
    }
}
