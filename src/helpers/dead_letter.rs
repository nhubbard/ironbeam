//! Dead-letter / error-routing pattern for fallible element transforms.
//!
//! This module provides the foundational pattern for resilient ETL
//! pipelines: a fallible transform that splits its output into two
//! independent collections — successful results and failure records —
//! instead of panicking or short-circuiting on the first error.
//!
//! The pattern is the Ironbeam analogue of Apache Beam's
//! `error_handling.py` and is described in Beam's "robust pipelines"
//! cookbook as the canonical way to keep an ETL job running in the
//! presence of a few bad records.
//!
//! ## When to use this vs. [`try_map`](crate::PCollection::try_map)
//!
//! Both helpers expose fallible per-element transforms, but with
//! different downstream contracts:
//!
//! - [`try_map`](crate::PCollection::try_map) returns a
//!   `PCollection<Result<O, E>>`. Both arms travel through the same
//!   stream; callers typically resolve them with
//!   [`collect_fail_fast`](crate::PCollection::collect_fail_fast) (panic
//!   on the first `Err`) or by manually pattern-matching downstream.
//! - [`map_catching`](crate::PCollection::map_catching) and
//!   [`flat_map_catching`](crate::PCollection::flat_map_catching) **split
//!   immediately** into `(good, errors)`. The two outputs can be wired
//!   to different sinks: write `good` to your data warehouse and
//!   `errors` to a quarantine bucket / log file / Slack channel.
//!
//! ## Example — guarded parse
//!
//! ```no_run
//! # use anyhow::Result;
//! use ironbeam::*;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let lines = from_vec(&p, vec!["1".to_string(), "oops".into(), "42".into()]);
//!
//! let (parsed, errors) = lines.map_catching(|s: &String| {
//!     s.parse::<u64>().map_err(|e| e.to_string())
//! });
//!
//! let mut good = parsed.collect_seq()?;
//! good.sort_unstable();
//! assert_eq!(good, vec![1u64, 42]);
//!
//! let bad = errors.collect_seq()?;
//! assert_eq!(bad.len(), 1);
//! assert_eq!(bad[0].element, "oops");
//! assert!(bad[0].error.contains("invalid digit"));
//! # Ok(()) }
//! ```

use crate::{PCollection, RFBound};
use std::fmt::Display;

/// A record diverted to the dead-letter branch of a fallible transform.
///
/// Carries the *original* input element alongside a human-readable error
/// message rendered from the failure value's [`Display`] implementation.
/// `DeadLetter<T>` itself implements [`RFBound`] when `T` does, so it can
/// flow through the rest of the pipeline like any other element type —
/// you can `map` it, `key_by` it, `write_jsonl` it, and so on.
///
/// # Field accessors
///
/// `element` and `error` are public for ergonomic destructuring and
/// match-based extraction; treat the struct as a value type rather than
/// an opaque handle.
#[derive(Clone, Debug)]
pub struct DeadLetter<T> {
    /// The original input element that the transform was applied to.
    pub element: T,
    /// Human-readable error string produced by `error.to_string()` (i.e.
    /// the failure's [`Display`] formatting).
    pub error: String,
}

impl<T> DeadLetter<T> {
    /// Construct a new dead-letter record. `error` accepts anything that
    /// converts into a `String` — most commonly a `String` produced by
    /// `e.to_string()` on the upstream error, or a `&str` literal for
    /// hand-curated messages.
    pub fn new(element: T, error: impl Into<String>) -> Self {
        Self {
            element,
            error: error.into(),
        }
    }
}

/// Internal classification enum used as an intermediate carrier between
/// the user's fallible function and the two output `filter_map`s. Never
/// appears in any public type signature.
#[derive(Clone)]
enum Classified<O, T> {
    Ok(O),
    Err(DeadLetter<T>),
}

impl<T: RFBound> PCollection<T> {
    /// Fallible 1→1 transform that routes failures to a dead-letter
    /// collection instead of failing the pipeline.
    ///
    /// Applies `f` to every element. Each `Ok(out)` flows into the first
    /// returned [`PCollection`], each `Err(e)` is wrapped in a
    /// [`DeadLetter`] (carrying the original input element and
    /// `e.to_string()`) and flows into the second.
    ///
    /// The two output collections are **independent** — you can apply
    /// further transforms, side-input joins, or distinct sinks to each.
    /// The planner's dominator-based cache placement (feature 3.13)
    /// ensures the underlying classification pass runs only once even
    /// though both collections share the same upstream node.
    ///
    /// # Type bounds
    /// - `O: RFBound` — the success element type.
    /// - `E: Display` — used only to render the error message. The
    ///   error value does not propagate through any collection, so no
    ///   `Send + Sync` bounds are required on `E` itself.
    /// - `F: Fn(&T) -> Result<O, E>` — must be `Send + Sync + 'static`
    ///   like every other transform closure.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let raw = from_vec(&p, vec!["1".to_string(), "x".into(), "3".into()]);
    /// let (parsed, errors) = raw.map_catching(|s: &String| {
    ///     s.parse::<i32>().map_err(|e| e.to_string())
    /// });
    /// assert_eq!(parsed.collect_seq()?.len(), 2);
    /// assert_eq!(errors.collect_seq()?.len(), 1);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn map_catching<O, E, F>(self, f: F) -> (PCollection<O>, PCollection<DeadLetter<T>>)
    where
        O: RFBound,
        E: Display,
        F: 'static + Send + Sync + Fn(&T) -> Result<O, E>,
    {
        let classified: PCollection<Classified<O, T>> = self.map(move |t| match f(t) {
            Ok(o) => Classified::Ok(o),
            Err(e) => Classified::Err(DeadLetter::new(t.clone(), e.to_string())),
        });

        let good = classified.filter_map(|c: &Classified<O, T>| match c {
            Classified::Ok(o) => Some(o.clone()),
            Classified::Err(_) => None,
        });
        let errors = classified.filter_map(|c: &Classified<O, T>| match c {
            Classified::Err(d) => Some(d.clone()),
            Classified::Ok(_) => None,
        });

        (good, errors)
    }

    /// Fallible 1→N transform that routes failures to a dead-letter
    /// collection.
    ///
    /// Like [`map_catching`](Self::map_catching), but the user function
    /// expands each input into zero or more outputs. A single `Err(e)`
    /// from `f` produces one [`DeadLetter`]; an `Ok(vec)` flattens every
    /// element of `vec` into the success collection.
    ///
    /// # Example
    /// ```no_run
    /// # use anyhow::Result;
    /// use ironbeam::*;
    ///
    /// # fn main() -> Result<()> {
    /// let p = Pipeline::default();
    /// let raw = from_vec(&p, vec!["1,2,3".to_string(), "bad".into(), "4,5".into()]);
    /// let (numbers, errors) = raw.flat_map_catching(|s: &String| {
    ///     s.split(',')
    ///         .map(|t| t.parse::<u32>().map_err(|e| e.to_string()))
    ///         .collect::<Result<Vec<_>, _>>()
    /// });
    /// assert_eq!(numbers.collect_seq()?.len(), 5); // 1,2,3,4,5
    /// assert_eq!(errors.collect_seq()?.len(), 1);
    /// # Ok(()) }
    /// ```
    #[must_use]
    pub fn flat_map_catching<O, E, F>(self, f: F) -> (PCollection<O>, PCollection<DeadLetter<T>>)
    where
        O: RFBound,
        E: Display,
        F: 'static + Send + Sync + Fn(&T) -> Result<Vec<O>, E>,
    {
        // Classify into Vec<Classified<O, T>>: success expands to one
        // `Ok(o)` per produced element; failure produces a single `Err`.
        let classified: PCollection<Classified<O, T>> = self.flat_map(move |t| match f(t) {
            Ok(outs) => outs.into_iter().map(Classified::Ok).collect(),
            Err(e) => vec![Classified::Err(DeadLetter::new(t.clone(), e.to_string()))],
        });

        let good = classified.filter_map(|c: &Classified<O, T>| match c {
            Classified::Ok(o) => Some(o.clone()),
            Classified::Err(_) => None,
        });
        let errors = classified.filter_map(|c: &Classified<O, T>| match c {
            Classified::Err(d) => Some(d.clone()),
            Classified::Ok(_) => None,
        });

        (good, errors)
    }
}
