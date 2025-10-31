//! Tumbling window helpers for timestamped and keyed streams.
//!
//! This module provides convenience transforms to key and group elements into
//! fixed-size, non-overlapping windows (aka *tumbling* windows). It works both
//! for unkeyed `Timestamped<T>` streams and keyed `(K, Timestamped<V>)` streams.
//!
//! ## Overview
//! - **`key_by_window(size_ms, offset_ms)`**: attaches a [`Window`] (computed
//!   from each element's timestamp) to the element/key.
//! - **`group_by_window(size_ms, offset_ms)`**: shorthand for
//!   `key_by_window(...).group_by_key()` for unkeyed streams.
//! - **`group_by_key_and_window(size_ms, offset_ms)`**: keyed variant that
//!   groups by `(K, Window)`.
//!
//! Windows are computed with [`Window::tumble`] using:
//! - `size_ms`: the length of each window in milliseconds.
//! - `offset_ms`: an optional phase offset to shift window boundaries.
//!
//! ## Examples
//! ```ignore
//! use rustflow::*;
//!
//! let p = Pipeline::default();
//!
//! // Unkeyed: Timestamped<String> -> (Window, String) -> (Window, Vec<String>)
//! let events = from_vec(&p, vec![
//!     Timestamped::new(1_000, "a".to_string()),
//!     Timestamped::new(9_000, "b".to_string()),
//!     Timestamped::new(11_000, "c".to_string()),
//! ]);
//!
//! let grouped = events
//!     .group_by_window(10_000, 0); // [0,10_000), [10_000,20_000), ...
//!
//! // Keyed: (K, Timestamped<V>) -> ((K, Window), V) -> grouped
//! let keyed = from_vec(&p, vec![
//!     ("k1".to_string(), Timestamped::new(1_000,  1u32)),
//!     ("k1".to_string(), Timestamped::new(9_000,  2u32)),
//!     ("k2".to_string(), Timestamped::new(11_000, 3u32)),
//! ]);
//!
//! let per_key = keyed
//!     .group_by_key_and_window(10_000, 0); // ((K,Window), Vec<u32>)
//!
//! // Execute (examples only; ignore results).
//! let _ = grouped.collect_seq()?;
//! let _ = per_key.collect_seq()?;
//! # anyhow::Result::<()>::Ok(())
//! ```

use crate::{PCollection, RFBound, Timestamped, Window};
use std::hash::Hash;

impl<T: RFBound> PCollection<Timestamped<T>> {
    /// Attach a tumbling window key computed from each element's timestamp.
    ///
    /// For each `Timestamped<T>` input element, computes `Window::tumble(ts, size_ms, offset_ms)`
    /// and emits `(Window, T)`. Useful for later aggregations per window.
    ///
    /// - `size_ms`: window size (must be > 0 in meaningful pipelines).
    /// - `offset_ms`: phase offset to shift window boundaries (commonly `0`).
    ///
    /// ### Example
    /// ```ignore
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_500, "e1".to_string()),
    ///     Timestamped::new(9_900, "e2".to_string()),
    /// ]);
    ///
    /// // Key by 10s windows
    /// let keyed = events.key_by_window(10_000, 0); // (Window, String)
    /// let _ = keyed.collect_seq()?;
    /// # anyhow::Result::<()>::Ok(())
    /// ```
    pub fn key_by_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, T)> {
        self.map(move |ev: &Timestamped<T>| {
            let w = Window::tumble(ev.ts, size_ms, offset_ms);
            (w, ev.value.clone())
        })
    }

    /// Group `Timestamped<T>` by their tumbling windows.
    ///
    /// This is shorthand for:
    /// ```ignore
    /// self.key_by_window(size_ms, offset_ms).group_by_key()
    /// ```
    ///
    /// ### Example
    /// ```ignore
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_000,  "a".to_string()),
    ///     Timestamped::new(9_000,  "b".to_string()),
    ///     Timestamped::new(11_000, "c".to_string()),
    /// ]);
    ///
    /// let grouped = events.group_by_window(10_000, 0); // (Window, Vec<String>)
    /// let _ = grouped.collect_seq()?;
    /// # anyhow::Result::<()>::Ok(())
    /// ```
    pub fn group_by_window(self, size_ms: u64, offset_ms: u64) -> PCollection<(Window, Vec<T>)> {
        self.key_by_window(size_ms, offset_ms).group_by_key()
    }
}

// -------------- Tumbling windows: keyed --------------
// Input: (K, Timestamped<V>) → output: ((K, Window), V)
impl<K: RFBound + Eq + Hash, V: RFBound> PCollection<(K, Timestamped<V>)> {
    /// Attach a `(K, Window)` key to each `(K, Timestamped<V>)` element.
    ///
    /// Computes the tumbling window from the element's timestamp, returning
    /// `((K, Window), V)` so you can aggregate by the key *and* window.
    ///
    /// ### Example
    /// ```ignore
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let keyed = from_vec(&p, vec![
    ///     ("k1".to_string(), Timestamped::new(1_000, 1u32)),
    ///     ("k1".to_string(), Timestamped::new(9_000, 2u32)),
    /// ]);
    ///
    /// let kw = keyed.key_by_window(10_000, 0); // ((K, Window), V)
    /// let _ = kw.collect_seq()?;
    /// # anyhow::Result::<()>::Ok(())
    /// ```
    pub fn key_by_window(self, size_ms: u64, offset_ms: u64) -> PCollection<((K, Window), V)> {
        self.map(move |kv: &(K, Timestamped<V>)| {
            let w = Window::tumble(kv.1.ts, size_ms, offset_ms);
            ((kv.0.clone(), w), kv.1.value.clone())
        })
    }

    /// Group by `(K, Window)` to get `((K, Window), Vec<V>)`.
    ///
    /// Shorthand for:
    /// ```ignore
    /// self.key_by_window(size_ms, offset_ms).group_by_key()
    /// ```
    ///
    /// ### Example
    /// ```ignore
    /// use rustflow::*;
    ///
    /// let p = Pipeline::default();
    /// let keyed = from_vec(&p, vec![
    ///     ("k1".to_string(), Timestamped::new(1_000,  1u32)),
    ///     ("k1".to_string(), Timestamped::new(9_000,  2u32)),
    ///     ("k2".to_string(), Timestamped::new(11_000, 3u32)),
    /// ]);
    ///
    /// let grouped = keyed.group_by_key_and_window(10_000, 0); // ((K, Window), Vec<u32>)
    /// let _ = grouped.collect_seq()?;
    /// # anyhow::Result::<()>::Ok(())
    /// ```
    pub fn group_by_key_and_window(
        self,
        size_ms: u64,
        offset_ms: u64,
    ) -> PCollection<((K, Window), Vec<V>)> {
        self.key_by_window(size_ms, offset_ms).group_by_key()
    }
}
