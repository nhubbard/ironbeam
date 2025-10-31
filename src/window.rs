//! Event-time windowing primitives.
//!
//! This module defines two core types used by the windowing helpers:
//!
//! - [`TimestampMs`]: a millisecond-precision timestamp since the Unix epoch (UTC).
//! - [`Window`]: a **closed–open** time interval `[start, end)` with total ordering and hashing,
//!   so it can be used as a key and sorted deterministically.
//!
//! It also provides a convenience wrapper, [`Timestamped<T>`], for values that carry an
//! event-time timestamp, which integrates with helpers like `attach_timestamps` and
//! `key_by_window`.
//!
//! ## Tumbling windows
//! The function [`Window::tumble`] computes the tumbling window that contains a given
//! timestamp, parameterized by a **window size** (`size_ms`) and an **alignment offset**
//! (`offset_ms`).  Windows are aligned so that valid window starts are:
//!
//! ```text
//! offset_ms + k * size_ms, for integer k
//! ```
//!
//! For example, with `size_ms = 10` and `offset_ms = 0`, the timestamp `27` falls into
//! the window `[20, 30)`. With `offset_ms = 5`, the same timestamp falls into `[25, 35)`.
//!
//! See also the higher-level helpers in `helpers/tumbling.rs` that derive window keys
//! from `Timestamped<T>` streams.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// Milliseconds since the Unix epoch (UTC).
///
/// This alias is used throughout the API to clarify when values represent
/// event-time timestamps.
pub type TimestampMs = u64;

/// A closed–open time interval: `[start, end)`.
///
/// Windows are comparable and hashable, which makes them usable as map keys and
/// sortable in a deterministic order (by `start`, then `end`).
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq)]
pub struct Window {
    /// Inclusive window start (milliseconds since epoch).
    pub start: TimestampMs,
    /// Exclusive window end (milliseconds since epoch).
    pub end: TimestampMs,
}

impl Window {
    /// Construct a window `[start, end)`. Panics in debug builds if `end < start`.
    #[inline]
    pub fn new(start: TimestampMs, end: TimestampMs) -> Self {
        debug_assert!(end >= start);
        Self { start, end }
    }

    /// Compute the **tumbling** window for a timestamp.
    ///
    /// The returned window has length `size_ms` and is aligned so that all window starts
    /// are of the form `offset_ms + k * size_ms` for integer `k`.
    ///
    /// # Parameters
    /// - `ts`: the event-time timestamp (milliseconds since epoch)
    /// - `size_ms`: the window size in milliseconds (must be > 0)
    /// - `offset_ms`: the alignment offset in milliseconds
    ///
    /// # Returns
    /// The window `[win_start, win_start + size_ms)` that contains `ts`.
    ///
    /// # Example
    /// ```ignore
    /// use rustflow::window::{Window, TimestampMs};
    /// let w = Window::tumble(27, 10, 0);
    /// assert_eq!(w.start, 20);
    /// assert_eq!(w.end, 30);
    ///
    /// let w2 = Window::tumble(27, 10, 5);
    /// assert_eq!(w2.start, 25);
    /// assert_eq!(w2.end, 35);
    /// ```
    #[inline]
    pub fn tumble(ts: TimestampMs, size_ms: u64, offset_ms: u64) -> Self {
        debug_assert!(size_ms > 0);
        // Position relative to the offset; windows start at offset + k*size.
        let rel = ts - offset_ms;
        // For u64, floor division equals integer division.
        let k = div_floor(rel, size_ms);
        let win_start = k * size_ms + offset_ms;
        Self {
            start: win_start,
            end: win_start + size_ms,
        }
    }
}

/// Floor division helper for `u64`.
///
/// For unsigned integers this is just integer division; this function exists
/// for readability and symmetry with potential signed variants.
#[inline]
fn div_floor(a: u64, b: u64) -> u64 {
    let q = a / b;
    let r = a % b;
    if (r != 0) && ((r > 0) != (b > 0)) {
        q - 1
    } else {
        q
    }
}

// Hash/Ord so `Window` can be used as keys and sorted deterministically.
impl PartialEq for Window {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end
    }
}
impl Hash for Window {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.start.hash(state);
        self.end.hash(state);
    }
}
impl Ord for Window {
    #[inline]
    fn cmp(&self, o: &Self) -> Ordering {
        self.start.cmp(&o.start).then(self.end.cmp(&o.end))
    }
}
impl PartialOrd for Window {
    #[inline]
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

/// An event-time stamped element.
///
/// This is a lightweight carrier for values that participates in windowing transforms.
/// See `helpers/timestamped.rs` for ways to attach timestamps to existing collections.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Timestamped<T> {
    /// Event-time timestamp (milliseconds since epoch).
    pub ts: TimestampMs,
    /// The associated value.
    pub value: T,
}

impl<T> Timestamped<T> {
    /// Construct a new [`Timestamped`] value.
    ///
    /// # Example
    /// ```ignore
    /// use rustflow::window::{Timestamped, TimestampMs};
    /// let ev = Timestamped::new(1_700_000_000_000u64, "payload");
    /// assert_eq!(ev.value, "payload");
    /// ```
    #[inline]
    pub fn new(ts: TimestampMs, value: T) -> Self {
        Self { ts, value }
    }
}
