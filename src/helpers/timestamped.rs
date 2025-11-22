//! Event-time helpers: attaching timestamps and normalizing timestamped inputs.
//!
//! This module provides lightweight utilities for working in **event time**.
//! It lets you attach a millisecond timestamp to each element or normalize an
//! existing `(timestamp, value)` stream into the internal [`Timestamped<T>`]
//! representation used by windowing transforms.
//!
//! ## Available operations
//! - [`PCollection::attach_timestamps`](crate::PCollection::attach_timestamps) - Attach event timestamps using a function
//! - [`PCollection::to_timestamped`](crate::PCollection::to_timestamped) - Normalize `(timestamp, value)` pairs
//!
//! ### What this is (and isn't)
//! - ✅ Attaches/normalizes event timestamps, preserving data and order within a partition
//! - ✅ Plays nicely with tumbling window helpers (e.g., `key_by_window(...)`)
//! - ❌ Not a full watermark/late data engine -- timestamps are metadata used by
//!   later operators; there's no lateness tracking or triggers.
//!
//! ### Quick start
//! ```no_run
//! use ironbeam::*;
//!
//! let p = Pipeline::default();
//!
//! // From plain values: attach event-time from a field/computed fn
//! #[derive(Clone)]
//! struct Rec { ts: u64, v: String }
//!
//! let events = from_vec(&p, vec![
//!     Rec { ts: 1_000, v: "a".into() },
//!     Rec { ts: 1_250, v: "b".into() },
//! ]);
//!
//! let stamped = events.attach_timestamps(|r| r.ts);
//! // stamped: PCollection<Timestamped<Rec>>
//!
//! // From (ts, value) pairs: normalize to Timestamped<T>
//! let pairs = from_vec(&p, vec![ (1000_u64, "x".to_string()), (1250_u64, "y".to_string()) ]);
//! let stamped2 = pairs.to_timestamped();
//! ```

use crate::{PCollection, RFBound, TimestampMs, Timestamped};

impl<T: RFBound> PCollection<T> {
    /// Attach event timestamps using a user-provided function.
    ///
    /// This converts a `PCollection<T>` into a `PCollection<Timestamped<T>>` by
    /// calling `ts_fn` on each element to get an event-time timestamp (in
    /// milliseconds). The element is carried along unchanged inside
    /// [`Timestamped<T>`].
    ///
    /// Use this when your data doesn't already come as `(timestamp, value)` pairs.
    ///
    /// ### Arguments
    /// - `ts_fn` -- A pure function that returns the event timestamp for an element.
    ///
    /// ### Returns
    /// A timestamped stream: `PCollection<Timestamped<T>>`.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// #[derive(Clone)]
    /// struct LogLine { ts_ms: u64, msg: String }
    ///
    /// let p = Pipeline::default();
    /// let logs = from_vec(&p, vec![
    ///     LogLine { ts_ms: 10, msg: "a".into() },
    ///     LogLine { ts_ms: 21, msg: "b".into() },
    /// ]);
    ///
    /// let stamped = logs.attach_timestamps(|l| l.ts_ms);
    /// // stamped: PCollection<Timestamped<LogLine>>
    /// ```
    pub fn attach_timestamps<F>(self, ts_fn: F) -> PCollection<Timestamped<T>>
    where
        F: 'static + Send + Sync + Fn(&T) -> TimestampMs,
    {
        self.map(move |t| Timestamped::new(ts_fn(t), t.clone()))
    }
}

impl<T: RFBound> PCollection<(TimestampMs, T)> {
    /// Normalize a `(timestamp, value)` stream into `Timestamped<T>`.
    ///
    /// This is a convenience adapter when your upstream already produces
    /// `(ts_ms, value)` tuples. Converting to [`Timestamped<T>`] ensures
    /// downstream windowing transforms receive a consistent representation.
    ///
    /// ### Returns
    /// A timestamped stream: `PCollection<Timestamped<T>>`.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    ///
    /// let p = Pipeline::default();
    /// let pairs = from_vec(&p, vec![(100_u64, "x".to_string()), (180_u64, "y".to_string())]);
    ///
    /// let stamped = pairs.to_timestamped();
    /// // stamped: PCollection<Timestamped<String>>
    /// ```
    #[must_use]
    pub fn to_timestamped(self) -> PCollection<Timestamped<T>> {
        self.map(|p| Timestamped::new(p.0, p.1.clone()))
    }
}
