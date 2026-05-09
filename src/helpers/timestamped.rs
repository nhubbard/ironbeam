//! Event-time helpers: attaching timestamps and normalizing timestamped inputs.
//!
//! This module provides lightweight utilities for working in **event time**.
//! It lets you attach a millisecond timestamp to each element, normalize an
//! existing `(timestamp, value)` stream into the internal [`Timestamped<T>`]
//! representation used by windowing transforms, or project timestamps back out
//! into the data stream as explicit tuple fields.
//!
//! ## Available operations
//! - [`PCollection::attach_timestamps`](PCollection::attach_timestamps) - Attach event timestamps using a function
//! - [`PCollection::to_timestamped`](crate::PCollection::to_timestamped) - Normalize `(timestamp, value)` pairs into `Timestamped<T>`
//! - [`PCollection::reify_timestamps`](crate::PCollection::reify_timestamps) - Make timestamps explicit as `(TimestampMs, T)` tuples
//!
//! ### What this is (and isn't)
//! - ✅ Attaches/normalizes event timestamps, preserving data and order within a partition
//! - ✅ Plays nicely with tumbling window helpers (e.g., `key_by_window(...)`)
//! - ✅ Reifies timestamps as explicit data fields (inverse of `to_timestamped`)
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
//!
//! // Reify timestamps back out as explicit tuple fields
//! let tuples = stamped2.reify_timestamps();
//! // tuples: PCollection<(TimestampMs, String)>
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

impl<T: RFBound> PCollection<Timestamped<T>> {
    /// Make event-time timestamps explicit in the data stream.
    ///
    /// Projects each [`Timestamped<T>`] element into a `(timestamp_ms, value)` tuple,
    /// dropping the wrapper type. This is the inverse of
    /// [`PCollection::to_timestamped`].
    ///
    /// Primarily useful as a debugging aid: it lets you inspect timestamps as
    /// ordinary data without needing to reach inside the [`Timestamped`] wrapper
    /// downstream.
    ///
    /// ### Returns
    /// A `PCollection<(TimestampMs, T)>` containing one tuple per input element,
    /// where the first field is the event-time timestamp in milliseconds and the
    /// second is the original value.
    ///
    /// ### Example
    /// ```no_run
    /// use ironbeam::*;
    /// use ironbeam::window::Timestamped;
    ///
    /// let p = Pipeline::default();
    /// let events = from_vec(&p, vec![
    ///     Timestamped::new(1_000u64, "hello".to_string()),
    ///     Timestamped::new(2_000u64, "world".to_string()),
    /// ]);
    ///
    /// let tuples = events.reify_timestamps();
    /// // tuples: PCollection<(TimestampMs, String)>
    /// ```
    #[must_use]
    pub fn reify_timestamps(self) -> PCollection<(TimestampMs, T)> {
        self.map(|ts: &Timestamped<T>| (ts.ts, ts.value.clone()))
    }
}
