use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// Milliseconds since UNIX epoch (UTC).
pub type TimestampMs = i64;

/// A closed-open time range: [start, end).
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq)]
pub struct Window {
    pub start: TimestampMs,
    pub end: TimestampMs,
}

impl Window {
    #[inline]
    pub fn new(start: TimestampMs, end: TimestampMs) -> Self {
        debug_assert!(end >= start);
        Self { start, end }
    }

    /// Compute the tumbling window [win_start, win_start + size) for a timestamp.
    /// `size_ms` > 0; `offset_ms` may be negative or positive.
    #[inline]
    pub fn tumble(ts: TimestampMs, size_ms: i64, offset_ms: i64) -> Self {
        debug_assert!(size_ms > 0);
        // normalized position relative to offset
        let rel = ts - offset_ms;
        // floor division that handles negative timestamps correctly
        let k = div_floor(rel, size_ms);
        let win_start = k * size_ms + offset_ms;
        Self { start: win_start, end: win_start + size_ms }
    }
}

/// Floor division for i64 (unlike `/` which truncates toward zero).
#[inline]
fn div_floor(a: i64, b: i64) -> i64 {
    let q = a / b;
    let r = a % b;
    if (r != 0) && ((r > 0) != (b > 0)) { q - 1 } else { q }
}

// Hash/Ord so Windows can be used as keys and sorted deterministically.
impl PartialEq for Window {
    #[inline] fn eq(&self, other: &Self) -> bool { self.start == other.start && self.end == other.end }
}
impl Hash for Window {
    #[inline] fn hash<H: Hasher>(&self, state: &mut H) { self.start.hash(state); self.end.hash(state); }
}
impl Ord for Window {
    #[inline] fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.start.cmp(&o.start).then(self.end.cmp(&o.end))
    }
}
impl PartialOrd for Window {
    #[inline] fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(o)) }
}

/// A timestamped element (event-time semantics).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Timestamped<T> {
    pub ts: TimestampMs,
    pub value: T,
}

impl<T> Timestamped<T> {
    #[inline] pub fn new(ts: TimestampMs, value: T) -> Self { Self { ts, value } }
}