//! Offset restrictions for splittable work.

use std::sync::{Arc, Mutex};

use super::Error;

/// A half-open range of positions that can be claimed by a splittable function.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OffsetRange {
    pub start: i64,
    pub stop: i64,
}

impl OffsetRange {
    pub fn new(start: i64, stop: i64) -> Result<Self, Error> {
        if start > stop {
            return Err(Error::graph(
                "restriction",
                "offset range starts after its stop",
            ));
        }
        Ok(Self { start, stop })
    }
}

/// The current primary and the residual created by a successful split.
pub type RestrictionSplit<R> = Option<(R, R)>;

/// Claim and divide one restriction while a splittable function runs.
/// Implementations must coordinate concurrent claims and dynamic splits.
pub trait RestrictionTracker: Send + Sync {
    type Position;
    type Restriction;

    fn try_claim(&self, position: Self::Position) -> Result<bool, Error>;
    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Result<RestrictionSplit<Self::Restriction>, Error>;
    fn restriction(&self) -> Result<Self::Restriction, Error>;
    fn progress(&self) -> Result<(f64, f64), Error>;
    fn is_done(&self) -> Result<bool, Error>;
    fn check_done(&self) -> Result<(), Error>;
}

#[derive(Debug)]
struct TrackerState {
    range: OffsetRange,
    last_attempt: Option<i64>,
    last_claim: Option<i64>,
}

/// A shared tracker whose claim and split operations serialize through one lock.
/// A failed claim must end processing of the current primary restriction.
#[derive(Clone, Debug)]
pub struct OffsetRangeTracker {
    state: Arc<Mutex<TrackerState>>,
}

impl OffsetRangeTracker {
    pub fn new(range: OffsetRange) -> Result<Self, Error> {
        OffsetRange::new(range.start, range.stop)?;
        Ok(Self {
            state: Arc::new(Mutex::new(TrackerState {
                range,
                last_attempt: None,
                last_claim: None,
            })),
        })
    }

    /// Claim one position. Positions must increase strictly, even after a
    /// failed attempt. An attempt at or beyond the stop returns `false`.
    pub fn try_claim(&self, position: i64) -> Result<bool, Error> {
        let mut state = self.lock()?;
        if state
            .last_attempt
            .is_some_and(|last| last >= state.range.stop)
        {
            return Err(Error::execution(
                "restriction",
                "sdf",
                "claim attempted after the restriction ended",
            ));
        }
        if position < state.range.start {
            return Err(Error::execution(
                "restriction",
                "sdf",
                "claim precedes the restriction start",
            ));
        }
        if state.last_attempt.is_some_and(|last| position <= last) {
            return Err(Error::execution(
                "restriction",
                "sdf",
                "claim positions must increase strictly",
            ));
        }
        state.last_attempt = Some(position);
        if position >= state.range.stop {
            return Ok(false);
        }
        state.last_claim = Some(position);
        drop(state);
        Ok(true)
    }

    /// Split the remaining positions. Zero checkpoints immediately after the
    /// last claim, leaving no unclaimed positions in the primary. Returns
    /// `None` when the residual would be empty.
    // Beam specifies a floating-point fraction, so a range wider than f64's
    // exact integer span necessarily rounds the requested split point.
    #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
    pub fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Result<RestrictionSplit<OffsetRange>, Error> {
        if !fraction_of_remainder.is_finite() || !(0.0..=1.0).contains(&fraction_of_remainder) {
            return Err(Error::execution(
                "restriction",
                "sdf",
                "split fraction must be finite and between zero and one",
            ));
        }
        let mut state = self.lock()?;
        if state
            .last_attempt
            .is_some_and(|last| last >= state.range.stop)
        {
            return Ok(None);
        }
        let next = state.last_claim.map_or_else(
            || i128::from(state.range.start),
            |last| i128::from(last) + 1,
        );
        let stop = i128::from(state.range.stop);
        if next >= stop {
            return Ok(None);
        }
        let offset = if fraction_of_remainder == 0.0 {
            0
        } else {
            ((stop - next) as f64 * fraction_of_remainder).floor() as i128
        };
        let split = next + offset.max(i128::from(fraction_of_remainder > 0.0));
        if split >= stop {
            return Ok(None);
        }
        // The split lies between the range's i64 start and stop.
        let split = split as i64;
        let residual = OffsetRange {
            start: split,
            stop: state.range.stop,
        };
        state.range.stop = split;
        let primary = state.range;
        drop(state);
        Ok(Some((primary, residual)))
    }

    pub fn restriction(&self) -> Result<OffsetRange, Error> {
        Ok(self.lock()?.range)
    }

    /// Return done and remaining work in offset units.
    // Beam's progress scalars are f64, including for a range wider than 2^53.
    #[allow(clippy::cast_precision_loss)]
    pub fn progress(&self) -> Result<(f64, f64), Error> {
        let state = self.lock()?;
        let start = i128::from(state.range.start);
        let stop = i128::from(state.range.stop);
        let done_until = state
            .last_claim
            .map_or(start, |last| (i128::from(last) + 1).min(stop));
        drop(state);
        Ok(((done_until - start) as f64, (stop - done_until) as f64))
    }

    pub fn is_done(&self) -> Result<bool, Error> {
        let state = self.lock()?;
        Ok(state.range.start == state.range.stop
            || state
                .last_attempt
                .is_some_and(|last| i128::from(last) + 1 >= i128::from(state.range.stop)))
    }

    /// Reject an incomplete function return before its output can commit.
    pub fn check_done(&self) -> Result<(), Error> {
        if self.is_done()? {
            Ok(())
        } else {
            Err(Error::execution(
                "restriction",
                "sdf",
                "splittable function returned before claiming its restriction",
            ))
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, TrackerState>, Error> {
        self.state
            .lock()
            .map_err(|_| Error::execution("restriction", "sdf", "offset tracker lock poisoned"))
    }
}

impl RestrictionTracker for OffsetRangeTracker {
    type Position = i64;
    type Restriction = OffsetRange;

    fn try_claim(&self, position: i64) -> Result<bool, Error> {
        Self::try_claim(self, position)
    }

    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Result<RestrictionSplit<OffsetRange>, Error> {
        Self::try_split(self, fraction_of_remainder)
    }

    fn restriction(&self) -> Result<OffsetRange, Error> {
        Self::restriction(self)
    }

    fn progress(&self) -> Result<(f64, f64), Error> {
        Self::progress(self)
    }

    fn is_done(&self) -> Result<bool, Error> {
        Self::is_done(self)
    }

    fn check_done(&self) -> Result<(), Error> {
        Self::check_done(self)
    }
}
