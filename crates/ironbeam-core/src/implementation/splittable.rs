//! Processing contract for a finite offset restriction.

use std::sync::{Arc, Mutex};

use super::{Error, OffsetRangeTracker, Timestamped};

/// A shared, monotonic output watermark for one active restriction.
#[derive(Clone, Debug)]
pub struct OffsetWatermarkEstimator {
    value: Arc<Mutex<Option<i64>>>,
}

impl OffsetWatermarkEstimator {
    /// Begin with the watermark restored for this restriction, if any.
    #[must_use]
    pub fn new(initial_ns: Option<i64>) -> Self {
        Self {
            value: Arc::new(Mutex::new(initial_ns)),
        }
    }

    /// Advance the watermark. A regression is an execution error.
    pub fn update(&self, watermark_ns: i64) -> Result<(), Error> {
        let mut current = self.value.lock().map_err(|_| {
            Error::execution("restriction", "sdf", "watermark estimator lock poisoned")
        })?;
        if current.is_some_and(|previous| watermark_ns < previous) {
            return Err(Error::execution(
                "restriction",
                "sdf",
                "output watermark regressed",
            ));
        }
        *current = Some(watermark_ns);
        drop(current);
        Ok(())
    }

    /// Read the watermark for a runner split or checkpoint.
    pub fn current(&self) -> Result<Option<i64>, Error> {
        Ok(*self.value.lock().map_err(|_| {
            Error::execution("restriction", "sdf", "watermark estimator lock poisoned")
        })?)
    }
}

/// Claims offset work and collects timestamped output for one restriction.
///
/// A runner may clone `tracker()` and `estimator()` before invoking the user
/// callback, allowing a concurrent split to see the same progress state.
pub struct OffsetProcessContext<U> {
    tracker: OffsetRangeTracker,
    estimator: OffsetWatermarkEstimator,
    claimed: bool,
    outputs: Vec<Timestamped<U>>,
}

impl<U> OffsetProcessContext<U> {
    /// Create a context for an already validated tracker and watermark.
    #[must_use]
    pub const fn new(tracker: OffsetRangeTracker, estimator: OffsetWatermarkEstimator) -> Self {
        Self {
            tracker,
            estimator,
            claimed: false,
            outputs: Vec::new(),
        }
    }

    /// Claim a position before processing or emitting its output.
    /// A failed claim ends output from the current primary restriction.
    pub fn try_claim(&mut self, position: i64) -> Result<bool, Error> {
        self.claimed = false;
        let accepted = self.tracker.try_claim(position)?;
        self.claimed = accepted;
        Ok(accepted)
    }

    /// Emit a value with its event timestamp after a successful claim.
    pub fn emit(&mut self, value: U, timestamp_ns: i64) -> Result<(), Error> {
        if !self.claimed {
            return Err(Error::execution(
                "restriction",
                "sdf",
                "splittable output requires a successful claim",
            ));
        }
        self.outputs.push(Timestamped {
            value,
            timestamp_ns,
        });
        Ok(())
    }

    /// Advance the user-defined output watermark.
    pub fn update_watermark(&self, watermark_ns: i64) -> Result<(), Error> {
        self.estimator.update(watermark_ns)
    }

    /// Share the tracker with a runner handling a concurrent split request.
    #[must_use]
    pub fn tracker(&self) -> OffsetRangeTracker {
        self.tracker.clone()
    }

    /// Share the estimator with a runner handling a concurrent split request.
    #[must_use]
    pub fn estimator(&self) -> OffsetWatermarkEstimator {
        self.estimator.clone()
    }

    /// Finish one primary restriction after every position has been claimed.
    /// Unfinished work must be split into a residual before this succeeds.
    pub fn finish(self) -> Result<Vec<Timestamped<U>>, Error> {
        self.tracker.check_done()?;
        Ok(self.outputs)
    }
}
