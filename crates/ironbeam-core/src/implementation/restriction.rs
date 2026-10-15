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

    /// Divide a finite restriction into consecutive ranges of at most
    /// `max_positions` offsets. The iterator creates each range on demand, so
    /// even the full signed 64-bit span does not allocate a range list.
    pub fn chunks(self, max_positions: u64) -> Result<OffsetRangeChunks, Error> {
        Self::new(self.start, self.stop)?;
        if max_positions == 0 {
            return Err(Error::graph(
                "restriction",
                "offset chunk size must be positive",
            ));
        }
        Ok(OffsetRangeChunks {
            next: self.start,
            stop: self.stop,
            max_positions,
        })
    }

    /// Encode this range as Beam Java's `OffsetRange.Coder`: two signed
    /// 64-bit `VarInt` values, start followed by stop.
    pub fn encode_beam(self) -> Result<Vec<u8>, Error> {
        Self::new(self.start, self.stop)?;
        let mut bytes = Vec::with_capacity(20);
        encode_varint(self.start.cast_unsigned(), &mut bytes);
        encode_varint(self.stop.cast_unsigned(), &mut bytes);
        Ok(bytes)
    }

    /// Decode one complete Beam `OffsetRange.Coder` body.
    pub fn decode_beam(mut bytes: &[u8]) -> Result<Self, Error> {
        let start = decode_varint(&mut bytes)?.cast_signed();
        let stop = decode_varint(&mut bytes)?.cast_signed();
        if !bytes.is_empty() {
            return Err(Error::execution(
                "restriction",
                "coder",
                "offset range has trailing bytes",
            ));
        }
        Self::new(start, stop)
    }
}

/// Lazy initial restrictions for parallel offset processing.
#[derive(Clone, Debug)]
pub struct OffsetRangeChunks {
    next: i64,
    stop: i64,
    max_positions: u64,
}

impl Iterator for OffsetRangeChunks {
    type Item = OffsetRange;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.stop {
            return None;
        }
        let start = self.next;
        let stop = (i128::from(start) + i128::from(self.max_positions)).min(i128::from(self.stop));
        self.next = i64::try_from(stop).expect("chunk boundary lies inside the offset range");
        Some(OffsetRange {
            start,
            stop: self.next,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = u128::try_from(i128::from(self.stop) - i128::from(self.next))
            .expect("valid offset range has nonnegative length");
        let chunk_size = u128::from(self.max_positions);
        let chunks = remaining.div_ceil(chunk_size);
        let lower = usize::try_from(chunks).unwrap_or(usize::MAX);
        (lower, usize::try_from(chunks).ok())
    }
}

fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).expect("seven bits fit in u8") | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).expect("final VarInt byte fits in u8"));
}

fn decode_varint(input: &mut &[u8]) -> Result<u64, Error> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let Some((&byte, rest)) = input.split_first() else {
            return Err(Error::execution(
                "restriction",
                "coder",
                "truncated offset VarInt",
            ));
        };
        if shift == 63 && byte > 1 {
            return Err(Error::execution(
                "restriction",
                "coder",
                "offset VarInt exceeds 64 bits",
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        *input = rest;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Error::execution(
        "restriction",
        "coder",
        "offset VarInt is too long",
    ))
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
        // Beam's OffsetRangeTracker measures the fraction from the last
        // attempted offset, or one before the start if no claim was made.
        let current = state
            .last_attempt
            .map_or_else(|| i128::from(state.range.start) - 1, i128::from);
        let stop = i128::from(state.range.stop);
        if current >= stop {
            return Ok(None);
        }
        let offset = ((stop - current) as f64 * fraction_of_remainder)
            .max(1.0)
            .floor() as i128;
        let split = current + offset;
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

    /// Return Beam's done and remaining work estimates in offset units. The
    /// last attempted position counts as the progress boundary; claiming
    /// position zero in `[0, 10)` therefore reports `(0, 10)`.
    // Beam's progress scalars are f64, including for a range wider than 2^53.
    #[allow(clippy::cast_precision_loss)]
    pub fn progress(&self) -> Result<(f64, f64), Error> {
        let state = self.lock()?;
        let start = i128::from(state.range.start);
        let stop = i128::from(state.range.stop);
        let done_until = state
            .last_attempt
            .map_or(start, |last| i128::from(last).min(stop));
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
