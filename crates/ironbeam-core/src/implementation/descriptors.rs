/// Major and minor version for a graph, coder, handler, or checkpoint contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Version {
    pub major: u16,
    pub minor: u16,
}

/// Whether a source has a finite input snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Boundedness {
    Bounded,
    Unbounded,
}

/// Window assignment declared for a collection's event time.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Window {
    Global,
    Fixed { size_ns: i64 },
    FixedOffset { size_ns: i64, offset_ns: i64 },
    Sliding { size_ns: i64, period_ns: i64, offset_ns: i64 },
    Session { gap_ns: i64 },
}

/// Firing policy for a windowed collection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum WindowTrigger {
    #[default]
    Default,
    /// Repeated early firings after at least `count` elements arrive.
    EarlyElementCount { count: u64 },
    /// Repeated early firings that retain prior values in subsequent panes.
    EarlyElementCountAccumulating { count: u64 },
    /// Repeat count panes until `final_count` total elements end early firing.
    EarlyCountOrFinally { count: u64, final_count: u64 },
    /// Count-or-finally panes that retain earlier values.
    EarlyCountOrFinallyAccumulating { count: u64, final_count: u64 },
    /// Repeated early firings after a processing-time delay from new input.
    EarlyProcessingTime { delay_ns: i64 },
    /// Repeated processing-time firings that retain earlier values.
    EarlyProcessingTimeAccumulating { delay_ns: i64 },
    /// Repeated early firings when either a count or processing-time delay is reached.
    EarlyCountOrProcessingTime { count: u64, delay_ns: i64 },
    /// Count-or-processing-time firings that retain earlier values.
    EarlyCountOrProcessingTimeAccumulating { count: u64, delay_ns: i64 },
    /// Count-or-time early panes and count-based late panes after window end.
    EarlyCountOrProcessingTimeAndLateCount { count: u64, delay_ns: i64, late_count: u64 },
    /// Count-or-time early and count-based late panes retain prior values.
    EarlyCountOrProcessingTimeAndLateCountAccumulating { count: u64, delay_ns: i64, late_count: u64 },
    /// Fire early after both count and processing time, then on a late count.
    EarlyCountAndProcessingTimeAndLateCount { count: u64, delay_ns: i64, late_count: u64 },
    /// Retain values across count-and-time early and count-based late panes.
    EarlyCountAndProcessingTimeAndLateCountAccumulating { count: u64, delay_ns: i64, late_count: u64 },
    /// Repeated early firings after both a count and processing-time delay.
    EarlyCountAndProcessingTime { count: u64, delay_ns: i64 },
    /// Count-and-processing-time firings that retain earlier values.
    EarlyCountAndProcessingTimeAccumulating { count: u64, delay_ns: i64 },
    /// Repeated count pane followed by a processing-time pane for later input.
    EarlyCountThenProcessingTime { count: u64, delay_ns: i64 },
    /// Sequential count and processing-time panes that retain earlier values.
    EarlyCountThenProcessingTimeAccumulating { count: u64, delay_ns: i64 },
    /// Repeated late firings after `count` new elements; panes discard prior values.
    LateElementCount { count: u64 },
    /// Repeated late firings after `count` new elements; panes retain prior values.
    LateElementCountAccumulating { count: u64 },
    /// Repeated count firings before and after the window-end watermark.
    EarlyAndLateElementCount { early_count: u64, late_count: u64 },
    /// Repeated early and late count firings that retain prior values.
    EarlyAndLateElementCountAccumulating { early_count: u64, late_count: u64 },
}

impl WindowTrigger {
    /// Return whether the policy emits only the default on-time pane.
    #[must_use]
    pub const fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }

    /// Return the early element-count threshold, if the policy has one.
    #[must_use]
    pub const fn early_count(&self) -> Option<u64> {
        match self {
            Self::Default | Self::LateElementCount { .. } | Self::LateElementCountAccumulating { .. }
            | Self::EarlyProcessingTime { .. } | Self::EarlyProcessingTimeAccumulating { .. } => None,
            Self::EarlyElementCount { count } | Self::EarlyElementCountAccumulating { count }
            | Self::EarlyCountOrFinally { count, .. }
            | Self::EarlyCountOrFinallyAccumulating { count, .. }
            | Self::EarlyCountOrProcessingTime { count, .. }
            | Self::EarlyCountOrProcessingTimeAccumulating { count, .. }
            | Self::EarlyCountOrProcessingTimeAndLateCount { count, .. }
            | Self::EarlyCountOrProcessingTimeAndLateCountAccumulating { count, .. }
            | Self::EarlyCountAndProcessingTimeAndLateCount { count, .. }
            | Self::EarlyCountAndProcessingTimeAndLateCountAccumulating { count, .. }
            | Self::EarlyCountAndProcessingTime { count, .. }
            | Self::EarlyCountAndProcessingTimeAccumulating { count, .. }
            | Self::EarlyCountThenProcessingTime { count, .. }
            | Self::EarlyCountThenProcessingTimeAccumulating { count, .. } => {
                Some(*count)
            }
            Self::EarlyAndLateElementCount { early_count, .. }
            | Self::EarlyAndLateElementCountAccumulating { early_count, .. } => Some(*early_count),
        }
    }

    /// Return the cumulative count that stops early firings, if configured.
    #[must_use]
    pub const fn final_count(&self) -> Option<u64> {
        match self {
            Self::EarlyCountOrFinally { final_count, .. }
            | Self::EarlyCountOrFinallyAccumulating { final_count, .. } => Some(*final_count),
            _ => None,
        }
    }

    /// Return the early processing-time delay in nanoseconds, if configured.
    #[must_use]
    pub const fn early_processing_time(&self) -> Option<i64> {
        match self {
            Self::EarlyProcessingTime { delay_ns }
            | Self::EarlyProcessingTimeAccumulating { delay_ns }
            | Self::EarlyCountOrProcessingTime { delay_ns, .. }
            | Self::EarlyCountOrProcessingTimeAccumulating { delay_ns, .. }
            | Self::EarlyCountOrProcessingTimeAndLateCount { delay_ns, .. }
            | Self::EarlyCountOrProcessingTimeAndLateCountAccumulating { delay_ns, .. }
            | Self::EarlyCountAndProcessingTimeAndLateCount { delay_ns, .. }
            | Self::EarlyCountAndProcessingTimeAndLateCountAccumulating { delay_ns, .. }
            | Self::EarlyCountAndProcessingTime { delay_ns, .. }
            | Self::EarlyCountAndProcessingTimeAccumulating { delay_ns, .. }
            | Self::EarlyCountThenProcessingTime { delay_ns, .. }
            | Self::EarlyCountThenProcessingTimeAccumulating { delay_ns, .. } => Some(*delay_ns),
            _ => None,
        }
    }

    /// Return whether count and processing time must both become ready.
    #[must_use]
    pub const fn is_count_and_processing_time(&self) -> bool {
        matches!(
            self,
            Self::EarlyCountAndProcessingTime { .. }
                | Self::EarlyCountAndProcessingTimeAccumulating { .. }
                | Self::EarlyCountAndProcessingTimeAndLateCount { .. }
                | Self::EarlyCountAndProcessingTimeAndLateCountAccumulating { .. }
        )
    }

    /// Return whether the trigger runs count before processing time.
    #[must_use]
    pub const fn is_count_then_processing_time(&self) -> bool {
        matches!(
            self,
            Self::EarlyCountThenProcessingTime { .. }
                | Self::EarlyCountThenProcessingTimeAccumulating { .. }
        )
    }

    /// Return the late element-count threshold, if configured.
    #[must_use]
    pub const fn late_count(&self) -> Option<u64> {
        match self {
            Self::LateElementCount { count } | Self::LateElementCountAccumulating { count } => Some(*count),
            Self::EarlyAndLateElementCount { late_count, .. }
            | Self::EarlyAndLateElementCountAccumulating { late_count, .. }
            | Self::EarlyCountOrProcessingTimeAndLateCount { late_count, .. }
            | Self::EarlyCountOrProcessingTimeAndLateCountAccumulating { late_count, .. }
            | Self::EarlyCountAndProcessingTimeAndLateCount { late_count, .. }
            | Self::EarlyCountAndProcessingTimeAndLateCountAccumulating { late_count, .. } => Some(*late_count),
            _ => None,
        }
    }

    /// Return whether successive panes retain previously emitted values.
    #[must_use]
    pub const fn is_accumulating(&self) -> bool {
        matches!(
            self,
            Self::EarlyElementCountAccumulating { .. }
                | Self::EarlyCountOrFinallyAccumulating { .. }
                | Self::EarlyProcessingTimeAccumulating { .. }
                | Self::EarlyCountOrProcessingTimeAccumulating { .. }
                | Self::EarlyCountOrProcessingTimeAndLateCountAccumulating { .. }
                | Self::EarlyCountAndProcessingTimeAndLateCountAccumulating { .. }
                | Self::EarlyCountAndProcessingTimeAccumulating { .. }
                | Self::EarlyCountThenProcessingTimeAccumulating { .. }
                | Self::LateElementCountAccumulating { .. }
                | Self::EarlyAndLateElementCountAccumulating { .. }
        )
    }
}

/// Timestamp assigned to a grouped window's output panes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum WindowOutputTime {
    #[default]
    EarliestInPane,
    EndOfWindow,
    LatestInPane,
}

impl WindowOutputTime {
    /// Return whether output uses the earliest timestamp in its pane.
    #[must_use]
    pub const fn is_default(&self) -> bool {
        matches!(self, Self::EarliestInPane)
    }
}

/// Whether a window fires an on-time pane after early panes consumed its values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum WindowOnTimeBehavior {
    #[default]
    FireIfNonempty,
    FireAlways,
}

impl WindowOnTimeBehavior {
    /// Return whether this is Ironbeam's default on-time policy.
    #[must_use]
    pub const fn is_default(&self) -> bool {
        matches!(self, Self::FireIfNonempty)
    }
}

/// Whether expiration fires a final pane without new values since the last firing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum WindowClosingBehavior {
    #[default]
    EmitIfNonempty,
    EmitAlways,
}

impl WindowClosingBehavior {
    /// Return whether this is Ironbeam's default closing policy.
    #[must_use]
    pub const fn is_default(&self) -> bool {
        matches!(self, Self::EmitIfNonempty)
    }
}

/// Half-open event-time interval `[start_ns, end_ns)`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct WindowInterval {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Timing assigned by a runner to one firing of a window.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PaneTiming {
    Early,
    OnTime,
    Late,
    Unknown,
}

/// Metadata for one window firing. Early firings have no non-speculative index.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PaneInfo {
    pub is_first: bool,
    pub is_last: bool,
    pub timing: PaneTiming,
    pub index: i64,
    pub nonspeculative_index: i64,
    pub contains_element_metadata: bool,
}

impl PaneInfo {
    pub const NO_FIRING: Self = Self {
        is_first: true,
        is_last: true,
        timing: PaneTiming::Unknown,
        index: 0,
        nonspeculative_index: 0,
        contains_element_metadata: false,
    };

    pub const FIRST_LAST_ON_TIME: Self = Self {
        is_first: true,
        is_last: true,
        timing: PaneTiming::OnTime,
        index: 0,
        nonspeculative_index: 0,
        contains_element_metadata: false,
    };

    fn validate(self) -> Result<(), Error> {
        if self.index < 0
            || self.nonspeculative_index < -1
            || (self.timing == PaneTiming::Early && self.nonspeculative_index != -1)
            || (self.timing != PaneTiming::Early && self.nonspeculative_index < 0)
            || (self.is_first
                && (self.index != 0
                    || self.nonspeculative_index
                        != if self.timing == PaneTiming::Early {
                            -1
                        } else {
                            0
                        }))
            || (self.timing == PaneTiming::Unknown
                && (self.index != 0 || self.nonspeculative_index != 0))
        {
            return Err(Error::incompatible("invalid pane indices for timing"));
        }
        Ok(())
    }
}

impl Element for PaneInfo {
    const TYPE_ID: &'static str = "ironbeam/pane-info";
    const CODER_ID: &'static str = "ironbeam/pane-info-v1";

    fn supports_coder() -> bool {
        true
    }

    fn fixed_encoded_len() -> Option<usize> {
        Some(18)
    }

    fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let flags = u8::from(self.is_first)
            | (u8::from(self.is_last) << 1)
            | (u8::from(self.contains_element_metadata) << 2);
        let timing = match self.timing {
            PaneTiming::Early => 0,
            PaneTiming::OnTime => 1,
            PaneTiming::Late => 2,
            PaneTiming::Unknown => 3,
        };
        let mut bytes = Vec::with_capacity(18);
        bytes.extend_from_slice(&[flags, timing]);
        bytes.extend_from_slice(&self.index.to_le_bytes());
        bytes.extend_from_slice(&self.nonspeculative_index.to_le_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let [flags, timing, index @ ..] = bytes else {
            return Err(Error::incompatible("pane info requires 18 bytes"));
        };
        if index.len() != 16 || flags & !0b111 != 0 {
            return Err(Error::incompatible("invalid pane info bytes"));
        }
        let timing = match timing {
            0 => PaneTiming::Early,
            1 => PaneTiming::OnTime,
            2 => PaneTiming::Late,
            3 => PaneTiming::Unknown,
            _ => return Err(Error::incompatible("invalid pane timing")),
        };
        let pane = Self {
            is_first: flags & 1 != 0,
            is_last: flags & 2 != 0,
            contains_element_metadata: flags & 4 != 0,
            timing,
            index: i64::from_le_bytes(index[..8].try_into().expect("eight bytes")),
            nonspeculative_index: i64::from_le_bytes(
                index[8..].try_into().expect("eight bytes"),
            ),
        };
        pane.validate()?;
        Ok(pane)
    }
}

impl Window {
    pub(crate) const fn supports_output_time_policy(&self) -> bool {
        matches!(self, Self::Global) || self.is_millisecond_aligned_finite()
    }

    pub(crate) const fn is_millisecond_aligned_finite(&self) -> bool {
        match *self {
            Self::Session { gap_ns } => gap_ns > 0 && gap_ns % 1_000_000 == 0,
            _ => self.is_millisecond_aligned_fixed_or_sliding(),
        }
    }

    pub(crate) const fn is_millisecond_aligned_fixed_or_sliding(&self) -> bool {
        match *self {
            Self::Fixed { size_ns } => size_ns > 0 && size_ns % 1_000_000 == 0,
            Self::FixedOffset { size_ns, offset_ns } => {
                size_ns > 0 && size_ns % 1_000_000 == 0 && offset_ns % 1_000_000 == 0
            }
            Self::Sliding { size_ns, period_ns, offset_ns } => {
                size_ns > 0
                    && period_ns > 0
                    && size_ns % 1_000_000 == 0
                    && period_ns % 1_000_000 == 0
                    && offset_ns % 1_000_000 == 0
            }
            Self::Global | Self::Session { .. } => false,
        }
    }

    /// Return the assigned interval for one event timestamp.
    ///
    /// Global windows return `None`. Sliding windows return one interval here;
    /// use `assign_all` to obtain every overlapping interval.
    pub fn assign(&self, timestamp_ns: i64) -> Result<Option<WindowInterval>, Error> {
        match *self {
            Self::Global => Ok(None),
            Self::Fixed { size_ns } if size_ns > 0 => {
                let start_ns = timestamp_ns
                    .div_euclid(size_ns)
                    .checked_mul(size_ns)
                    .ok_or_else(|| {
                        Error::graph("time", "window start overflows i64 nanoseconds")
                    })?;
                let end_ns = start_ns
                    .checked_add(size_ns)
                    .ok_or_else(|| Error::graph("time", "window end overflows i64 nanoseconds"))?;
                Ok(Some(WindowInterval { start_ns, end_ns }))
            }
            Self::FixedOffset { size_ns, offset_ns } if size_ns > 0 => {
                let size = i128::from(size_ns);
                let offset = i128::from(offset_ns);
                let start = (i128::from(timestamp_ns) - offset).div_euclid(size) * size + offset;
                let end = start + size;
                let start_ns = i64::try_from(start)
                    .map_err(|_| Error::graph("time", "window start overflows i64 nanoseconds"))?;
                let end_ns = i64::try_from(end)
                    .map_err(|_| Error::graph("time", "window end overflows i64 nanoseconds"))?;
                Ok(Some(WindowInterval { start_ns, end_ns }))
            }
            Self::Fixed { .. } => Err(Error::graph("time", "fixed window size must be positive")),
            Self::FixedOffset { .. } => {
                Err(Error::graph("time", "fixed window size must be positive"))
            }
            Self::Sliding { .. } => Err(Error::graph(
                "time",
                "sliding windows may assign several intervals; use assign_all",
            )),
            Self::Session { gap_ns } if gap_ns > 0 => {
                let end_ns = timestamp_ns.checked_add(gap_ns).ok_or_else(|| {
                    Error::graph("time", "session window end overflows i64 nanoseconds")
                })?;
                Ok(Some(WindowInterval { start_ns: timestamp_ns, end_ns }))
            }
            Self::Session { .. } => Err(Error::graph("time", "session gap must be positive")),
        }
    }

    /// Return every interval containing this event timestamp. The result is
    /// ordered by window start and capped to avoid unbounded fanout.
    pub fn assign_all(&self, timestamp_ns: i64) -> Result<Vec<Option<WindowInterval>>, Error> {
        let Self::Sliding { size_ns, period_ns, offset_ns } = *self else {
            return Ok(vec![self.assign(timestamp_ns)?]);
        };
        if size_ns <= 0 || period_ns <= 0 {
            return Err(Error::graph("time", "sliding size and period must be positive"));
        }
        let size = i128::from(size_ns);
        let period = i128::from(period_ns);
        let offset = i128::from(offset_ns.rem_euclid(period_ns));
        let timestamp = i128::from(timestamp_ns);
        let earliest = timestamp - size + 1 - offset;
        let first = earliest.div_euclid(period) + i128::from(earliest.rem_euclid(period) != 0);
        let last = (timestamp - offset).div_euclid(period);
        let count = last - first + 1;
        if count > 1_024 {
            return Err(Error::resource("time", "sliding window fanout exceeds 1024"));
        }
        (first..=last)
            .map(|index| {
                let start = offset + index * period;
                let end = start + size;
                Ok(Some(WindowInterval {
                    start_ns: i64::try_from(start).map_err(|_| {
                        Error::graph("time", "window start overflows i64 nanoseconds")
                    })?,
                    end_ns: i64::try_from(end).map_err(|_| {
                        Error::graph("time", "window end overflows i64 nanoseconds")
                    })?,
                }))
            })
            .collect()
    }
}

impl Element for WindowInterval {
    const TYPE_ID: &'static str = "ironbeam/window-interval";
    const CODER_ID: &'static str = "ironbeam/window-interval-i64-le";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.start_ns.to_le_bytes().to_vec();
        bytes.extend_from_slice(&self.end_ns.to_le_bytes());
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != 16 {
            return Err(Error::incompatible("window interval requires 16 bytes"));
        }
        let start_ns = i64::from_le_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| Error::incompatible("invalid window start"))?,
        );
        let end_ns = i64::from_le_bytes(
            bytes[8..]
                .try_into()
                .map_err(|_| Error::incompatible("invalid window end"))?,
        );
        if end_ns <= start_ns {
            return Err(Error::incompatible("window end must follow start"));
        }
        Ok(Self { start_ns, end_ns })
    }
}

/// Timestamp, window, firing, output-time, and lateness policy for a node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Time {
    pub initial_timestamp_ns: i64,
    pub window: Window,
    #[serde(default, skip_serializing_if = "WindowTrigger::is_default")]
    pub trigger: WindowTrigger,
    #[serde(default, skip_serializing_if = "WindowOutputTime::is_default")]
    pub output_time: WindowOutputTime,
    #[serde(default, skip_serializing_if = "WindowOnTimeBehavior::is_default")]
    pub on_time_behavior: WindowOnTimeBehavior,
    #[serde(default, skip_serializing_if = "WindowClosingBehavior::is_default")]
    pub closing_behavior: WindowClosingBehavior,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub allowed_lateness_ns: i64,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // Serde skip_serializing_if passes a field reference.
const fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

impl Default for Time {
    fn default() -> Self {
        Self {
            initial_timestamp_ns: 0,
            window: Window::Global,
            trigger: WindowTrigger::Default,
            output_time: WindowOutputTime::EarliestInPane,
            on_time_behavior: WindowOnTimeBehavior::FireIfNonempty,
            closing_behavior: WindowClosingBehavior::EmitIfNonempty,
            allowed_lateness_ns: 0,
        }
    }
}

/// Replay guarantee declared by a source or sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Replay {
    None,
    AtLeastOnce,
    Idempotent,
}

/// Resume and event-time properties declared by a source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceProgress {
    pub stable_split_id: bool,
    pub replay: Replay,
    pub reports_event_time: bool,
}

/// A control signal from one stable source split, separate from data envelopes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProgressSignal {
    pub source_key: String,
    pub split_id: String,
    pub checkpoint_position: Option<Vec<u8>>,
    pub event_time_ns: Option<i64>,
    pub completed: bool,
}

/// A deterministic, finite script for testing an unbounded source. Time is in
/// nanoseconds, like the rest of the logical graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestStreamEvent<T> {
    Elements(Vec<(T, i64)>),
    AdvanceWatermark(i64),
    AdvanceProcessingTime(i64),
}

/// Wire-encoded value, watermark, or clock step in a test stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EncodedTestStreamEvent {
    Elements(Vec<(Vec<u8>, i64)>),
    AdvanceWatermark(i64),
    AdvanceProcessingTime(i64),
}

/// Write and publication behavior of a sink terminal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SinkMode {
    Append,
    Update,
    FinalResult,
}

/// Replay, commit, and publication contract for a sink.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SinkSpec {
    pub mode: SinkMode,
    pub replay: Replay,
    pub commit: bool,
}

/// Runner-neutral operation recorded by one graph node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Source {
        progress: SourceProgress,
    },
    Map,
    Filter,
    FlatMap,
    SplittableParDo,
    AssignTimestamp,
    Flatten,
    Reshuffle,
    WaitOn,
    WaitOnWindowed,
    GroupByKey,
    CoGroupByKey,
    JoinInner,
    JoinLeft,
    JoinRight,
    JoinFull,
    SideInputMap,
    FixedWindow,
    SlidingWindow,
    SessionWindow,
    GlobalCombine,
    FoldGlobal,
    FoldPerKey,
    FoldOrderedGlobal,
    FoldOrderedPerKey,
    BatchMapValues {
        batch_size: usize,
    },
    StatefulParDo {
        contract: Box<StatefulParDoContract>,
        state_element: ElementDescriptor,
    },
    Custom {
        semantic_id: String,
        version: Version,
    },
    Output,
    Sink {
        spec: SinkSpec,
    },
}

/// Semantic element type and its wire coder declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ElementDescriptor {
    pub type_id: String,
    pub coder_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub coder_payload: Vec<u8>,
    pub coder_version: Version,
}

/// An additional input edge and its expected element type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InputEdge {
    pub key: String,
    pub element: ElementDescriptor,
}

/// One named operation and its contracts in the logical graph.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub key: String,
    pub input: Option<String>,
    pub input_element: Option<ElementDescriptor>,
    #[serde(default)]
    pub additional_inputs: Vec<InputEdge>,
    pub operation: Operation,
    pub semantic_id: String,
    pub semantic_version: Version,
    pub type_id: String,
    pub coder_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub coder_payload: Vec<u8>,
    pub coder_version: Version,
    pub boundedness: Boundedness,
    pub time: Time,
    /// Build-local index; deliberately excluded from the portable descriptor.
    #[serde(skip)]
    pub local_id: usize,
}

/// Versioned logical nodes submitted to a runner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Graph {
    pub version: Version,
    pub checkpoint_version: Version,
    pub nodes: Vec<Node>,
}

/// Identity checked by a remote worker before any bundle is processed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionIdentity {
    pub graph_fingerprint: String,
    pub binary_build_id: String,
    pub registry_manifest: String,
    pub coder_manifest: String,
    pub source_config_digest: String,
    pub sink_config_digest: String,
}
impl ExecutionIdentity {
    /// Reject a worker whose graph and configuration identity differs.
    pub fn verify(&self, worker: &Self) -> Result<(), Error> {
        if self != worker {
            return Err(Error::incompatible(
                "driver/worker execution identity mismatch",
            ));
        }
        Ok(())
    }
}

/// Whether an error is safe to retry automatically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Permanent,
    Transient,
    Unknown,
}

/// Category used to classify a graph, execution, or recovery error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Graph,
    UnsupportedCapability { capability: String },
    Incompatible,
    Resource,
    Execution,
    Io,
    Recovery,
    Cancelled,
}

/// Structured Ironbeam failure with node, backend, and retry context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    pub kind: ErrorKind,
    pub node: Option<String>,
    pub backend: Option<String>,
    pub retry: RetryClass,
    pub message: String,
}

impl Error {
    /// Construct a permanent graph-construction error for `node`.
    pub fn graph(node: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Graph,
            node: Some(node.into()),
            backend: None,
            retry: RetryClass::Permanent,
            message: message.into(),
        }
    }
    /// Construct a permanent missing-capability error for a runner.
    pub fn unsupported(
        node: impl Into<String>,
        backend: impl Into<String>,
        capability: impl Into<String>,
    ) -> Self {
        let capability = capability.into();
        Self {
            kind: ErrorKind::UnsupportedCapability {
                capability: capability.clone(),
            },
            node: Some(node.into()),
            backend: Some(backend.into()),
            retry: RetryClass::Permanent,
            message: format!("missing capability {capability}"),
        }
    }
    /// Construct a permanent wire or execution-identity mismatch error.
    pub fn incompatible(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Incompatible,
            node: None,
            backend: None,
            retry: RetryClass::Permanent,
            message: message.into(),
        }
    }
    /// Construct an execution error with node and backend context.
    ///
    /// Its retry class is `Unknown` until a caller assigns a policy.
    pub fn execution(
        node: impl Into<String>,
        backend: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: ErrorKind::Execution,
            node: Some(node.into()),
            backend: Some(backend.into()),
            retry: RetryClass::Unknown,
            message: message.into(),
        }
    }
    /// Construct an I/O error with node and backend context.
    ///
    /// Its retry class is `Unknown` until a caller assigns a policy.
    pub fn io(
        node: impl Into<String>,
        backend: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: ErrorKind::Io,
            node: Some(node.into()),
            backend: Some(backend.into()),
            retry: RetryClass::Unknown,
            message: message.into(),
        }
    }
    /// Construct a permanent checkpoint or replay recovery error.
    pub fn recovery(node: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Recovery,
            node: Some(node.into()),
            backend: Some("direct".into()),
            retry: RetryClass::Permanent,
            message: message.into(),
        }
    }
    /// Construct a permanent resource-limit error for `node`.
    pub fn resource(node: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Resource,
            node: Some(node.into()),
            backend: Some("direct".into()),
            retry: RetryClass::Permanent,
            message: message.into(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} at {:?} on {:?}: {}",
            self.kind, self.node, self.backend, self.message
        )
    }
}
impl std::error::Error for Error {}

/// Operations supported by a runner or execution backend.
#[derive(Clone, Debug, Default)]
pub struct Capabilities {
    pub external_bounded_source: bool,
    pub unbounded_source: bool,
    pub flatten: bool,
    pub reshuffle: bool,
    pub wait_on: bool,
    pub group_by_key: bool,
    pub co_group_by_key: bool,
    pub join_inner: bool,
    pub join_outer: bool,
    pub side_input_map: bool,
    pub global_combine: bool,
    pub incremental_combine: bool,
    pub ordered_fold: bool,
    pub batch_map_values: bool,
    pub stateful_value: bool,
    pub splittable_pardo: bool,
    pub fixed_window: bool,
    pub assign_timestamp: bool,
    pub append_sink: bool,
    pub final_result_sink: bool,
    pub update_sink: bool,
    pub custom_operations: BTreeSet<String>,
}
