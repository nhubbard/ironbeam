# Event time and windows

Event time describes when a record occurred. Ironbeam 5 stores it as metadata measured in nanoseconds. It is distinct from processing time, which describes when a runner handles the record. A timestamp field in an application record does not become event-time metadata until a transform assigns it.

## Fixed windows

A fixed window has a duration and offset. The following pipeline assigns event time from `(timestamp, value)` pairs and groups two records into one ten-second interval:

```rust
use ironbeam::{Pipeline, prelude::*};

const TEN_SECONDS_NS: i64 = 10_000_000_000;

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let windows = pipeline
        .from_vec("events", vec![(1_000_000_000_i64, 10_i32), (2_000_000_000, 20)])?
        .timestamps_from_pairs_named("event-time")?
        .group_by_window_named("ten-second-windows", TEN_SECONDS_NS, 0)?
        .collect_seq()?;
    assert_eq!(windows.len(), 1);
    Ok(())
}
```

Window grouping operates on the assigned metadata. A keyed window group also retains the key, allowing separate aggregates for each key and interval. The [windowing example](https://github.com/nhubbard/ironbeam/blob/main/examples/windowing_aggregations.rs) computes global and per-key statistics.

### Reading pane information

A pane identifies one firing of a window. The default trigger produces one
on-time pane for a nonempty grouped window. `reify_panes_named` pairs each
value with its `PaneInfo` so a transform can inspect its timing, first and last
flags, and firing indices:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let grouped = pipeline
        .from_vec("events", vec![(1_000_000_i64, ("a".to_owned(), 2_i32))])?
        .timestamps_from_pairs_named("event-time")?
        .fixed_windows("ten-ms-window", 10_000_000)?
        .group_by_key_named("group")?
        .reify_panes_named("pane")?
        .collect_seq()?;
    assert_eq!(grouped[0].0.timing, ironbeam::PaneTiming::OnTime);
    Ok(())
}
```

The same pane is available to a custom sink through `ElementMetadata.pane`.
Source elements have `Unknown` timing before a window fires.

### Early count firings on Prism

`early_count_trigger_named` configures repeated early firings for global,
fixed, or sliding windows. The count must be positive and fit in a signed
32-bit integer:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let _grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32), ("a".to_owned(), 3)])?
        .fixed_windows("windows", 10_000_000)?
        .early_count_trigger_named("early-count", 1)?
        .group_by_key_named("group")?
        .reify_panes_named("pane")?;
    Ok(())
}
```

The setting is part of the graph fingerprint and lowers to Beam's repeated
early element-count trigger. The local Prism proof emits one early pane for
two values in one bounded bundle. A TestStream proof with the values in
separate advances emits panes numbered 0 and 1. Bundle scheduling can affect
which elements share a pane. Direct produces the same panes for the tested
bounded bundle and separate streaming polls. Its streaming path checkpoints
the next pane index with the source cursor and restores it after restart.
`accumulating_count_trigger_named` uses the same threshold but retains prior
values. With separate values `2` and `3`, the first early pane contains `[2]`,
the second contains `[2, 3]`, and the on-time pane contains `[2, 3]` again.
Direct checkpoints both the retained values and the count at the last firing.
Its accumulating keyed fold requires a value coder. A bounded overlapping
sliding-window proof matches Direct and pinned Prism in both accumulation modes. This support covers
single-input keyed grouping in global, fixed, and sliding windows. An unbounded
global group needs a count trigger: without one, its only normal firing is at
source completion, which may never occur. For example, a test stream can apply
`early_count_trigger_named("early", 2)` directly to its global keyed elements,
then call `group_by_key_named("group")`. Direct and Prism both emit an early
pane after the second element and a final pane when the finite test stream
completes. For a continuing source, the trigger supplies regular output while
the key remains live; retained state grows when accumulation is enabled.
`combine_values_lifted_named` also accepts an unbounded global keyed input
with this trigger. It combines each emitted group pane, so the example input
`2, 3, 4` yields an early sum of `5` and a final sum of `4` in discarding
mode, or `9` in accumulating mode. This path materializes the values in each
pane before applying the combiner; use finite windows when long-lived keys
must have bounded retained state.

To combine early and late count firings, set allowed lateness and use the combined policy
described below.

### Count and processing time together

`count_and_processing_time_trigger_named` waits for both a positive element
count and a positive processing-time delay before each early firing. The delay
starts when the first element arrives for a key and window. The order of the
two conditions does not matter: if the clock reaches the deadline first, the
group waits for enough elements; if the count is reached first, it waits for
the clock. After a firing, the next element starts a new delay. The
`accumulating_count_and_processing_time_trigger_named` variant retains values
from earlier panes.

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let _grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32), ("a".to_owned(), 3)])?
        .fixed_windows("windows", 10_000_000)?
        .count_and_processing_time_trigger_named("both", 2, 5_000_000)?
        .group_by_key_named("group")?;
    Ok(())
}
```

The delay is expressed in nanoseconds and must be a whole number of
milliseconds. Direct checkpoints both an armed deadline and a deadline that
has elapsed while the count is still pending. A local Prism test uses the same
fixed-window pipeline in discarding and accumulating modes and compares pane
contents with Direct. Processing-time firings depend on the runner clock and
bundle schedule, so a different schedule can change which elements share a
pane.

### Group output timestamps

By default, GroupByKey uses the earliest event timestamp in its pane. A
fixed, sliding, or session window can instead timestamp grouped output at the final
whole millisecond in the interval:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let _grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])?
        .fixed_windows("windows", 10_000_000)?
        .end_of_window_output_time_named("output-time")?
        .group_by_key_named("group")?
        .reify_timestamps_named("timestamp")?;
    Ok(())
}
```

The interval `[0, 10 ms)` produces a grouped timestamp of 9 ms. To use the
latest event timestamp in each pane instead, call
`latest_in_pane_output_time_named("output-time")` in place of
`end_of_window_output_time_named`. For events at 2 ms and 7 ms, the grouped
output is stamped 7 ms, regardless of arrival order. An accumulating pane
retains the latest timestamp of all values it contains.

Both policies also apply to a global window. Its Beam end timestamp is
`9_223_371_950_454_775` milliseconds. That value exceeds the signed
nanosecond range used inside Ironbeam, so timestamp reification reports
`i64::MAX` for a global end-of-window result. The portable worker converts
that sentinel back to Beam's timestamp on the wire. Latest-in-pane remains
the latest input event time; for global inputs at 2 ms and 7 ms, it is 7 ms.
Direct and the pinned local Prism runner agree on both global cases.
They also agree when a count trigger produces an early pane before source
completion. In a discarding group, the early pane keeps the latest input time
from its first two elements and the final pane uses the remaining element's
time. In an accumulating group, the final pane keeps all three elements.
End-of-window reports the global sentinel for both panes.

Both policies are part of the graph identity and Beam windowing strategy.
Window sizes, periods, offsets, and session gaps must be whole milliseconds. In
a merged session, end-of-window uses the final merged interval; latest-in-pane
uses the latest input event time, including events seen before a checkpoint or
before a later event bridged two sessions. Direct and the pinned local Prism
runner agree on bounded and one-source unbounded merged-session fixtures for
both policies. The unbounded fixture inserts a bridge after a watermark
advance and drops an event after closure.

The policies also apply to co-groups and joins when every input has the same
windowing declaration. A bounded two-input session join with a bridge from the
second input and a three-branch session co-group agree on both policies in
Direct and local Prism.
For a join, the latest-in-pane timestamp is the latest event timestamp for
the entire key and window, including input rows that have no match. The default
join timestamp remains the earlier timestamp of each matched pair; an unmatched
row retains its own timestamp. Vector and lifted global combines, and lifted
keyed combines, follow the same policies. Ordered folds retain their explicit
ordering and timestamp rules.

### Late elements

Direct and portable runners can retain a fixed or sliding window after its end. Set
`allowed_lateness_named("lateness", 5_000_000)` after assigning windows to
retain it for five milliseconds. The duration must be a nonnegative whole
number of milliseconds and becomes part of the graph identity.

With the default trigger, a late element received before expiry produces a
`Late` pane. The pinned Prism fixture closes `[0, 10 ms)` at a 10 ms
watermark, emits an element arriving afterward at 3 ms in a late pane, and
drops another element after the 15 ms expiry watermark. Direct emits the same
pane values, event timestamps, timing, and indices. Its checkpoint retains
the closed window until expiry, including the index for the next late pane.
Direct supports this policy for single-input GroupByKey with the default
trigger, a late count trigger, or combined early and late counts. The
multi-source path retains coded GroupByKey state through expiry with the
default or late element-count trigger. It also serves homogeneous co-groups built from
Flatten and GroupByKey. In a two-source test, both runners emit an on-time
pane containing `[[2], [4]]`, then a late pane containing `[[3], []]`;
a row after expiry is dropped. Direct restores both source cursors and the
closed group's pane index from a checkpoint. Typed joins and heterogeneous
co-groups retain their windows through expiry on Direct and pinned Prism.

For two-source late count firings, set the same positive allowed lateness and
late count on each input before `co_group_homogeneous_named`. With a count of
two, Direct and pinned Prism emit `[[2], [4]]` on time, `[[3, 5], []]`
when the second late element arrives, and a final partial `[[7], []]` pane
at expiry. The source watermarks must leave the window open while those late
elements arrive. Combined early and late count triggers remain limited to
single-input grouping on Direct.

The same count policy applies to typed joins and heterogeneous co-groups.
With a main value `2` and side value `"right"`, a heterogeneous co-group
emits `([2], ["right"])` on time. Two later main values, `3` and `5`,
produce one late pane containing `([3, 5], [])`. Direct and pinned Prism
produce the same pane timing, indices, and flags when both branches come
from one tagged source. Direct reads that source once and checkpoints its
single cursor and the unfinished pane. Inner and right joins emit no
unmatched main-side late rows; left and full joins emit those rows, with
a final partial pane at expiry in the tested schedule.

### Late count firings

`late_count_trigger_named` waits for a specified number of new late elements
before emitting a pane. Set positive allowed lateness first:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let _grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])?
        .fixed_windows("windows", 10_000_000)?
        .allowed_lateness_named("lateness", 5_000_000)?
        .late_count_trigger_named("late-count", 2)?
        .group_by_key_named("group")?;
    Ok(())
}
```

The count is per key and window. In a streaming job, two late elements in
`[0, 10 ms)` produce one late pane. If only one more late element arrives,
the closing behavior emits that partial pane when the window expires at
15 ms. The local Prism and Direct fixtures agree on the values, timestamps,
timing, pane indices, and final flag for this schedule. Direct stores a
partial late pane with the source cursor, so a restart does not lose the
count or emit the pane twice. The policy uses discarding panes. The tested
fixed-window paths include single-input GroupByKey, homogeneous and
heterogeneous co-groups, and typed joins.

`accumulating_late_count_trigger_named` retains the on-time values in every
later pane. Direct fires after two new late elements and checkpoints the
retained values and count. In the pinned local Prism runner, the same
TestStream schedule coalesces the late elements and fires one final pane at
expiry. Both runners retain the same values, but applications that act on
intermediate accumulating late panes must account for this scheduling
difference. Direct requires coders for retained values. Checkpointed Direct
fixtures cover both a multi-source homogeneous co-group and a typed full
join. A shared-source three-input co-group has matching values, timing
labels, and pane indices on Direct and Prism. In the typed join fixture,
Direct emits a threshold pane and a later final pane, while Prism coalesces
the late rows into one final pane; both include the on-time join rows again.

`early_and_late_count_trigger_named` combines repeated early and late
thresholds in one Beam `AfterEndOfWindow` trigger. The call follows
`allowed_lateness_named` and takes the early count before the late count:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let _grouped = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])?
        .fixed_windows("windows", 10_000_000)?
        .allowed_lateness_named("lateness", 5_000_000)?
        .early_and_late_count_trigger_named("early-late", 2, 2)?
        .group_by_key_named("group")?;
    Ok(())
}
```

On the tested stream, `[2, 3]` fires early, `[4]` fires at the watermark,
`[5, 7]` fires late at the count threshold, and `[11]` fires when the window
expires. Direct and pinned Prism agree on the values, event times, timing,
indices, and final flag for this schedule. Direct also restores a partial
late count after an early pane when there was no on-time value. Pinned Prism
placed the first post-watermark element in an on-time pane in that separate
empty-on-time schedule, so intermediate pane agreement is not established
there. The combined policy uses discarding panes.

`accumulating_early_and_late_count_trigger_named` uses the same thresholds
and retains previous values in later panes. Direct checkpoints those values
and the pane index. In a local Prism TestStream with `[2, 3]` before the
watermark and `[5, 7]` afterward, Direct emits early `[2, 3]`, on-time
`[2, 3]`, and late `[2, 3, 5, 7]`. Pinned Prism also emits an early and an
on-time pane containing `[2, 3, 5, 7]` before its final late pane. This policy
therefore has no cross-runner guarantee for the number or timing of
accumulating panes in this schedule.

For overlapping sliding windows, each assigned interval keeps its own pane
index and expiry. A tested late element is a late pane in `[0, 10 ms)` while
remaining part of the on-time pane in `[5, 15 ms)`. Prism marked the first
late pane final because it fired after that interval expired; Direct had
already emitted it before expiry and marked it nonfinal. The values,
timestamps, timing, and indices agree.

## Sliding windows

Sliding windows have a size and a period. The size determines each interval's
duration; the period determines how often a new interval starts. When the size
exceeds the period, one element can belong to several intervals. For example,
a four-second size and two-second period assign an event at one second to the
intervals `[-2, 2)` and `[0, 4)` seconds. The optional offset moves the
sequence of window starts.

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let _windows = pipeline
        .from_vec("events", vec![(1_000_000_000_i64, ("a".to_owned(), 7_i32))])?
        .timestamps_from_pairs_named("event-time")?
        .sliding_windows("overlapping", 4_000_000_000, 2_000_000_000, 0)?;
    Ok(())
}
```

`sliding_windows` requires cloneable elements. An element may produce up to
1,024 interval assignments. Portable lowering requires whole-millisecond size,
period, and offset values. Direct and Prism fixtures compare grouped values,
timestamps, and intervals across negative and positive event times. The
unbounded fixture also checks that both runners close overlapping windows and
discard a row that arrives after their watermarks. Direct checkpoints coded
open-window rows with each source cursor.

## Keyed sessions

A session starts with an event and extends for a chosen inactivity gap. Two
sessions for the same key merge when their intervals overlap or touch on Direct
and the pinned local Prism runner. A later
event can bridge sessions that were separate when they first arrived. Sessions
for different keys remain separate.

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let sessions = pipeline
        .from_vec("events", vec![
            (0_i64, ("a".to_owned(), 1_i32)),
            (8_000_000, ("a".to_owned(), 2)),
            (4_000_000, ("a".to_owned(), 3)),
        ])?
        .timestamps_from_pairs_named("event-time")?
        .group_by_session_named("sessions", 5_000_000)?
        .with_window_named("session-bounds")?
        .collect_seq()?;
    assert_eq!(sessions.len(), 1);
    assert_eq!((sessions[0].0.start_ns, sessions[0].0.end_ns), (0, 13_000_000));
    Ok(())
}
```

The three rows for `a` form one interval `[0, 13_000_000)` ns. The event at
`4_000_000` ns joins the intervals initially assigned at zero and eight
milliseconds. The grouped result keeps the earliest input event time. Direct
and local Prism agree on that overlapping bridge and on a second
key. Single-source unbounded keyed groups also merge in Direct and local
Prism. Direct restores their open sessions and source cursor from a checkpoint;
the shared fixture closes one negative-time session, then bridges two open
sessions and discards a row after its session closes. Direct also resolves an
incoming element against its still-open merged session before checking expiry,
including after a checkpoint restore. In the pinned local Prism comparison,
Direct groups values `[1, 2, 3]` into `[0, 9_000_000)` ns after a six-millisecond
watermark. Prism instead emits `[1]` in `[0, 5_000_000)` ns and `[2, 3]` in
`[0, 9_000_000)` ns. The comparison remains an ignored diagnostic test while
this portable mismatch is unresolved. Bounded session joins and co-groups
merge intervals across their inputs before matching keyed values. Direct and
local Prism agree when another input bridges two sessions;
Direct also checks inner, left, right, and full join results. A three-input
co-group fixture verifies an empty side and an isolated session on both
runners. For two unbounded inputs, session joins and co-groups wait for both
source watermarks before closing a keyed session. Direct and local Prism agree
on a bridge formed by the other input and on the merged interval. Direct can
restore the open rows and both source cursors from a checkpoint. Separate
two-source join and co-group recovery tests save an open `[0, 9_000_000)` ns
session, restart after a six-millisecond watermark, and accept a second-source
row whose original `[0, 5_000_000)` ns interval has passed that watermark.
Direct also accepts an expired original interval when it merges into a live keyed session
from either input; its join and co-group fixtures drop a later row after
closure. This intermediate-watermark schedule still needs a local Prism
comparison. Sessions
across three independent unbounded inputs have one tested bridge schedule;
other watermark orders remain unproved.
Portable session gaps must be positive whole milliseconds.

The worker's runner-issued `MergeWindows` function and Direct use the Beam
Java rule of strict interval overlap. They report endpoint-touching windows
separately. That worker function has a wire-level test, but no live Prism job
has been observed requesting it. Pinned Prism merged a touching pair in an
earlier pipeline fixture. For portable jobs that depend on this boundary,
verify the result on the chosen runner.

`group_by_session_named` assigns sessions and then groups the keyed elements.
Use `session_windows_named` to assign intervals separately when another
transform needs to run before `group_by_key_named`.

For a bounded join, assign the same session gap to both inputs before joining:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![(0_i64, ("a".to_owned(), 1_i32))])?
        .timestamps_from_pairs_named("left-time")?
        .session_windows_named("left-sessions", 5_000_000)?;
    let right = pipeline
        .from_vec("right", vec![(4_000_000_i64, ("a".to_owned(), 10_i32))])?
        .timestamps_from_pairs_named("right-time")?
        .session_windows_named("right-sessions", 5_000_000)?;
    let joined = left.join_inner_named("session-join", &right)?.collect_seq()?;
    assert_eq!(joined, vec![("a".to_owned(), (1, 10))]);
    Ok(())
}
```

Both rows belong to the merged interval `[0, 9_000_000)` ns. The join uses
the key and merged interval, so an event from either input can connect two
sessions. The same bounded rule applies to left, right, and full joins.

## Migration of timestamps

Ironbeam 4 examples commonly expressed timestamps in milliseconds. Multiplying a millisecond value by one million converts it to nanoseconds. The [operational migration guide](../migration/operations.md) covers other stateful changes. For unbounded input, a source's progress and event-time reports affect when a window can close; [portable execution](portable.md) describes the tested runner boundary.
