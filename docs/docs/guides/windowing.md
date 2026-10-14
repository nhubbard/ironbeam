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

`early_count_trigger_named` configures repeated early firings for a fixed
window. The count must be positive and fit in a signed 32-bit integer:

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
which elements share a pane. Direct rejects this trigger before execution;
late firings, accumulation, and allowed lateness remain unavailable.

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
sessions for the same key merge when their intervals overlap or touch. A later
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
and local Prism agree on that result, on a touching boundary, and on a second
key. Single-source unbounded keyed groups also merge in Direct and local
Prism. Direct restores their open sessions and source cursor from a checkpoint;
the shared fixture closes one negative-time session, then bridges two open
sessions and discards a row after its session closes. Bounded session joins
and co-groups merge intervals across their inputs before matching keyed
values. Direct and local Prism agree when another input bridges two sessions;
Direct also checks inner, left, right, and full join results. A three-input
co-group fixture verifies an empty side and an isolated session on both
runners. For two unbounded inputs, session joins and co-groups wait for both
source watermarks before closing a keyed session. Direct and local Prism agree
on a bridge formed by the other input and on the merged interval. Direct can
restore the open rows and both source cursors from a checkpoint. Sessions
across more than two unbounded inputs have not been tested.
Portable session gaps must be positive whole milliseconds.

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
