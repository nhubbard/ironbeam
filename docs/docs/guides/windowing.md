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

`sliding_windows` currently accepts bounded collections with cloneable
elements. An element may produce up to 1,024 interval assignments. Portable
lowering requires whole-millisecond size, period, and offset values. The
Direct and Prism regression fixture compares grouped values, timestamps, and
all six intervals across negative and positive event times. Session windows
and unbounded sliding windows are outside the current collection contract.

## Migration of timestamps

Ironbeam 4 examples commonly expressed timestamps in milliseconds. Multiplying a millisecond value by one million converts it to nanoseconds. The [operational migration guide](../migration/operations.md) covers other stateful changes. For unbounded input, a source's progress and event-time reports affect when a window can close; [portable execution](portable.md) describes the tested runner boundary.
