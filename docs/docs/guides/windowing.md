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

## Migration of timestamps

Ironbeam 4 examples commonly expressed timestamps in milliseconds. Multiplying a millisecond value by one million converts it to nanoseconds. The [operational migration guide](../migration/operations.md) covers other stateful changes. For unbounded input, a source's progress and event-time reports affect when a window can close; [portable execution](portable.md) describes the tested runner boundary.
