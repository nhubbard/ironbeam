# Event time and windows

Event time describes when a record occurred. In Ironbeam 5 it is metadata measured in nanoseconds. It is distinct from processing time and from any timestamp field in a user record. A pipeline can assign event time from `(timestamp, value)` pairs and group records into fixed windows.

```rust
use ironbeam::{Pipeline, prelude::*};
const TEN_SECONDS_NS: i64 = 10_000_000_000;
let pipeline = Pipeline::default();
let windows = pipeline
    .from_vec("events", vec![(1_000_000_000_i64, 10_i32), (2_000_000_000, 20)])?
    .timestamps_from_pairs_named("event-time")?
    .group_by_window_named("ten-second-windows", TEN_SECONDS_NS, 0)?
    .collect_seq()?;
assert_eq!(windows.len(), 1);
# Ok::<(), ironbeam::Error>(())
```

The window size and offset are explicit. The [windowing example](https://github.com/nhubbard/ironbeam/blob/main/examples/windowing_aggregations.rs) covers global and per-key aggregation. Ironbeam 4 timestamp examples used milliseconds; multiply those values by one million when migrating them to v5 nanoseconds.
