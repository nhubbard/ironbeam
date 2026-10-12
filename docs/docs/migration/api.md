# API mapping

The principal change is that graph construction is named and fallible. This makes graph identity visible at call sites.

| Ironbeam 4 | Ironbeam 5 | Migration action |
| --- | --- | --- |
| `from_vec(&pipeline, values)` | `pipeline.from_vec("source", values)?` | Assign a stable source key. |
| `.map(f)` | `.map_named("map", f)?` | Name the transform and handle construction errors. |
| `.filter(f)` | `.filter_named("filter", f)?` | Name the transform and handle construction errors. |
| `.flat_map(f)` | `.flat_map_named("expand", f)?` | Name the transform and handle construction errors. |
| `.group_by_key()` | `.group_by_key_named("group")?` | Use a unique grouping key. |
| `.collect_seq()` | `.collect_seq()?` | Import the v5 Direct extension trait from `prelude`. |
| Implicit terminal | `.output("result")?` | Name portable or durable terminals. |
| Optional partition/thread arguments | `JobConfig` and concrete parallelism | Move runner settings into a job. |

## Before and after

Ironbeam 4:

```rust
let values = from_vec(&pipeline, vec![1_i32, 2, 3, 4]);
let result = values.map(|n| n * 2).filter(|n| n > &4).collect_seq()?;
```

Ironbeam 5:

```rust
use ironbeam::{Pipeline, prelude::*};
let pipeline = Pipeline::default();
let result = pipeline.from_vec("numbers", vec![1_i32, 2, 3, 4])?
    .map_named("double", |n| n * 2)?
    .filter_named("large", |n| *n > 4)?
    .collect_seq()?;
# Ok::<(), ironbeam::Error>(())
```

Some helper methods live in `ironbeam-transforms`, while Direct collection helpers live in `ironbeam-direct`. The root prelude imports the common v5 extension traits. For specialized helpers, consult the [repository migration table](https://github.com/nhubbard/ironbeam/blob/main/V4_TO_V5_MIGRATION.md).
