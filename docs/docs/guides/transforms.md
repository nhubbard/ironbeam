# Transforms

Transforms create new `PCollection` handles. Most are named methods; their keys identify graph nodes and their `Result` reports invalid construction.

## Map and filter

```rust
use ironbeam::{Pipeline, prelude::*};
let pipeline = Pipeline::default();
let values = pipeline.from_vec("source", vec![1_i32, 2, 3, 4])?
    .map_named("square", |n| n * n)?
    .filter_named("even-square", |n| n % 2 == 0)?
    .collect_seq()?;
assert_eq!(values, vec![4, 16]);
# Ok::<(), ironbeam::Error>(())
```

## Keyed grouping

```rust
use ironbeam::{Pipeline, prelude::*};
let pipeline = Pipeline::default();
let groups = pipeline.from_vec("pairs", vec![("a".to_string(), 1_i32), ("a".to_string(), 2)])?
    .group_by_key_named("group")?
    .collect_seq()?;
assert_eq!(groups.len(), 1);
# Ok::<(), ironbeam::Error>(())
```

The transform crate supplies additional named helpers for keyed reductions, joins, sampling, selection, validation, and side inputs. Browse the [examples](../reference/examples.md) for complete programs. Stateful operations can have different supported graph shapes on Direct and Prism; portable preflight checks report unsupported shapes before execution.
