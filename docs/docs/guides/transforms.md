# Transforms

A transform adds a node to a pipeline graph and returns a new `PCollection` handle. Named methods identify the operation and return `Result` so invalid graph construction can be reported before execution.

## Element operations

`map_named` changes each element. `filter_named` retains elements for which a predicate returns true. `flat_map_named` can emit zero or more elements for each input.

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("source", vec![1_i32, 2, 3, 4])?
        .map_named("square", |value| value * value)?
        .filter_named("even-square", |value| value % 2 == 0)?
        .collect_seq()?;
    assert_eq!(values, vec![4, 16]);
    Ok(())
}
```

Elementwise operations can often be executed as a fused Direct route. Grouping and similar operations require a materialization boundary; `DirectRunner.explain` identifies those choices.

## Grouping by key

Pairs can be grouped by their first component:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let groups = pipeline
        .from_vec("pairs", vec![("a".to_string(), 1_i32), ("a".to_string(), 2)])?
        .group_by_key_named("group")?
        .collect_seq()?;
    assert_eq!(groups.len(), 1);
    Ok(())
}
```

A grouped result contains a key and its values. A shuffle may change value order, so an application that depends on order must provide an explicit order key through the corresponding ordered operation. The transform crates also include keyed reductions, joins, sampling, selection, validation, and side inputs. The [ETL example](https://github.com/nhubbard/ironbeam/blob/main/examples/etl_pipeline.rs) applies several of these operations in one pipeline.

## Batches and custom handlers

Version 5 batch transforms use an explicit size rather than an implicit physical partition boundary. Registered custom handlers have semantic identifiers and versions; those values contribute to graph identity. The [migration guide](../migration/operations.md) describes the effect on v4 programs. Runner-specific capability checks determine whether a stateful graph can execute on Direct or the portable worker.
