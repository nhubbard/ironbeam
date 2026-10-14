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

## Partitioning

`partition_named` routes each input element to one of a fixed number of
outputs. The classifier returns an index starting at zero. The resulting
collections have stable keys formed from the output prefix and index:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("numbers", vec![0_i32, 1, 2, 3, 4, 5])?;
    let parts = source.partition_named("classify", "part", 2, |value| {
        value.rem_euclid(2) as usize
    })?;
    assert_eq!(parts[0].collect_seq()?, vec![0, 2, 4]);
    assert_eq!(parts[1].collect_seq()?, vec![1, 3, 5]);
    Ok(())
}
```

An index outside the declared range fails execution. Direct may evaluate the
classifier separately for downstream branches, and any runner may retry an
element. The classifier must return the same index on each evaluation and
should have no observable side effects. Use
`partition_with_semantic_named` to include an application semantic ID and
version in graph identity; change that version when routing behavior changes.
The local Prism fixtures check all three bounded outputs and a two-branch
unbounded source against Direct, including event timestamps and fixed windows.

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

## Side views

A side view lets each main element consult a bounded collection. The global
`map_with_side_named` method supplies a slice of all side values. Three typed
helpers give that slice a specific shape:

| Method | View | Invalid input |
| --- | --- | --- |
| `map_with_singleton_side_named` | One side value | Zero or multiple values fail execution |
| `map_with_map_side_named` | One value per key | Duplicate keys fail execution |
| `map_with_multimap_side_named` | All values per key | Value order is unspecified |

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let orders = pipeline.from_vec("orders", vec!["a".to_owned(), "b".to_owned()])?;
    let rates = pipeline.from_vec("rates", vec![("a".to_owned(), 10_i32)])?;
    let totals = orders
        .map_with_map_side_named("rate-lookup", &rates, |key, rates| {
            rates.get(&key).copied().unwrap_or_default()
        })?
        .collect_seq()?;
    assert_eq!(totals, vec![10, 0]);
    Ok(())
}
```

These helpers materialize one global view from a bounded side collection. For
matching values in each event-time window, use `map_with_windowed_side_named`,
`map_with_windowed_singleton_side_named`,
`map_with_windowed_map_side_named`, or
`map_with_windowed_multimap_side_named`. The windowed methods accept an
unbounded side collection when both inputs have watermarks. They apply one view
after both inputs close the window and preserve each main element's timestamp.
They do not update a result already emitted from that window. The singleton
requires exactly one side value in each main window; a map rejects duplicate
keys within a window. Values under a multimap key have no guaranteed order.

`map_with_windowed_materialized_side_named` accepts a fallible view builder and
a fallible mapping function for application-defined view shapes. For a
transform that emits zero or more values per input and can fail, use
`flat_map_fallible_named`. Its errors fail execution on Direct and the portable
worker.

## Completion barriers

`wait_on_named` holds a main collection until a bounded signal collection
completes. For matching event-time windows on watermarked streams,
`wait_on_windowed_named` releases main values after both sources close
their matching window. Signal values are ignored, and a window with no signal
elements still closes when the signal watermark passes it. The windowed method
requires matching time and window declarations. It may buffer a full window;
it does not release early when the signal closes before the main watermark.

## Batches and custom handlers

Version 5 batch transforms use an explicit size rather than an implicit physical partition boundary. Registered custom handlers have semantic identifiers and versions; those values contribute to graph identity. The [migration guide](../migration/operations.md) describes the effect on v4 programs. Runner-specific capability checks determine whether a stateful graph can execute on Direct or the portable worker.
