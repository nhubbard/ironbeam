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

`flatten_named` accepts bounded and unbounded inputs when their element types
and time declarations match. A shared fixed-window fixture preserves each
input's value, event timestamp, and window on Direct and local Prism. With a
checkpointed Direct job, a bounded source resumes by skipping rows already
committed before continuing with the stream. Use a source whose configured
snapshot remains stable across that restart.

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

For an unbounded keyed collection, call `early_processing_time_trigger_named("timer", 5_000_000)` before `group_by_key_named` to emit an early pane five milliseconds after the first row in a key and window. A later row can arm another firing. `accumulating_processing_time_trigger_named` retains earlier values in each new pane; the other method discards values after each firing. The delay is a positive whole number of milliseconds expressed in nanoseconds. The policy accepts global, fixed, and sliding windows; session windows are rejected. Direct uses the source's scripted clock when supplied and wall time otherwise, so an idle source can still fire. Direct and local Prism have matching fixed-window results for a single TestStream source with explicit processing-time advances.

`group_into_batches_named` emits vectors of values under each key. Its size
must be greater than zero and smaller than `i64::MAX`; invalid sizes fail
while the graph is built. For an unbounded input without an existing early
count trigger, Ironbeam installs one so a full batch can fire before the
window closes. A final pane can contain fewer values than the requested size.
Batch membership can depend on arrival and firing order after a shuffle.

`co_group_homogeneous_named` groups collections whose keys and values have
the same types. The result is a key and one value vector
per input. The first vector belongs to the collection on which the method is
called; the remaining vectors follow the order of `others`. A missing key in
one input produces an empty vector. Inputs must share time and window
declarations; bounded and unbounded inputs may be mixed. Any unbounded input
requires fixed, sliding, or session windows. The method uses Flatten and
GroupByKey in the portable graph, so
value order within each vector is unspecified. A twelve-input
fixed-window job and a twelve-branch merged-session job pass on Direct and
local Prism. The session job derives its branches from one bounded source so
the bridge record precedes source completion on both runners. Three independent
unbounded sources and three branches from one unbounded source also pass on
both runners in fixed windows. Direct checkpoints their open groups and source
cursors; late records after window closure are dropped. A common transform
before the branches runs once per input element. Three independent unbounded
inputs also merge a keyed session when their watermarks advance after the
bridging record. Direct can restore its open intervals and source cursors
before that bridge arrives. Intermediate-watermark session schedules still
need runner agreement.

A shared three-input fixed-window job combines one unbounded input, one bounded
input, and an empty bounded input. Direct and local Prism produce the same two
keyed groups, including a key found only in the bounded input. Direct stores
the bounded rows with the open group; after a checkpoint restart, it resumes
the stream without reading those bounded rows again.

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

## Flatten

`flatten_named` combines collections with the same element type and window
declaration. An empty `others` slice creates a one-input Flatten. For example,
`rows.flatten_named("one-input", &[])?` leaves the rows intact while retaining
a portable Flatten transform in the graph.

## Completion barriers

`wait_on_named` holds a main collection until a bounded signal collection
completes. `wait_on_all_named` accepts several bounded signals of the same
element type and requires all of them to finish. For signals with different
element types, use `wait_on_signals_named` and convert each collection with
`wait_signal`. For matching event-time windows on watermarked streams,
`wait_on_windowed_named` releases main values after both sources close
their matching window. Signal values are ignored, and a window with no signal
elements still closes when the signal watermark passes it. The windowed method
requires matching time and window declarations. It may buffer a full window;
it does not release early when the signal closes before the main watermark.
`wait_on_all_windowed_named` accepts several signals of the same element type.
It uses one keyed co-group to wait for the main input and every signal watermark
to close the matching window. Direct and local Prism agree in a three-source
fixed-window test, including windows without signal elements.

For signals with different element types, convert each collection with
`windowed_wait_signal_named` and pass the handles to
`wait_on_windowed_signals_named`. This also uses one keyed co-group. A shared
job checks three independently watermarked sources on Direct and local Prism.
The two signal branches carry text and Boolean values after their sources.

```rust
let ready = main.wait_on_all_windowed_named(
    "after-windowed-imports",
    &[users_done, orders_done],
)?;
```

```rust
let users_done = users.windowed_wait_signal_named("users-done")?;
let orders_done = orders.windowed_wait_signal_named("orders-done")?;
let ready = main.wait_on_windowed_signals_named(
    "after-windowed-imports",
    &[users_done, orders_done],
)?;
```

```rust
let ready = main.wait_on_signals_named(
    "after-imports",
    &[users_done.wait_signal(), orders_done.wait_signal()],
)?;
```

The signal collections may have different element types. Their values do not
appear in `ready`.

## Batches and custom handlers

Version 5 batch transforms use an explicit size rather than an implicit physical partition boundary. Registered custom handlers have semantic identifiers and versions; those values contribute to graph identity. The [migration guide](../migration/operations.md) describes the effect on v4 programs. Runner-specific capability checks determine whether a stateful graph can execute on Direct or the portable worker.
