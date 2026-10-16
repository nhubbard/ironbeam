# API reference

Ironbeam's Rust API is documented in the Rustdoc for each crate. The tables
below identify the entry points most applications use. Method names ending
in `_named` take a graph key. Keep that key stable when a job may resume or
when a portable worker rebuilds its graph.

The 5.0.0 packages are still unpublished. To read the exact API for this
checkout, build Rustdoc and open `target/doc/ironbeam/index.html`:

```sh
cargo doc --workspace --all-features --no-deps
```

The root `ironbeam` crate reexports the common graph and Direct APIs. A
library can depend on `ironbeam-core` when it only constructs graphs. The
[package map](packages.md) lists the remaining crates.

## Construct a pipeline

| Task | Entry point | Result |
| --- | --- | --- |
| Start a graph | `Pipeline::default` | An empty graph owner |
| Add in-memory data | `Pipeline::from_vec`, `from_iter` | A bounded `PCollection<T>` |
| Add a replayable source | `Pipeline::bounded_source`, `unbounded_source` | A collection whose reader runs when the job starts |
| Script event-time input | `Pipeline::test_stream_source` | A test collection with explicit values and progress |
| Inspect the graph | `Pipeline::snapshot` | A runner-neutral graph description |

Source construction does not read records. A runner starts reading when it
executes an output or sink. See [pipelines and collections](../concepts/pipelines.md).

## Transform a collection

| Task | Entry point | Result or constraint |
| --- | --- | --- |
| Change or select values | `map_named`, `filter_named`, `flat_map_named` | One, zero, or several outputs per input |
| Use a portable Rust handler | `map_with_semantic_named`, `filter_with_semantic_named`, `flat_map_with_semantic_named` | Adds a semantic ID and version to graph identity |
| Merge compatible inputs | `flatten_named` | One collection with the inputs' element type and time declaration |
| Redistribute values | `reshuffle_named` | A shuffle boundary; encounter order is unspecified |
| Group keyed values | `group_by_key_named` | One keyed group per window and firing |
| Join keyed inputs | `join_inner_named`, `join_left_named`, `join_right_named`, `join_full_named` | Keyed pairs with the selected unmatched-side behavior |
| Reduce values | `combine_per_key_and_window_named`, `combine_globally_named` | A windowed or global aggregate |
| Preserve order in a fold | `fold_values_ordered_by_named`, `fold_globally_ordered_by_named` | Uses an explicit order key across runners |
| Split keyed work | `group_into_batches_named`, `group_into_batches_with_limits_named` | Bounded vectors per key and window |

A portable runner may evaluate a handler again after a failed bundle. Handler
code should give the same result for the same input. A shuffle does not keep
the order in which values arrived. The [transform guide](../guides/transforms.md)
includes examples and the constraints on ordered operations.

## Assign time and control firings

| Task | Entry point | Result or constraint |
| --- | --- | --- |
| Set event time | `assign_timestamps_named` | Adds or replaces element timestamps |
| Use fixed or sliding windows | `fixed_windows`, `sliding_windows` | Finite event-time intervals |
| Use sessions | `session_windows_named`, `group_by_session_named` | Intervals merged by a keyed session operation |
| Emit early panes | `early_count_trigger_named`, `early_processing_time_trigger_named` | Firings before ordinary window closure |
| Retain values across panes | `accumulating_count_trigger_named`, `accumulating_processing_time_trigger_named` | Accumulating early firings |
| Accept late input | `allowed_lateness_named`, `late_count_trigger_named` | A bounded period and late firing policy |
| Inspect time metadata | `reify_timestamps_named`, `reify_panes_named` | Values paired with timestamp or pane information |

Trigger methods set the collection's window policy. The subsequent grouping
or aggregation produces panes. See [event time and windows](../guides/windowing.md)
and [streaming pipelines](../guides/streaming.md) for the supported schedules.

## Execute and publish

| Task | Entry point | Behavior |
| --- | --- | --- |
| Name a result | `PCollection::output` | Registers a stable terminal key |
| Collect bounded values | `collect_seq`, `DirectRunner::run_collect` | Executes through Direct and returns values |
| Start a streaming job | `DirectRunner::start_stream_sink` | Returns a `StreamJob` for progress, cancellation, and completion |
| Attach an external sink | `sink_named_with_config_and_finalization` | Records sink identity and finalization policy |
| Lower to Beam | `ironbeam-graphx` | Converts the logical graph to a portable pipeline protobuf |

External sinks need retry-safe writes. A `DONE` runner status alone does not
prove that a staged result was published. The [I/O guide](../guides/io.md)
describes finalization, and the [support boundary](support.md) records the
runner proofs for this release.
