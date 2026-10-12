# Pipelines and collections

A `Pipeline` owns a logical graph. A `PCollection<T>` refers to a typed collection produced by a graph node; it does not contain the resulting rows. Source and transform calls extend the graph and return new collection handles. A named `Output<T>` identifies a terminal result for execution.

## Graph keys

Every source, transform, and output has a key supplied by the application. Keys must be unique in the graph. Keep them stable across runs when using checkpoints or portable jobs. A duplicate or invalid graph operation returns an `Error` during construction.

```rust
use ironbeam::{Pipeline, prelude::*};

let pipeline = Pipeline::default();
let words = pipeline.from_vec("input-words", vec!["a".to_string(), "bb".to_string()])?;
let lengths = words.map_named("word-length", |word| word.len())?;
let output = lengths.output("lengths")?;
# Ok::<(), ironbeam::Error>(())
```

For callbacks that may change between deployments, use the versioned transform APIs and advance the semantic version when behavior changes. The graph descriptor contains stable metadata; executable Rust closures remain in the local registry.

## Outputs

`collect_seq` and related Direct helpers add a private terminal to a snapshot of the graph. An explicit `output("key")` is preferred when a runner, checkpoint, or external system needs the same terminal identity on subsequent runs. Recollecting a collection does not mutate the source pipeline with another terminal.
