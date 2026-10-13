# Pipelines and collections

A `Pipeline` owns a logical graph. A `PCollection<T>` is a typed handle to a graph node; it is not a container of completed rows. Source and transform calls extend the graph and return handles for later operations. A named `Output<T>` identifies a terminal result that a runner can execute.

## Graph construction

Each node records its operation, input relationships, element type, and stable key. Construction checks the validity of the proposed node before a job starts. A repeated key or invalid connection therefore returns an `Error` at the call that adds it.

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let words = pipeline.from_vec("input-words", vec!["a".to_string(), "bb".to_string()])?;
    let lengths = words.map_named("word-length", |word| word.len())?;
    let output = lengths.output("lengths")?;
    let _ = output;
    Ok(())
}
```

The source key `input-words` and transform key `word-length` name distinct nodes. `lengths` names the output. These keys are part of graph identity; a pipeline that resumes from a checkpoint or is lowered to another runner relies on stable names.

## Handles and execution

Building a graph does not read its source or invoke a transform callback. Execution begins when a runner receives an output or sink. The Direct helper `collect_seq` takes a snapshot, adds a private terminal, and returns values from that execution. It does not add a permanent terminal to the original pipeline.

A collection handle can feed more than one branch. Each branch receives its own node key, and a runner selects the nodes needed for its requested terminal. The [planning example](https://github.com/nhubbard/ironbeam/blob/main/examples/explain_pipeline.rs) shows a fused elementwise route and a grouping route that needs materialization.

## Handler identity

The graph descriptor stores stable metadata. Executable Rust closures remain in a local handler registry. A changed callback can therefore require a new semantic version even if its node key stays the same. Versioned transform APIs make that change explicit. Portable execution also requires the corresponding handler and coder to be present in the worker; see [portable execution](../guides/portable.md).
