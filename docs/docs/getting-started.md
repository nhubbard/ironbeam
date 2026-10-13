# Getting started

Ironbeam 5 requires Rust 1.94 or newer. This example uses the Direct runner, which executes a pipeline in the application process. It does not require a service or cloud account.

## Project setup

The 5.0.0 packages are not yet published. An application can use a local checkout as a path dependency:

```toml
[dependencies]
ironbeam = { path = "/path/to/ironbeam" }
```

The path refers to the root of the Ironbeam checkout. For a new application, `cargo new ironbeam-first-pipeline` creates the package structure. The dependency belongs in its `Cargo.toml`.

## A bounded pipeline

The following `src/main.rs` reads four values from memory, doubles them, and retains values greater than four:

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    let result = pipeline
        .from_vec("numbers", vec![1_i32, 2, 3, 4])?
        .map_named("double", |value| value * 2)?
        .filter_named("large", |value| *value > 4)?
        .collect_seq()?;

    println!("{result:?}");
    assert_eq!(result, vec![6, 8]);
    Ok(())
}
```

`cargo run` prints `[6, 8]`. `from_vec` adds a bounded source to the graph. `map_named` and `filter_named` add transforms. `collect_seq` creates a private output and asks Direct to execute it. The calls return `Result` because graph construction can fail before a runner starts.

## A named output

A stable terminal key is useful when a runner, checkpoint, or external system must identify the same output in later runs:

```rust
use ironbeam::{DirectRunner, JobConfig, Pipeline, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("numbers", vec![1_i32, 2, 3])?
        .map_named("double", |value| value * 2)?
        .output("result")?;

    let (values, job) = DirectRunner.run_collect(&output, &JobConfig::default())?;
    assert_eq!(values, vec![2, 4, 6]);
    println!("read {} elements", job.metrics().source_elements);
    Ok(())
}
```

`numbers`, `double`, and `result` are graph keys. Each key must be unique within this pipeline. Stable keys also matter to [checkpoint recovery](guides/checkpoints.md) and [portable execution](guides/portable.md).

The [pipeline concepts](concepts/pipelines.md) page describes collection handles. The [I/O guide](guides/io.md) replaces the in-memory source with a file source. Complete programs appear in the [example index](reference/examples.md).
