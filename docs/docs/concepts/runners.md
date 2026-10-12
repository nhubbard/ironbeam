# Runners

A runner executes the graph built by a `Pipeline`. Ironbeam 5 has a local Direct runner in `ironbeam-direct`. The separate `ironbeam-dataflow` project lowers supported graphs to Apache Beam protobuf and has been checked with local Prism and a separately launched worker. These are different execution environments for the same logical graph, subject to each runner's capability checks.

## Direct execution

```rust
use ironbeam::{DirectRunner, JobConfig, Pipeline};

let output = Pipeline::default()
    .from_vec("numbers", vec![1_i32, 2, 3])?
    .map_named("double", |n| n * 2)?
    .output("result")?;
let (values, job) = DirectRunner.run_collect(&output, &JobConfig::default())?;
assert_eq!(values, vec![2, 4, 6]);
println!("Processed {} elements", job.metrics().source_elements);
# Ok::<(), ironbeam::Error>(())
```

`JobConfig` owns per-job settings such as parallelism, memory budgets, checkpointing, and spilling. A runner may reject a graph before opening a source or sink if an operation is unsupported. Consult the [portable execution guide](../guides/portable.md) before moving a pipeline to Prism.

## Local Prism proof

The portable integration lives in a sibling repository, `ironbeam-dataflow`. Its [Prism proof instructions](https://github.com/vgough/ironbeam-dataflow/blob/main/examples/prism-proof/README.md) cover the local worker and fixture. The present release proof does not claim hosted cloud execution.
