# Runners

A runner turns a selected logical graph into executable work. Ironbeam 5 includes the local Direct runner in `ironbeam-direct`. Its `ironbeam-graphx`, `ironbeam-harness`, and `ironbeam-dataflow` crates provide Apache Beam graph lowering, a worker, and a Dataflow driver.

## Direct execution

Direct runs in the application process. A named output and `JobConfig` provide the graph selection and per-job settings:

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
    println!("processed {} source elements", job.metrics().source_elements);
    Ok(())
}
```

`JobConfig` contains settings for parallelism, memory budgets, spilling, and [checkpoints](../guides/checkpoints.md). A runner can reject an unsupported graph before it opens a source or sink. `DirectRunner.explain` reports its physical planning choices without executing a source; the [planning example](https://github.com/nhubbard/ironbeam/blob/main/examples/explain_pipeline.rs) demonstrates that report.

## Job lifecycle

A Direct job has a `JobHandle` with an identifier, status, processed-element count, and metrics snapshot. Its status values are `Submitted`, `Running`, `Draining`, `Succeeded`, `Failed`, and `Cancelled`. The `cancel` method signals cancellation; a running operation observes that signal at supported boundaries. A completed `run_collect` call returns the values and the handle, while an execution error is returned as `Error`. The exact status transition depends on the selected graph and whether the work completes, fails, or is cancelled.

## Beam portability

The portable integration translates supported nodes into Beam protobuf and supplies a Rust worker. Local Apache Beam Prism has been used to verify the translation and worker. The same logical graph is compared with Direct for cases supported by both runners. Its [proof fixture](https://github.com/nhubbard/ironbeam/blob/main/examples/prism-proof/README.md) covers bounded and unbounded input, event time, windowing, and final results.

A successful local Prism job does not establish hosted Dataflow operation. Hosted deployment also requires artifact distribution, credentials, remote resource access, and worker lifecycle handling. The [portable execution guide](../guides/portable.md) states the local requirements and scope.

Prism owns the worker process environment and the active bundle schedule. If its active worker disconnects, the current local job fails and Ironbeam's final result is not published. Ironbeam can reopen checkpointed source and finalization state in a new worker process, but replacing a failed worker inside the same Prism job has not been demonstrated.
