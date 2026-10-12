# Ironbeam 5

Ironbeam is a Rust library for constructing typed data pipelines. A pipeline describes sources, transforms, and outputs as a logical graph. A runner executes that graph. Version 5 separates the graph, Direct runner, transforms, file formats, and cloud contracts into individual crates.

The source tree contains an unpublished 5.0.0 candidate. Its local execution and local Prism portability checks are recorded in the [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md). Hosted Dataflow execution is outside that local proof.

## Start here

- [Getting started](getting-started.md) builds and runs a small pipeline.
- [Pipeline concepts](concepts/pipelines.md) explains graph nodes, keys, and outputs.
- [Transforms](guides/transforms.md) shows mapping, filtering, grouping, and joins.
- [I/O](guides/io.md) explains the format crates and feature flags.
- [Migration](migration/index.md) provides a path from Ironbeam 4.
- [Example index](reference/examples.md) links to programs in the repository.

The documentation describes the current source branch. Its code examples use the v5 API and name graph nodes explicitly.
