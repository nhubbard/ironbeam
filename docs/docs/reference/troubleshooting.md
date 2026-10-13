# Troubleshooting

Graph-construction errors occur before execution. Runner errors occur during planning or execution and usually identify the operation or capability involved. The following cases cover common causes.

## Duplicate or invalid graph key

A source, transform, sink, or output key must be unique within a pipeline. A duplicate key returns an `Error` from the call that adds the node. Stable keys are also required when later runs refer to the same graph through a checkpoint or portable job. [Pipelines and collections](../concepts/pipelines.md) describes graph identity.

## Missing method or format import

Common v5 collection methods are supplied by extension traits in `ironbeam::prelude::*`. A format may also require its own crate or root feature, such as `v5-io-csv`. The [I/O guide](../guides/io.md) lists the format features. Cargo recompiles affected packages when a feature selection changes.

## File not found

Relative paths are resolved from the application's working directory. Repository examples assume execution from the repository root. An absolute path removes that dependence. An in-memory `from_vec` source can isolate transform behavior from file access while investigating an error.

## Checkpoint rejected

Recovery validates the saved job identity against the current graph, source, coder, and binary build ID. A v4 checkpoint cannot be loaded by version 5. A mismatch must be resolved without overwriting state required by an interrupted job. The [checkpoint guide](../guides/checkpoints.md) describes the identity fields.

## Portable preflight rejected

Direct executes callbacks from a local registry. A portable worker needs the corresponding handler and coder, and its lowering must support the graph shape. Ordered folds require explicit order keys. The [portable execution guide](../guides/portable.md) describes the local Prism proof and its limits.

## Documentation site absent after a pull request

Pull requests build the Zensical site to check its sources. Publication occurs after a push to `main` or a manual CI run on `main`. GitHub Pages uses the workflow artifact as its source. The configured address is [nhubbard.github.io/ironbeam](https://nhubbard.github.io/ironbeam/). The [documentation project README](https://github.com/nhubbard/ironbeam/blob/main/docs/README.md) describes local preview.
