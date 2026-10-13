# Ironbeam 5

Ironbeam is a Rust library for data pipelines. An application builds a typed graph of sources, transforms, and outputs; a runner executes that graph. Version 5 places the graph, Direct runner, format adapters, transforms, and cloud interfaces in separate crates.

The 5.0.0 source is a release candidate. Local Direct execution and the workspace's Apache Beam integration have been tested with Prism. The [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md) records the evidence and limits of those tests. Hosted Dataflow execution has not been established by the local Prism proof.

## Documentation

| Subject | Description |
| --- | --- |
| [Getting started](getting-started.md) | A complete local pipeline and its output |
| [Pipelines and collections](concepts/pipelines.md) | Graph nodes, typed handles, and stable keys |
| [Runners](concepts/runners.md) | Direct execution and the portable boundary |
| [Input and output](guides/io.md) | File formats, sinks, and feature selection |
| [Transforms](guides/transforms.md) | Element operations, grouping, and keyed work |
| [Direct checkpoints](guides/checkpoints.md) | Recovery state and identity |
| [Migration from v4](migration/index.md) | API and operational changes |
| [Package reference](reference/packages.md) | Crate ownership |
| [Example programs](reference/examples.md) | Runnable programs in the repository |

The pages describe the v5 source branch. Examples use explicit graph keys so that the relationship between source code and executed work remains visible.
