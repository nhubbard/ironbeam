# Portable execution

A portable runner consumes an Apache Beam graph rather than executing a Rust pipeline directly. The separate `ironbeam-dataflow` project lowers supported Ironbeam 5 graph nodes to Beam protobuf and supplies a worker for Rust handlers. Apache Beam Prism is used as a local job service for the current portability proof.

## Execution path

A pipeline is constructed with stable source, transform, and output keys. Lowering checks whether its operations and coders can be represented in the Beam graph. Prism receives the graph and schedules work. The separate worker executes registered handlers, reads supported sources, and writes supported sinks. The job result is checked after Prism reports completion.

The same logical pipeline can also run with Direct. Runner behavior is compared using element values, event-time metadata, window intervals, and observable errors where both runners support the graph. A shuffle does not retain input encounter order, so portable ordered folds and larger ordered keyed batches require an explicit order key. A handler's semantic version identifies its behavior across deployments.

## Local proof

The [Prism proof README](https://github.com/vgough/ironbeam-dataflow/blob/main/examples/prism-proof/README.md) describes the local JobService, worker, and fixtures. Once Prism is serving `localhost:8073`, its principal test is:

```sh
IRONBEAM_PRISM_ENDPOINT=localhost:8073 cargo test -p ironbeam-prism-proof --test prism_v5_run
```

That command runs from the separate `ironbeam-dataflow` checkout. The fixture includes bounded and unbounded sources, multiple file formats, keyed and windowed transforms, and finalized outputs. Optional database proofs require local services and additional features.

## Portability requirements

A graph intended for this path needs coders available to the worker, supported source and sink adapters, stable keys, and registered custom handlers with semantic versions. Preflight rejects an unsupported operation before opening its source or sink. The [runner concepts](../concepts/runners.md) page describes local execution; the [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md) records the verified Prism cases.

The local Prism proof does not establish a hosted Dataflow deployment. Cloud credentials, artifact staging, worker replacement, and remote object storage have separate operational requirements. The [GCP client guide](gcp.md) covers resource access from application or worker code; it does not configure a hosted runner.
