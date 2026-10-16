# Portable execution

A portable runner consumes an Apache Beam graph rather than executing a Rust pipeline directly. The `ironbeam-graphx` crate lowers supported Ironbeam 5 graph nodes to Beam protobuf, and `ironbeam-harness` runs Rust handlers through the Beam Fn API. `ironbeam-dataflow` supplies job preparation and Google Cloud Dataflow submission. Apache Beam Prism serves as the local job service for the portability tests.

## Execution path

A pipeline is constructed with stable source, transform, and output keys. Lowering checks whether its operations and coders can be represented in the Beam graph. Prism receives the graph and schedules work. An application binary that embeds the worker harness executes registered handlers, reads supported sources, and writes supported sinks. The job result is checked after Prism reports completion.

The same logical pipeline can also run with Direct. Runner behavior is compared using element values, event-time metadata, window intervals, and observable errors where both runners support the graph. A shuffle does not retain input encounter order, so portable ordered folds and larger ordered keyed batches require an explicit order key. A handler's semantic version identifies its behavior across deployments.

## Local proof

The [Prism proof README](https://github.com/nhubbard/ironbeam/blob/main/examples/prism-proof/README.md) describes the local JobService, worker, and fixtures. Once Prism is serving `localhost:8073`, its principal test is:

```sh
IRONBEAM_PRISM_ENDPOINT=localhost:8073 cargo test -p ironbeam-prism-proof --test prism_v5_run
```

That command runs from the Ironbeam repository root. The fixture includes bounded and unbounded sources, multiple file formats, keyed and windowed transforms, and finalized outputs. Optional database proofs require local services and additional features. The [Dataflow WordCount example](https://github.com/nhubbard/ironbeam/tree/main/examples/dataflow-wordcount) shows how an application supplies its pipeline and worker entry point.

## Portability requirements

A graph intended for this path needs coders available to the worker, supported source and sink adapters, stable keys, and registered custom handlers with semantic versions. Preflight rejects an unsupported operation before opening its source or sink. The [runner concepts](../concepts/runners.md) page describes local execution; the [release gate report](https://github.com/nhubbard/ironbeam/blob/main/V5_RELEASE_GATE_STATUS.md) records the verified Prism cases.

The local Prism proof does not establish a hosted Dataflow deployment. Cloud credentials, artifact staging, worker replacement, and remote object storage have separate operational requirements. The [GCP client guide](gcp.md) covers resource access from application or worker code; it does not configure a hosted runner.

## Supervised worker recovery

A worker process can keep its Fn Control connection in a stable parent while
running each bundle in an isolated child. Set
`IRONBEAM_V5_SUPERVISE_EXECUTION=1` in the worker process environment. The
parent buffers a bundle's Fn Data input, runs the child, and forwards output
only after that child succeeds. If the child exits unexpectedly, the parent
can replay the input once. The current implementation limits buffered input
to 64 MiB per bundle and reports an error when it is exceeded.

This mode checks the graph before opening sources. Each source needs a replay
contract. Each sink must be idempotent or implement bundle finalization. A
finalizing sink also needs a durable journal. The local fixture uses
`IRONBEAM_V5_FINALIZATION_JOURNAL_DIR` pointing to a directory shared by
replacement children. The production Prism fixture
uses those settings and verifies same-job recovery across a killed child,
source residual, multiple sinks, and finalized formats. If the parent process
itself disconnects from Prism, the active job still fails closed.

## Object-backed finalization

A worker whose replacement processes do not share a filesystem can store
bundle-finalization records under one object URI prefix. The
`ObjectFinalizationJournal` validates each record, its completion marker, the
execution identity, and the set of declared sinks. After a runner reports
`DONE`, `PortablePublicationPlan::complete_object` combines that journal with
the runner's committed sink counts to authorize result publication. The object
provider must support conditional creation, read-after-write visibility, and
consistent prefix listing.

Use `ironbeam_dataflow::run_v5_from_env_with_object_journal` to pass an
`ObjectStores` registry and journal URI to the worker. The harness carries the
same binding into supervised execution children. The application must rebuild
the same registry and pass the same URI whenever the worker binary starts.
The file-journal environment variable must be unset when using an object
journal.

Local object-provider tests cover a replacement worker recovering a pending
bundle and completing its finalization callback. The current Dataflow driver
does not yet retrieve committed Fn metrics or prove the hosted GCS provider
path, so remote result publication remains unverified.
