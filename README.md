# Ironbeam

Ironbeam 5 is a modular Rust workspace for typed data pipelines. The root
`ironbeam` crate now uses the v5 graph, transforms, Direct runner, and shared
I/O by default on this conversion branch. File formats, databases, and hosted
object storage live in separate crates so applications build the extensions
they use.

The root package is an unpublished 5.0.0 candidate on this branch. The v4
reference corpus remains available through the compatibility crate against
published 4.1.1. Final release checks and ordered workspace publication are
still in progress.

The [Ironbeam 5 documentation site](https://nhubbard.github.io/ironbeam/) contains getting-started guidance, concepts, format and runner guides, migration steps, and runnable example links. Its Zensical source is in [`docs/`](docs/README.md).

## Quick start

```rust
use ironbeam::{Pipeline, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    let result = pipeline.from_vec("numbers", vec![1_i32, 2, 3, 4])?
        .map_named("double", |value| value * 2)?
        .filter_named("large", |value| *value > 4)?
        .collect_seq()?;
    assert_eq!(result, vec![6, 8]);
    Ok(())
}
```

Graph nodes take stable unique keys. Building a graph returns `Result`;
Direct collection on a `PCollection` creates a private terminal. Use an
explicit `.output("result")?` for a named portable or durable terminal. Import
`ironbeam::prelude::*` for helper methods and Direct collection terminals.
The same v5 API remains available under `ironbeam::v5::*` during migration.

## Workspace packages

| Package | Responsibility |
| --- | --- |
| `ironbeam-core` | Logical graph, `PCollection`, typed elements and coders, errors, source and sink contracts |
| `ironbeam-coder-postcard` | Opt-in Postcard coding for Serde user records with stable type and coder IDs |
| `ironbeam-direct` | Local execution, planning, metrics, checkpointing, and spill behavior |
| `ironbeam-transforms` | Common convenience transforms and keyed helpers |
| `ironbeam-io` | Shared file lifecycle and bounded source/sink infrastructure |
| `ironbeam-cloud-objects` | Provider-neutral object URIs and storage operations |
| `ironbeam-cloud-service-api` | Provider-neutral contracts for non-object service extensions |
| `ironbeam-cloud-fakes` | In-memory implementations of the cloud service contracts for tests |
| `ironbeam-cloud-ops` | Provider-neutral retry, pagination, batching, and operation helpers |
| `ironbeam-cloud-object-store` | Opt-in S3, GCS, and Azure Blob clients |
| `ironbeam-cloud-gcp` | Opt-in official GCP Storage, Pub/Sub, BigQuery, and Secret Manager clients |

Regex and approximate distinct helpers have separate extension crates. Avro,
Arrow IPC, CBOR, CSV, JSONL, MessagePack, MongoDB, Parquet, Protobuf, SQL,
TFRecord, and XML also have individual crates. The root facade exposes them
through `v5-io-*` features; the common default keeps heavyweight connectors
out of the build. Existing `io-*` format flags also enable their matching v5
crate for migration; both spellings now select the same isolated v5 format
crate.

`ironbeam-io-jsonl` also owns eager object JSONL read, write, and glob
helpers when its `cloud` feature is enabled.

Enable the root `coders` feature, or depend on `ironbeam-coder-postcard`
directly, to use `impl_postcard_element!(Row, "my-app/row/v1")` for a Serde
record. Increment the ID when its wire representation changes; the same type
and coder must be present in portable workers.

For example, a program using CSV and native Avro can enable
`v5-io-csv` and `v5-io-avro`. A program using hosted storage can depend on
`ironbeam-cloud-object-store` and register the providers it needs with the
object URI router. The object contract itself does not depend on a provider
SDK.

For Google Cloud applications, enable the root `cloud-gcp` feature or depend
on `ironbeam-cloud-gcp` directly. Its four service features can be selected
individually, including through root `cloud-gcp-*` flags; each client uses
Application Default Credentials. The
[Google Cloud guide](docs/docs/guides/gcp.md) shows common operations and
required access.

## Execution and compatibility

`DirectRunner` executes bounded and supported unbounded pipelines locally. The
workspace's `beam-proto`, `ironbeam-graphx`, `ironbeam-harness`, and
`ironbeam-dataflow` crates lower v5 graphs to Beam protobuf and run bounded
and unbounded feature cases through a separately launched worker with local
Prism. The root's optional `v5-dataflow` feature exposes the Dataflow driver.
The [portable execution guide](docs/docs/guides/portable.md) describes the
worker and local proof. Its release gaps and the root cutover gates are tracked
in [V5_RELEASE_GATE_STATUS.md](V5_RELEASE_GATE_STATUS.md).

The root implementation is v5-only. The published 4.1.1 package remains the
reference for the separate `ironbeam-compat` test corpus, and the
[v4 README](V4_README.md) describes its old API. See the
[migration guide](V4_TO_V5_MIGRATION.md) for concrete API changes and the
[replacement roadmap](V5_REPLACEMENT_ROADMAP.md) for the workspace boundary.
The local release candidate is undergoing its final connector audit;
publication remains staged at the user's request.

Run the comparable local benchmark examples with:

```sh
cargo run --example v5_benchmark_corpus -- 1000
cargo run -p ironbeam-compat --example v4_benchmark_corpus -- 1000
```
