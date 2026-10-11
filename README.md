# Ironbeam

Ironbeam 5 is a modular Rust workspace for typed data pipelines. The root
`ironbeam` crate now uses the v5 graph, transforms, Direct runner, and shared
I/O by default on this conversion branch. File formats, databases, and hosted
object storage live in separate crates so applications build the extensions
they use.

The 5.0.0 release is still staged locally. The package version remains 4.1.1
while the final v4 removal, portable execution, and release checks are in
progress; this branch is not a published 5.0.0 release.

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
| `ironbeam-cloud-object-store` | Opt-in S3, GCS, and Azure Blob clients |

Regex and approximate distinct helpers have separate extension crates. Avro,
Arrow IPC, CBOR, CSV, JSONL, MessagePack, MongoDB, Parquet, Protobuf, SQL,
TFRecord, and XML also have individual crates. The root facade exposes them
through `v5-io-*` features; the common default keeps heavyweight connectors
out of the build. Existing `io-*` format flags also enable their matching v5
crate for migration; both spellings now select the same isolated v5 format
crate.

Enable the root `coders` feature, or depend on `ironbeam-coder-postcard`
directly, to use `impl_postcard_element!(Row, "my-app/row/v1")` for a Serde
record. Increment the ID when its wire representation changes; the same type
and coder must be present in portable workers.

For example, a program using CSV and native Avro can enable
`v5-io-csv` and `v5-io-avro`. A program using hosted storage can depend on
`ironbeam-cloud-object-store` and register the providers it needs with the
object URI router. The object contract itself does not depend on a provider
SDK.

## Execution and compatibility

`DirectRunner` executes bounded pipelines locally. The separate
[`ironbeam-dataflow`](https://github.com/vgough/ironbeam-dataflow) integration
lowers v5 graphs to Beam protobuf and runs a bounded feature corpus through a
separately launched v5 worker with local Prism. Its release gaps and the root
cutover gates are tracked in [V5_RELEASE_GATE_STATUS.md](V5_RELEASE_GATE_STATUS.md).

The root implementation is v5-only. The published 4.1.1 package remains the
reference for the separate `ironbeam-compat` test corpus, and the
[v4 README](V4_README.md) describes its old API. See the
[migration guide](V4_TO_V5_MIGRATION.md) for concrete API changes and the
[replacement roadmap](V5_REPLACEMENT_ROADMAP.md) for the remaining parity
and release gates. Publication is staged until those gates pass.

Run the comparable local benchmark examples with:

```sh
cargo run --example v5_benchmark_corpus -- 1000
cargo run -p ironbeam-compat --example v4_benchmark_corpus -- 1000
```
