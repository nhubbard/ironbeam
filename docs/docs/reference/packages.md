# Package map

Ironbeam 5 is a workspace of small crates. Applications can depend on the root facade or select individual crates.

| Package | Responsibility |
| --- | --- |
| `ironbeam` | User-facing facade and prelude |
| `ironbeam-core` | Graph, collections, elements, coders, source and sink contracts |
| `ironbeam-direct` | Local runner, planning, checkpointing, and spill support |
| `ironbeam-transforms` | Common convenience transforms |
| `ironbeam-io` | Shared bounded I/O and publication infrastructure |
| `ironbeam-io-jsonl`, `ironbeam-io-csv`, `ironbeam-io-avro`, `ironbeam-io-parquet`, `ironbeam-io-arrow` | Individual file formats |
| `ironbeam-io-xml`, `ironbeam-io-msgpack`, `ironbeam-io-cbor`, `ironbeam-io-protobuf`, `ironbeam-io-tfrecord` | Additional file formats |
| `ironbeam-io-sql`, `ironbeam-io-mongodb` | Database adapters |
| `ironbeam-cloud-objects` | Provider-neutral object URI and storage contract |
| `ironbeam-cloud-service-api` | Other provider-neutral service contracts |
| `ironbeam-cloud-fakes`, `ironbeam-cloud-ops` | Test implementations and generic operations |
| `ironbeam-cloud-object-store` | Optional S3, GCS, and Azure Blob clients |
| `ironbeam-testing`, `ironbeam-metrics` | Test helpers and metrics |
| `ironbeam-coder-postcard`, `ironbeam-compression` | Optional coding and compression |

The separate `ironbeam-dataflow` workspace contains graph lowering, Beam protobuf definitions, the worker harness, and a local Prism proof. See [portable execution](../guides/portable.md).
