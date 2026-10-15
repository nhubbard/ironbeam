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
| `ironbeam-cloud-gcp` | Official Google Cloud Storage, Pub/Sub, BigQuery, and Secret Manager clients |
| `ironbeam-testing`, `ironbeam-metrics` | Test helpers and metrics |
| `ironbeam-coder-postcard`, `ironbeam-coder-row`, `ironbeam-compression` | Optional coding, Beam schema rows, and compression |
| `beam-proto`, `ironbeam-graphx` | Beam protocol types and portable graph lowering |
| `ironbeam-harness`, `ironbeam-dataflow` | Fn API worker and Dataflow job driver |

The nonpublishable `ironbeam-prism-proof` package exercises these crates against a local Prism job service. See [portable execution](../guides/portable.md).

The GCP crate has one feature per service. See the [Google Cloud guide](../guides/gcp.md) for authentication and examples.
