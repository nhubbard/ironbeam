# Packages and Cargo features

Ironbeam 5 separates graph construction, execution, formats, and cloud
services into crates. The root `ironbeam` crate reexports the common API. An
application can use that facade or depend on a narrower crate when it does
not need the whole surface.

## Use the facade

During development, version 5.0.0 is unpublished. A local application can
point at the workspace root:

```toml
[dependencies]
ironbeam = { path = "/path/to/ironbeam" }
```

After publication, use a registry version and enable only the optional
formats or services the application needs:

```toml
[dependencies]
ironbeam = { version = "5.0.0", features = ["v5-io-parquet", "cloud-gcp-storage"] }
```

`Pipeline`, `PCollection`, and the Direct runner are available from the
facade. `v5-io-parquet` adds the Parquet adapter; `cloud-gcp-storage` adds the
official Google Cloud Storage client. The root crate's default features are
`cloud` and `validation-json`. Format and provider clients remain separate
features so applications can avoid compiling unrelated dependencies.

## Depend on a focused crate

A library that constructs graphs without running them can use
`ironbeam-core`. A runner integration can add `ironbeam-graphx` and
`ironbeam-harness`. A format extension can depend on `ironbeam-core` and the
shared `ironbeam-io` contracts without importing every format crate. See the
[package map](../reference/packages.md) for each crate's role.

The Ironbeam workspace uses dependency entries with both a local `path` and a
`version`. Cargo resolves the path while developing the workspace and records
the version requirement in a published package. Applications outside the
workspace should use registry versions after the release. The repository does
not require a separate local clone of Apache Beam to compile ordinary
Ironbeam applications; the `beam-proto` crate contains generated protocol
types.

## Select a format

| Data | Crate or facade feature | Typical use |
| --- | --- | --- |
| JSON Lines | `ironbeam-io-jsonl` | Line-oriented records and portable final results |
| CSV | `v5-io-csv` | Tabular text files |
| Avro | `v5-io-avro` | Avro object containers with reader schemas |
| Parquet | `v5-io-parquet` | Columnar files |
| Arrow IPC | `v5-io-arrow` | Arrow record batches or typed rows |
| Protobuf and TFRecord | `v5-io-protobuf`, `v5-io-tfrecord` | Length-delimited messages and TFRecord files |
| XML, CBOR, MessagePack | `v5-io-xml`, `v5-io-cbor`, `v5-io-msgpack` | Optional interchange formats |
| SQL and MongoDB | `v5-io-sql`, `v5-io-mongodb` | Database sources and sinks |

Some formats also expose `parallel-io` or compression features. Enable those
only when the application uses their corresponding readers or writers. The
[I/O guide](io.md) describes source, sink, and publication behavior.

## Keep worker dependencies aligned

A portable worker rebuilds the application's graph and registers its Rust
handlers. It must use compatible package versions, coder identities, graph
keys, and semantic handler versions. A different source or sink configuration
digest fails the worker contract before work is accepted. Build the driver and
worker from the same application revision whenever possible.
