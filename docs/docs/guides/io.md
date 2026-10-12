# Input and output

Ironbeam 5 separates common I/O infrastructure from format implementations. The default root facade includes JSONL. CSV, Avro, Parquet, Arrow IPC, XML, CBOR, MessagePack, Protobuf, TFRecord, SQL, and MongoDB have opt-in crates and root feature flags. This keeps unused connector dependencies out of a common build.

## Enable a format

```toml
[dependencies]
ironbeam = { path = "/path/to/ironbeam", features = ["v5-io-csv", "v5-io-avro"] }
```

The older `io-csv` and `io-avro` flags are aliases for their v5 format crates. The dependency entry above is a local-checkout example; use a registry version when the packages have been published.

## JSONL example

```rust
use ironbeam::v5::{Pipeline, jsonl_source, prelude::*};

let pipeline = Pipeline::default();
let values = jsonl_source::<i64>(&pipeline, "input", "values.jsonl")?
    .map_named("double", |n| n * 2)?
    .collect_seq()?;
# Ok::<(), ironbeam::v5::Error>(())
```

See the repository's [ETL example](https://github.com/nhubbard/ironbeam/blob/main/examples/etl_pipeline.rs) for CSV and JSONL in one pipeline, and [cloud I/O example](https://github.com/nhubbard/ironbeam/blob/main/examples/cloud_io_demo.rs) for object storage. SQL and MongoDB have opt-in bounded Direct adapters. Their legacy append sinks require application-managed replay behavior; do not assume exactly-once effects from an append operation.
