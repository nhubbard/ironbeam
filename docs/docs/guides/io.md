# Input and output

Ironbeam 5 separates common I/O contracts from format implementations. The root package includes JSON Lines (JSONL) by default. Other formats are selected individually, which limits the dependencies compiled by an application that uses only a few formats.

## Format selection

| Data or service | Package or root feature | Typical use |
| --- | --- | --- |
| JSON Lines | `ironbeam-io-jsonl`; included by default | One JSON value per line |
| CSV | `v5-io-csv` | Delimited records |
| Avro | `v5-io-avro` | Records with an Avro schema |
| Parquet | `v5-io-parquet` | Columnar files |
| Arrow IPC | `v5-io-arrow` | Arrow arrays and record batches |
| Protobuf, TFRecord | `v5-io-protobuf`, `v5-io-tfrecord` | Coded messages and TensorFlow frames |
| XML, CBOR, MessagePack | Corresponding `v5-io-*` features | Structured interchange |
| SQL, MongoDB | `v5-io-sql`, `v5-io-mongodb` | Database sources and sinks |

The [package reference](../reference/packages.md) lists the full crate set. Older `io-*` feature names remain aliases for the corresponding v5 format crates; they do not restore v4 method signatures.

The following dependency enables CSV and Avro in a local checkout:

```toml
[dependencies]
ironbeam = { path = "/path/to/ironbeam", features = ["v5-io-csv", "v5-io-avro"] }
```

## Reading JSONL

A JSONL file contains one JSON value per line. For example, `values.jsonl` containing the following two lines can be read as `i64` values:

```json
2
3
```

```rust
use ironbeam::{Pipeline, jsonl_source, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    let values = jsonl_source::<i64>(&pipeline, "input", "values.jsonl")?
        .map_named("double", |value| value * 2)?
        .collect_seq()?;
    assert_eq!(values, vec![4, 6]);
    Ok(())
}
```

The source key identifies the graph node; the file path identifies the data. A relative path is resolved from the program's working directory.

## Reading Avro with a reader schema

An Avro container stores its writer schema. A reader schema can select fields
or supply defaults for fields added after the file was written. The following
reader accepts a file written with `id` and `name` fields and supplies
`active: true` for older records:

```rust
use ironbeam::{Element, Pipeline, avro_source_with_reader_schema, prelude::*};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
struct Account {
    id: i32,
    name: String,
    active: bool,
}

impl Element for Account {
    const TYPE_ID: &'static str = "example/account-v2";
    const CODER_ID: &'static str = "example/account-v2";
}

fn main() -> Result<(), ironbeam::Error> {
    let schema = r#"{"type":"record","name":"Account","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"active","type":"boolean","default":true}]}"#;
    let pipeline = Pipeline::default();
    let accounts = avro_source_with_reader_schema::<Account>(
        &pipeline, "accounts", "accounts.avro", schema,
    )?.collect_seq()?;
    println!("{} accounts", accounts.len());
    Ok(())
}
```

Use `avro_streaming_source_with_reader_schema` with the same arguments to
decode records incrementally from a large file. For multiple local files,
`avro_glob_source_with_reader_schema` accepts a glob pattern in place of the
path. `avro_sharded_source_with_reader_schema` accepts a file path, a positive
record count per shard, and then the reader schema:

```rust
let accounts = avro_sharded_source_with_reader_schema::<Account>(
    &pipeline, "accounts", "accounts.avro", 1_000, schema,
)?;
```

All four forms validate the schema while building the pipeline and include it
in source identity. A resumed job rejects a different schema or changed file
bytes. Sharded reads count the records once while building the graph, then
rescan the file for each shard. These file sources read local paths on Direct;
a portable job needs an accessible storage source and a stable element coder.

Native Avro objects can also be read through `cloud_avro_uri_source` or
`cloud_avro_uri_source_with_reader_schema`. Register a provider in
`ObjectStores`, then pass a URI such as `file://bucket/records.avro` and, for
the second form, the reader schema. The URI and object bytes are bound to source
configuration; the reader schema also changes the graph fingerprint. The
source reads the object once while building the graph and again when it runs.
It rejects a changed object. `cloud_avro_uri_prefix_source` and its
`_with_reader_schema` form read all objects under a literal URI prefix in
sorted order. The object set is fixed when the graph is built; a later object
does not silently enter a resumed job. This adapter currently uses the
registered provider in the local process. The bundled object-store backend is
local. GCP's official storage client has a separate asynchronous API and does
not yet implement this registry's `ObjectIO` contract. Portable worker access also
needs an execution binding. Enable the Avro crate's `cloud` feature for these
functions; the root `cloud` feature forwards to it when `v5-io-avro` is also
enabled.

## Writing a result

A sink is a terminal operation. It is run with `DirectRunner.run_sink` rather than collected into a vector:

```rust
use ironbeam::{DirectRunner, JobConfig, Pipeline, jsonl_sink, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("values", vec![2_i64, 3])?
        .map_named("double", |value| value * 2)?;
    let sink = jsonl_sink(&values, "output", "doubled.jsonl")?;
    DirectRunner.run_sink(&sink, &JobConfig::default())?;
    Ok(())
}
```

The complete [ETL example](https://github.com/nhubbard/ironbeam/blob/main/examples/etl_pipeline.rs) reads, aggregates, writes, and reopens CSV and JSONL files.

## Replay and publication

A file or database write is an external effect. An append sink can repeat that effect if a job retries. The sink's replay contract therefore matters independently of the graph's element results. Portable final-result sinks use accepted bundle fragments and publish the complete result after the runner reports completion. Database append sinks require application-level replay handling unless a replay-safe adapter is used.

The [checkpoint guide](checkpoints.md) describes local recovery state. The [portable execution guide](portable.md) describes the separate worker and its finalization boundary. Provider-neutral object routing is in `ironbeam-cloud-object-store`; direct Google Cloud clients are described in the [GCP guide](gcp.md).
