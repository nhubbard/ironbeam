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

### JSONL in object storage

`cloud_jsonl_streaming_uri_source` reads a JSONL object through a registered
provider. The `cloud` feature of `ironbeam-io-jsonl` enables this source.
`cloud_jsonl_streaming_uri_glob_source` selects matching objects in sorted key
order when the graph is built. For example:

```rust
use ironbeam_cloud_object_store::object_stores_from_env;
use ironbeam_core::Pipeline;
use ironbeam_io_jsonl::cloud_jsonl_streaming_uri_source;

let stores = object_stores_from_env()?;
let pipeline = Pipeline::default();
let values = cloud_jsonl_streaming_uri_source::<i64>(
    &pipeline, "input", &stores, "gs://example-bucket/values.jsonl",
)?;
```

The object-store crate needs its `gcp` feature for `gs://` URIs. The source
hashes the object when building the graph and checks it again before emitting
records. It streams the second read into a temporary file, then decodes lines
as requested. Temporary disk use grows with the object size; the provider
reader and decoder do not hold the whole object in memory. A changed object
fails recovery. A portable worker needs access to the same provider and
credentials as the submitting process. A local Prism fixture has also reopened
a JSONL object glob after an execution child was replaced and committed each
row once.

## CSV in object storage

CSV objects can be read through `cloud_csv_streaming_uri_source` in the
`ironbeam-io-csv` crate. Its `cloud` feature enables provider-bound input;
the root `cloud` feature forwards to it when `v5-io-csv` is enabled. The
`cloud_csv_streaming_uri_prefix_source` variant fixes a sorted set of objects
when the graph is built. Both forms verify each object's bytes before
emitting records and decode CSV rows as requested from a temporary file. The
temporary file requires disk space proportional to the object size. A
portable worker needs access to the same provider and credentials as the
submitting process. A local Prism fixture has reopened a two-object CSV prefix
after execution-child replacement and committed each row once.

## Protobuf in object storage

`cloud_protobuf_streaming_uri_source` reads length-delimited Protobuf messages
through a registered object provider. Its type parameter is a Rust type that
implements `prost::Message`, `Default`, and Ironbeam's `Element` trait. The
`cloud_protobuf_streaming_uri_prefix_source` variant fixes a sorted set of
objects when the graph is built. Both forms verify object bytes before
emitting messages and decode incrementally from a temporary file. Temporary
disk use grows with each object. The crate's `cloud` feature enables these
functions; the root `cloud` feature forwards to it when `v5-io-protobuf` is
enabled. A portable worker needs the same provider and credentials as the
submitting process. A local Prism job reopened a two-object prefix after an
execution child was replaced and committed both messages once.

## CBOR and MessagePack in object storage

`ironbeam-io-cbor` and `ironbeam-io-msgpack` each provide a provider-bound
streaming source. `cloud_cbor_streaming_uri_source` and
`cloud_msgpack_streaming_uri_source` read one object; their
`*_uri_prefix_source` variants fix a sorted set of objects when the graph is
built. The input is a sequence of encoded records, as written by the
corresponding format sink. The Rust record type implements `Element` and
`serde::Deserialize`.

Enable the format crate's `cloud` feature. The root `cloud` feature forwards
to it when `v5-io-cbor` or `v5-io-msgpack` is selected. Each object is hashed
when the graph is built and verified again when a worker opens it. A changed
object fails recovery. The worker streams one object to a temporary file and
decodes records as requested, so temporary disk use grows with the largest
object in a source split. Portable workers need access to the same provider
and credentials as the submitting process.
The local Prism corpus reopens both prefixes after an execution child is
replaced and commits each record once.

## XML in object storage

`cloud_xml_uri_source` reads one XML document through a registered object
provider. `cloud_xml_uri_prefix_source` reads a sorted set of documents under
one URI prefix. Both functions bind each object's bytes to the graph and check
them again when the source opens. A changed object fails recovery. The XML
decoder loads one complete document into memory, so memory use grows with the
size of the largest object. The `cloud` feature of `ironbeam-io-xml` enables
these functions; the root `cloud` feature forwards to it when `v5-io-xml` is
enabled. In the following example, `Record` implements `Element` and
`serde::Deserialize`.

```rust
use ironbeam_cloud_object_store::object_stores_from_env;
use ironbeam_io_xml::cloud_xml_uri_prefix_source;

let stores = object_stores_from_env()?;
let rows = cloud_xml_uri_prefix_source::<Record>(
    &pipeline,
    "records",
    &stores,
    "gs://data-bucket/records/",
)?;
```

`gs://` requires the `gcp` feature of `ironbeam-cloud-object-store`. The worker
must register a provider for the same URI scheme. A local Prism
job reopened a two-object XML prefix after execution-child replacement and
committed each row once.

## Reading TFRecord frames

`tfrecord_streaming_source` reads frames incrementally and checks their length
and data checksums. Each collection element is the frame's byte payload; an
application can decode a TensorFlow `Example` or another payload format in a
subsequent transform.

```rust
use ironbeam::{Pipeline, prelude::*};
use ironbeam_io_tfrecord::tfrecord_streaming_source;

fn main() -> Result<(), ironbeam::Error> {
    let pipeline = Pipeline::default();
    let frames = tfrecord_streaming_source(&pipeline, "input", "records.tfrecord")?
        .collect_seq()?;
    println!("{} frames", frames.len());
    Ok(())
}
```

The file's content hash is part of the source configuration. A worker opening
the source after submission rejects changed file bytes. In a portable job,
the worker must be able to read the same path as the submitting process. A
local Prism job has exercised the incremental source in a separate worker
process and matched Direct's decoded frames.

TFRecord objects can also be read with `cloud_tfrecord_streaming_uri_source`.
The `cloud_tfrecord_streaming_uri_prefix_source` variant fixes a sorted set of
objects at graph construction. Both functions use registered object providers,
verify each object's bytes before emitting frames, and check frame checksums
as they decode. They need temporary disk space proportional to each object.
The crate's `cloud` feature enables these functions; the root `cloud` feature
forwards to it when `v5-io-tfrecord` is enabled. A portable worker needs the
same provider and credentials as the submitting process. In a local Prism
job, the source reopened a two-object prefix after an execution child was
replaced and committed each frame once.

## Reading Avro with a reader schema

An Avro container stores its writer schema. A reader schema can select fields
or supply defaults for fields added after the file was written. It can also
promote an `int` to a `long` and use a field alias when a reader renames a
writer field. Aliases are resolved in records nested inside arrays, maps,
nullable unions, and named schema references, including namespaced references;
ambiguous aliases fail. The following
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
does not silently enter a resumed job. For large objects, the corresponding
`cloud_avro_streaming_uri_source` and
`cloud_avro_streaming_uri_prefix_source` forms use a provider's `open_object`
method. Both have `_with_reader_schema` forms. They hash each object at graph
construction, then spool and verify it before reading records incrementally.
The temporary file uses disk space proportional to the object size and is
removed when the source closes. This adapter uses the registered
provider in the local process. For a Direct job reading Google Cloud Storage,
enable `ironbeam-io-avro`'s `cloud` feature and
`ironbeam-cloud-object-store`'s `gcp` feature, then build the registry with
`object_stores_from_env()` and pass a `gs://bucket/key.avro` URI. The hosted
adapter uses the `object_store` GCS client and Application Default
Credentials. Its sequential reader keeps at most two unread transfer chunks
and passes stream errors to the format reader. Avro and Parquet streaming
sources have been tested through this adapter with an in-memory `object_store`
backend. The separate `ironbeam-cloud-gcp` client does not itself
implement this registry's `ObjectIO` contract. A local Prism fixture now
rebuilds the same `file://` provider binding in the worker and streams an Avro
object with a reader schema that adds a defaulted field. Direct and Prism
produce the same decoded rows. In a separate local Prism test, the execution
child is killed during the first decoded Avro row. Its replacement reopens the
`file://` object and commits both rows once through an idempotent sink. A worker
using `gs://` still needs its own GCS provider configuration, credentials, and a
controlled service proof. The root `cloud` feature
forwards to Avro's `cloud` feature when `v5-io-avro` is also enabled. With an
enabled compression feature, object names such as `records.avro.gz` are
decoded before Avro schema resolution.

For example, the `Account` type and reader schema above can read one GCS
object through the hosted registry:

```rust
use ironbeam_cloud_object_store::object_stores_from_env;
use ironbeam_io_avro::cloud_avro_streaming_uri_source_with_reader_schema;

let stores = object_stores_from_env()?;
let pipeline = Pipeline::default();
let accounts = cloud_avro_streaming_uri_source_with_reader_schema::<Account>(
    &pipeline, "accounts", &stores, "gs://example-bucket/accounts.avro", schema,
)?.collect_seq()?;
```

The source reads the object at graph construction, so this form requires
credentials and network access in the process that builds the graph. The
provider must implement sequential object reads. The original nonstreaming
forms still load the full object and decoded rows into memory.

Parquet has opt-in `cloud_parquet_uri_source` and
`cloud_parquet_uri_prefix_source` functions. For larger objects, use
`cloud_parquet_streaming_uri_source` or
`cloud_parquet_streaming_uri_prefix_source` with a provider that implements
`open_object`. Enable the Parquet crate's `cloud` feature, register an
`ObjectIO` provider, and pass a URI such as
`file://bucket/records.parquet` or `gs://bucket/records.parquet`. The prefix
form fixes its matching object set when the graph is built. New objects require
a new graph; each matched object is checked for changes when it opens.
The URI and object bytes identify the source, and a changed object is rejected
when execution opens it. The root `cloud` feature forwards to Parquet's
`cloud` feature when `v5-io-parquet` is enabled. A local Prism job rebuilds a
`file://` provider in the worker and streams the same three rows from two
Parquet objects as Direct. Flatten may deliver those rows in a different order.
In a separate local Prism test, the execution child is killed while it processes
a row from the second object. The replacement reopens the prefix and commits
all three rows once through an idempotent sink.
The streaming variant hashes each object at graph construction. When a source
opens, it spools the object to an automatically removed temporary file,
checks its hash, and decodes one Parquet record batch at a time. It uses disk
space proportional to the object size. The original variants still read the
complete object and decoded rows into memory. Remote worker provider binding
remains to be proved.

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
