# Ironbeam 4 to 5 migration guide

Ironbeam 5 is currently exposed as `ironbeam::v5` behind `v5-preview`, and as the `ironbeam-core`, `ironbeam-direct`, `ironbeam-transforms`, and `ironbeam-io` packages. The root `ironbeam` package is still version 4.1.1. This guide describes the implemented v5 surface; it does not imply that the root package has been released as version 5.

## A bounded pipeline

Ironbeam 4:

```rust
use ironbeam::{Pipeline, from_vec};
let pipeline = Pipeline::default();
let values = from_vec(&pipeline, vec![1_i32, 2, 3, 4]);
let result = values.map(|n| n * 2).filter(|n| n % 4 == 0).collect_seq()?;
```

Ironbeam 5, using the compatibility source helper and terminal prelude:

```rust
use ironbeam_compat::{from_vec, prelude::*};
use ironbeam_core::Pipeline;
let pipeline = Pipeline::default();
let result = from_vec(&pipeline, "numbers", vec![1_i32, 2, 3, 4])?
    .map_named("double", |n| n * 2)?
    .filter_named("divisible-by-four", |n| n % 4 == 0)?
    .output("result")?
    .collect_seq()?;
```

Every v5 node needs a stable, unique key. Transform construction returns `Result`; `output` creates an explicit terminal. Use the same key and semantic closure version when restarting a job from a checkpoint. `collect_par` on the v5 output takes a concrete parallelism value, whereas v4 accepts optional threads and partitions. Direct execution settings belong to `JobConfig`, not global runner state.

## Public API mapping

| Ironbeam 4 | Ironbeam 5 | Important change |
| --- | --- | --- |
| `Pipeline`, `PCollection<T>` | `ironbeam_core::{Pipeline, PCollection<T>}` | Separate graph and element types; no type cast or graph conversion. |
| `from_vec(&pipeline, values)` | `pipeline.from_vec(key, values)` or `ironbeam_compat::from_vec(&pipeline, key, values)` | Stable source key is required; construction can fail. |
| `.map(f)`, `.filter(f)`, `.flat_map(f)` | `.map_named(key, f)`, `.filter_named(key, f)`, `.flat_map_named(key, f)` | Values, keys, and graph errors are explicit. |
| `.key_by(f)`, `.group_by_key()` | `ElementwiseExt::key_by_named(key, f)`, `PCollection::group_by_key_named(key)` | Import `ironbeam_transforms::ElementwiseExt` for `key_by_named`. |
| `.count_globally()`, `.distinct()`, `.sum_globally()` and related helpers | `ElementwiseExt` / `KeyedExt` methods such as `.count_globally_named(key)` | Import the extension traits from `ironbeam_compat::prelude` or `ironbeam_transforms`. |
| Joins, flatten, combines, side inputs | Corresponding `*_named` graph operations | Keys, finite-input requirements, and backend capabilities are checked at construction/validation. Consult `PHASE4_TRANSFORMS_IO.md`. |
| `.collect_seq()`, `.collect_par(Some(threads), Some(partitions))` | `.output(key)?.collect_seq()`, `.output(key)?.collect_par(parallelism)` | Import `DirectCollectExt`; collection happens through an explicit output and `DirectRunner`. |
| Immediate `.write_jsonl(path)` and similar methods | `ironbeam_io::jsonl_sink(&input, key, path)?`, then `DirectRunner.run_sink(&sink, &config)` | Sink construction is deferred; `FinalResult` files publish after job success. No immediate row count is returned. |
| `read_jsonl(&pipeline, path)` and other file readers | `ironbeam_io::jsonl_source(&pipeline, key, path)` | v5 file sources bind path and content digest at graph build time; a changed file fails on execution. |
| `write_avro_with_schema(path, schema)` | `avro_sink(&input, key, path, schema)` | v5 writes native Avro records; `avro_source` reads native containers. `avro_json_*` handles earlier v5 preview wrappers. |
| v4 custom `DynOp` and local `NodeId` | Registered v5 semantic operation and stable graph key | Compiled v4 closures and node IDs cannot be imported into a v5 graph. |

The full v4 public method inventory, including helpers that have no v5 equivalent yet, is in `V5_PHASE0_API_INVENTORY.md`. In particular, v4 `read_*_streaming` describes lazy **bounded** file reads, not an unbounded event stream. MongoDB, SQL, TFRecord, Arrow IPC, and the other optional v4 connectors are not v5 I/O backends yet. Keep those workloads on v4 or hand off finite materialized data.

## Materialized data handoff

`ironbeam-compat` supports `v4_to_v5(v4_collection, &v5_pipeline, key)` and `v5_to_v4(&v5_collection, &v4_pipeline, output_key, &job_config)`. Both execute the source locally and copy all values into a new bounded in-memory source. The element type must implement both versions' `Element` traits, plus `Clone` and `Sync`; built-in integers and strings work directly. A custom type implements v5 `Element` with stable `TYPE_ID` and `CODER_ID` declarations in addition to the v4 bounds. Errors identify the v4 or v5 side of the bridge.

These helpers are for finite data that fits in memory. They do not transfer graph topology, lazy execution, event-time/window metadata, source progress, sink state, metrics, checkpoint state, or remote execution. The v4 side uses its sequential local runner. The v5 side uses Direct and the supplied `JobConfig`. A backend-specific Dataflow graph cannot cross this boundary.

## Features and behavior

| v4 feature | v5 status |
| --- | --- |
| `io-jsonl`, `io-csv`, `io-parquet`, `io-avro`, `io-xml` | `ironbeam-io` features `jsonl`, `csv`, `parquet`, `avro`, `xml`; the current facade forwards these only with `v5-preview`. |
| `io-msgpack`, `io-cbor`, `io-protobuf`, `io-tfrecord`, `io-arrow`, `io-sql`, `io-mongodb` | v4 only; no v5 connector claim. |
| `compression-gzip`, `compression-zstd`, `compression-bzip2`, `compression-xz` | v4 only; no v5 I/O codec feature yet. |
| `parallel-io` | v4 only; v5 Direct uses per-job parallelism. |
| `metrics` | v5 `JobHandle::metrics` is part of Direct; no feature flag. |
| `checkpointing`, `spilling` | v5 Direct `JobConfig` has optional `CheckpointConfig` and `SpillConfig`; no feature flag. v4 checkpoint files are not v5 state. |
| `coders` | v5 `Element` declares type/coder identity and version; built-in coders are implemented, custom coder support must be supplied explicitly. No global Serde bound is added by a feature. |
| `default` | v4 keeps its existing broad defaults; v5 packages have their own defaults. `ironbeam-core` and `ironbeam-direct` are minimal, while `ironbeam-io` defaults to five file formats. |

Sequential elementwise output is compared in order in the migration tests. Grouped, joined, sampled, and parallel results should be compared as multisets unless the operation documents an order. V5 validates boundedness and backend capabilities before opening sources or sinks. V5 graph, coder, and checkpoint versions are independent; unsupported versions fail explicitly rather than being interpreted as v4 state.

## Release boundary

`ironbeam-compat` depends on published Ironbeam 4.1.1 under the `ironbeam-v4` alias and directly on the v5 packages. The root facade still exposes v5 as a preview module and must not depend on `ironbeam-compat`. See `PHASE5_COMPAT_RELEASE.md` for the staged publish order and checks.
