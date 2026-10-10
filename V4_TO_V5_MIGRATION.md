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
| `.count_globally()`, `.distinct()`, `.sum_globally()` and related helpers | `ElementwiseExt` / `KeyedExt` methods such as `.count_globally_named(key)` | `ironbeam_compat::prelude` imports all v5 helper extension traits and Direct collection terminals; individual traits can also be imported from `ironbeam_transforms`. |
| Predicate filters, regex, top/bottom K, set/dictionary, and fallible map helpers | `PredicateExt`, `RegexExt`, `SelectionExt`, `KeyedSelectionExt`, `CollectionExt`, `KeyedCollectionExt`, `TryProcessExt` | Each graph node needs a stable key; invalid regex patterns now return a typed graph error. |
| Top and bottom K performance | `SelectionExt`, `KeyedSelectionExt` | Linear bounded Direct inputs now feed size-K lifted heaps without materializing the source; complex upstream graphs may still materialize before the fold. |
| `collect_seq_sorted`, `collect_par_sorted`, `collect_par_sorted_by_key`, `to_hashmap`, `collect_fail_fast` | `DirectSortedExt`, `DirectKeySortedExt`, `DirectKeyedCollectExt`, `DirectFallibleExt` | Direct-only terminals take an output key and can accept `JobConfig`; fallible errors become typed execution errors. |
| `sample_globally`, `sample_per_key`, seeded reservoir variants | `SamplingExt`, `KeyedSamplingExt` | Seeded priority ranking matches v4; linear Direct sources now feed an ordered size-K reservoir. |
| Ordered sampling performance | `OrderedFoldFn` in core and the same sampling extension traits | Linear bounded Direct inputs now stream into size-K reservoirs. The fold has no merge operation, preserving v4's encounter-order seed sequence. |
| `ApproxQuantiles`, `ApproxMedian`, `TDigest` | `StatisticalExt`, `KeyedStatisticalExt`, `TDigest` | V5 validates quantile and compression parameters when building the graph. Non-finite observations are ignored as in v4. |
| `side_vec`, `side_hashmap`, `side_singleton`, `side_multimap` and their map/filter helpers | `ConstantSideInputExt` in `ironbeam-transforms` | Small immutable constants are captured in a graph transform; side values used by a parallel closure must be `Sync`. For bounded side collections produced by a pipeline, use core `map_with_side_named` or `DynamicSideInputExt` for singleton, map, and multimap views. |
| `Validate`, `ValidationMode`, `ErrorCollector`, `validate_*` | `ValidationExt`, `KeyedValidationExt` in `ironbeam-transforms` | Direct reports fail-fast validation as a typed execution error. Log collection is a local side effect; record ordinals can vary with parallel execution. |
| `map_catching`, `flat_map_catching`, `DeadLetter` | `DeadLetterExt`, `DeadLetter` in `ironbeam-transforms` | The success and error collections branch from one classification node. Use `classify_catching_named` with Direct's `collect_partitioned_named` (or the flat variants) to collect both arms in one job. Running two branch collections still executes the shared node twice. |
| `tee`, `tee_n` | `TeeExt` in `ironbeam-transforms` | Clones graph handles without cloning elements. Separate Direct jobs execute the common source separately; shared execution across multiple outputs needs a multi-output job. |
| `reshuffle`, `wait_on` | `reshuffle_named(key)`, `wait_on_named(key, &signal)` | Direct uses a materialized stage boundary and drains a bounded signal before evaluating data. These paths currently require memory for the materialized stage; shared dependencies may be evaluated twice. |
| `map_batches`, `batch_elements`, `batch_by_size`, `map_values_batches`, `group_into_batches` | Corresponding `*_named` methods on v5 `PCollection` | Bounded Direct batch helpers use stable keys and typed errors. V5 forms deterministic chunks per assigned window or key instead of v4's physical partition boundaries; the current paths materialize input. Keyed value mapping preserves each record's timestamp and requires one output per input. |
| `join_inner`, `join_left`, `join_right`, `join_full` | `PCollection::join_*_named(key, &right)` | Direct preserves duplicate matches and emits `Option` for missing outer sides. The v5 `Option<T>` coder has a stable tagged representation. Bounded fixed-window joins match by key and assigned window. |
| Timestamp and fixed-window helpers | `assign_timestamps_named(key, f)`, `timestamps_from_pairs_named(key)`, `reify_timestamps_named(key)`, then `fixed_windows(key, size_ns)` | Timestamps live in envelope metadata as signed nanoseconds. Tuple conversion uses `(i64, T)` and replaces v4's unsigned-millisecond `Timestamped<T>` wrapper; convert millisecond data to nanoseconds explicitly. Direct groups, joins, and combines separately per bounded fixed window; assign timestamps before windowing. Watermarks, triggers, and unbounded execution remain separate work. |
| `key_by_window`, `group_by_window`, keyed `key_by_window`, `group_by_key_and_window` | `WindowedExt::{key_by_window_named, group_by_window_named}` and `KeyedWindowedExt::{key_by_key_and_window_named, group_by_key_and_window_named}` | These produce the same window/key/value group shapes using signed nanosecond `WindowInterval` keys. The keyed keying method has a distinct name so importing both extension traits does not make method calls ambiguous. |
| `latest_globally`, `latest_per_key` | `latest_globally_named(key)`, `latest_per_key_named(key)` | Selects the maximum envelope event timestamp per global or fixed window, with first-seen ties. Empty global input returns a typed execution error. |
| `combine_per_window` and numeric/windowed keyed variants | `combine_per_window_named`, `combine_per_key_and_window_named` in core; `WindowedExt` and `KeyedWindowedExt` for sum/count/min/max/average | Windows use signed nanoseconds and `WindowInterval` values with a stable coder. Offset windows are supported. Each convenience helper expands into stable keyed graph nodes and executes on bounded Direct. |
| `cogroup_by_key!` for two through ten keyed inputs | `cogroup_by_key_named!(key, inputs...)` or the corresponding `cogroup_by_key_N_named` function | Returns one tuple of value vectors per key and assigned window. V5 adds a stable key, typed construction errors, and tuple coders through arity ten. Direct currently materializes every input. |
| `CombineFn`, lifted global and keyed combines | `ironbeam_core::CombineFn`, `combine_globally_lifted_named`, `combine_values_lifted_named` | Linear bounded Direct sources flow into typed accumulators in source batches, including fixed-window scopes. Complex upstream graphs currently materialize before folding, and aggregate checkpointing remains unavailable. |
| KMV and HLL++ approximate distinct helpers | `ApproxDistinctExt`, `KeyedApproxDistinctExt` | Linear bounded Direct inputs now flow into mergeable sketches. Precision and KMV size are validated or normalized when building the graph. |
| T-digest quantiles and median | `StatisticalExt`, `KeyedStatisticalExt` | Linear bounded Direct inputs now flow into compressed digests. Non-finite observations are ignored, and empty global median remains `NaN`. |
| `read_arrow_ipc`, `write_arrow_ipc_rows`, batch variants | `arrow_ipc_source`, `arrow_ipc_sink`, `arrow_ipc_batches_source`, `arrow_ipc_batches_sink` behind `io-arrow` | V5 supports bounded row and batch files, lazy and finite range sources, and deferred parallel sinks. `arrow_ipc_batch_sharded_source` and `arrow_ipc_batches_indexed_source` use the file's batch index as v4 does; older row and batch range sources can rescan earlier batches. |
| `ironbeam::testing` assertions, builders, fixtures, mock files, and debug helpers | `ironbeam-testing` package | Test utilities are isolated from production core. Its debug count runs as a Direct terminal; graph inspection and sampling are passthrough transforms. The package stays local while parity and release gates are reviewed. |
| `read_jsonl_streaming`, `read_csv_streaming` | `jsonl_streaming_source`, `csv_streaming_source` | These are lazy readers for bounded files. They hash the file at graph construction and reject changes before reading, then parse records on demand during Direct execution. They do not declare an unbounded source. |
| `read_avro_streaming`, `read_parquet_streaming`, Arrow IPC streaming readers | `avro_streaming_source`, `parquet_streaming_source`, `arrow_ipc_streaming_source`, `arrow_ipc_batches_streaming_source` | These also read bounded files lazily after validating source identity. `parquet_row_group_sharded_source` accepts v4-style row groups per shard and reads only those groups. |
| `read_xml_streaming` | `xml_streaming_source` | As in v4, XML uses one whole-file shard; the record count hint is accepted but does not split the input. |
| `read_*` with a local file glob | corresponding `*_glob_source(pipeline, key, pattern)` in `ironbeam-io` | Matches are sorted before flattening; each file's source key derives from its path and its content is checked before execution. An empty match returns an I/O error. |
| `read_cloud_jsonl_glob` | `cloud_jsonl_glob_source` with `ironbeam-io/cloud` | A provider-neutral object glob expands in sorted order; each object keeps a path-derived source key and content digest. The v5 source is bounded and runs on Direct. |
| `build_*_shards` followed by streaming reads | corresponding `*_sharded_source` in `ironbeam-io` | Pass a positive line, row, record, or batch count per shard. Each finite range has a stable graph key and verifies the bound file content before execution. JSONL blank lines count toward line ranges; CSV headers do not count as data rows. Container formats and compressed inputs can rescan from the beginning for each shard. |
| SQL/MongoDB shard descriptors and range reads | `sql_sharded_source`, `mongodb_sharded_source` | Supply the known total count, a positive shard size, and a snapshot ID so graph construction stays free of database reads. SQL requires a total `ORDER BY`; MongoDB sorts by `_id`. A short range fails recovery, and the underlying database snapshot must remain unchanged across retries. |
| `write_sql_par_with`, `write_mongodb_par` | `sql_parallel_sink_with`, `mongodb_parallel_sink` | The v5 sinks append at Direct job commit using independent shard connections or clients. Successful shards are not rolled back when another fails; use application-level deduplication before retrying. |
| `read_msgpack_streaming`, `read_cbor_streaming`, `read_proto_streaming`, `read_tfrecord_streaming` | Corresponding `*_streaming_source` and `*_sharded_source` functions in `ironbeam-io` | These decode bounded files and validate the source fingerprint before execution. TFRecord CRCs and protobuf framing errors are checked while records are read. Finite range sources can rescan earlier records. Typed `tf.Example` records use `tfrecord_examples_source` and its lazy or sharded forms. |
| Joins, flatten, combines, side inputs | Corresponding `*_named` graph operations | Keys, finite-input requirements, and backend capabilities are checked at construction/validation. Consult `PHASE4_TRANSFORMS_IO.md`. |
| `.collect_seq()`, `.collect_par(Some(threads), Some(partitions))` | `.output(key)?.collect_seq()`, `.output(key)?.collect_par(parallelism)` | Import `DirectCollectExt`; collection happens through an explicit output and `DirectRunner`. |
| Immediate `.write_jsonl(path)` and similar methods | `ironbeam_io::jsonl_sink(&input, key, path)?`, then `DirectRunner.run_sink(&sink, &config)` | Sink construction is deferred; `FinalResult` files publish after job success. No immediate row count is returned. |
| `read_jsonl(&pipeline, path)` and other file readers | `ironbeam_io::jsonl_source(&pipeline, key, path)` | v5 file sources bind path and content digest at graph build time; a changed file fails on execution. |
| `write_avro_with_schema(path, schema)` | `avro_sink(&input, key, path, schema)` | v5 writes native Avro records; `avro_source` reads native containers. `avro_json_*` handles earlier v5 preview wrappers. |
| v4 custom `DynOp` and local `NodeId` | Registered v5 semantic operation and stable graph key | Compiled v4 closures and node IDs cannot be imported into a v5 graph. |

The full v4 public method inventory is in `V5_PHASE0_API_INVENTORY.md`; current implementation status and physical execution gaps are in `V5_HELPER_PARITY_TRACKER.md`. In particular, v4 `read_*_streaming` describes lazy **bounded** file reads, not an unbounded event stream. Provider-neutral cloud contracts, helpers, and compressed JSONL object readers/writers live in `ironbeam-cloud`; `cloud_jsonl_source` and `cloud_jsonl_sink` attach a provider-neutral object store to a bounded Direct graph. SQL, MongoDB, TFRecord, and Arrow IPC have opt-in bounded Direct adapters. Keep unsupported remote or unbounded connector workloads on v4 until runner support exists.

## Materialized data handoff

`ironbeam-compat` supports `v4_to_v5(v4_collection, &v5_pipeline, key)` and `v5_to_v4(&v5_collection, &v4_pipeline, output_key, &job_config)`. Both execute the source locally and copy all values into a new bounded in-memory source. The element type must implement both versions' `Element` traits, plus `Clone` and `Sync`; built-in integers and strings work directly. A custom type implements v5 `Element` with stable `TYPE_ID` and `CODER_ID` declarations in addition to the v4 bounds. Errors identify the v4 or v5 side of the bridge.

These helpers are for finite data that fits in memory. They do not transfer graph topology, lazy execution, event-time/window metadata, source progress, sink state, metrics, checkpoint state, or remote execution. The v4 side uses its sequential local runner. The v5 side uses Direct and the supplied `JobConfig`. A backend-specific Dataflow graph cannot cross this boundary.

## Features and behavior

| v4 feature | v5 status |
| --- | --- |
| `io-jsonl`, `io-csv`, `io-parquet`, `io-avro`, `io-xml` | `ironbeam-io` features `jsonl`, `csv`, `parquet`, `avro`, `xml`; the current facade forwards these only with `v5-preview`. |
| `io-msgpack`, `io-cbor`, `io-protobuf`, `io-tfrecord` | v5 bounded source/sink adapters, lazy and sharded readers, parallel sinks, and external compression are available under corresponding `ironbeam-io` features. `tfrecord_examples_source` (plus lazy and sharded variants) decodes wire-compatible TensorFlow `tf.Example` records when both TFRecord and Protobuf are enabled. |
| `io-arrow`, `io-sql`, `io-mongodb` | Arrow IPC has bounded row and batch v5 adapters. SQL and MongoDB forward to opt-in `ironbeam-io` features when `v5-preview` is enabled. SQL uses `sql_source`, `sql_streaming_source`, and `sql_sink_with`; MongoDB uses `mongodb_source`, `mongodb_streaming_source`, and `mongodb_sink`. Both database sinks are deferred append operations with no replay guarantee. |
| provider-neutral cloud utilities | `ironbeam-cloud` or `ironbeam_io::cloud` with the `cloud` feature | The v4 `ironbeam::io::cloud` and `ironbeam::helpers::cloud` paths reexport the same implementation, including compressed JSONL object readers/writers. |
| `compression-gzip`, `compression-zstd`, `compression-bzip2`, `compression-xz` | Shared `ironbeam-cloud` codec features and matching opt-in `ironbeam-io` features. Cloud JSONL object readers/writers and v5 JSONL/CSV file adapters use them; remaining file-format combinations are tracked separately. |
| `parallel-io` | `ironbeam-io` parallel sinks for all local file formats | Pass an explicit positive shard count; v5 writes ordered fragments and assembles them before final atomic publish. XML, Avro, Parquet, and Arrow fragments are complete containers that are read and rewritten into the final container. Direct job parallelism still controls execution separately. |
| `metrics` | v5 `JobHandle::metrics` is part of Direct; no feature flag. |
| `checkpointing`, `spilling` | v5 Direct `JobConfig` has optional `CheckpointConfig` and `SpillConfig`; no feature flag. v4 checkpoint files are not v5 state. |
| `coders` | v5 `Element` declares type/coder identity and version; built-in coders are implemented, custom coder support must be supplied explicitly. No global Serde bound is added by a feature. |
| `default` | v4 keeps its existing broad defaults; v5 packages have their own defaults. `ironbeam-core` and `ironbeam-direct` are minimal, while `ironbeam-io` defaults to five file formats. |

Sequential elementwise output is compared in order in the migration tests. Grouped, joined, sampled, and parallel results should be compared as multisets unless the operation documents an order. V5 validates boundedness and backend capabilities before opening sources or sinks. V5 graph, coder, and checkpoint versions are independent; unsupported versions fail explicitly rather than being interpreted as v4 state.

## Release boundary

`ironbeam-compat` depends on published Ironbeam 4.1.1 under the `ironbeam-v4` alias and directly on the v5 packages. The root facade still exposes v5 as a preview module and must not depend on `ironbeam-compat`. See `PHASE5_COMPAT_RELEASE.md` for the staged publish order and checks.
