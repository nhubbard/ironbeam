# Ironbeam 4 to 5 migration guide

The root source on this branch is a v5-only `ironbeam` facade in a multi-crate
workspace. The [replacement roadmap](V5_REPLACEMENT_ROADMAP.md) records the
extraction history and the remaining 5.0.0 release gates. The package and
feature descriptions below reflect the current migration state.

Ironbeam 5 is now the root `ironbeam` default on this branch and remains available as `ironbeam::v5`. The root package version is still 4.1.1 while the release gates remain open. Core, Direct, transforms, each file format, SQL, MongoDB, and cloud objects have separate packages. The root facade exposes optional connectors through `v5-io-*` features; old `io-*` names also enable the matching v5 connector. This guide describes the implemented v5 surface, not a published 5.0.0 release.

## A bounded pipeline

Ironbeam 4:

```rust
use ironbeam::{Pipeline, from_vec};
let pipeline = Pipeline::default();
let values = from_vec(&pipeline, vec![1_i32, 2, 3, 4]);
let result = values.map(|n| n * 2).filter(|n| n % 4 == 0).collect_seq()?;
```

Ironbeam 5, using the root v5 facade and its prelude:

```rust
use ironbeam::v5::{Pipeline, prelude::*};
let pipeline = Pipeline::default();
let result = pipeline.from_vec("numbers", vec![1_i32, 2, 3, 4])?
    .map_named("double", |n| n * 2)?
    .filter_named("divisible-by-four", |n| n % 4 == 0)?
    .collect_seq()?;
```

Every v5 graph node needs a stable, unique key. Transform construction returns
`Result`. The Direct `collect_seq` and `collect_par` traits accept a
`PCollection` directly, creating a terminal on a private graph snapshot;
repeating either call does not add nodes to the source pipeline. Use
`.output("result")?` when the terminal must be named explicitly, especially
for portable lowering or durable job identity. Use the same keys and semantic
closure versions when restarting from a checkpoint. `collect_par` takes a
concrete parallelism value, whereas v4 accepts optional threads and
partitions. Direct execution settings belong to `JobConfig`, not global
runner state.

## Public API mapping

| Ironbeam 4 | Ironbeam 5 | Important change |
| --- | --- | --- |
| `Pipeline`, `PCollection<T>` | `ironbeam_core::{Pipeline, PCollection<T>}` | Separate graph and element types; no type cast or graph conversion. |
| `from_vec(&pipeline, values)` | `pipeline.from_vec(key, values)` or `ironbeam_compat::from_vec(&pipeline, key, values)` | Stable source key is required; construction can fail. |
| `build_plan(&pipeline, node_id)?.explain()` | `DirectRunner.explain(&output, &JobConfig::default())?.explain()` | A named `Output` selects the terminal. V5 reports the execution path, route and batch estimates, graph fingerprint, and `PlanDecision` entries for pruning, materialization, and stateless fusion. V4-specific optimization decisions appear only if v5 actually implements the corresponding pass. |
| `.map(f)`, `.filter(f)`, `.flat_map(f)` | `.map_named(key, f)`, `.filter_named(key, f)`, `.flat_map_named(key, f)` | Values, keys, and graph errors are explicit. |
| `.key_by(f)`, `.group_by_key()` | `ElementwiseExt::key_by_named(key, f)`, `PCollection::group_by_key_named(key)` | Import `ironbeam_transforms::ElementwiseExt` for `key_by_named`. |
| `.count_globally()`, `.distinct()`, `.sum_globally()` and related helpers | `ElementwiseExt` / `KeyedExt` methods such as `.count_globally_named(key)` | `ironbeam::v5::prelude` imports v5 helper extension traits and Direct collection terminals without a v4 dependency; individual traits can also be imported from `ironbeam_transforms`. |
| Predicate filters, regex, top/bottom K, set/dictionary, and fallible map helpers | `PredicateExt`, `RegexExt`, `SelectionExt`, `KeyedSelectionExt`, `CollectionExt`, `KeyedCollectionExt`, `TryProcessExt` | Each graph node needs a stable key; invalid regex patterns now return a typed graph error. |
| Top and bottom K performance | `SelectionExt`, `KeyedSelectionExt` | Linear bounded Direct inputs now feed size-K lifted heaps without materializing the source; complex upstream graphs may still materialize before the fold. |
| `collect_seq_sorted`, `collect_par_sorted`, `collect_par_sorted_by_key`, `to_hashmap`, `collect_fail_fast` | `DirectSortedExt`, `DirectKeySortedExt`, `DirectKeyedCollectExt`, `DirectFallibleExt` | Direct-only terminals take an output key and can accept `JobConfig`; fallible errors become typed execution errors. |
| `sample_globally`, `sample_per_key`, seeded reservoir variants | `SamplingExt`, `KeyedSamplingExt` | Seeded priority ranking matches v4; linear Direct sources now feed an ordered size-K reservoir. |
| Ordered sampling performance | `OrderedFoldFn` in core and the same sampling extension traits | Linear bounded Direct inputs now stream into size-K reservoirs. The fold has no merge operation, preserving v4's encounter-order seed sequence. |
| `ApproxQuantiles`, `ApproxMedian`, `TDigest` | `StatisticalExt`, `KeyedStatisticalExt`, `TDigest` | V5 validates quantile and compression parameters when building the graph. Non-finite observations are ignored as in v4. V5 interpolates between ordered centroid midpoints so increasing requested quantiles cannot return decreasing values; this corrects a v4 interpolation defect and can change approximate percentile estimates. |
| `side_vec`, `side_hashmap`, `side_singleton`, `side_multimap` and their map/filter helpers | `ConstantSideInputExt` in `ironbeam-transforms` | Small immutable constants are captured in a graph transform; side values used by a parallel closure must be `Sync`. For bounded side collections produced by a pipeline, use core `map_with_side_named` or `DynamicSideInputExt` for singleton, map, and multimap views. |
| `Validate`, `ValidationMode`, `ErrorCollector`, `validate_*` | `ValidationExt`, `KeyedValidationExt` in `ironbeam-transforms` | Direct reports fail-fast validation as a typed execution error; local Prism fails the job and does not publish output. Skip-invalid results match on both runners. The chosen mode is part of graph identity. Log collection is a process-local side effect, not a portable result; record ordinals can vary with parallel execution. |
| `map_catching`, `flat_map_catching`, `DeadLetter` | `DeadLetterExt`, `DeadLetter` in `ironbeam-transforms` | The success and error collections branch from one classification node. Use `classify_catching_named` with Direct's `collect_partitioned_named` (or the flat variants) to collect both arms in one job. Running two branch collections still executes the shared node twice. |
| `tee`, `tee_n` | `TeeExt` in `ironbeam-transforms` | Clones graph handles without cloning elements. Separate Direct jobs execute the common source separately; shared execution across multiple outputs needs a multi-output job. |
| `reshuffle`, `wait_on` | `reshuffle_named(key)`, `wait_on_named(key, &signal)` | Direct uses a materialized stage boundary and drains a bounded signal before evaluating data. Portable `wait_on` uses a bounded side-input completion barrier, including windowed signals. Shared Direct dependencies may be evaluated twice. |
| `map_batches`, `batch_elements`, `batch_by_size`, `map_values_batches`, `group_into_batches` | Corresponding `*_named` methods on v5 `PCollection` | V5 forms chunks per assigned window or key instead of v4's physical partition boundaries. Size-one row and keyed value batches run incrementally on unbounded input. `map_values_batches_ordered_by_named` runs larger keyed batches on bounded Direct and Prism and on unbounded finite-window input with one source on both runners, when each key/window has unique explicit order keys; it preserves each row's event time and requires one output per input. Direct checkpoints coded fold state with the source cursor for this grouped route; other stateful stream operations remain unsupported. The encounter-order `map_values_batches_named` path above size one remains bounded Direct only. Explicit batch limits are part of graph identity. |
| `join_inner`, `join_left`, `join_right`, `join_full` | `PCollection::join_*_named(key, &right)` | Direct preserves duplicate matches and emits `Option` for missing outer sides. The v5 `Option<T>` coder has a stable tagged representation. Bounded and supported unbounded fixed-window joins match by key and assigned window. Each matched pair uses the earlier of its two event timestamps, independent of runner scheduling or which side the join indexes; an unmatched outer row retains its input timestamp. |
| Timestamp and fixed-window helpers | `assign_timestamps_named(key, f)`, `timestamps_from_pairs_named(key)`, `reify_timestamps_named(key)`, then `fixed_windows(key, size_ns)` | Timestamps live in envelope metadata as signed nanoseconds. Tuple conversion uses `(i64, T)` and replaces v4's unsigned-millisecond `Timestamped<T>` wrapper; convert millisecond data to nanoseconds explicitly. Direct groups, joins, and combines separately per fixed window; assign timestamps before windowing. A v5 `GroupByKey` output uses the earliest input event timestamp in its key/window, independent of arrival order. One-source unbounded Direct GroupByKey and supported two-source joins/co-groups close fixed windows on watermarks and checkpoint open state; custom triggers remain open. |
| `OrdF64` | `ironbeam_core::OrdF64` | V5 keeps v4's `f64::total_cmp` ordering and adds a stable eight-byte coder. Equality and hashing now follow the same total order, so signed zeros and distinct NaN bit patterns remain distinct; v4's derived float equality did not agree with its ordering for those values. |
| `key_by_window`, `group_by_window`, keyed `key_by_window`, `group_by_key_and_window` | `WindowedExt::{key_by_window_named, group_by_window_named}` and `KeyedWindowedExt::{key_by_key_and_window_named, group_by_key_and_window_named}` | These produce the same window/key/value group shapes using signed nanosecond `WindowInterval` keys. The keyed keying method has a distinct name so importing both extension traits does not make method calls ambiguous. |
| `latest_globally`, `latest_per_key` | `latest_globally_named(key)`, `latest_per_key_named(key)` | Selects the maximum envelope event timestamp per global or fixed window. Direct keeps the first-seen value on equal-time ties; Beam shuffle does not guarantee tie order for portable keyed latest. Empty global input returns a typed execution error. Keyed latest lowers through native Beam GroupByKey with event time restored after selection. |
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
| `read_*` with a local file glob | corresponding `*_glob_source(pipeline, key, pattern)` in the format package | Matches are sorted before flattening; each file's source key derives from its path and its content is checked before execution. An empty match returns an I/O error. |
| `read_cloud_jsonl_glob` | `cloud_jsonl_glob_source` with `ironbeam-io/cloud` | A provider-neutral object glob expands in sorted order; each object keeps a path-derived source key and content digest. The v5 source is bounded and runs on Direct. |
| `build_*_shards` followed by streaming reads | corresponding `*_sharded_source` in the format package | Pass a positive line, row, record, or batch count per shard. Each finite range has a stable graph key and verifies the bound file content before execution. JSONL blank lines count toward line ranges; CSV headers do not count as data rows. Container formats and compressed inputs can rescan from the beginning for each shard. |
| SQL/MongoDB shard descriptors and range reads | `sql_sharded_source`, `mongodb_sharded_source` | Supply the known total count, a positive shard size, and a snapshot ID so graph construction stays free of database reads. SQL requires a total `ORDER BY`; MongoDB sorts by `_id`. A short range fails recovery, and the underlying database snapshot must remain unchanged across retries. |
| `write_sql_par_with`, `write_mongodb_par` | `sql_parallel_sink_with`, `mongodb_parallel_sink` | The v5 sinks append at Direct job commit using independent shard connections or clients. Successful shards are not rolled back when another fails; use application-level deduplication before retrying. |
| `read_msgpack_streaming`, `read_cbor_streaming`, `read_proto_streaming`, `read_tfrecord_streaming` | Corresponding `*_streaming_source` and `*_sharded_source` functions in `ironbeam-io` | These decode bounded files and validate the source fingerprint before execution. TFRecord CRCs and protobuf framing errors are checked while records are read. Finite range sources can rescan earlier records. Typed `tf.Example` records use `tfrecord_examples_source` and its lazy or sharded forms. |
| Joins, flatten, combines, side inputs | Corresponding `*_named` graph operations | Keys, finite-input requirements, and backend capabilities are checked at construction/validation. The [replacement roadmap](V5_REPLACEMENT_ROADMAP.md) tracks runner support. |
| One global side view or a side view per fixed window | `map_with_side_named` for one view; `map_with_windowed_side_named` for matching-window values | The per-window method requires matching time/window declarations and a bounded side collection. It expands to a co-group, can buffer a whole window, and restores each main element's event timestamp. Direct and local Prism produce the same values and window metadata in bounded and unbounded-main shared fixtures. Direct requires fixed windows, an idempotent append sink, and source watermarks, and it checkpoints coded open main windows. |
| `.collect_seq()`, `.collect_par(Some(threads), Some(partitions))` | `.collect_seq()`, `.collect_par(parallelism)` on a `PCollection`; explicit `.output(key)?` remains available | Import `DirectCollectExt`. The collection convenience uses a detached Direct terminal and keeps the original graph unchanged. |
| Immediate `.write_jsonl(path)` and similar methods | `ironbeam_io::jsonl_sink(&input, key, path)?`, then `DirectRunner.run_sink(&sink, &config)` | Sink construction is deferred; `FinalResult` files publish after job success. No immediate row count is returned. |
| `read_jsonl(&pipeline, path)` and other file readers | `ironbeam_io::jsonl_source(&pipeline, key, path)` | v5 file sources bind path and content digest at graph build time; a changed file fails on execution. |
| `write_avro_with_schema(path, schema)` | `avro_sink(&input, key, path, schema)` | v5 writes native Avro records; `avro_source` reads native containers. `avro_json_*` handles earlier v5 preview wrappers. |
| v4 custom `DynOp` and local `NodeId` | `DynOp` and `apply_dyn_transform_with_semantic_named`, typed versioned handlers, and stable graph keys | Reimplement the v4 operator against the v5 `DynOp` trait, then register it under the same application ID, version, and explicit batch size in driver and worker. The boxed partition remains `Vec<T>` and wrong output types return an execution error. Batch size one is stateless and works on unbounded Direct and Prism inputs; larger batches use bounded/windowed materialization. The shared bounded fixture also changes row type. Compiled v4 closures and node IDs cannot be imported. Beam shuffle can change encounter order within a batch; v4's implicit partition boundaries and optimizer hints are not yet implemented. `custom_named` remains a marker rejected by Direct. |

The full v4 public method inventory is in `V5_PHASE0_API_INVENTORY.md`;
current implementation status and physical execution gaps are in
`V5_HELPER_PARITY_TRACKER.md`. In particular, v4 `read_*_streaming` describes
lazy **bounded** file reads, not an unbounded event stream. Provider-neutral
object contracts live in `ironbeam-cloud-objects`, service contracts in
`ironbeam-cloud-service-api`, and generic helpers in `ironbeam-cloud-ops`,
with optional provider clients in `ironbeam-cloud-object-store`.
`ironbeam-io-jsonl` supplies eager object read/write/glob helpers and
`cloud_jsonl_source`/`cloud_jsonl_sink` for bounded Direct graphs. SQL,
MongoDB, TFRecord, and Arrow IPC have opt-in bounded Direct adapters.
Unsupported remote or unbounded connector graphs fail capability validation
before source or sink side effects. Published v4 remains a separate reference
package, not a fallback inside this facade.

## Final-result publication

Manual final-result publishers must bind their fragment store with
`require_bundle_attempts` before publishing. The IDs must come from the
completed runner's attempt manifest; for portable local jobs,
`PortablePublicationPlan::complete_local` validates the terminal state,
finalization journal, and committed row counts, then supplies the IDs through
`ReadyLocalPublication::bundle_attempts`. Publication rejects an unbound store,
including when it has no fragments. Pass an empty manifest for a completed
zero-attempt output.

For object-backed portable JSONL, build the graph with
`cloud_jsonl_fragment_sink`, using the same `ObjectIO` location and job,
graph, and sink IDs on the worker and coordinator. After a `DONE` job, reopen
the scope with `cloud_jsonl_fragment_store`, bind its completed attempt IDs,
and call `publish_cloud_jsonl_fragments` with the committed row count. This
creates an uncompressed `.jsonl` object conditionally; identical publication
retries succeed. The older `cloud_jsonl_sink` remains a deferred Direct sink.
The current object API buffers each bundle and the final object in memory.

## Materialized data handoff

`ironbeam-compat` supports `v4_to_v5(v4_collection, &v5_pipeline, key)` and `v5_to_v4(&v5_collection, &v4_pipeline, output_key, &job_config)`. Both execute the source locally and copy all values into a new bounded in-memory source. The element type must implement both versions' `Element` traits, plus `Clone` and `Sync`; built-in integers and strings work directly. A custom type implements v5 `Element` with stable `TYPE_ID` and `CODER_ID` declarations in addition to the v4 bounds. Errors identify the v4 or v5 side of the bridge.

These helpers are for finite data that fits in memory. They do not transfer graph topology, lazy execution, event-time/window metadata, source progress, sink state, metrics, checkpoint state, or remote execution. The v4 side uses its sequential local runner. The v5 side uses Direct and the supplied `JobConfig`. A backend-specific Dataflow graph cannot cross this boundary.

## Features and behavior

| v4 feature | v5 status |
| --- | --- |
| `io-jsonl`, `io-csv`, `io-parquet`, `io-avro`, `io-xml` | JSONL is in the default v5 facade. CSV, Parquet, Avro, and XML each have an isolated crate and a `v5-io-*` feature. The old `io-*` flags remain aliases for the v5 crates; the v4 API is available from published Ironbeam 4.1.1. |
| `io-msgpack`, `io-cbor`, `io-protobuf`, `io-tfrecord` | Each format has a separate v5 crate and `v5-io-*` feature. The old feature names alias to those crates. `tfrecord_examples_source` requires both TFRecord and Protobuf. |
| `io-arrow`, `io-sql`, `io-mongodb` | Arrow IPC, SQL, and MongoDB each have a separate v5 crate and `v5-io-*` feature. Old names alias to the v5 crates. SQL and MongoDB sinks are deferred append operations with application-managed replay. |
| provider-neutral cloud utilities | `ironbeam-cloud-objects` supplies URI routing and local objects; `ironbeam-cloud-object-store` opts into S3, GCS, and Azure Blob clients. `ironbeam-cloud-service-api` owns non-object contracts, `ironbeam-cloud-fakes` supplies in-memory services, and `ironbeam-cloud-ops` owns generic operation helpers. Eager object JSONL read, write, and glob helpers now live in `ironbeam-io-jsonl` behind `cloud`, alongside graph sources and sinks. The old `ironbeam-cloud` paths remain in published Ironbeam 4.1.1 for migration; the legacy crate is absent from the v5 workspace. |
| `compression-gzip`, `compression-zstd`, `compression-bzip2`, `compression-xz` | `ironbeam-compression` owns codec streams. Enabling one of these flags exposes its pluggable codec API under `ironbeam::v5`; `v5-compression` exposes the codec registry without a built-in codec. Shared I/O and JSONL activate matching codecs without legacy cloud helpers. |
| `parallel-io` | `ironbeam-io` parallel sinks and the `parallel-io` feature of each enabled format crate | Pass an explicit positive shard count; v5 writes ordered fragments and assembles them before final atomic publish. The root flag forwards only to formats already selected by their `io-*` or `v5-io-*` feature. XML, Avro, Parquet, and Arrow fragments are complete containers that are read and rewritten into the final container. Direct job parallelism still controls execution separately. |
| `metrics` | v5 `JobHandle::metrics` is part of Direct; no feature flag. |
| `checkpointing`, `spilling` | v5 Direct `JobConfig` has optional `CheckpointConfig` and `SpillConfig` without feature gates. The old feature names are lightweight aliases for the v5 facade; v4 checkpoint files are not v5 state. |
| `coders` | Enables the opt-in `ironbeam-coder-postcard` crate and reexports `impl_postcard_element!(Type, "stable/type/v1")` from `ironbeam::v5`. Serde records can use this macro instead of hand-writing `Element`; wire changes require a new coder ID. V5 keeps an explicit `Element` contract rather than v4's global Serde bound. |
| `default` | The v5-only root defaults to core, Direct runner, common transforms, shared I/O, and JSONL. Other formats remain opt-in. Use published Ironbeam 4.1.1 or `ironbeam-compat` to compare old behavior. |

### Checkpoint policy migration

V4's checkpoint policies schedule **progress markers by graph node, barrier, or
elapsed time**. Its type-erased intermediate partitions are not restored from
those markers; recovery re-executes the graph. V5 Direct checkpoints coded
output or fold state at supported batch boundaries and checks graph, source,
coder, and binary identity before recovery. V4 checkpoint files cannot be
loaded as v5 state.

| V4 setting | V5 Direct status |
| --- | --- |
| `enabled` | `JobConfig::checkpoint: Some(CheckpointConfig)` enables checkpoints; `None` disables them. |
| `auto_recover` | `CheckpointConfig::recover` controls loading the latest checkpoint. |
| `AfterEveryBarrier` | `interval_batches: 1` saves at every supported batch boundary, but is not an exact graph-barrier schedule. |
| `EveryNNodes(n)` | `interval_batches: n` counts processed source batches, not completed graph nodes. V5 does not expose a node-count schedule. |
| `TimeInterval(secs)` and `Hybrid` | V5 does not expose an elapsed-time schedule. Streaming requires an interval of one batch and saves on source progress or completion. |
| `max_checkpoints` | V5 Direct retains `CheckpointConfig::max_checkpoints` bounded or streaming snapshots (1–128, default 1). Recovery tries older verified snapshots if a newer one is corrupt; version or execution identity mismatches still fail. Streaming fallback can replay a previously committed batch, so its sink must be idempotent. Give independent jobs separate directories. |

For 5.0.0, v5 batch/progress checkpoints are the chosen replacement for
v4's node, barrier, and time schedules. This is an intentional scheduling API
change, not a claim that `interval_batches` reproduces those schedules.
Bounded checkpoint history applies to coded output and fold state. Streaming
history retains source progress; its sink must be idempotent across retries.

Sequential elementwise output is compared in order in the migration tests. Grouped, joined, sampled, and parallel results should be compared as multisets unless the operation documents an order. V5 validates boundedness and backend capabilities before opening sources or sinks. V5 graph, coder, and checkpoint versions are independent; unsupported versions fail explicitly rather than being interpreted as v4 state.

## Release boundary

`ironbeam-compat` depends on published Ironbeam 4.1.1 under the `ironbeam-v4`
alias and directly on the v5 packages. The root facade exports only v5 source
and does not depend on `ironbeam-compat`. Its core, Direct, transforms, and
JSONL API remain available with no default features; `cloud` and
`validation-json` are the two default extras. The package remains versioned
4.1.1 while the release gates are open. See the
[release gate status](V5_RELEASE_GATE_STATUS.md) for current checks and the
staged publication decision.
