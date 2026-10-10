# Ironbeam v4 helper parity tracker

The authoritative v4 method inventory is `V5_PHASE0_API_INVENTORY.md`.
Parity here means a v5 graph or direct terminal with the same useful result,
documented behavior changes, and tests for empty input, failure, and execution
mode where applicable. V5 names take stable node keys and return typed errors.
The v4 bounded-helper behavior is represented by the v5 API and Direct runner,
with intentional semantic changes listed in `V4_TO_V5_MIGRATION.md`. The
remaining notes describe runtime, durability, and connector work needed before
v5 can replace the entire v4 package; they are not hidden by helper parity.

| v4 family | v5 owner | Status | Remaining contract or implementation |
| --- | --- | --- | --- |
| `common`, `stdlib`, `named` | core, direct | Present with v5 naming contract | Core map/filter/flat-map/from_vec/from_iter, bounded custom source, explicit unbounded source metadata, and Direct collect are present. Every v5 graph node takes a stable key at construction; v4's after-the-fact `with_name` is intentionally replaced by that contract. |
| `keyed`, `values` | core, transforms, direct | Present for bounded values | Keying, grouping, projections, value mapping/filtering, swap, `with_keys`, and `to_hashmap` exist; verify all runner result modes. |
| `count` | transforms | Present | Global, per-element, and per-key counts have Direct tests. All three use lifted folds over linear inputs. |
| `filter` | transforms | Present | Nine predicate helpers have sequential/parallel boundary tests. |
| `collect_values` | transforms | Present | Global list/set, per-key list/set, and dictionary are implemented with folds over linear Direct inputs; hash collections have canonical coders and tests. Lists and maps retain their required output-sized memory. |
| `basic` | transforms | Present for supported numeric types | Sum, min/max, f64 averages, and generic f32/f64 means exist globally/per key and use lifted folds over linear inputs. Extend numeric `Element` coverage as other v4 types migrate. |
| `distinct` | transforms | Present for values | Exact global/per-key distinct and counts, distinct-by, KMV, and HLL++ approximations exist. All stream linear Direct inputs into ordered sets or sketches; complex upstreams may materialize. Exact distinct outputs still require space proportional to unique values. |
| `topk` | transforms | Present for bounded Direct values | Global/per-key top and bottom K now use size-K lifted heaps over linear sources and pass a memory-budget test. Other upstream graphs can still materialize before folding. |
| `regex` | transforms | Present | Six methods pass value tests; invalid patterns are typed construction errors in v5. |
| `display` | transforms | Present | Display mapping passes Direct tests. |
| `flatten` | core | Present | Fan-in graph and Direct tests exist. |
| `combine`, `combine_global` | core, direct | Present for bounded Direct values | Typed lifted global and keyed `CombineFn` methods stream linear bounded Direct sources through incremental accumulators, including fixed windows. Other upstream graphs fall back to materialization; accumulator memory accounting and checkpoint support remain. |
| `joins`, `co_gbk` | core, direct | Present for bounded Direct values | Inner and outer joins plus two through ten input co-groups execute with key/window scoping, duplicates, and empty-side coverage. Current execution materializes all inputs; lifted shuffle and unbounded runner semantics remain. |
| `side_inputs` | core, transforms, direct | Present for bounded Direct values | Bounded collection side input and v4-style immutable vector, singleton, map, and multimap helpers exist. Pipeline-produced singleton, map, and multimap side views materialize once per Direct execution, with map and filter forms; singleton cardinality errors are typed. Bounded side-view size limits and window-aware side materialization remain. |
| `latest`, `timestamped`, `tumbling`, `windowed_combine` | core, transforms, direct | Present for bounded Direct values | Explicit timestamp assignment, conversion from `(i64, T)` pairs to event-time metadata, reification into `(i64, T)` values, unkeyed and keyed tumbling key/group helpers, fixed windows with signed offsets, latest globally/per key, and all twelve generic/numeric global/keyed windowed combine helpers execute. The ten numeric window helpers use lifted folds over linear inputs; generic `Vec` combines still materialize. V5 timestamps are signed nanoseconds in envelope metadata; v4's unsigned-millisecond `Timestamped<T>` wrapper and streaming triggers are not imported. |
| `sampling` | transforms | Present for bounded Direct values | Eight seeded/default global and per-key methods use v4's priority sequence through an ordered size-K reservoir. Linear Direct sources stream without materialization; complex upstreams and distributed ordered execution remain. |
| `statistical` | transforms | Present for bounded Direct values | V4-compatible t-digest quantiles and median work globally/per key, with non-finite observations ignored. Linear Direct inputs now stream into compressed digests; complex upstreams may materialize. |
| `batches`, `reshuffle`, `tee`, `wait_on` | core, transforms, direct | Present with documented v5 chunking change | All five bounded batch helper shapes, `tee`/`tee_n`, a materialized Direct reshuffle barrier, and bounded `wait_on` execute. Batches are deterministic per window/key in v5 rather than per v4 physical partition; physical chunking and repartition planning remain. Shared upstreams may re-execute through separate dependency paths. |
| `collect_sorted` | direct | Present | Sequential/parallel sorted collection and stable keyed sorting exist. |
| `log_elements` | transforms/direct | Present for Direct debug use | Formatted/default passthroughs print at node execution. Like v4, retries or re-execution can repeat output; this is a debug aid. |
| `dead_letter`, `try_process`, `validation` | core, transforms, direct | Present for bounded Direct values | Fallible transforms, fail-fast collection, validation modes, and dead-letter branches exist. Direct can collect both classification arms in one run; collecting branch outputs separately still reruns the shared source. |
| `jsonl`, `csv`, `xml`, `avro`, `parquet` | JSONL, CSV, XML, MessagePack, CBOR, Avro, and Parquet in dedicated `ironbeam-io-*` crates; shared file lifecycle in `ironbeam-io` | Present for bounded Direct files | Bounded sources, lazy bounded readers, file globs, and deferred sinks work. JSONL and CSV have stable bounded line/row shard sources and deterministic parallel sinks, including gzip. XML, Avro, and Parquet have parallel sinks that merge complete shard containers before atomic publish; XML retains one whole-file input shard. Parquet additionally splits directly on row-group boundaries as in v4. Avro and record-range Parquet readers rescan from the start per shard; compressed shard reads restart decompression per shard. Parquet has native internal compression but no external codec wrapper. Seek-optimized Avro splits and external Parquet compression remain future optimizations. |
| `cbor`, `msgpack`, `protobuf`, `tfrecord` | optional I/O | Present for bounded Direct values | Bounded source/sink adapters, file glob sources, lazy bounded readers, finite record-range graph sources, and deterministic parallel sinks pass. TensorFlow `tf.Example` has a typed v5 source, lazy/sharded forms, and a wire-compatibility test against v4 output. All four formats round-trip gzip with lazy readers; compressed lazy paths decode the whole file when opened. Record-range readers rescan earlier records per shard; offset indexes and seek-optimized split planning are future physical optimizations. |
| `arrow_ipc` | optional I/O | Present for bounded Direct values | Bounded row and record-batch sources/sinks plus file glob sources, lazy readers, finite row/batch range sources, and parallel row/batch container sinks exist. Indexed batch sources split directly at Arrow IPC batch boundaries as in v4; older finite row/batch range readers still rescan earlier batches. |
| `sql` | `ironbeam-io-sql` | Present for bounded Direct values | Opt-in SQL eager, paged, and explicit finite shard sources plus deferred single and parallel append sinks execute on Direct. SQLite, live Postgres, and live MySQL round trips pass, along with snapshot-range length checks, deferred write, typed failure, and capability rejection. Shard keys include stable offsets and a caller supplied snapshot ID; the query must have total ordering and its database snapshot must stay unchanged. Parallel append shards use independent connections and can partially commit. |
| `mongodb` | `ironbeam-io-mongodb` | Present for bounded Direct values | Opt-in bounded `find`, paged `skip`/`limit`, and explicit finite shard sources plus deferred single and parallel append sinks execute on Direct; live Rancher Desktop round trips, snapshot-range length check, typed failure, and capability rejection pass. Shards sort by `_id` and include stable offsets and a caller supplied snapshot ID; the collection must remain unchanged across retries. Multi-batch and parallel writes can partially commit and require application deduplication for retries. |
| `cloud` | `ironbeam-cloud`, optional I/O | Present for v4 provider-neutral helpers | Provider-neutral traits, fake services, retry, timeout, batch, pagination, operation helpers, compressed cloud JSONL readers/writers, and the codec registry live in a shared package reexported through v4 and opt-in v5 I/O. Bounded JSONL object and sorted glob sources plus deferred final-result sink live in `ironbeam-io-jsonl` and execute on Direct with fake storage; digest mismatch produces a recovery error. Provider-specific storage clients, streaming object reads, and remote runner lowering remain. |
| v4 test utilities | `ironbeam-testing` | Present with v5 execution contract | Assertion, builder, fixture, mock I/O, test pipeline, and debug helpers live in a separate package. Debug count is a Direct terminal; inspect/sample are graph passthroughs. The package is included in local release verification and remains unpublished by choice. |

## Bounded Direct parity evidence

- The Phase 0 public method inventory was checked against the v5 core,
  transforms, Direct, I/O, cloud, testing, and migration facade APIs. Renamed
  operations and intentional semantic changes are mapped in
  `V4_TO_V5_MIGRATION.md`.
- Typed TensorFlow `tf.Example` records are decoded from v4-produced TFRecord
  bytes. Timestamp tuple conversion and unkeyed/keyed tumbling shapes have
  sequential and parallel Direct tests.
- Parquet row-group and Arrow IPC batch-index sources have multi-split tests;
  file changes after graph construction fail recovery. SQL and MongoDB helper
  round trips were also exercised against live local containers.
- `scripts/check_v5_packages.sh` passed on 2026-10-10 after these changes:
  workspace and facade tests, all-feature I/O tests, all-feature docs, and
  local package verification for all nine v5 crates after extracting SQL and
  MongoDB. Their live container tests also pass. No package was published.

## Closure gates for each family

1. Match the v4 input/output behavior in the representative corpus, or record
   an intentional v5 change in `V4_TO_V5_MIGRATION.md`.
2. Exercise the helper on Direct in sequential and parallel modes when order
   or partitioning matters; test error and empty-input behavior.
3. Verify the graph declares the required backend capabilities and that the
   Dataflow preview rejects unsupported work before source or sink side effects.
4. Check the package alone and through the v5 facade, including its feature
   gate, docs, and coverage. Keep large optional dependencies out of core.

The next replacement gates are multi-output execution without re-running shared
upstreams, durable accumulator and side-view state, physical scheduling for
complex graphs, unbounded processing, and remote runner lowering. These gates
remain open even though bounded Direct helper parity is closed.
