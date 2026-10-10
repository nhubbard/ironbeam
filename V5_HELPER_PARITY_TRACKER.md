# Ironbeam v4 helper parity tracker

The authoritative v4 method inventory is `V5_PHASE0_API_INVENTORY.md`.
Parity here means a v5 graph or direct terminal with the same useful result,
documented behavior changes, and tests for empty input, failure, and execution
mode where applicable. V5 names take stable node keys and return typed errors.
This tracker is deliberately explicit about incomplete families; the v4 facade
remains available until every required family is closed.

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
| `combine`, `combine_global` | core, direct | Partial | Typed lifted global and keyed `CombineFn` methods now stream linear bounded Direct sources through incremental accumulators, including fixed windows. Other upstream graphs fall back to materialization; accumulator memory accounting and checkpoint support remain. |
| `joins`, `co_gbk` | core, direct | Present for bounded Direct values | Inner and outer joins plus two through ten input co-groups execute with key/window scoping, duplicates, and empty-side coverage. Current execution materializes all inputs; lifted shuffle and unbounded runner semantics remain. |
| `side_inputs` | core, transforms, direct | Partial | Bounded collection side input and v4-style immutable vector, singleton, map, and multimap helpers exist. Dynamic singleton/map/multimap materialization and resource policy remain. |
| `latest`, `timestamped`, `tumbling`, `windowed_combine` | core, transforms, direct | Present for bounded Direct values | Explicit timestamp assignment, fixed windows with signed offsets, latest globally/per key, and all twelve generic/numeric global/keyed windowed combine helpers execute. The ten numeric window helpers use lifted folds over linear inputs; generic `Vec` combines still materialize. V5 uses nanosecond envelope metadata; v4's `Timestamped<T>` wrapper and streaming triggers are not imported. |
| `sampling` | transforms | Present for bounded Direct values | Eight seeded/default global and per-key methods use v4's priority sequence through an ordered size-K reservoir. Linear Direct sources stream without materialization; complex upstreams and distributed ordered execution remain. |
| `statistical` | transforms | Present for bounded Direct values | V4-compatible t-digest quantiles and median work globally/per key, with non-finite observations ignored. Linear Direct inputs now stream into compressed digests; complex upstreams may materialize. |
| `batches`, `reshuffle`, `tee`, `wait_on` | core, transforms, direct | Partial | All five bounded batch helper shapes, `tee`/`tee_n`, a materialized Direct reshuffle barrier, and bounded `wait_on` execute. Batches are deterministic per window/key in v5 rather than per v4 physical partition; physical chunking and repartition planning remain. Shared upstreams may re-execute through separate dependency paths. |
| `collect_sorted` | direct | Present | Sequential/parallel sorted collection and stable keyed sorting exist. |
| `log_elements` | transforms/direct | Present for Direct debug use | Formatted/default passthroughs print at node execution. Like v4, retries or re-execution can repeat output; this is a debug aid. |
| `dead_letter`, `try_process`, `validation` | core, transforms, direct | Present for bounded Direct values | Fallible transforms, fail-fast collection, validation modes, and dead-letter branches exist. Direct can collect both classification arms in one run; collecting branch outputs separately still reruns the shared source. |
| `jsonl`, `csv`, `xml`, `avro`, `parquet` | I/O | Partial | Bounded sources and deferred sinks work; JSONL, CSV, Avro, and Parquet have lazy bounded readers. XML retains v4's single whole-file shard semantics. Shard planning and parallel writers remain. |
| `cbor`, `msgpack`, `protobuf`, `tfrecord` | optional I/O | Partial | Bounded source/sink adapters and lazy bounded readers pass. Shard planning, parallel writers, and compression remain. |
| `arrow_ipc` | optional I/O | Partial | Bounded row and record-batch sources/sinks plus lazy readers exist; shard planning and parallel writers remain. |
| `sql` | optional I/O | Partial | Opt-in SQL eager and paged bounded sources plus deferred append sink execute on Direct; SQLite round-trip, deferred write, typed failure, and capability rejection pass. Paged reads use `LIMIT`/`OFFSET` and require caller ordering and a stable database snapshot for repeatability. Parallel writer, portable split/checkpoint IDs, and multi-database live tests remain. |
| `mongodb`, `cloud` | optional I/O | Pending | Specify source splits, credentials, retries, and sink commit/replay before adding clients. |
| v4 test utilities | `ironbeam-testing` | Partial | Assertion, builder, fixture, mock I/O, test pipeline, and debug helpers live in a separate local package. Debug count is a Direct terminal; inspect/sample are graph passthroughs. Verify all v4 utility methods and release packaging. |

## Closure gates for each family

1. Match the v4 input/output behavior in the representative corpus, or record
   an intentional v5 change in `V4_TO_V5_MIGRATION.md`.
2. Exercise the helper on Direct in sequential and parallel modes when order
   or partitioning matters; test error and empty-input behavior.
3. Verify the graph declares the required backend capabilities and that the
   Dataflow preview rejects unsupported work before source or sink side effects.
4. Check the package alone and through the v5 facade, including its feature
   gate, docs, and coverage. Keep large optional dependencies out of core.

The next implementation tranche is migrating bounded-memory helpers to lifted
combines and completing physical scheduling. Optional connectors and I/O shard planning
remain separate parity gates.
