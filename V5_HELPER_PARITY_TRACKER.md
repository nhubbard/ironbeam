# Ironbeam v4 helper parity tracker

The authoritative v4 method inventory is `V5_PHASE0_API_INVENTORY.md`.
Parity here means a v5 graph or direct terminal with the same useful result,
documented behavior changes, and tests for empty input, failure, and execution
mode where applicable. V5 names take stable node keys and return typed errors.
This tracker is deliberately explicit about incomplete families; the v4 facade
remains available until every required family is closed.

| v4 family | v5 owner | Status | Remaining contract or implementation |
| --- | --- | --- | --- |
| `common`, `stdlib`, `named` | core, direct | Partial | Core map/filter/flat-map/from_vec/from_iter and Direct collect are present; custom source and naming conveniences need mapping. |
| `keyed`, `values` | core, transforms, direct | Partial | Keying, grouping, projections, value mapping/filtering, swap, and `to_hashmap` exist; `with_keys` spelling and runner result checks remain. |
| `count` | transforms | Present | Global, per-element, and per-key counts have Direct tests. |
| `filter` | transforms | Present | Nine predicate helpers have sequential/parallel boundary tests. |
| `collect_values` | transforms | Present | Global list/set, per-key list/set, and dictionary are implemented; hash collections have canonical coders and tests. |
| `basic` | transforms | Present for supported numeric types | Sum, min/max, f64 averages, and generic f32/f64 means exist globally/per key; extend numeric `Element` coverage as other v4 types migrate. |
| `distinct` | transforms | Partial | Distinct, distinct count, and distinct-by exist; approximate and per-key variants remain. |
| `topk` | transforms | Present for values | Global/per-key top and bottom K pass value tests; the current global combine materializes input and needs a bounded-memory implementation before performance parity is claimed. |
| `regex` | transforms | Present | Six methods pass value tests; invalid patterns are typed construction errors in v5. |
| `display` | transforms | Present | Display mapping passes Direct tests. |
| `flatten` | core | Present | Fan-in graph and Direct tests exist. |
| `combine`, `combine_global` | core, direct | Partial | Global combine graph and Direct execution exist; typed incremental combiners and lifted planning remain. |
| `joins`, `co_gbk` | core, direct | Partial | Inner join exists; outer joins and multi-way co-group remain. |
| `side_inputs` | core, direct | Partial | Bounded map-with-side exists; singleton/map/multimap variants and materialization policy remain. |
| `latest`, `timestamped`, `tumbling`, `windowed_combine` | core, transforms, direct | Pending | Event-time assignment and window execution need a shared contract; Direct currently rejects fixed-window execution. |
| `sampling`, `statistical` | transforms | Pending | Specify deterministic seeds, approximation/error bounds, and finite-input behavior. |
| `batches`, `reshuffle`, `tee`, `wait_on` | direct | Pending | Specify scheduling, ordering, buffer limits, and dependency semantics. |
| `collect_sorted` | direct | Partial | Sequential/parallel sorted collection exists; custom sort-by-key remains. |
| `log_elements` | transforms/direct | Pending | Define observation timing and side-effect guarantees. |
| `dead_letter`, `try_process`, `validation` | core, transforms, direct | Partial | `try_map`, `try_flat_map`, and fail-fast collection work with a tagged Result coder; dead-letter side outputs and validation policies remain. |
| `jsonl`, `csv`, `xml`, `avro`, `parquet` | I/O | Partial | Bounded source and deferred sink work; v4 shard/lazy reader and immediate writer variants need parity checks. |
| `cbor`, `msgpack`, `protobuf`, `tfrecord` | optional I/O | Partial | Bounded source/sink adapters and format tests pass; shard/lazy readers, parallel writers, and compression need parity checks. |
| `arrow_ipc` | optional I/O | Pending | Define record-batch versus row API and add bounded source/sink adapters. |
| `sql`, `mongodb`, `cloud` | optional I/O | Pending | Specify source splits, credentials, retries, and sink commit/replay before adding clients. |
| v4 test utilities | testing package | Pending | Move fixtures and assertions after Direct dependency is explicit. |

## Closure gates for each family

1. Match the v4 input/output behavior in the representative corpus, or record
   an intentional v5 change in `V4_TO_V5_MIGRATION.md`.
2. Exercise the helper on Direct in sequential and parallel modes when order
   or partitioning matters; test error and empty-input behavior.
3. Verify the graph declares the required backend capabilities and that the
   Dataflow preview rejects unsupported work before source or sink side effects.
4. Check the package alone and through the v5 facade, including its feature
   gate, docs, and coverage. Keep large optional dependencies out of core.

The next implementation tranche is typed error/side-output semantics and
windowed execution, followed by the remaining optional formats and connector
infrastructure. Those contracts unlock several pending families at once.
