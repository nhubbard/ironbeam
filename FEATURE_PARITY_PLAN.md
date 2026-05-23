# Ironbeam Feature Parity Plan

This document outlines missing features from Apache Beam that are relevant for batch processing
in Ironbeam. Features are organized by priority tier.

**Excluded from scope:**
- Streaming-specific features (triggers, watermarks, allowed lateness, timers, stateful DoFn)
- Cloud runner integrations (Dataflow, Flink, Spark)
- Cloud I/O connectors (GCS, S3, BigQuery, Kafka, Pub/Sub)
- DoFn lifecycle hooks (Setup, StartBundle, FinishBundle, Teardown)
- I/O formats without first-class Serde support (ORC, Thrift, FlatBuffers)

---

## Rules of Engagement

1. Code must be written in **idiomatic Rust**.
2. Code must be **correct and complete**.
3. Code must be **well-documented**.
4. Code must be **exhaustively tested** with **full coverage** as analyzed by `llvm-cov`.
5. Code must be **properly formatted** with `cargo format`.
6. Code must be **lint-free** when `cargo clippy` is run with warnings as errors and the `pedantic` and `nursery`
   profiles are enabled. If a lint is not applicable, you can allow it at the site of the lint, but do not ignore lints
   unless they are truly inapplicable.
7. Documentation for the code must be **lint-free** when `cargo doc` is run.

To expedite analysis and ensure that errors are caught, you must do the following:

1. `cargo doc --no-deps` must produce no warnings.
2. `cargo fmt` must be run.
3. `cargo clippy --all-targets --all-features --fix --allow-dirty -- -D warnings -W clippy::pedantic -W clippy::nursery`
   must be run and any manual-fix issues must be corrected unless they genuinely do not apply to the situation.
4. `cargo llvm-cov --workspace --all-features --html --branch` must execute all tests and produce no compilation or test
   errors. You should analyze the coverage report, both before and after, to make sure that coverage has not dropped.

---

## Implemented Features

| Feature                                     | Description                                                                                                                                                                                                                                              | Since  |
|---------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| 1.1 Flatten                                 | `flatten()` merges multiple `PCollection<T>` into one                                                                                                                                                                                                    | 2.1.0  |
| 1.2 Side Outputs                            | Enum + `partition!` macro; compile-time type-safe multi-output                                                                                                                                                                                           | 2.1.0  |
| 1.3 Filter (Enhanced)                       | `filter_eq/ne/lt/le/gt/ge/range/range_inclusive/by`                                                                                                                                                                                                      | 2.1.0  |
| 1.4 WithKeys                                | `with_keys()`, `with_constant_key()`, `key_by()`                                                                                                                                                                                                         | 2.1.0  |
| 1.5 CoGroupByKey                            | `cogroup_by_key!` macro for 2–10 inputs                                                                                                                                                                                                                  | 2.2.0  |
| 1.6 Combiners                               | `Count`, `ToList`, `ToSet`, `Latest` built-in combiners                                                                                                                                                                                                  | 2.4.0  |
| 2.0 Combiner Helpers                        | `sum/min/max/average/approx_median/approx_quantiles/distinct_count` per-key & globally; `to_list/to_set/top_k` globally                                                                                                                                  | 2.5.0  |
| 2.1 Partition                               | Numeric partitioning via enum + `partition!` macro                                                                                                                                                                                                       | 2.1.0  |
| 2.2 Distinct By                             | `distinct_by(key_fn)` — deduplicate by projection, retaining full element                                                                                                                                                                                | 2.8.0  |
| 2.3 BottomK                                 | `BottomK` combiner; `bottom_k_per_key(k)` / `bottom_k_globally(k)` helpers                                                                                                                                                                               | 2.9.0  |
| 2.4 Side Input Views                        | `filter_with_side_map`, `SideSingleton`/`side_singleton`, `SideMultimap`/`side_multimap` + map/filter methods                                                                                                                                            | 2.9.0  |
| 2.5 Regex Transforms                        | `regex_matches/extract/extract_kv/find/replace_all/split` on `PCollection<String>`                                                                                                                                                                       | 2.10.0 |
| 2.5b Windowed Combine                       | `combine/sum/count/min/max/average_per_window` + `_per_key_and_window` helpers                                                                                                                                                                           | 2.10.0 |
| 2.6 Avro I/O                                | `read_avro`/`write_avro` helpers with glob support; `AvroReader`/`AvroWriter` behind `io-avro` feature                                                                                                                                                   | 2.10.0 |
| 2.7 XML I/O                                 | `read_xml`/`write_xml`/`read_xml_streaming`/`write_xml_par` with glob support; `XmlShards`/`XmlVecOps` behind `io-xml`                                                                                                                                   | 2.11.0 |
| 3.1 Reshuffle                               | Graph-level barrier; prevents fusion and redistributes elements across the pipeline graph                                                                                                                                                                | 2.11.0 |
| 3.2 WithTimestamps                          | `attach_timestamps()` / `Timestamped<T>`                                                                                                                                                                                                                 | 1.0.0  |
| 3.3 Reify                                   | `reify_timestamps()` — project `Timestamped<T>` into `(TimestampMs, T)` tuples; inverse of `to_timestamped`                                                                                                                                              | 2.11.0 |
| 3.4 PAssert                                 | `PAssert::that(&result).contains_in_any_order/is_empty/has_count/all_match` fluent assertion builder                                                                                                                                                     | 3.0.0  |
| 3.5 Reshuffle Elim                          | `eliminate_reshuffle_pass()` — drops leading `Reshuffle` before barriers or consecutive pairs; `EliminatedReshuffle` opt                                                                                                                                 | 3.0.0  |
| 3.6 Predicate Pushdown Past Reshuffle       | `push_down_before_barrier_pass()` — extends predicate pushdown to treat `Reshuffle` as a transparent barrier alongside `GroupByKey`                                                                                                                      | 3.0.0  |
| 3.7 Flatten Input Predicate Pushdown        | `push_down_into_flatten_pass()` — clones `value_only + cardinality_reducing` ops into each Flatten subplan tail, removing them from the post-Flatten block                                                                                               | 3.0.0  |
| 3.8 Dead Subtree Elimination                | `prune_dead_subtrees()` — backward BFS pre-pass removes nodes with no forward path to the target terminal before chain extraction; `PrunedDeadSubtrees` opt                                                                                              | 3.0.0  |
| 3.9 CoGroup Join Ordering                   | `reorder_cogroup_inputs_pass()` — sorts `Flatten` subchains by estimated cardinality ascending; `ReorderedCoGroupInputs { original_order, new_order }` opt                                                                                               | 3.0.0  |
| 3.10 Tree Reduction                         | `CombineFn::is_associative_commutative()` marker; `Node::CombineGlobal { tree_reduce }` flag; Rayon `reduce_with` for O(log n) parallel merge depth; parallel-within-key local closure for `combine_values`; `TreeReduction` opt                         | 3.0.0  |
| 3.11 Early Termination / Limit              | `TakeOp<T>` stateless op; `PCollection::take(n)` / `first()`; `DynOp::limit_n()`; `Plan::limit`; `exec_par` merge-phase early stopping; `LimitPushdown` opt                                                                                              | 3.0.0  |
| 3.12 Bloom Filter Semi-Join                 | In-tree `BloomFilter` (K=4, ~1% FPR); `join_inner/left/right` build from smaller side and pre-filter larger; `uses_bloom_semi_join` metadata flag; `BloomSemiJoin { smaller_side, estimated_reduction_pct }` opt                                         | 3.0.0  |
| 3.13 Dominator-Based Cache Placement        | Cooper's dominance algorithm on the pipeline DAG; `build_dominator_tree` + `find_cache_node_via_dominators` replace `find_deepest_fanout_ancestor`; enables CSE caching for linear pipelines and diamond topologies                                      | 3.0.0  |
| 3.14 Adaptive Inter-Stage Partition Count   | `DynOp::cardinality_multiplier_hint() -> f64` (default 1.0); `exec_par` tracks `current_parts` updated per barrier (GBK→×0.1, CombineGlobal→1, Flatten→×N, CoGroup→×0.5); Reshuffle uses `current_parts`; `AdaptivePartitionCount { barrier_count }` opt | 3.0.0  |
| 3.15 Empty / Singleton Source Short-Circuit | `Plan::is_empty` + `Plan::is_singleton` flags; runner fast-paths empty → `Vec::new()` (skips executor); singleton → forces `exec_seq`; `EmptySourceShortCircuit` / `SingletonSourceShortCircuit` opts; empty-guard excludes `CombineGlobal` chains       | 3.0.0  |
| 4.1 Keys / Values                           | `keys()` — `PCollection<(K, V)>` → `PCollection<K>`; `values()` — `PCollection<(K, V)>` → `PCollection<V>`; thin wrappers over `map`                                                                                                                     | 3.1.0  |
| 4.2 KvSwap                                  | `kv_swap()` — `PCollection<(K, V)>` → `PCollection<(V, K)>`; thin wrapper over `map`; permits non-`Hash` keys                                                                                                                                            | 3.1.0  |
| 4.3 Mean Combiners                          | `Mean<O>` typed `CombineFn` (`f32` / `f64` output) implementing `LiftableCombiner`; `mean_globally::<O>()` / `mean_per_key::<O>()` helpers complementing `average_*`                                                                                     | 3.1.0  |
| 4.4 Count.PerElement                        | `count_per_element()` — `PCollection<T: Hash + Eq>` → `PCollection<(T, u64)>`; counts occurrences of each distinct element                                                                                                                               | 3.1.0  |
| 4.5 ToDict Combiner                         | `ToDict<K, V>` typed `CombineFn` + `LiftableCombiner` materializing `(K, V)` pairs into a single `HashMap<K, V>`; `to_dict()` helper on `PCollection<(K, V)>`                                                                                            | 3.1.0  |
| 4.6 GroupIntoBatches                        | `group_into_batches(n)` — `PCollection<(K, V)>` → `PCollection<(K, Vec<V>)>`; per-key chunking with each batch ≤ `n` elements (final per-key batch may be smaller)                                                                                       | 3.1.0  |
| 4.7 BatchElements                           | `batch_elements(n)` / `batch_by_size(max_bytes, size_fn)` — `PCollection<T>` → `PCollection<Vec<T>>`; partition-local count- or size-bounded batching with explicit size estimator and lone-oversized-element semantics                                  | 3.1.0  |
| 4.8 ToString                                | `to_display_string()` — `PCollection<T: Display>` → `PCollection<String>`; named to avoid collision with the inherent `ToString::to_string`                                                                                                              | 3.1.0  |
| 4.9 Tee                                     | `tee()` → `(PCollection<T>, PCollection<T>)`; `tee_n(n)` → `Vec<PCollection<T>>`; ergonomic fan-out wrappers leveraging the v3.0.0 dominator-based cache placement                                                                                       | 3.1.0  |
| 4.10 LogElements                            | `log_elements()` (requires `T: Debug`) and `log_elements_with(formatter)` — passthrough debug taps that print each element to stdout (formatted via `{:?}` or a user-supplied `Fn(&T) -> String`) without modifying the downstream collection            | 3.1.0  |

---

## Tier 4: Convenience Transforms and Aggregation Patterns

These transforms were identified in the initial survey of Beam features but not assigned to
earlier tiers. They are primarily convenience wrappers and less common aggregation, sampling,
and pipeline-shape patterns. All additional I/O formats — both file-based and database — are
covered separately in Tier 5.

### 4.11 WaitOn (Pipeline Dependency Barrier)

**Status:** Not implemented.

Introduces a data-flow dependency between two `PCollection`s so that downstream processing of
one collection is held until a separate signal collection has fully drained — without consuming
the signal collection's data in the primary path.

**Beam equivalent:** `WaitOn` in `util.py`

**Proposed API:**
```rust
data_collection.wait_on(&signal) // -> same PCollection<T>, but runner delays execution until signal drains
```

**Estimated complexity:** Medium — requires the planner to insert phantom dependency edges in
the execution graph and the runner to honor them without deadlock.

---

### 4.12 ApproximateUnique / HyperLogLog Distinct Count

**Status:** Not implemented.

Estimate the number of distinct elements using HyperLogLog. Unlike the exact `distinct_count`
combiner, this runs in `O(1)` space with a configurable relative error bound. Useful for huge
collections where an exact count is impractical.

**Beam equivalent:** `ApproximateUnique` in `stats.py`

**Proposed API:**
```rust
collection.approx_count_distinct()                  // -> PCollection<u64>, ~2% default error
collection.approx_count_distinct_with_error(0.01)   // 1% relative error
collection.approx_count_distinct_per_key()          // -> PCollection<(K, u64)>
```

**Dependencies:** a HyperLogLog crate (`hyperloglogplus`) or a ~200-line in-tree implementation.

**Estimated complexity:** Medium — the algorithm is well-understood, but the Rust CombineFn
integration and correct serialization of the HLL sketch state need care.

---

### 4.13 Sample Combiner

**Status:** Not implemented.

Randomly sample exactly `N` elements from a collection (or per-key) using reservoir sampling,
without replacement.

**Beam equivalent:** `Sample.FixedSizeGlobally()` / `Sample.FixedSizePerKey()` in `combiners.py`

**Proposed API:**
```rust
collection.sample_globally(n: usize)  // -> PCollection<Vec<T>>
collection.sample_per_key(n: usize)   // PCollection<(K, V)> -> PCollection<(K, Vec<V>)>
```

**Estimated complexity:** Low. Reservoir sampling is a straightforward `CombineFn` implementation.

---

### 4.14 Error Handling / Dead-Letter Pattern

**Status:** Not implemented.

Route elements that cause processing failures to a separate dead-letter `PCollection` instead of
panicking or failing the pipeline. This is the foundational pattern for resilient ETL pipelines.

**Beam equivalent:** `error_handling.py`

**Proposed API:**
```rust
let (good, errors): (PCollection<Out>, PCollection<DeadLetter<In>>) =
    collection.map_catching(|elem| my_fallible_fn(elem));

// DeadLetter<T> contains the original element and the error message.
// errors can be written to a log file, a database, or discarded.
```

**Estimated complexity:** Medium — requires a well-designed `DeadLetter<T>` type and ergonomic
integration with the existing side-output / `partition!` machinery.

---

## Tier 5: Additional I/O Formats

These features provide read/write support for additional serialization formats and storage
backends. All are non-streaming and non-cloud-specific (self-hosted databases only), and live
behind feature flags to minimize default dependencies.

### 5.1 MessagePack I/O

**Status:** Not implemented. Behind `io-msgpack` feature flag.

Read and write `PCollection<T: Serialize + DeserializeOwned>` as MessagePack files using
the `rmp-serde` crate. A compact binary format, common in cache/streaming systems.

**Proposed API:**
```rust
read_msgpack::<T>("path/*.msgpack")
write_msgpack("output/part-*.msgpack", collection)
```

**Dependencies:** `rmp-serde`

**Estimated complexity:** Low — same pattern as the existing Avro and XML I/O modules.

---

### 5.2 CBOR I/O

**Status:** Not implemented. Behind `io-cbor` feature flag.

Read and write `PCollection<T: Serialize + DeserializeOwned>` as CBOR files using the
`ciborium` crate. A compact binary format, common in IoT and embedded contexts.

**Proposed API:**
```rust
read_cbor::<T>("path/*.cbor")
write_cbor("output/part-*.cbor", collection)
```

**Dependencies:** `ciborium`

**Estimated complexity:** Low

---

### 5.3 Arrow IPC I/O

**Status:** Not implemented. Behind `io-arrow` feature flag.

Read and write Arrow IPC stream files using the `arrow` crate (already a transitive dependency).
The natural element type is `RecordBatch`, but a row-level generic wrapper can map to `T: ArrowSchema`.

**Proposed API:**
```rust
read_arrow_ipc("path/**/*.arrow")                // -> PCollection<RecordBatch>
write_arrow_ipc("output/", collection)
read_arrow_ipc_rows::<T>("path/**/*.arrow")      // -> PCollection<T> via row conversion
```

**Dependencies:** `arrow`, `arrow-ipc`

**Estimated complexity:** Medium. Arrow IPC format is well-specified, but schema mapping and row-level
generic conversion add non-trivial complexity.

---

### 5.4 Protocol Buffers I/O

**Status:** Not implemented. Behind `io-protobuf` feature flag.

Read and write length-delimited protobuf message files for `T` generated by `prost`. Because
`prost`-generated types do not implement `serde::Serialize`, the I/O layer bypasses Serde and
uses the `prost::Message` encode/decode path directly.

**Proposed API:**
```rust
read_proto::<MyMessage>("path/*.pb")    // -> PCollection<MyMessage>
write_proto("output/part-*.pb", collection)
```

**Dependencies:** `prost`

**Estimated complexity:** Medium — the file format (length-delimited records) is trivial, but the
non-Serde encode path and integration with the I/O framework require custom work.

---

### 5.5 Parquet I/O

**Status:** Not implemented. Behind `io-parquet` feature flag.

Read and write Apache Parquet files using the `parquet` crate (part of the Arrow ecosystem).
Parquet is a columnar format with excellent compression and broad toolchain support. The natural
element type is `RecordBatch`; a row-level generic wrapper maps to `T` via Arrow's type system.

**Beam equivalent:** `parquetio.py`

**Proposed API:**
```rust
read_parquet("path/**/*.parquet")             // -> PCollection<RecordBatch>
read_parquet_rows::<T>("path/**/*.parquet")   // -> PCollection<T> via Arrow row conversion
write_parquet("output/", collection)
```

**Dependencies:** `parquet`, `arrow`

**Estimated complexity:** Medium — row-group-level parallelism, predicate pushdown, and schema
mapping between Arrow types and Rust structs add non-trivial complexity.

---

### 5.6 TFRecord I/O

**Status:** Not implemented. Behind `io-tfrecord` feature flag.

Read and write TensorFlow TFRecord files. TFRecord is a simple length-prefixed binary container
using masked CRC-32C checksums; it does not require a TensorFlow installation. Each record is
either raw `Bytes` or a `tf.Example` proto message.

**Beam equivalent:** `tfrecordio.py`

**Proposed API:**
```rust
read_tfrecord("path/*.tfrecord")          // -> PCollection<Bytes>
write_tfrecord("output/", collection)
read_tfrecord_examples("path/*.tfrecord") // -> PCollection<Example> (prost-generated)
```

**Dependencies:** `prost` (for `tf.Example`), custom masked CRC-32C framing (~100 LOC).

**Estimated complexity:** Medium — the container format is straightforward, but CRC verification and
the `prost`-generated proto dependency add complexity.

---

### 5.7 SQL Database I/O

**Status:** Not implemented. Behind `io-sql` feature flag.

Read from and write to SQL databases (PostgreSQL, MySQL, SQLite, etc.) using `sqlx`. Covers
local and self-hosted databases; cloud-managed variants (Cloud SQL, RDS) are out of scope.

**Beam equivalent:** `jdbc.py` (implemented via external Java transforms in Beam)

**Proposed API:**
```rust
read_sql::<MyRow>("postgres://localhost/db", "SELECT id, name FROM users WHERE active = true")
write_sql("postgres://localhost/db", "INSERT INTO sink ...", collection)
```

**Dependencies:** `sqlx` with the appropriate driver features (`postgres`, `mysql`, `sqlite`)

**Estimated complexity:** High. Connection pooling, type mapping via `sqlx::FromRow`, and
parallel read strategies (cursor-based or `LIMIT`/`OFFSET` splitting) are all non-trivial.

---

### 5.8 MongoDB I/O

**Status:** Not implemented. Behind `io-mongodb` feature flag.

Read from and write to self-hosted MongoDB collections as batch operations using the official
async Rust driver.

**Beam equivalent:** `mongodbio.py`

**Proposed API:**
```rust
read_mongodb::<T>(uri: &str, db: &str, coll: &str, filter: Document) // -> PCollection<T>
write_mongodb(uri: &str, db: &str, coll: &str, collection: PCollection<T>)
```

**Dependencies:** `mongodb` (official async driver), `bson`

**Estimated complexity:** High. Parallel reads via `_id`-range splitting or `$sample`, BSON↔Rust
type mapping, and handling of schema heterogeneity all require significant effort.
