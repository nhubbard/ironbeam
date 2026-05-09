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

---

## Implemented Features

| Feature                               | Description                                                                                                                                                                                                                      | Since  |
|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|
| 1.1 Flatten                           | `flatten()` merges multiple `PCollection<T>` into one                                                                                                                                                                            | 2.1.0  |
| 1.2 Side Outputs                      | Enum + `partition!` macro; compile-time type-safe multi-output                                                                                                                                                                   | 2.1.0  |
| 1.3 Filter (Enhanced)                 | `filter_eq/ne/lt/le/gt/ge/range/range_inclusive/by`                                                                                                                                                                              | 2.1.0  |
| 1.4 WithKeys                          | `with_keys()`, `with_constant_key()`, `key_by()`                                                                                                                                                                                 | 2.1.0  |
| 1.5 CoGroupByKey                      | `cogroup_by_key!` macro for 2–10 inputs                                                                                                                                                                                          | 2.2.0  |
| 1.6 Combiners                         | `Count`, `ToList`, `ToSet`, `Latest` built-in combiners                                                                                                                                                                          | 2.4.0  |
| 2.0 Combiner Helpers                  | `sum/min/max/average/approx_median/approx_quantiles/distinct_count` per-key & globally; `to_list/to_set/top_k` globally                                                                                                          | 2.5.0  |
| 2.1 Partition                         | Numeric partitioning via enum + `partition!` macro                                                                                                                                                                               | 2.1.0  |
| 2.2 Distinct By                       | `distinct_by(key_fn)` — deduplicate by projection, retaining full element                                                                                                                                                        | 2.8.0  |
| 2.3 BottomK                           | `BottomK` combiner; `bottom_k_per_key(k)` / `bottom_k_globally(k)` helpers                                                                                                                                                       | 2.9.0  |
| 2.4 Side Input Views                  | `filter_with_side_map`, `SideSingleton`/`side_singleton`, `SideMultimap`/`side_multimap` + map/filter methods                                                                                                                    | 2.9.0  |
| 2.5 Regex Transforms                  | `regex_matches/extract/extract_kv/find/replace_all/split` on `PCollection<String>`                                                                                                                                               | 2.10.0 |
| 2.5b Windowed Combine                 | `combine/sum/count/min/max/average_per_window` + `_per_key_and_window` helpers                                                                                                                                                   | 2.10.0 |
| 2.6 Avro I/O                          | `read_avro`/`write_avro` helpers with glob support; `AvroReader`/`AvroWriter` behind `io-avro` feature                                                                                                                           | 2.10.0 |
| 2.7 XML I/O                           | `read_xml`/`write_xml`/`read_xml_streaming`/`write_xml_par` with glob support; `XmlShards`/`XmlVecOps` behind `io-xml`                                                                                                           | 2.11.0 |
| 3.1 Reshuffle                         | Graph-level barrier; prevents fusion and redistributes elements across the pipeline graph                                                                                                                                        | 2.11.0 |
| 3.2 WithTimestamps                    | `attach_timestamps()` / `Timestamped<T>`                                                                                                                                                                                         | 1.0.0  |
| 3.3 Reify                             | `reify_timestamps()` — project `Timestamped<T>` into `(TimestampMs, T)` tuples; inverse of `to_timestamped`                                                                                                                      | 2.12.0 |
| 3.4 PAssert                           | `PAssert::that(&result).contains_in_any_order/is_empty/has_count/all_match` fluent assertion builder                                                                                                                             | 2.12.0 |
| 3.5 Reshuffle Elim                    | `eliminate_reshuffle_pass()` — drops leading `Reshuffle` before barriers or consecutive pairs; `EliminatedReshuffle` opt                                                                                                         | 2.12.0 |
| 3.6 Predicate Pushdown Past Reshuffle | `push_down_before_barrier_pass()` — extends predicate pushdown to treat `Reshuffle` as a transparent barrier alongside `GroupByKey`                                                                                              | 2.12.0 |
| 3.7 Flatten Input Predicate Pushdown  | `push_down_into_flatten_pass()` — clones `value_only + cardinality_reducing` ops into each Flatten subplan tail, removing them from the post-Flatten block                                                                       | 2.12.0 |
| 3.8 Dead Subtree Elimination          | `prune_dead_subtrees()` — backward BFS pre-pass removes nodes with no forward path to the target terminal before chain extraction; `PrunedDeadSubtrees` opt                                                                      | 2.12.0 |
| 3.9 CoGroup Join Ordering             | `reorder_cogroup_inputs_pass()` — sorts `Flatten` subchains by estimated cardinality ascending; `ReorderedCoGroupInputs { original_order, new_order }` opt                                                                       | 2.12.0 |
| 3.10 Tree Reduction                   | `CombineFn::is_associative_commutative()` marker; `Node::CombineGlobal { tree_reduce }` flag; Rayon `reduce_with` for O(log n) parallel merge depth; parallel-within-key local closure for `combine_values`; `TreeReduction` opt | 2.13.0 |
| 3.11 Early Termination / Limit        | `TakeOp<T>` stateless op; `PCollection::take(n)` / `first()`; `DynOp::limit_n()`; `Plan::limit`; `exec_par` merge-phase early stopping; `LimitPushdown` opt                                                                      | 2.14.0 |

---

## Tier 3: Nice-to-Have Features

### 3.12 Bloom Filter Semi-Join for CoGroup

**Status:** Not implemented.

Before executing a `CoGroup`, build a Bloom filter of the smaller input's key set and pre-filter
the larger input, dropping elements whose key cannot appear in the join output. Elements eliminated
by the Bloom filter never reach the shuffle step, reducing both memory and CPU cost for sparse joins.

**Implementation sketch:**
- At the start of `CoGroup` execution, identify the smaller and larger input chains by estimated
  cardinality (same estimate used in 3.9).
- Build a `BloomFilter<K>` from the smaller input's keys (first pass).
- Pre-filter the larger input: discard any element whose key hash is absent in the filter.
- Execute the hash-join phase on the filtered larger input.
- Record filter statistics in a new `OptimizationDecision::BloomSemiJoin { smaller_side, estimated_reduction_pct }`.

**Dependencies:** A Bloom filter crate (`bloomfilter` or `fastbloom`) or a small in-tree
implementation (~100 LOC with `ahash`).

**Estimated complexity:** Medium — the Bloom filter itself is straightforward; the complexity is
integrating the two-pass execution into the existing single-pass CoGroup runner.

---

### 3.13 Dominator-Based Cache Placement

**Status:** Not implemented.

The current CSE cache places the materialization point at the "deepest fanout ancestor" — the node
with out-degree > 1 closest to the terminal. A more principled approach uses the dominator tree of
the execution DAG: a node `d` dominates node `n` if every path from the source to `n` passes
through `d`. Materializing at the immediate dominator of a shared subtree guarantees that all
consumers benefit and that no work upstream of the dominator is repeated.

This generalizes CSE from "shared fanout" to "any shared work", including cases where multiple
terminals share a long common prefix even without an explicit fan-out node in the current graph
snapshot.

**Implementation sketch:**
- Implement the Lengauer–Tarjan algorithm for dominator tree construction on the pipeline DAG.
- In `run_collect_cached()`, replace `find_deepest_fanout_ancestor()` with a dominator-tree
  lookup: cache at the immediate dominator of the set of nodes reachable from the target terminal.
- The cache key remains `NodeId`; only the selection of *which* node to cache changes.

**Estimated complexity:** High — Lengauer–Tarjan is a well-known O(E log V) algorithm but requires
careful implementation over the existing `HashMap`/`Vec` graph representation.

---

### 3.14 Adaptive Inter-Stage Partition Count

**Status:** Not implemented.

The current partition count is derived once from the source element count and held fixed throughout
the pipeline. Barriers (especially `GroupByKey` and `FlatMap`) can change cardinality significantly:
`GroupByKey` reduces element count to the number of distinct keys; `FlatMap` can multiply it.
Using the source-derived count for all downstream stages can lead to over- or under-partitioning.

**Implementation sketch:**
- Add a `cardinality_multiplier_hint() -> f64` method to `DynOp` (default `1.0`; `FlatMap` can
  set > 1.0 if known, `Filter`-class ops can set < 1.0).
- After each barrier in the plan chain, compute a new `suggested_partitions` estimate by multiplying
  the previous estimate by the barrier's output-to-input cardinality ratio (using actual element
  counts where available, hints otherwise).
- Pass the updated count to the next stage's `exec_par()` split.

**Estimated complexity:** Medium — the estimation logic is straightforward; the complexity is
threading a per-stage partition count through the runner rather than a single pipeline-wide value.

---

### 3.15 Empty / Singleton Source Short-Circuit

**Status:** Not implemented.

Two degenerate source cases warrant special treatment at plan time:

1. **Empty source** — if `VecOps::len()` returns 0, skip all planning and execution and return
   `Vec::new()` immediately. No ops can produce output from zero input.
2. **Singleton source** — if `len() == 1`, force `suggested_partitions = 1` and use `exec_seq()`
   regardless of the partition heuristic. Partitioning a single element across N workers adds
   overhead with no benefit.

**Implementation sketch:**
- In `build_plan()`, after extracting the source node, check `VecOps::len()`:
  - `0` → return a sentinel `Plan::empty()` that the runner recognizes and fast-paths.
  - `1` → set `suggested_partitions = 1`.
- Add a `Runner::run_collect` fast-path that returns `vec![]` for `Plan::empty()` without entering
  the executor.

**Estimated complexity:** Very Low — two boundary checks in `build_plan()` and one guard in the
runner; no algorithmic complexity.

---

## Tier 4: Convenience Transforms and Additional I/O

These transforms were identified in the initial survey of Beam features but not assigned to earlier
tiers. They are primarily convenience wrappers, less common aggregation patterns, or additional
serialization formats.

### 4.1 Keys / Values

**Status:** Not implemented.

Extract only the keys or only the values from a `PCollection<(K, V)>`.

**Beam equivalent:** `Keys.create()` / `Values.create()` in `util.py`

**Proposed API:**
```rust
kv_collection.keys()   // PCollection<(K, V)> -> PCollection<K>
kv_collection.values() // PCollection<(K, V)> -> PCollection<V>
```

**Estimated complexity:** Very Low — thin wrappers over `map(|(k, _)| k)` / `map(|(_, v)| v)`.

---

### 4.2 KvSwap

**Status:** Not implemented.

Swap keys and values in a `PCollection<(K, V)>`.

**Beam equivalent:** `KvSwap.create()` in `util.py`

**Proposed API:**
```rust
kv_collection.kv_swap() // PCollection<(K, V)> -> PCollection<(V, K)>
```

**Estimated complexity:** Very Low — thin wrapper over `map(|(k, v)| (v, k))`.

---

### 4.3 Mean Combiners

**Status:** Not implemented.

Generic mean over any numeric type, exposed as a named `CombineFn` struct. Complements the
existing `average_globally` / `average_per_key` helpers by providing typed combiner structs
for use with `combine_globally` / `combine_per_key`.

**Beam equivalent:** `Mean.PerKey()` / `Mean.Globally()` in `combiners.py`

**Proposed API:**
```rust
collection.mean_globally::<f64>()
collection.mean_per_key::<f64>()
// Combiner form:
collection.combine_globally(MeanGlobally::new())
collection.combine_per_key(MeanPerKey::new())
```

**Estimated complexity:** Low — the math is trivial; main work is implementing the `CombineFn` trait
cleanly for generic numeric types.

---

### 4.4 Count.PerElement

**Status:** Not implemented.

Count occurrences of each distinct element, producing `(T, u64)` pairs. Requires `T: Eq + Hash`.

**Beam equivalent:** `Count.PerElement()` in `combiners.py`

**Proposed API:**
```rust
collection.count_per_element() // PCollection<T> -> PCollection<(T, u64)>
```

**Estimated complexity:** Low — equivalent to `with_constant_key(1u64).sum_per_key()` with a key
swap, or a direct `GroupByKey` + count.

---

### 4.5 ToDict Combiner

**Status:** Not implemented.

Collect `(K, V)` pairs into a `HashMap<K, V>` via a combiner. Useful as a `combine_globally` target
when the entire collection should be materialized as a single map (e.g., for use as a side input).

**Beam equivalent:** `ToDict` in `combiners.py`

**Proposed API:**
```rust
kv_collection.to_dict() // PCollection<(K, V)> -> PCollection<HashMap<K, V>>
// Combiner form:
kv_collection.combine_globally(ToDict::new())
```

**Estimated complexity:** Low

---

### 4.6 GroupIntoBatches

**Status:** Not implemented.

Group per-key values into fixed-size `Vec` batches of at most `N` elements. Unlike `GroupByKey`
which collects all values, values are emitted as batches fill; the final batch may be smaller.

**Beam equivalent:** `GroupIntoBatches.of(N)` in `util.py`

**Proposed API:**
```rust
kv_collection.group_into_batches(n: usize) // PCollection<(K, V)> -> PCollection<(K, Vec<V>)>
```

**Estimated complexity:** Medium — requires per-key buffering in the runner and interaction with
the graph optimizer to avoid excessive memory pressure.

---

### 4.7 BatchElements

**Status:** Not implemented.

Group consecutive elements into `Vec<T>` batches bounded by element count or estimated byte size.
Unlike `GroupIntoBatches`, this is not per-key. Unlike `map_batches`, it controls where batch
boundaries fall rather than adapting to the runner's internal shard boundaries.

**Beam equivalent:** `BatchElements` in `util.py`

**Proposed API:**
```rust
collection.batch_elements(max_count: usize) // PCollection<T> -> PCollection<Vec<T>>
collection.batch_by_size(max_bytes: usize)  // size-bounded variant
```

**Estimated complexity:** Medium

---

### 4.8 ToString

**Status:** Not implemented.

Convert each element to its `Display` representation as a `String`.

**Beam equivalent:** `ToString.elements()` in `util.py`

**Proposed API:**
```rust
collection.to_display_string() // PCollection<T: Display> -> PCollection<String>
// Named to_display_string to avoid collision with the inherent to_string() method.
```

**Estimated complexity:** Very Low

---

### 4.9 Tee

**Status:** Not implemented.

Duplicate a `PCollection` to N downstream branches without re-executing upstream work. Inserts a
fan-out node in the execution graph so that the source transform runs exactly once and all branches
consume from the same materialized output.

**Beam equivalent:** `Tee` in `util.py`

**Proposed API:**
```rust
let (branch_a, branch_b) = collection.tee();
let branches: Vec<PCollection<_>> = collection.tee_n(5);
```

**Estimated complexity:** Medium — requires runner-level fan-out support and graph optimizer
awareness of the shared-output semantics.

---

### 4.10 MessagePack I/O

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

### 4.11 CBOR I/O

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

### 4.12 Arrow IPC I/O

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

### 4.13 Protocol Buffers I/O

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

## Tier 5: Extended Beam Features

These features were discovered during a fresh research pass of the Apache Beam Python SDK source
(`sdks/python/apache_beam/`) and have not previously appeared in this plan. All are non-streaming,
non-cloud-specific, and fit within the current exclusion policy.

### 5.1 LogElements (Debug Tap)

**Status:** Not implemented.

A passthrough transform that logs each element via `tracing` (or stdout) without modifying the
collection. Analogous to `tap` in iterator-style pipelines. Primarily a debugging aid.

**Beam equivalent:** `LogElements` in `util.py`

**Proposed API:**
```rust
collection.log_elements()                              // debug-prints each element, passes through
collection.log_elements_with(|e| format!("{e:?}"))    // custom format function
```

**Estimated complexity:** Very Low

---

### 5.2 WaitOn (Pipeline Dependency Barrier)

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

### 5.3 ApproximateUnique / HyperLogLog Distinct Count

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

### 5.4 Sample Combiner

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

### 5.5 Error Handling / Dead-Letter Pattern

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

### 5.6 Parquet I/O

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

### 5.7 TFRecord I/O

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

### 5.8 JDBC / SQL Database I/O

**Status:** Not implemented. Behind `io-jdbc` feature flag.

Read from and write to SQL databases (PostgreSQL, MySQL, SQLite, etc.) using `sqlx`. Covers
local and self-hosted databases; cloud-managed variants (Cloud SQL, RDS) are out of scope.

**Beam equivalent:** `jdbc.py` (implemented via external Java transforms in Beam)

**Proposed API:**
```rust
read_jdbc::<MyRow>("postgres://localhost/db", "SELECT id, name FROM users WHERE active = true")
write_jdbc("postgres://localhost/db", "INSERT INTO sink ...", collection)
```

**Dependencies:** `sqlx` with the appropriate driver features (`postgres`, `mysql`, `sqlite`)

**Estimated complexity:** High. Connection pooling, type mapping via `sqlx::FromRow`, and
parallel read strategies (cursor-based or `LIMIT`/`OFFSET` splitting) are all non-trivial.

---

### 5.9 MongoDB I/O

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
