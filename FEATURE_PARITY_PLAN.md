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

| Feature               | Description                                                                                                             | Since       |
|-----------------------|-------------------------------------------------------------------------------------------------------------------------|-------------|
| 1.1 Flatten           | `flatten()` merges multiple `PCollection<T>` into one                                                                   | 2.1.0       |
| 1.2 Side Outputs      | Enum + `partition!` macro; compile-time type-safe multi-output                                                          | 2.1.0       |
| 1.3 Filter (Enhanced) | `filter_eq/ne/lt/le/gt/ge/range/range_inclusive/by`                                                                     | 2.1.0       |
| 1.4 WithKeys          | `with_keys()`, `with_constant_key()`, `key_by()`                                                                        | 2.1.0       |
| 1.5 CoGroupByKey      | `cogroup_by_key!` macro for 2–10 inputs                                                                                 | 2.2.0       |
| 1.6 Combiners         | `Count`, `ToList`, `ToSet`, `Latest` built-in combiners                                                                 | 2.3.0–2.4.0 |
| 2.0 Combiner Helpers  | `sum/min/max/average/approx_median/approx_quantiles/distinct_count` per-key & globally; `to_list/to_set/top_k` globally | 2.5.0       |
| 2.1 Partition         | Numeric partitioning via enum + `partition!` macro                                                                      | 2.1.0       |
| 2.2 Distinct By       | `distinct_by(key_fn)` — deduplicate by projection, retaining full element                                               | 2.8.0       |
| 2.3 BottomK           | `BottomK` combiner; `bottom_k_per_key(k)` / `bottom_k_globally(k)` helpers                                              | next        |
| 2.4 Side Input Views  | `filter_with_side_map`, `SideSingleton`/`side_singleton`, `SideMultimap`/`side_multimap` + map/filter methods           | next        |
| 2.5 Regex Transforms  | `regex_matches/extract/extract_kv/find/replace_all/split` on `PCollection<String>`                                      | next        |
| 2.5b Windowed Combine | `combine/sum/count/min/max/average_per_window` + `_per_key_and_window` helpers                                          | next        |
| 3.2 WithTimestamps    | `attach_timestamps()` / `Timestamped<T>`                                                                                | 1.0.0       |

---

## Tier 2: High-Value Features (Incomplete)

### 2.6 I/O: Avro Format Support

**Current state:** Not implemented. JSON Lines, CSV, and Parquet are supported.

**Serde support:** ✅ The `apache-avro` crate provides full Serde integration via
`apache_avro::from_value` / `apache_avro::to_value`.

**Proposed API:**
```rust
// Read
let records: PCollection<MyRecord> = read_avro(&p, "data.avro");
let records: PCollection<MyRecord> = read_avro_glob(&p, "data/year=*/part-*.avro");

// Write
records.write_avro("output.avro")?;
```

**Implementation plan:**

1. Add to `Cargo.toml`:
   ```toml
   [dependencies]
   apache-avro = { version = "0.20", optional = true }
   
   [features]
   io-avro = ["apache-avro"]
   ```
2. Create `src/io/avro.rs` following the existing JSONL/CSV/Parquet structure (`AvroReader`,
   `AvroWriter`, streaming reader variant).
3. Add `read_avro` / `write_avro` helpers to `src/helpers/` behind `#[cfg(feature = "io-avro")]`.
4. Export from `src/lib.rs`.

**Estimated complexity:** Medium

**Files to modify/create:** `Cargo.toml`, `src/io/avro.rs` (new), `src/io/mod.rs`,
`src/helpers/mod.rs`, `src/lib.rs`

---

### 2.7 I/O: XML Format Support

**Current state:** Not implemented.

**Serde support:** ✅ The `quick-xml` crate provides Serde integration via its `serialize`
feature.

**Caveat:** XML is not naturally splittable. The reader should produce all elements from a single
partition; the writer should produce a single output file regardless of parallelism.

**Proposed API:**
```rust
// Read: each top-level child element deserializes to one T
let records: PCollection<MyRecord> = read_xml(&p, "data.xml");

// Write
records.write_xml("output.xml")?;
```

**Implementation plan:**

1. Add to `Cargo.toml`:
   ```toml
   [dependencies]
   quick-xml = { version = "0.39", features = ["serialize"], optional = true }
   
   [features]
   io-xml = ["quick-xml"]
   ```
2. Create `src/io/xml.rs`. Reader loads the whole file in partition 0 and returns empty for all
   other partitions. Writer serializes to a single file.
3. Add `read_xml` / `write_xml` helpers to `src/helpers/` behind `#[cfg(feature = "io-xml")]`.
4. Export from `src/lib.rs`.

**Estimated complexity:** Medium

**Files to modify/create:** `Cargo.toml`, `src/io/xml.rs` (new), `src/io/mod.rs`,
`src/helpers/mod.rs`, `src/lib.rs`

---

## Tier 3: Nice-to-Have Features

### 3.1 Reshuffle

**Status:** Not implemented.

Forces a barrier that prevents operation fusion and redistributes elements. In a local-only
framework this is primarily useful for forcing checkpointing between stages or breaking
processing skew.

**Simple implementation:**
```rust
impl<T: RFBound> PCollection<T> {
    pub fn reshuffle(self) -> PCollection<T> {
        self.map(|elem| (rand::random::<u64>(), elem.clone()))
            .group_by_key()
            .flat_map(|(_, vs): &(u64, Vec<T>)| vs.clone())
    }
}
```

**Estimated complexity:** Low (simple form); Medium (true graph-level barrier)

---

### 3.3 Reify

**Status:** Not implemented. Primarily a debugging aid for windowed pipelines.

Makes implicit metadata (timestamps) explicit in the data stream:

```rust
impl<T: RFBound> PCollection<Timestamped<T>> {
    pub fn reify_timestamps(self) -> PCollection<(i64, T)> {
        self.map(|ts: &Timestamped<T>| (ts.timestamp, ts.value.clone()))
    }
}
```

**Estimated complexity:** Low

---

### 3.4 PAssert Builder API

**Status:** Not implemented. The existing functional assertions (`assert_collections_equal`,
`assert_all`, etc.) are enough; this is a more ergonomic wrapper.

**Proposed API:**
```rust
PAssert::that(&result).contains_in_any_order(&[1, 2, 3])?;
PAssert::that(&result).is_empty()?;
PAssert::that(&result).has_count(42)?;
PAssert::that(&result).all_match(|x| *x > 0)?;
```

**Estimated complexity:** Low

---

## Additional Beam Features Not Yet in This Plan

The following features exist in Apache Beam's batch SDK but are not currently planned. Listed
here for consideration before deciding whether to add them.

### Transforms

| Feature          | Beam API                            | Description                                                                                       |
|------------------|-------------------------------------|---------------------------------------------------------------------------------------------------|
| Keys / Values    | `Keys.create()` / `Values.create()` | Extract only keys or only values from a KV collection                                             |
| KvSwap           | `KvSwap.create()`                   | Swap keys and values                                                                              |
| Mean (generic)   | `Mean.PerKey()` / `Mean.Globally()` | Average of any numeric type; `AverageF64` covers most cases                                       |
| Count.PerElement | `Count.PerElement()`                | Count occurrences of each distinct element → `(T, u64)`                                           |
| ToDict combiner  | `ToDict`                            | Collect `(K, V)` pairs into a `HashMap<K, V>`                                                     |
| GroupIntoBatches | `GroupIntoBatches.of(N)`            | Group per-key values into fixed-size sub-batches                                                  |
| BatchElements    | `BatchElements`                     | Group consecutive elements into size-bounded batches (different from `map_batches`)               |
| ToString         | `ToString.elements()`               | Convert elements to `String` via `Display`                                                        |
| Tee              | —                                   | Duplicate a collection to N downstream branches without re-executing upstream; graph optimization |

### Additional I/O Formats (Serde-backed)

| Format           | Crate       | Notes                                                              |
|------------------|-------------|--------------------------------------------------------------------|
| MessagePack      | `rmp-serde` | Compact binary; common in streaming/cache systems                  |
| CBOR             | `ciborium`  | Compact binary; common in IoT                                      |
| Arrow IPC        | `arrow`     | Already a dependency; efficient in-memory columnar exchange        |
| Protocol Buffers | `prost`     | Widely used; Serde support is partial and may need custom handling |

---

## Implementation Priority Order

1. **Active (next):**
   1. ~~2.4 — `filter_with_side_map`, `side_singleton`, `side_multimap`~~ ✅ complete
   2. ~~2.5 — Regex transforms~~ ✅ complete
   3. ~~2.5b — Windowed combining helpers~~ ✅ complete
2. **Medium-term:**
   1. 2.6 — Avro I/O
   2. 2.7 — XML I/O
3. **Polish:**
   1. 3.1 — Reshuffle
   2. 3.3 — Reify
   3. 3.4 — PAssert builder API
