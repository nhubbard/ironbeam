# Ironbeam

A data processing framework for Rust inspired by Apache Beam and Google Cloud Dataflow. Ironbeam provides a declarative API for building batch data pipelines with support for transformations, aggregations, joins, and I/O operations.

## Features

### Pipeline & execution

- **Declarative pipeline API** with fluent interface
- **Type-safe** with compile-time correctness
- **Sequential and parallel execution** modes, with independent-subgraph parallelism
- **Optimizing planner**: reshuffle elimination, predicate pushdown (incl. past reshuffle and into flatten subplans), dead-subtree elimination, CoGroup join reordering, tree reduction for associative combiners, limit pushdown, Bloom-filter semi-join, dominator-based CSE cache placement, adaptive inter-stage partition counts, empty/singleton source short-circuits

### Element-wise transformations

- **Stateless operations**: `map`, `filter`, `flat_map`, `map_batches`, `filter_map`
- **Enhanced filters**: `filter_eq`, `filter_ne`, `filter_lt`, `filter_le`, `filter_gt`, `filter_ge`, `filter_range`, `filter_range_inclusive`, `filter_by`
- **Keyed projections**: `keys`, `values`, `kv_swap`, `map_values`, `filter_values`
- **Batching**: `batch_elements`, `batch_by_size`, `group_into_batches`
- **Fan-out helpers**: `tee()` / `tee_n(n)` for ergonomic graph splits

### Aggregation

- **Stateful operations**: `group_by_key`, `combine_values`, keyed aggregations
- **Built-in aggregations**: `count_globally`, `count_per_key`, `count_per_element`, `top_k_per_key`, `bottom_k_per_key`, `to_list_per_key`, `to_set_per_key`, `to_dict`, `latest_globally`, `latest_per_key`, `distinct`, `distinct_by`, `distinct_per_key`
- **Lower-level combiners** for `combine_values` / `combine_globally`: `Sum`, `Min`, `Max`, `Mean`, `AverageF64`, `BottomK`, `TopK`, `ToDict`, `ApproxMedian`, `ApproxQuantiles`, `TDigest`
- **Approximate algorithms**: `approx_distinct_count` (KMV), `approx_count_distinct` (HyperLogLog++), `approx_median_*`, `approx_quantiles_*`
- **Reservoir sampling**: `sample_reservoir` (global), `sample_values_reservoir` (per-key), plus Beam-style `sample_globally(n)` / `sample_per_key(n)` helpers

### Multi-collection operations

- **Flatten**: merge multiple `PCollection`s of the same type into one
- **Multi-output / side outputs**: enum-based routing with the `partition!` macro, compile-time type-safe
- **N-way CoGroupByKey**: `cogroup_by_key!` macro for 2–10 inputs
- **Join support**: inner, left, right, and full outer joins, with automatic Bloom-filter semi-join pre-filtering
- **Side inputs** for enriching streams with auxiliary data (Vec, HashMap, singleton, and multimap views)

### Time & windowing

- **Windowing**: fixed/sliding windows with windowed combiner helpers (`sum_per_window`, `combine_per_key_and_window`, …)
- **Timestamp helpers**: `attach_timestamps`, `Timestamped<T>`, `reify_timestamps`

### Text processing

- **Regex transforms** on `PCollection<String>`: `regex_matches`, `regex_extract`, `regex_extract_kv`, `regex_find`, `regex_replace_all`, `regex_split`

### Control flow & observability

- **Control-flow operators**: `reshuffle` (graph-level fusion barrier), `wait_on` (signal-only dependency barrier)
- **Named operations**: `with_name` and `Pipeline::named_scope` for human-readable step labels in `explain` output
- **Debug taps**: `log_elements`, `log_elements_with`, `debug_inspect`, `debug_count`
- **Error handling**: dead-letter pattern via `map_catching` / `flat_map_catching`, plus `try_map` / `collect_fail_fast` / `collect_skip_errors`
- **Metrics collection** for runtime introspection

### I/O

- **Optional I/O backends**: JSON Lines, CSV, Parquet, Avro, XML
- **Optional compression**: gzip, zstd, bzip2, xz
- **Cloud I/O abstractions** for provider-agnostic cloud integrations

### Reliability & production

- **Checkpointing** for fault tolerance
- **Automatic memory spilling** to disk for memory-constrained environments
- **Data validation utilities** for production-grade error handling
- **Comprehensive testing framework** with fixtures, `PAssert` fluent assertions, and pre-built data fixtures

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ironbeam = "3"
```

By default, all features are enabled. To use a minimal configuration:

```toml
[dependencies]
ironbeam = { version = "3", default-features = false }
```

Available feature flags:

- `io-jsonl` - JSON Lines support
- `io-csv` - CSV support
- `io-parquet` - Parquet support
- `io-avro` - Apache Avro support
- `io-xml` - XML support
- `compression-gzip` - gzip compression
- `compression-zstd` - zstd compression
- `compression-bzip2` - bzip2 compression
- `compression-xz` - xz compression
- `parallel-io` - parallel I/O operations
- `metrics` - pipeline metrics collection
- `checkpointing` - checkpoint and recovery support
- `spilling` - automatic memory spilling to disk

## Quick Start

```rust
use ironbeam::*;

fn main() -> anyhow::Result<()> {
    // Create a pipeline
    let p = Pipeline::default();

    // Build a word count pipeline
    let lines = from_vec(&p, vec![
        "hello world".to_string(),
        "hello rust".to_string(),
    ]);

    let counts = lines
        .flat_map(|line: &String| {
            line.split_whitespace()
                .map(|w| w.to_string())
                .collect::<Vec<_>>()
        })
        .key_by(|word: &String| word.clone())
        .map_values(|_word: &String| 1u64)
        .combine_values(Count);

    // Execute and collect results
    let results = counts.collect_seq()?;

    for (word, count) in results {
        println!("{}: {}", word, count);
    }

    Ok(())
}
```

## Core Concepts

### Pipeline

A `Pipeline` is the container for your computation graph. Create one with `Pipeline::default()`, then attach data sources and transformations.

### PCollection

A `PCollection<T>` represents a distributed collection of elements. Collections are immutable, lazy, and type-safe. Transformations create new collections, and computation happens when you call an execution method like `collect_seq()` or `collect_par()`.

### Transformations

**Stateless operations** work on individual elements:

- `map` - transform each element
- `filter` - keep elements matching a predicate
- `flat_map` - transform each element into zero or more outputs
- `map_batches` - process elements in batches
- `batch_elements(n)` / `batch_by_size(max_bytes, size_fn)` - group consecutive elements into `Vec<T>` batches
- `tee()` / `tee_n(n)` - fan a collection out into multiple identical downstream branches
- `to_display_string()` - render `T: Display` into `PCollection<String>`

**Keyed projections:**

- `keys()` / `values()` - project a `PCollection<(K, V)>` to its keys or values
- `kv_swap()` - swap key and value

**Stateful operations** work on keyed data:

- `key_by` / `with_keys` / `with_constant_key` - convert to a keyed collection
- `map_values` / `filter_values` - transform or filter values while preserving keys
- `group_by_key` - group values by key
- `group_into_batches(n)` - per-key batching into `Vec<V>` chunks of size ≤ `n`
- `combine_values` / `combine_values_lifted` - aggregate values per key
- `top_k_per_key` / `bottom_k_per_key` - select top/bottom K values per key
- `distinct` / `distinct_by` / `distinct_per_key` - remove duplicate elements or values

### Combiners

Ironbeam provides convenience methods for common aggregations, plus lower-level combiners for
use with `combine_values` / `combine_globally`.

**Convenience methods (call directly on a collection):**
- `count_globally()` — total element count → `PCollection<u64>`
- `count_per_key()` — element count per key → `PCollection<(K, u64)>`
- `count_per_element()` — frequency of each distinct element → `PCollection<(T, u64)>`
- `sum_globally()` / `sum_per_key()` / `min_*` / `max_*` / `average_*` / `mean_*` — typed numeric aggregations, globally and per-key
- `top_k_globally(k)` / `top_k_per_key(k)` — the K largest values
- `bottom_k_globally(k)` / `bottom_k_per_key(k)` — the K smallest values
- `to_list_globally()` / `to_list_per_key()` — collect all values → `Vec<V>`
- `to_set_globally()` / `to_set_per_key()` — collect unique values → `HashSet<V>`
- `to_dict()` — collect `(K, V)` pairs into a single `HashMap<K, V>`
- `latest_globally()` / `latest_per_key()` — value with the latest timestamp
- `sample_globally(n)` / `sample_per_key(n)` — Beam-style fixed-size reservoir samples (also `*_with_seed`)
- `sample_reservoir(k, seed)` / `sample_reservoir_vec(k, seed)` / `sample_values_reservoir(k, seed)` — lower-level reservoir sampling
- `distinct()` / `distinct_per_key()` / `distinct_by(key_fn)` — exact deduplication
- `approx_distinct_count(k)` / `approx_distinct_count_per_key(k)` — estimated cardinality (KMV) → `PCollection<f64>`
- `approx_count_distinct()` / `approx_count_distinct_per_key()` — estimated cardinality (HyperLogLog++) → `PCollection<u64>` (also `*_with_error(e)` variants)
- `approx_median_*` / `approx_quantiles_*` — quantile estimators globally and per-key

**Lower-level combiners for use with `combine_values` / `combine_globally`:**
- `Sum` / `Min` / `Max` — basic numeric aggregation
- `AverageF64` / `Mean<O>` — arithmetic mean (typed output)
- `TopK` / `BottomK` — extreme-value selection
- `ToDict<K, V>` — materialize `(K, V)` pairs into a `HashMap<K, V>`
- `ApproxMedian` / `ApproxQuantiles` / `TDigest` — statistical estimators
- `DistinctCount` (KMV) / `HllApproxDistinctCount` (HyperLogLog++) — cardinality estimators

Custom combiners can be implemented via the `CombineFn` trait. Combiners that opt in to `is_associative_commutative()` automatically get tree-reduction at parallel-execution time.

### Flatten

Merge multiple `PCollection`s of the same type into one:

```rust
let merged = flatten(&[&pc1, &pc2, &pc3]);
```

### Multi-Output / Side Outputs

Split a collection into multiple typed output streams using enums and the `partition!` macro.
All output types are checked at compile time:

```rust
#[derive(Clone)]
enum ParseResult {
    Integer(i64),
    Float(f64),
    Invalid(String),
}

let classified = lines.flat_map(|s: &&str| {
    if let Ok(i) = s.parse::<i64>() { vec![ParseResult::Integer(i)] }
    else if let Ok(f) = s.parse::<f64>() { vec![ParseResult::Float(f)] }
    else { vec![ParseResult::Invalid(s.to_string())] }
});

partition!(classified, ParseResult, {
    Integer => integers,
    Float   => floats,
    Invalid => errors
});
// integers, floats, and errors are now independent PCollections
```

### N-Way CoGroupByKey

Full outer join of N-keyed collections on a shared key type:

```rust
// cogroup_by_key! supports 2–10 inputs
let grouped = cogroup_by_key!(users, orders, addresses);

let results = grouped.flat_map(|(user_id, (users, orders, addrs))| {
    // users: Vec<User>, orders: Vec<Order>, addrs: Vec<Address>
    vec![...]
});
```

### Joins

Two-collection keyed joins:

```rust
let joined = left.join_inner(&right);
```

Available: `join_inner`, `join_left`, `join_right`, `join_full`.

### Side Inputs

Enrich elements with small, broadcast auxiliary data:

```rust
// Vec side input
let primes = side_vec(vec![2u32, 3, 5, 7]);
let flagged = nums.map_with_side(&primes, |n, ps| ps.contains(n));

// HashMap side input (O(1) lookup)
let lookup = side_hashmap(vec![("a".to_string(), 1u32), ("b".to_string(), 2)]);
let enriched = data.map_with_side_map(&lookup, |(k, v), m| {
    (k.clone(), v + m.get(k).copied().unwrap_or(0))
});
```

### Sampling

Uniform reservoir sampling, deterministic across sequential and parallel execution:

```rust
// Beam-style fixed-size sample (returns PCollection<Vec<T>>)
let sample = data.sample_globally(1000);
let per_key = keyed.sample_per_key(10);

// Or the lower-level streaming reservoir
let sample = data.sample_reservoir_vec(1000, /*seed=*/ 42);
let per_key = keyed.sample_values_reservoir(10, 42);
```

### Batching

Group adjacent elements into batches for downstream APIs that prefer chunked input:

```rust
// Count-bounded batches
let chunks: PCollection<Vec<Record>> = records.batch_elements(500);

// Byte-size-bounded batches with an explicit size estimator
let chunks = records.batch_by_size(/*max_bytes=*/ 1_048_576, |r: &Record| r.size_estimate());

// Per-key batching: PCollection<(K, V)> -> PCollection<(K, Vec<V>)>
let keyed_chunks = keyed.group_into_batches(100);
```

### Tee / Fan-out

Split a collection into multiple identical downstream branches. The optimizer's dominator-based cache placement ensures the upstream pipeline runs once even when both branches consume it:

```rust
let (a, b) = data.tee();
let branches: Vec<PCollection<T>> = data.tee_n(4);
```

### Reshuffle and WaitOn

`reshuffle()` inserts an explicit graph-level barrier — preventing fusion and redistributing
elements across partitions. `wait_on()` is a signal-only dependency barrier: the returned
collection has the same elements as the original, but downstream stages don't start until the
signal collection has fully drained.

```rust
let balanced = uneven.reshuffle();              // force a fusion barrier
let after = data.wait_on(&prerequisite_sink);   // gate on completion of another branch
```

### Named Operations

Annotate steps with human-readable labels that show up in `explain` output. Use `with_name`
for individual nodes or `Pipeline::named_scope` to qualify every node created inside a closure:

```rust
let filtered = data
    .filter(|x: &i32| x > 0)
    .with_name("positive-only");

let result = p.named_scope("normalize", |_p| {
    raw.map(parse).filter(valid).map(canonicalize)
});
```

### Regex Transforms

String-collection helpers backed by the `regex` crate:

```rust
let matched = lines.regex_matches(r"^ERROR");
let codes = lines.regex_extract(r"code=(\d+)", 1);
let pairs = lines.regex_extract_kv(r"(\w+)=(\w+)", 1, 2);
let cleaned = lines.regex_replace_all(r"\s+", " ");
let tokens = lines.regex_split(r"[,;\s]+");
```

### Error Handling (Dead-Letter)

Split a fallible transform into a good-path collection and a dead-letter collection of failed
inputs paired with their errors:

```rust
let (parsed, errors): (PCollection<Record>, PCollection<DeadLetter<String>>) =
    raw_lines.map_catching(|line: &String| serde_json::from_str::<Record>(line));

parsed.write_jsonl("output/clean.jsonl")?;
errors.write_jsonl("output/quarantine.jsonl")?;
```

Use `flat_map_catching` for fallible 1→N transforms. For fail-fast behavior, use the existing
`try_map` / `collect_fail_fast` / `collect_skip_errors` family.

### Debug Taps

Inspect data passing through the pipeline without affecting downstream results:

```rust
let result = data
    .log_elements()                                   // requires T: Debug
    .filter(|x: &i32| x > 10)
    .log_elements_with(|x| format!("kept: {x:#x}")) // custom formatter
    .collect_seq()?;
```

### Execution

Execute pipelines in sequential or parallel mode:

```rust
let results = collection.collect_seq()?;  // Single-threaded
let results = collection.collect_par()?;  // Multithreading with Rayon
```

## I/O Examples

### JSON Lines

```rust
use ironbeam::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    id: u32,
    name: String,
}

fn main() -> anyhow::Result<()> {
    let p = Pipeline::default();

    // Read file
    let data = read_jsonl::<Record>(&p, "data.jsonl")?;

    // Process
    let filtered = data.filter(|r: &Record| r.id > 100);

    // Write results
    filtered.write_jsonl("output.jsonl")?;

    Ok(())
}
```

### CSV

```rust
let data = read_csv::<Record>(&p, "data.csv")?;
data.write_csv("output.csv")?;
```

### Parquet

```rust
let data = read_parquet::<Record>(&p, "data.parquet")?;
data.write_parquet("output.parquet")?;
```

### Avro

```rust
let data = read_avro::<Record>(&p, "data.avro")?;
data.write_avro("output.avro")?;
```

### XML

```rust
let data = read_xml::<Record>(&p, "data.xml")?;
data.write_xml("output.xml")?;
```

All I/O helpers support glob patterns (`"data/*.jsonl"`, `"shards/**/*.avro"`) and have
streaming and parallel-write variants (`read_*_streaming`, `write_*_par`).

### Compression

Compression is automatically detected by file extension:

```rust
// Read compressed file
let data = read_jsonl::<Record>(&p, "data.jsonl.gz")?;

// Write compressed file
data.write_jsonl("output.jsonl.zst")?;
```

Supported extensions: `.gz`, `.zst`, `.bz2`, `.xz`

## Advanced Features

### Windowing

Group time-series data into fixed or sliding windows:

```rust
let windowed = data
    .window_fixed(Duration::from_secs(60))
    .group_by_key()
    .combine_values(Sum);
```

### Checkpointing

Save and restore the pipeline's state for fault tolerance:

```rust
data.checkpoint("checkpoints/step1")?;

// Later, recover from checkpoint
let recovered = recover_checkpoint::<MyType>(&p, "checkpoints/step1")?;
```

### Metrics

Collect pipeline execution metrics:

```rust
let result = collection.collect_seq()?;
let metrics = p.get_metrics();
println!("Elements processed: {}", metrics.elements_processed);
```

### Automatic Memory Spilling

For memory-constrained environments, Ironbeam can automatically spill data to persistent storage when memory limits are exceeded:

```rust
use ironbeam::spill::SpillConfig;
use ironbeam::spill_integration::init_spilling;

// Configure automatic spilling with a 100MB memory limit
init_spilling(SpillConfig::new()
    .with_memory_limit(100 * 1024 * 1024)
    .with_spill_directory("/tmp/ironbeam-spill")
    .with_compression(true));

// Now run your pipeline - spilling happens automatically
let results = large_dataset
    .map(|x| heavy_computation(x))
    .collect_seq()?;
```

Data is transparently spilled to persistent storage when memory pressure is detected and automatically restored when needed. This allows processing datasets larger than available RAM without manual intervention.

[Learn more about spilling →](https://github.com/nhubbard/ironbeam/blob/main/src/spill.rs)

### Cloud I/O Abstractions

Write provider-agnostic code that works with any cloud service:

```rust
use ironbeam::io::cloud::*;

// Use fake implementations for testing
let storage = FakeObjectIO::new();
storage.put_object("bucket", "key", b"data")?;

// In production, swap in real implementations (AWS S3, GCS, Azure Blob, etc.)
// without changing your business logic
```

Supported cloud service categories:
- **Storage**: Object storage (S3/GCS/Azure), warehouses (BigQuery/Redshift), databases
- **Messaging**: Pub/sub (Kafka/Kinesis), queues (SQS), notifications (SNS)
- **Compute**: Serverless functions (Lambda/Cloud Functions)
- **ML**: Model inference (SageMaker/Vertex AI)
- **Observability**: Metrics, logging, configuration, caching

All traits are synchronous by design and include fake implementations for unit testing.

[Learn more about cloud I/O →](https://github.com/nhubbard/ironbeam/blob/main/src/io/cloud/mod.rs)

### Data Validation

Handle bad data gracefully in production pipelines:

```rust
use ironbeam::validation::*;

impl Validate for MyRecord {
    fn validate(&self) -> ValidationResult {
        let mut errors = Vec::new();

        if self.email.is_empty() {
            errors.push(ValidationError::field("email", "Email required"));
        }

        if self.age < 0 || self.age > 150 {
            errors.push(ValidationError::field("age", "Invalid age"));
        }

        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}

// Apply validation with configurable error handling
let validated = data
    .try_map(|record: &MyRecord| record.validate())
    .collect_fail_fast()?; // Or use .collect_skip_errors()
```

[Learn more about validation →](https://github.com/nhubbard/ironbeam/blob/main/src/validation.rs)

### Planner Optimizations

The planner transparently applies a number of rewrites before execution. You can inspect the
plan and the list of applied optimizations with `Pipeline::explain` (see
`examples/explain_pipeline.rs`):

- **Reshuffle elimination** — drops redundant reshuffles adjacent to barriers
- **Predicate pushdown** — past reshuffle and into individual `Flatten` subplans
- **Dead-subtree elimination** — prunes nodes that don't reach the terminal collection
- **CoGroup join ordering** — reorders inputs by estimated cardinality
- **Tree reduction** — O(log n) parallel merge for associative-commutative combiners
- **Limit pushdown / early termination** — `take(n)` / `first()` stops merge-phase work early
- **Bloom-filter semi-join** — pre-filters the larger side of `join_inner/left/right`
- **Dominator-based CSE cache placement** — caches shared subgraphs at the right node
- **Adaptive inter-stage partition counts** — partition count tracks cardinality across barriers
- **Empty / singleton source short-circuits** — fast-paths trivially small pipelines
- **Independent subgraph parallelism** — disjoint subgraphs of a single plan execute concurrently

## Examples

The `examples/` directory contains complete demonstrations:

- `etl_pipeline.rs` - Extract, transform, load workflow
- `advanced_joins.rs` - Join operations
- `co_gbk.rs` - N-way CoGroupByKey
- `multi_output.rs` - Side outputs via `partition!`
- `windowing_aggregations.rs` - Time-based windowing
- `combiners_showcase.rs` - Using built-in combiners
- `compressed_io.rs` - Working with compressed files
- `glob_patterns.rs` - Reading sharded inputs with glob patterns
- `checkpointing_demo.rs` / `checkpoint_recovery_demo.rs` - Checkpoint and recovery
- `metrics_example.rs` - Collecting metrics
- `cloud_io_demo.rs` - Cloud service integrations
- `data_quality_validation.rs` - Production data validation
- `explain_pipeline.rs` - Inspecting the planned execution graph
- `testing_pipeline.rs` - Testing patterns

Run examples with:

```bash
cargo run --example etl_pipeline --features io-jsonl,io-csv
cargo run --example cloud_io_demo
```

## Testing Your Pipelines

Ironbeam includes a comprehensive testing framework with utilities specifically designed for pipeline testing:

### Test Utilities

```rust
use ironbeam::testing::*;

#[test]
fn test_my_pipeline() -> anyhow::Result<()> {
    let p = TestPipeline::new();

    let result = from_vec(&p, vec![1, 2, 3])
        .map(|x: &i32| x * 2)
        .collect_seq()?;

    // Specialized assertions for collections
    assert_collections_equal(result.clone(), vec![2, 4, 6]);
    assert_all(&result, |x| x % 2 == 0);

    // PAssert fluent assertion builder
    PAssert::that(&result)
        .has_count(3)
        .contains_in_any_order(&[2, 4, 6])
        .all_match(|x| x % 2 == 0);

    Ok(())
}
```

### Pre-built Test Fixtures

```rust
use ironbeam::testing::fixtures::*;

// Ready-to-use test datasets
let logs = sample_log_entries();       // Web server logs
let users = user_product_interactions(); // Relational data
let series = time_series_data();       // Sensor readings
```

### Debug Utilities

```rust
// Inspect data during test execution
let result = data
    .debug_inspect("after filter")
    .filter(|x: &i32| x > 10)
    .debug_count("filtered count")
    .collect_seq()?;
```

[Learn more about testing →](https://github.com/nhubbard/ironbeam/blob/main/src/testing.rs)

### Running Tests

Run the full test suite:

```bash
cargo test
```

Run tests with specific features:

```bash
cargo test --features spilling
```

For coverage:

```bash
cargo llvm-cov --workspace --all-features --html --branch
```

## License

This project uses the Rust 2024 edition and requires at minimum Rust 1.88. It is developed primarily against Rust nightly.
