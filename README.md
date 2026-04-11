# Ironbeam

A data processing framework for Rust inspired by Apache Beam and Google Cloud Dataflow. Ironbeam provides a declarative API for building batch data pipelines with support for transformations, aggregations, joins, and I/O operations.

## Features

- **Declarative pipeline API** with fluent interface
- **Stateless operations**: `map`, `filter`, `flat_map`, `map_batches`, `filter_map`
- **Stateful operations**: `group_by_key`, `combine_values`, keyed aggregations
- **Built-in aggregations**: `count_globally`, `count_per_key`, `count_per_element`, `top_k_per_key`, `to_list_per_key`, `to_set_per_key`, `latest_globally`, `latest_per_key`, `sample_reservoir`, `sample_values_reservoir`, `distinct`, `distinct_per_key`, `approx_distinct_count`; lower-level combiners (`Sum`, `Min`, `Max`, `AverageF64`, `ApproxMedian`, `ApproxQuantiles`, `TDigest`) available via `combine_values` / `combine_globally`
- **Flatten**: merge multiple `PCollection`s of the same type into one
- **Multi-output / side outputs**: enum-based routing with the `partition!` macro, compile-time type-safe
- **N-way CoGroupByKey**: `cogroup_by_key!` macro for 2–10 inputs
- **Enhanced filters**: `filter_eq`, `filter_ne`, `filter_lt`, `filter_le`, `filter_gt`, `filter_ge`, `filter_range`, `filter_range_inclusive`, `filter_by`
- **Reservoir sampling**: `sample_reservoir` (global) and `sample_values_reservoir` (per-key)
- **Join support**: inner, left, right, and full outer joins
- **Side inputs** for enriching streams with auxiliary data (Vec and HashMap views)
- **Sequential and parallel execution** modes
- **Type-safe** with compile-time correctness
- **Optional I/O backends**: JSON Lines, CSV, Parquet
- **Optional compression**: gzip, zstd, bzip2, xz
- **Metrics collection** and **checkpointing** for fault tolerance
- **Automatic memory spilling** to disk for memory-constrained environments
- **Cloud I/O abstractions** for provider-agnostic cloud integrations
- **Data validation utilities** for production-grade error handling
- **Comprehensive testing framework** with fixtures and assertions

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ironbeam = "2.5.0"
```

By default, all features are enabled. To use a minimal configuration:

```toml
[dependencies]
ironbeam = { version = "2.5.0", default-features = false }
```

Available feature flags:

- `io-jsonl` - JSON Lines support
- `io-csv` - CSV support
- `io-parquet` - Parquet support
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

**Stateful operations** work on keyed data:

- `key_by` / `with_keys` / `with_constant_key` - convert to a keyed collection
- `map_values` / `filter_values` - transform or filter values while preserving keys
- `group_by_key` - group values by key
- `combine_values` / `combine_values_lifted` - aggregate values per key
- `top_k_per_key` - select top K values per key
- `distinct` / `distinct_per_key` - remove duplicate elements or values

### Combiners

Ironbeam provides convenience methods for common aggregations, plus lower-level combiners for
use with `combine_values` / `combine_globally`.

**Convenience methods (call directly on a collection):**
- `count_globally()` — total element count → `PCollection<u64>`
- `count_per_key()` — element count per key → `PCollection<(K, u64)>`
- `count_per_element()` — frequency of each distinct element → `PCollection<(T, u64)>`
- `top_k_per_key(k)` — K largest values per key → `PCollection<(K, Vec<V>)>`
- `to_list_per_key()` — all values per key → `PCollection<(K, Vec<V>)>`
- `to_set_per_key()` — unique values per key → `PCollection<(K, HashSet<V>)>`
- `latest_globally()` — value with the latest timestamp → `PCollection<T>`
- `latest_per_key()` — latest value per key → `PCollection<(K, T)>`
- `sample_reservoir(k, seed)` — global reservoir sample → `PCollection<T>`
- `sample_reservoir_vec(k, seed)` — global sample as a single Vec → `PCollection<Vec<T>>`
- `sample_values_reservoir(k, seed)` — per-key reservoir sample → `PCollection<(K, V)>`
- `distinct()` — remove global duplicates → `PCollection<T>`
- `distinct_per_key()` — remove duplicate values per key → `PCollection<(K, V)>`
- `approx_distinct_count(k)` — estimated cardinality (KMV) → `PCollection<f64>`
- `approx_distinct_count_per_key(k)` — estimated cardinality per key → `PCollection<(K, f64)>`

**Lower-level combiners for use with `combine_values` / `combine_globally`:**
- `Sum` / `Min` / `Max` — basic numeric aggregation
- `AverageF64` — arithmetic mean
- `ApproxMedian` / `ApproxQuantiles` / `TDigest` — statistical estimators

Custom combiners can be implemented via the `CombineFn` trait.

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
// Sample 1000 elements globally (returns PCollection<Vec<T>>)
let sample = data.sample_reservoir_vec(1000, /*seed=*/ 42);

// Sample 10 values per key
let per_key = keyed.sample_values_reservoir(10, 42);
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

## Examples

The `examples/` directory contains complete demonstrations:

- `etl_pipeline.rs` - Extract, transform, load workflow
- `advanced_joins.rs` - Join operations
- `windowing_aggregations.rs` - Time-based windowing
- `combiners_showcase.rs` - Using built-in combiners
- `compressed_io.rs` - Working with compressed files
- `checkpointing_demo.rs` - Checkpoint and recovery
- `metrics_example.rs` - Collecting metrics
- `cloud_io_demo.rs` - Cloud service integrations
- `data_quality_validation.rs` - Production data validation
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
    assert_collections_equal(result, vec![2, 4, 6]);
    assert_all(&result, |x| x % 2 == 0);

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
cargo tarpaulin --out Html
```

## License

This project uses the Rust 2024 edition and requires a recent Rust toolchain.
