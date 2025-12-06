# Ironbeam

A data processing framework for Rust inspired by Apache Beam and Google Cloud Dataflow. Ironbeam provides a declarative API for building batch data pipelines with support for transformations, aggregations, joins, and I/O operations.

## Features

- Declarative pipeline API with fluent interface
- Stateless operations: `map`, `filter`, `flat_map`, `map_batches`
- Stateful operations: `group_by_key`, `combine_values`, keyed aggregations
- Built-in combiners: Sum, Min, Max, Average, DistinctCount, TopK
- Join support: inner, left, right, and full outer joins
- Side inputs for enriching streams with auxiliary data
- Sequential and parallel execution modes
- Type-safe with compile-time correctness
- Optional I/O backends: JSON Lines, CSV, Parquet
- Optional compression: gzip, zstd, bzip2, xz
- Metrics collection and checkpointing support

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ironbeam = "1.0.0"
```

By default, all features are enabled. To use a minimal configuration:

```toml
[dependencies]
ironbeam = { version = "1.0.0", default-features = false }
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

- `key_by` - convert to a keyed collection
- `map_values` - transform values while preserving keys
- `group_by_key` - group values by key
- `combine_values` - aggregate values per key
- `top_k_per_key` - select top K values per key

### Combiners

Combiners provide efficient aggregation. Built-in options include:

- `Count` - count elements
- `Sum` - sum numeric values
- `Min` / `Max` - find minimum/maximum
- `AverageF64` - compute averages
- `DistinctCount` - count unique values
- `TopK` - select top K elements

Custom combiners can be implemented via the `CombineFn` trait.

### Joins

Keyed collections support all standard join operations:

```rust
let joined = left_collection.join_inner(&right_collection)?;
```

Available methods: `join_inner`, `join_left`, `join_right`, `join_full`.

### Side Inputs

Enrich pipelines with auxiliary data:

```rust
let lookup = side_hashmap(&p, my_map);
data.map_with_side(&lookup, |elem, map| {
    // Use map to enrich elem
})
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

Save and restore the pipeline's state:

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

## Examples

The `examples/` directory contains complete demonstrations:

- `etl_pipeline.rs` - Extract, transform, load workflow
- `advanced_joins.rs` - Join operations
- `windowing_aggregations.rs` - Time-based windowing
- `combiners_showcase.rs` - Using built-in combiners
- `compressed_io.rs` - Working with compressed files
- `checkpointing_demo.rs` - Checkpoint and recovery
- `metrics_example.rs` - Collecting metrics
- `testing_pipeline.rs` - Testing patterns

Run examples with:

```bash
cargo run --example etl_pipeline --features io-jsonl,io-csv
```

## Testing

Run tests with:

```bash
cargo test
```

For coverage:

```bash
cargo tarpaulin --out Html
```

## License

This project uses the Rust 2024 edition and requires a recent Rust toolchain.
