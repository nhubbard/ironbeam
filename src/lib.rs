//! # Ironbeam
//!
//! A **data processing framework** for Rust inspired by Apache Beam and Google Cloud Dataflow.
//! Ironbeam provides a declarative API for building batch data pipelines with support for
//! transformations, aggregations, joins, and I/O operations.
//!
//! ## Key Features
//!
//! - **Declarative pipeline API** - chain transformations with a fluent interface
//! - **Stateless and stateful operations** - `map`, `filter`, `flat_map`, `group_by_key`, `combine`
//! - **Built-in combiners** - `Sum`, `Min`, `Max`, `Average`, `DistinctCount`, `TopK`, and more
//! - **Join support** - inner, left, right, and full outer joins
//! - **Side inputs** - enrich streams with auxiliary data (vectors and hash maps)
//! - **Batch processing** - optimize CPU-heavy operations with batch transforms
//! - **Sequential and parallel execution** - choose the right mode for your workload
//! - **I/O integrations** - JSON Lines, CSV, and Parquet (all optional via feature flags)
//! - **Type-safe** - leverages Rust's type system for compile-time correctness
//!
//! ## Quick Start
//!
//! ```no_run
//! use ironbeam::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! // Create a pipeline
//! let p = Pipeline::default();
//!
//! // Build a word count pipeline
//! let lines = from_vec(&p, vec![
//!     "hello world".to_string(),
//!     "hello rust".to_string(),
//! ]);
//!
//! let counts = lines
//!     .flat_map(|line: &String| {
//!         line.split_whitespace()
//!             .map(|w| w.to_string())
//!             .collect::<Vec<_>>()
//!     })
//!     .key_by(|word: &String| word.clone())
//!     .map_values(|_word: &String| 1u64)
//!     .combine_values(Count);
//!
//! // Execute and collect results
//! let results = counts.collect_seq()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Core Concepts
//!
//! ### `Pipeline`
//!
//! A [`Pipeline`] is the container for your computation graph. Create one with
//! `Pipeline::default()`, then attach data sources and transformations to it.
//!
//! ### `PCollection`
//!
//! A [`PCollection<T>`] represents a distributed collection with elements of type `T`.
//! It's the fundamental abstraction for data in Ironbeam. Collections are:
//! - **Immutable** - transformations create new collections
//! - **Lazy** - computation happens when you call a collect method
//! - **Type-safe** - generic over the element type
//!
//! ### Transformations
//!
//! Ironbeam provides two categories of transforms:
//!
//! #### Stateless (element-wise)
//! - [`map`](PCollection::map) - transform each element
//! - [`filter`](PCollection::filter) - keep elements matching a predicate
//! - [`flat_map`](PCollection::flat_map) - transform each element into zero or more outputs
//! - [`map_batches`](PCollection::map_batches) - process elements in batches for efficiency
//!
//! #### Stateful (keyed operations)
//! - [`key_by`](PCollection::key_by) - convert `PCollection<T>` to `PCollection<(K, V)>`
//! - [`map_values`](PCollection::map_values) - transform values while preserving keys
//! - [`filter_values`](PCollection::filter_values) - filter-by-value predicate
//! - [`group_by_key`](PCollection::group_by_key) - group values by key into `Vec<V>`
//! - [`combine_values`](PCollection::combine_values) - aggregate values per key with a combiner
//! - [`top_k_per_key`](PCollection::top_k_per_key) - select the top-K largest values per key
//!
//! ### Combiners
//!
//! The [`combiners`] module provides reusable aggregation functions:
//! - [`Sum`] - sum numeric values
//! - [`Min`] / [`Max`] - find minimum/maximum values
//! - [`AverageF64`] - compute averages
//! - [`DistinctCount`] - count unique values
//! - [`TopK`] - select top K elements
//!
//! You can also implement custom combiners via the [`CombineFn`] trait.
//!
//! ### Joins
//!
//! Ironbeam supports all standard join types for `PCollection<(K, V)>`:
//! - [`join_inner`](PCollection::join_inner) - inner join
//! - [`join_left`](PCollection::join_left) - left outer join
//! - [`join_right`](PCollection::join_right) - right outer join
//! - [`join_full`](PCollection::join_full) - full outer join
//!
//! ### Side Inputs
//!
//! Enrich your pipeline with auxiliary data using side inputs:
//! - [`side_vec`] - create a side input from a vector
//! - [`side_hashmap`] - create a side input from a hash map
//! - [`map_with_side`](PCollection::map_with_side) - transform with side data
//! - [`filter_with_side`](PCollection::filter_with_side) - filter with side data
//!
//! ### Execution Modes
//!
//! Choose how to execute your pipeline:
//! - **Sequential** - [`collect_seq()`](PCollection::collect_seq) - single-threaded, in-order
//! - **Parallel** - [`collect_par()`](PCollection::collect_par) - multi-threaded with Rayon
//!
//! Both modes produce the same results; parallel execution is useful for CPU-intensive workloads.
//!
//! ## I/O Operations
//!
//! Ironbeam supports reading and writing common data formats (all optional via feature flags):
//!
//! ### JSON Lines (feature: `io-jsonl`)
//! ```no_run
//! use ironbeam::*;
//! use serde::{Deserialize, Serialize};
//! # use anyhow::Result;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Record { id: u32, name: String }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//!
//! // Read entire file into memory
//! let data = read_jsonl::<Record>(&p, "data.jsonl")?;
//!
//! // Or stream by chunks
//! let stream = read_jsonl_streaming::<Record>(&p, "data.jsonl", 1000)?;
//!
//! // Write results
//! data.write_jsonl("output.jsonl")?;
//! # Ok(())
//! # }
//! ```
//!
//! ### CSV (feature: `io-csv`)
//! ```no_run
//! use ironbeam::*;
//! use serde::{Deserialize, Serialize};
//! # use anyhow::Result;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Row { k: String, v: u64 }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let data = read_csv::<Row>(&p, "data.csv", true)?; // true = has headers
//! data.write_csv("output.csv", true)?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Parquet (feature: `io-parquet`)
//! ```no_run
//! use ironbeam::*;
//! use serde::{Deserialize, Serialize};
//! # use anyhow::Result;
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Record { id: u32, score: f64 }
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let data = read_parquet_streaming::<Record>(&p, "data.parquet", 1)?;
//! data.write_parquet("output.parquet")?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature Flags
//!
//! - `io-jsonl` - Enable JSON Lines I/O support
//! - `io-csv` - Enable CSV I/O support
//! - `io-parquet` - Enable Parquet I/O support (requires Arrow)
//! - `parallel-io` - Enable parallel I/O operations (`write_*_par` methods)
//! - `metrics` - Enable metrics collection and reporting (enabled by default)
//! - `checkpointing` - Enable automatic checkpointing for fault tolerance (enabled by default)
//!
//! ## Examples
//!
//! ### Word Count
//! ```no_run
//! use ironbeam::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let text = from_vec(&p, vec!["foo bar baz".to_string()]);
//!
//! let counts = text
//!     .flat_map(|s: &String| s.split_whitespace().map(String::from).collect::<Vec<_>>())
//!     .key_by(|w: &String| w.clone())
//!     .map_values(|_: &String| 1u64)
//!     .combine_values(Count);
//!
//! let results = counts.collect_seq()?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Group and Aggregate
//! ```no_run
//! use ironbeam::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let sales = from_vec(&p, vec![
//!     ("product_a".to_string(), 100u64),
//!     ("product_b".to_string(), 200u64),
//!     ("product_a".to_string(), 150u64),
//! ]);
//!
//! let totals = sales.combine_values(Sum::<u64>::default());
//! let results = totals.collect_seq()?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Join Two Collections
//! ```no_run
//! use ironbeam::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//!
//! let users = from_vec(&p, vec![
//!     (1u32, "Alice".to_string()),
//!     (2u32, "Bob".to_string()),
//! ]);
//!
//! let scores = from_vec(&p, vec![
//!     (1u32, 95u32),
//!     (2u32, 87u32),
//!     (3u32, 92u32), // no matching user
//! ]);
//!
//! let joined = users.join_inner(&scores);
//! let results = joined.collect_seq()?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Using Side Inputs
//! ```no_run
//! use ironbeam::*;
//! use std::collections::HashMap;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//!
//! let events = from_vec(&p, vec!["user_123".to_string(), "user_456".to_string()]);
//!
//! let user_names = side_hashmap::<String, String>(vec![
//!     ("user_123".into(), "Alice".into()),
//!     ("user_456".into(), "Bob".into()),
//! ]);
//!
//! let enriched = events.map_with_side_map(&user_names, |user_id, names| {
//!     let name = names.get(user_id).cloned().unwrap_or_else(|| "Unknown".into());
//!     format!("{}: {}", user_id, name)
//! });
//!
//! let results = enriched.collect_seq()?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Tracking Metrics
//! ```no_run
//! # #[cfg(feature = "metrics")]
//! # {
//! use ironbeam::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//!
//! // Enable metrics collection
//! let mut metrics = metrics::MetricsCollector::new();
//! metrics.register(Box::new(metrics::CounterMetric::with_value("input_records", 1000)));
//! p.set_metrics(metrics);
//!
//! // Build and execute pipeline
//! let data = from_vec(&p, (0..1000).collect::<Vec<i32>>());
//! let result = data
//!     .filter(|x: &i32| x % 2 == 0)
//!     .map(|x: &i32| x * 2)
//!     .collect_seq()?;
//!
//! // Print or save metrics after execution
//! if let Some(metrics) = p.take_metrics() {
//!     metrics.print();
//!     // Or save to a file
//!     metrics.save_to_file("pipeline_metrics.json")?;
//! }
//! # Ok(())
//! # }
//! # }
//! ```
//!
//! ### Using Checkpointing for Fault Tolerance
//! ```no_run
//! # #[cfg(feature = "checkpointing")]
//! # {
//! use ironbeam::*;
//! use ironbeam::checkpoint::{CheckpointConfig, CheckpointPolicy};
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//! let data = from_vec(&p, (0..1_000_000).collect::<Vec<i32>>());
//!
//! // Configure automatic checkpointing
//! let checkpoint_config = CheckpointConfig {
//!     enabled: true,
//!     directory: "./checkpoints".into(),
//!     policy: CheckpointPolicy::AfterEveryBarrier,
//!     auto_recover: true,
//!     max_checkpoints: Some(5),
//! };
//!
//! let runner = Runner {
//!     mode: ExecMode::Sequential,
//!     checkpoint_config: Some(checkpoint_config),
//!     ..Default::default()
//! };
//!
//! // Build pipeline - checkpoints will be created automatically
//! let result_collection = data
//!     .key_by(|x: &i32| x % 100)
//!     .map_values(|x: &i32| *x as u64)
//!     .combine_values(Sum::<u64>::default());
//!
//! // Pipeline will checkpoint after each barrier and can recover on failure
//! let result = runner.run_collect::<(i32, u64)>(&p, result_collection.node_id())?;
//! # Ok(())
//! # }
//! # }
//! ```
//!
//! ## Testing Your Pipelines
//!
//! Ironbeam provides comprehensive testing utilities in the [`testing`] module to help you
//! write idiomatic Rust tests for your data pipelines.
//!
//! ### Basic Testing
//! ```no_run
//! use ironbeam::*;
//! use ironbeam::testing::*;
//! use anyhow::Result;
//!
//! #[test]
//! fn test_simple_pipeline() -> Result<()> {
//!     let p = TestPipeline::new();
//!
//!     let result = from_vec(&p, vec![1, 2, 3])
//!         .map(|x: &i32| x * 2)
//!         .collect_seq()?;
//!
//!     assert_collections_equal(result, vec![2, 4, 6]);
//!     Ok(())
//! }
//! ```
//!
//! ### Testing with Assertions
//! The testing module provides specialized assertions for collections:
//!
//! - [`testing::assert_collections_equal`] - Exact order-dependent comparison
//! - [`testing::assert_collections_unordered_equal`] - Order-independent comparison
//! - [`testing::assert_kv_collections_equal`] - Compare key-value pairs (sorted by key)
//! - [`testing::assert_all`] / [`testing::assert_any`] / [`testing::assert_none`] - Predicate-based assertions
//!
//! ### Test Data Builders
//! Create test data fluently with builders:
//!
//! ```
//! use ironbeam::testing::*;
//!
//! let data = TestDataBuilder::<i32>::new()
//!     .add_range(1..=10)
//!     .add_value(100)
//!     .add_repeated(42, 3)
//!     .build();
//!
//! let kvs = KVTestDataBuilder::new()
//!     .add_kv("a", 1)
//!     .add_key_with_values("b", vec![2, 3, 4])
//!     .build();
//! ```
//!
//! ### Debug Utilities
//! Inspect pipelines during test execution:
//!
//! ```no_run
//! use ironbeam::*;
//! use ironbeam::testing::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = TestPipeline::new();
//! let result = from_vec(&p, vec![1, 2, 3])
//!     .debug_inspect("after source")  // Prints elements to stderr
//!     .map(|x: &i32| x * 2)
//!     .debug_count("after map")       // Prints count
//!     .debug_sample(5, "first 5")     // Prints first 5 elements
//!     .collect_seq()?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Pre-built Fixtures
//! Use common test datasets for realistic testing:
//!
//! ```
//! use ironbeam::testing::*;
//!
//! let logs = sample_log_entries();       // Web server logs
//! let words = word_count_data();         // Text data for word counting
//! let ts = time_series_data();           // Time-series measurements
//! let users = user_product_interactions(); // Relational data
//! ```
//!
//! For more examples, see the [`testing`] module documentation and
//! run `cargo run --example testing_pipeline`.
//!
//! ## Performance Tips
//!
//! - Use [`map_batches`](PCollection::map_batches) for CPU-intensive operations
//! - Use streaming I/O for large files that don't fit in memory
//! - Use parallel execution ([`collect_par`](PCollection::collect_par)) for CPU-bound workloads
//! - Combine multiple stateless operations before stateful ones (the planner will fuse them)
//! - Use [`combine_values_lifted`](PCollection::combine_values_lifted) after `group_by_key` for better performance
//!
//! ## Architecture
//!
//! Ironbeam uses a **deferred execution** model:
//! 1. Building a pipeline creates a computation graph (DAG) of transformations
//! 2. The [`planner`] optimizes the graph (fusion, etc.)
//! 3. The [`runner`] executes the optimized plan when you call a collect method
//! 4. Results are materialized into memory
//!
//! ## Module Overview
//!
//! - [`collection`] - Core `PCollection` type and transformation methods
//! - [`combiners`] - Built-in aggregation functions (Sum, Min, Max, etc.)
//! - [`pipeline`] - Pipeline construction and management
//! - [`io`] - I/O operations for JSON Lines, CSV, and Parquet
//! - [`runner`] - Execution engine (sequential and parallel modes)
//! - [`planner`] - Query optimization and graph transformations
//! - [`helpers`] - Convenience functions and side input builders
//! - [`extensions`] - Extension points for custom transforms and I/O
//! - [`metrics`] - Metrics collection and reporting (feature: `metrics`)
//! - [`checkpoint`] - Automatic checkpointing for fault tolerance (feature: `checkpointing`)
//!
//! ## Extensibility
//!
//! Ironbeam provides several extension points for adding custom functionality:
//!
//! ### Custom Transforms
//! Implement [`DynOp`] to create custom stateless transformations:
//! ```no_run
//! use ironbeam::*;
//! use ironbeam::node::DynOp;
//! use ironbeam::type_token::Partition;
//! use std::sync::Arc;
//!
//! struct MyCustomOp;
//! impl DynOp for MyCustomOp {
//!     fn apply(&self, input: Partition) -> Partition {
//!         // Your custom logic here
//!         input
//!     }
//! }
//!
//! let p = Pipeline::default();
//! let data = from_vec(&p, vec![1, 2, 3]);
//! let transformed = data.apply_transform::<i32>(Arc::new(MyCustomOp));
//! ```
//!
//! ### Custom I/O Sources
//! Implement [`VecOps`] to integrate custom data sources.
//! See [`from_custom_source`] for a complete example.
//!
//! ### Composite Transforms
//! Use [`CompositeTransform`] to package reusable pipelines:
//! ```no_run
//! use ironbeam::*;
//! use ironbeam::extensions::CompositeTransform;
//!
//! struct MyComposite;
//! impl CompositeTransform<String, String> for MyComposite {
//!     fn expand(&self, input: PCollection<String>) -> PCollection<String> {
//!         input.map(|s: &String| s.to_uppercase())
//!              .filter(|s: &String| !s.is_empty())
//!     }
//! }
//! ```

pub mod collection;
pub mod combiners;
pub mod extensions;
pub mod helpers;
pub mod io;
pub mod node;
pub mod node_id;
pub mod pipeline;
pub mod planner;
pub mod runner;
pub mod testing;
pub mod type_token;
pub mod utils;
pub mod validation;
pub mod window;

#[cfg(feature = "metrics")]
pub mod metrics;

#[cfg(feature = "checkpointing")]
pub mod checkpoint;

// General re-exports
pub use collection::{CombineFn, Count, PCollection, RFBound};
pub use combiners::{AverageF64, DistinctCount, Max, Min, Sum, TopK};
pub use helpers::*;
pub use node_id::NodeId;
pub use pipeline::Pipeline;
pub use planner::{
    CostEstimate, ExecutionExplanation, ExplainStep, OptimizationDecision, Plan, build_plan,
};
pub use runner::{ExecMode, Runner};
pub use type_token::Partition;
pub use utils::OrdF64;
pub use window::{TimestampMs, Timestamped, Window};

// Extension point exports
pub use extensions::CompositeTransform;
pub use node::DynOp;
pub use type_token::{TypeTag, VecOps};

// Gated re-exports
#[cfg(feature = "io-jsonl")]
pub use io::jsonl::{read_jsonl_range, read_jsonl_vec};

#[cfg(feature = "io-jsonl")]
pub use helpers::jsonl::read_jsonl_streaming;

#[cfg(all(feature = "io-jsonl", feature = "parallel-io"))]
pub use io::jsonl::write_jsonl_par;

#[cfg(feature = "io-csv")]
pub use io::csv::{read_csv_vec, write_csv, write_csv_vec};

#[cfg(all(feature = "io-csv", feature = "parallel-io"))]
pub use io::csv::write_csv_par;

#[cfg(feature = "io-parquet")]
pub use io::parquet::{read_parquet_vec, write_parquet_vec};

#[cfg(feature = "io-csv")]
pub use helpers::csv::read_csv;
#[cfg(feature = "io-csv")]
pub use helpers::csv::read_csv_streaming;
#[cfg(feature = "io-jsonl")]
pub use helpers::jsonl::read_jsonl;
#[cfg(feature = "io-parquet")]
pub use helpers::parquet::read_parquet_streaming;
