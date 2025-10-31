//! # Rustflow
//!
//! A **data processing framework** for Rust inspired by Apache Beam and Google Cloud Dataflow.
//! Rustflow provides a declarative API for building batch data pipelines with support for
//! transformations, aggregations, joins, and I/O operations.
//!
//! ## Key Features
//!
//! - **Declarative pipeline API** - chain transformations with a fluent interface
//! - **Stateless and stateful operations** - map, filter, flat_map, group_by_key, combine
//! - **Built-in combiners** - Sum, Min, Max, Average, DistinctCount, TopK, and more
//! - **Join support** - inner, left, right, and full outer joins
//! - **Side inputs** - enrich streams with auxiliary data (vectors and hash maps)
//! - **Batch processing** - optimize CPU-heavy operations with batch transforms
//! - **Sequential and parallel execution** - choose the right mode for your workload
//! - **I/O integrations** - JSON Lines, CSV, and Parquet (all optional via feature flags)
//! - **Type-safe** - leverages Rust's type system for compile-time correctness
//!
//! ## Quick Start
//!
//! ```ignore
//! use rustflow::*;
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
//! ### Pipeline
//!
//! A [`Pipeline`] is the container for your computation graph. Create one with
//! `Pipeline::default()`, then attach data sources and transformations to it.
//!
//! ### PCollection
//!
//! A [`PCollection<T>`] represents a distributed collection of elements of type `T`.
//! It's the fundamental abstraction for data in Rustflow. Collections are:
//! - **Immutable** - transformations create new collections
//! - **Lazy** - computation happens when you call a collect method
//! - **Type-safe** - generic over the element type
//!
//! ### Transformations
//!
//! Rustflow provides two categories of transforms:
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
//! - [`filter_values`](PCollection::filter_values) - filter by value predicate
//! - [`group_by_key`](PCollection::group_by_key) - group values by key into `Vec<V>`
//! - [`combine_values`](PCollection::combine_values) - aggregate values per key with a combiner
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
//! Rustflow supports all standard join types for `PCollection<(K, V)>`:
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
//! Rustflow supports reading and writing common data formats (all optional via feature flags):
//!
//! ### JSON Lines (feature: `io-jsonl`)
//! ```ignore
//! use rustflow::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Record { id: u32, name: String }
//!
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
//! ```
//!
//! ### CSV (feature: `io-csv`)
//! ```rust,ignore
//! use rustflow::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Row { k: String, v: u64 }
//!
//! let p = Pipeline::default();
//! let data = read_csv::<Row>(&p, "data.csv", true)?; // true = has headers
//! data.write_csv("output.csv", true)?;
//! ```
//!
//! ### Parquet (feature: `io-parquet`)
//! ```ignore
//! use rustflow::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Record { id: u32, score: f64 }
//!
//! let p = Pipeline::default();
//! let data = read_parquet_streaming::<Record>(&p, "data.parquet", 1)?;
//! data.write_parquet("output.parquet")?;
//! ```
//!
//! ## Feature Flags
//!
//! - `io-jsonl` - Enable JSON Lines I/O support
//! - `io-csv` - Enable CSV I/O support
//! - `io-parquet` - Enable Parquet I/O support (requires Arrow)
//! - `parallel-io` - Enable parallel I/O operations (`write_*_par` methods)
//!
//! ## Examples
//!
//! ### Word Count
//! ```ignore
//! use rustflow::*;
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
//! ```ignore
//! use rustflow::*;
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
//! ```ignore
//! use rustflow::*;
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
//! ```ignore
//! use rustflow::*;
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
//! let enriched = events.map_with_side_map(user_names, |user_id, names| {
//!     let name = names.get(user_id).cloned().unwrap_or_else(|| "Unknown".into());
//!     format!("{}: {}", user_id, name)
//! });
//!
//! let results = enriched.collect_seq()?;
//! # Ok(())
//! # }
//! ```
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
//! Rustflow uses a **deferred execution** model:
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

pub mod collection;
pub mod combiners;
pub mod io;
pub mod node;
pub mod node_id;
pub mod pipeline;
pub mod planner;
pub mod runner;
pub mod type_token;
pub mod window;
pub mod helpers;

// General re-exports
pub use collection::{CombineFn, Count, PCollection, RFBound};
pub use combiners::{AverageF64, DistinctCount, Max, Min, Sum, TopK};
pub use node_id::NodeId;
pub use pipeline::Pipeline;
pub use runner::{ExecMode, Runner};
pub use type_token::Partition;
pub use window::{TimestampMs, Timestamped, Window};
pub use helpers::*;

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

#[cfg(feature = "io-parquet")]
pub use helpers::parquet::read_parquet_streaming;
#[cfg(feature = "io-csv")]
pub use helpers::csv::read_csv;
#[cfg(feature = "io-csv")]
pub use helpers::csv::read_csv_streaming;
#[cfg(feature = "io-jsonl")]
pub use helpers::jsonl::read_jsonl;
