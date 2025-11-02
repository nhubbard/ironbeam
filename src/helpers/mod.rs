//! High-level convenience helpers and extension methods for [`PCollection`].
//!
//! This module extends the core [`PCollection`] API with specialized transformations
//! and utilities that build on top of the fundamental operations. These helpers provide
//! more ergonomic and domain-specific ways to work with collections.
//!
//! ## Module Organization
//!
//! The helpers are organized by functionality:
//!
//! ### Batching Operations
//! - [`batches`] - Process elements in fixed-size batches for efficiency
//!   - [`PCollection::map_batches`](crate::PCollection::map_batches)
//!   - [`PCollection::map_values_batches`](crate::PCollection::map_values_batches)
//!
//! ### Keyed Operations
//! - [`keyed`] - Helpers for working with keyed collections `PCollection<(K, V)>`
//!   - [`PCollection::key_by`](crate::PCollection::key_by)
//! - [`values`] - Value-only transformations on keyed collections
//!   - [`PCollection::map_values`](crate::PCollection::map_values)
//!   - [`PCollection::filter_values`](crate::PCollection::filter_values)
//!
//! ### Aggregations
//! - [`combine`] - Per-key aggregations with combiners
//!   - [`PCollection::combine_values`](crate::PCollection::combine_values)
//!   - [`PCollection::combine_values_lifted`](crate::PCollection::combine_values_lifted)
//! - [`combine_global`] - Global aggregations across the entire collection
//!   - [`PCollection::combine_global`](crate::PCollection::combine_global)
//!
//! ### Joins
//! - [`joins`] - Join operations for keyed collections
//!   - [`PCollection::join_inner`](crate::PCollection::join_inner)
//!   - [`PCollection::join_left`](crate::PCollection::join_left)
//!   - [`PCollection::join_right`](crate::PCollection::join_right)
//!   - [`PCollection::join_full`](crate::PCollection::join_full)
//!
//! ### Side Inputs
//! - [`side_inputs`] - Enrich streams with auxiliary data
//!   - [`side_vec`](side_vec) - Create a side input from a vector
//!   - [`side_hashmap`](side_hashmap) - Create a side input from a hash map
//!   - [`PCollection::map_with_side`](crate::PCollection::map_with_side)
//!   - [`PCollection::filter_with_side`](crate::PCollection::filter_with_side)
//!
//! ### Distinct Operations
//! - [`distinct`] - Remove duplicate elements
//!   - [`PCollection::distinct`](crate::PCollection::distinct)
//!   - [`PCollection::distinct_per_key`](crate::PCollection::distinct_per_key)
//!
//! ### Sampling
//! - [`sampling`] - Random sampling operations
//!   - [`PCollection::sample`](crate::PCollection::sample)
//!
//! ### Top-K Operations
//! - [`topk`] - Convenience API for selecting top K values per key
//!   - [`PCollection::top_k_per_key`](crate::PCollection::top_k_per_key)
//!
//! ### Error Handling
//! - [`try_process`] - Fallible transformations
//!   - [`PCollection::try_map`](crate::PCollection::try_map)
//!   - [`PCollection::try_flat_map`](crate::PCollection::try_flat_map)
//!   - [`PCollection::collect_fail_fast`](crate::PCollection::collect_fail_fast)
//!
//! ### Sorting
//! - [`collect_sorted`] - Collect results in sorted order
//!   - [`PCollection::collect_seq_sorted`](crate::PCollection::collect_seq_sorted)
//!   - [`PCollection::collect_par_sorted`](crate::PCollection::collect_par_sorted)
//!   - [`PCollection::collect_par_sorted_by_key`](crate::PCollection::collect_par_sorted_by_key)
//!
//! ### I/O Helpers
//! - [`jsonl`] - JSON Lines I/O utilities (feature: `io-jsonl`)
//!   - [`read_jsonl`](read_jsonl)
//!   - [`read_jsonl_streaming`](read_jsonl_streaming)
//!   - [`PCollection::write_jsonl`](crate::PCollection::write_jsonl)
//! - [`csv`] - CSV I/O utilities (feature: `io-csv`)
//!   - [`read_csv`](read_csv)
//!   - [`read_csv_streaming`](read_csv_streaming)
//!   - [`PCollection::write_csv`](crate::PCollection::write_csv)
//! - [`parquet`] - Parquet I/O utilities (feature: `io-parquet`)
//!   - [`read_parquet_streaming`](read_parquet_streaming)
//!   - [`PCollection::write_parquet`](crate::PCollection::write_parquet)
//!
//! ### Windowing
//! - [`tumbling`] - Tumbling window operations
//!   - [`PCollection::window_tumbling`](crate::PCollection::window_tumbling)
//! - [`timestamped`] - Timestamp utilities for windowed data
//!
//! ### Standard Library Integration
//! - [`stdlib`] - Convenience constructors for common sources
//!   - [`from_vec`](from_vec) - Create a collection from a vector
//!   - [`from_iter`](from_iter) - Create a collection from an iterator
//!
//! ### Internal Utilities
//! - [`common`] - Shared internal utilities used by other helpers
//!
//! ## Design Philosophy
//!
//! Helpers in this module follow these principles:
//!
//! 1. **Build on core primitives** - All helpers are implemented using the fundamental
//!    operations from [`PCollection`].
//!
//! 2. **Ergonomic APIs** - Provide convenient, type-safe interfaces that reduce boilerplate.
//!
//! 3. **Composable** - Helpers can be chained together naturally.
//!
//! 4. **Feature-gated** - I/O helpers are behind optional feature flags to minimize
//!    dependencies.
//!
//! 5. **Extension traits** - Many helpers are implemented as extension methods on
//!    [`PCollection`] for discoverability.
//!
//! ## Example Usage
//!
//! ```ignore
//! use rustflow::*;
//! # use anyhow::Result;
//!
//! # fn main() -> Result<()> {
//! let p = Pipeline::default();
//!
//! // Use stdlib helper to create a collection
//! let data = from_vec(&p, vec![1, 2, 3, 4, 5]);
//!
//! // Use batching for efficient processing
//! let processed = data.map_batches(2, |batch| {
//!     batch.iter().map(|x| x * 2).collect()
//! });
//!
//! // Use keyed operations
//! let keyed = processed
//!     .key_by(|x: &i32| x % 2)  // key by even/odd
//!     .map_values(|x| *x);
//!
//! // Use aggregation
//! let sums = keyed.combine_values(Sum::<i32>::default());
//!
//! // Use sorted collection
//! let results = sums.collect_seq_sorted()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## See Also
//!
//! - [`PCollection`](crate::PCollection) - Core collection type
//! - [`combiners`](crate::combiners) - Built-in aggregation functions
//! - [`Pipeline`](crate::Pipeline) - Pipeline construction

pub mod batches;
pub mod collect_sorted;
pub mod combine;
pub mod combine_global;
pub mod common;
pub mod csv;
pub mod distinct;
pub mod joins;
pub mod jsonl;
pub mod keyed;
pub mod parquet;
pub mod sampling;
pub mod side_inputs;
pub mod stdlib;
pub mod timestamped;
pub mod topk;
pub mod try_process;
pub mod tumbling;
pub mod values;

// Only re-export files with top-level functions
pub use csv::*;
pub use jsonl::*;
pub use parquet::*;
pub use side_inputs::*;
pub use stdlib::*;
