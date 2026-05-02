//! High-level convenience helpers and extension methods for [`crate::collection::PCollection`].
//!
//! This module extends the core [`crate::collection::PCollection`] API with specialized transformations
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
//!   - [`PCollection::combine_globally`](crate::PCollection::combine_globally)
//! - [`basic`] - Arithmetic aggregate convenience methods
//!   - [`PCollection::sum_globally`](crate::PCollection::sum_globally)
//!   - [`PCollection::sum_per_key`](crate::PCollection::sum_per_key)
//!   - [`PCollection::min_globally`](crate::PCollection::min_globally)
//!   - [`PCollection::min_per_key`](crate::PCollection::min_per_key)
//!   - [`PCollection::max_globally`](crate::PCollection::max_globally)
//!   - [`PCollection::max_per_key`](crate::PCollection::max_per_key)
//!   - [`PCollection::average_globally`](crate::PCollection::average_globally)
//!   - [`PCollection::average_per_key`](crate::PCollection::average_per_key)
//! - [`statistical`] - Statistical aggregate convenience methods
//!   - [`PCollection::approx_median_globally`](crate::PCollection::approx_median_globally)
//!   - [`PCollection::approx_median_per_key`](crate::PCollection::approx_median_per_key)
//!   - [`PCollection::approx_quantiles_globally`](crate::PCollection::approx_quantiles_globally)
//!   - [`PCollection::approx_quantiles_per_key`](crate::PCollection::approx_quantiles_per_key)
//!
//! ### Joins
//! - [`joins`] - Join operations for keyed collections
//!   - [`PCollection::join_inner`](crate::PCollection::join_inner)
//!   - [`PCollection::join_left`](crate::PCollection::join_left)
//!   - [`PCollection::join_right`](crate::PCollection::join_right)
//!   - [`PCollection::join_full`](crate::PCollection::join_full)
//! - [`co_gbk`] - Multi-way `CoGroupByKey` for N-way joins (2-10 collections)
//!   - [`cogroup_by_key!`](crate::cogroup_by_key) - Macro for N-way full outer joins
//!
//! ### Side Inputs
//! - [`side_inputs`] - Enrich streams with auxiliary data
//!   - [`side_vec`] - Create a side input from a vector
//!   - [`side_hashmap`] - Create a side input from a hash map
//!   - [`side_singleton`] - Broadcast a single scalar value to all workers
//!   - [`side_multimap`] - Create a one-to-many side input (`HashMap<K, Vec<V>>`)
//!   - [`PCollection::map_with_side`](crate::PCollection::map_with_side)
//!   - [`PCollection::filter_with_side`](crate::PCollection::filter_with_side)
//!   - [`PCollection::map_with_side_map`](crate::PCollection::map_with_side_map)
//!   - [`PCollection::filter_with_side_map`](crate::PCollection::filter_with_side_map)
//!   - [`PCollection::map_with_singleton`](crate::PCollection::map_with_singleton)
//!   - [`PCollection::filter_with_singleton`](crate::PCollection::filter_with_singleton)
//!   - [`PCollection::map_with_side_multimap`](crate::PCollection::map_with_side_multimap)
//!   - [`PCollection::filter_with_side_multimap`](crate::PCollection::filter_with_side_multimap)
//!
//! ### Distinct Operations
//! - [`distinct`] - Remove duplicate elements and count distinct values
//!   - [`PCollection::distinct`](crate::PCollection::distinct)
//!   - [`PCollection::distinct_by`](crate::PCollection::distinct_by)
//!   - [`PCollection::distinct_per_key`](crate::PCollection::distinct_per_key)
//!   - [`PCollection::distinct_count_globally`](crate::PCollection::distinct_count_globally)
//!   - [`PCollection::distinct_count_per_key`](crate::PCollection::distinct_count_per_key)
//!
//! ### Filter Operations
//! - [`filter`] - Enhanced filtering with convenience methods
//!   - [`PCollection::filter_eq`](crate::PCollection::filter_eq)
//!   - [`PCollection::filter_ne`](crate::PCollection::filter_ne)
//!   - [`PCollection::filter_lt`](crate::PCollection::filter_lt)
//!   - [`PCollection::filter_le`](crate::PCollection::filter_le)
//!   - [`PCollection::filter_gt`](crate::PCollection::filter_gt)
//!   - [`PCollection::filter_ge`](crate::PCollection::filter_ge)
//!   - [`PCollection::filter_range`](crate::PCollection::filter_range)
//!   - [`PCollection::filter_range_inclusive`](crate::PCollection::filter_range_inclusive)
//!   - [`PCollection::filter_by`](crate::PCollection::filter_by)
//!
//! ### Collection Operations
//! - [`collect_values`] - Collect elements into `Vec` or `HashSet`
//!   - [`PCollection::to_list_globally`](crate::PCollection::to_list_globally)
//!   - [`PCollection::to_set_globally`](crate::PCollection::to_set_globally)
//!   - [`PCollection::to_list_per_key`](crate::PCollection::to_list_per_key)
//!   - [`PCollection::to_set_per_key`](crate::PCollection::to_set_per_key)
//!
//! ### Sampling
//! - [`sampling`] - Random sampling operations
//!   - [`PCollection::sample_reservoir_vec`](crate::PCollection::sample_reservoir_vec)
//!   - [`PCollection::sample_reservoir`](crate::PCollection::sample_reservoir)
//!   - [`PCollection::sample_values_reservoir_vec`](crate::PCollection::sample_values_reservoir_vec)
//!   - [`PCollection::sample_values_reservoir`](crate::PCollection::sample_values_reservoir)
//!
//! ### Top-K / Bottom-K Operations
//! - [`topk`] - Convenience API for selecting top or bottom K values globally or per key
//!   - [`PCollection::top_k_globally`](crate::PCollection::top_k_globally)
//!   - [`PCollection::top_k_per_key`](crate::PCollection::top_k_per_key)
//!   - [`PCollection::bottom_k_globally`](crate::PCollection::bottom_k_globally)
//!   - [`PCollection::bottom_k_per_key`](crate::PCollection::bottom_k_per_key)
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
//!   - [`read_jsonl`]
//!   - [`read_jsonl_streaming`]
//!   - [`PCollection::write_jsonl`](crate::PCollection::write_jsonl)
//! - [`csv`] - CSV I/O utilities (feature: `io-csv`)
//!   - [`read_csv`]
//!   - [`read_csv_streaming`]
//!   - [`PCollection::write_csv`](crate::PCollection::write_csv)
//! - [`parquet`] - Parquet I/O utilities (feature: `io-parquet`)
//!   - [`read_parquet_streaming`]
//!   - [`PCollection::write_parquet`](crate::PCollection::write_parquet)
//! - [`avro`] - Avro I/O utilities (feature: `io-avro`)
//!   - [`read_avro`]
//!   - [`read_avro_streaming`]
//!   - [`PCollection::write_avro_with_schema`](crate::PCollection::write_avro_with_schema)
//!   - [`PCollection::write_avro_par`](crate::PCollection::write_avro_par)
//!
//! ### Cloud Operations
//! - [`cloud`] - Helpers for running custom cloud operations
//!   - [`run_with_retry`] - Execute with automatic retry logic
//!   - [`run_parallel`] - Execute multiple operations concurrently
//!   - [`run_with_timeout_and_retry`] - Combine timeout and retry
//!   - [`run_batch_operation`] - Process in configurable chunks
//!   - [`run_paginated_operation`] - Handle paginated API responses
//!   - [`OperationBuilder`] - Fluent API for operation configuration
//!   - [`run_with_context`] - Track execution metadata
//!
//! ### Regex Transforms
//! - [`regex`] - Regex-based transforms for `PCollection<String>`
//!   - [`PCollection::regex_matches`](crate::PCollection::regex_matches)
//!   - [`PCollection::regex_extract`](crate::PCollection::regex_extract)
//!   - [`PCollection::regex_extract_kv`](crate::PCollection::regex_extract_kv)
//!   - [`PCollection::regex_find`](crate::PCollection::regex_find)
//!   - [`PCollection::regex_replace_all`](crate::PCollection::regex_replace_all)
//!   - [`PCollection::regex_split`](crate::PCollection::regex_split)
//!
//! ### Windowing
//! - [`tumbling`] - Tumbling window operations
//!   - [`PCollection::key_by_window`](crate::PCollection::key_by_window)
//!   - [`PCollection::group_by_window`](crate::PCollection::group_by_window)
//!   - [`PCollection::group_by_key_and_window`](crate::PCollection::group_by_key_and_window)
//! - [`timestamped`] - Timestamp utilities for windowed data
//! - [`windowed_combine`] - One-call windowed aggregation helpers
//!   - [`PCollection::combine_per_window`](crate::PCollection::combine_per_window)
//!   - [`PCollection::sum_per_window`](crate::PCollection::sum_per_window)
//!   - [`PCollection::count_per_window`](crate::PCollection::count_per_window)
//!   - [`PCollection::min_per_window`](crate::PCollection::min_per_window)
//!   - [`PCollection::max_per_window`](crate::PCollection::max_per_window)
//!   - [`PCollection::average_per_window`](crate::PCollection::average_per_window)
//!   - [`PCollection::combine_per_key_and_window`](crate::PCollection::combine_per_key_and_window)
//!   - [`PCollection::sum_per_key_and_window`](crate::PCollection::sum_per_key_and_window)
//!   - [`PCollection::count_per_key_and_window`](crate::PCollection::count_per_key_and_window)
//!   - [`PCollection::min_per_key_and_window`](crate::PCollection::min_per_key_and_window)
//!   - [`PCollection::max_per_key_and_window`](crate::PCollection::max_per_key_and_window)
//!   - [`PCollection::average_per_key_and_window`](crate::PCollection::average_per_key_and_window)
//!
//! ### Standard Library Integration
//! - [`stdlib`] - Convenience constructors for common sources
//!   - [`from_vec`] - Create a collection from a vector
//!   - [`from_iter`] - Create a collection from an iterator
//!
//! ### Internal Utilities
//! - [`common`] - Shared internal utilities used by other helpers
//!
//! ## Design Philosophy
//!
//! Helpers in this module follow these principles:
//!
//! 1. **Build on core primitives** - All helpers are implemented using the fundamental
//!    operations from [`crate::PCollection`].
//!
//! 2. **Ergonomic APIs** - Provide convenient, type-safe interfaces that reduce boilerplate.
//!
//! 3. **Composable** - Helpers can be chained together naturally.
//!
//! 4. **Feature-gated** - I/O helpers are behind optional feature flags to minimize
//!    dependencies.
//!
//! 5. **Extension traits** - Many helpers are implemented as extension methods on
//!    [`crate::PCollection`] for discoverability.
//!
//! ## Example Usage
//!
//! ```no_run
//! use ironbeam::*;
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

pub mod avro;
pub mod basic;
pub mod batches;
pub mod cloud;
pub mod co_gbk;
pub mod collect_sorted;
pub mod collect_values;
pub mod combine;
pub mod combine_global;
pub mod common;
pub mod count;
pub mod csv;
pub mod distinct;
pub mod filter;
pub mod flatten;
pub mod joins;
pub mod jsonl;
pub mod keyed;
pub mod latest;
pub mod parquet;
pub mod partition;
pub mod regex;
pub mod sampling;
pub mod side_inputs;
pub mod statistical;
pub mod stdlib;
pub mod timestamped;
pub mod topk;
pub mod try_process;
pub mod tumbling;
pub mod validation;
pub mod values;
pub mod windowed_combine;
pub mod xml;

// Only re-export files with top-level functions
pub use avro::*;
pub use cloud::*;
pub use csv::*;
pub use flatten::*;
pub use jsonl::*;
pub use parquet::*;
pub use side_inputs::*;
pub use stdlib::*;
pub use xml::*;
