//! Low-level I/O primitives for data ingestion and export.
//!
//! This module provides the foundational I/O infrastructure for Rustflow pipelines,
//! including:
//! - **Type-safe vector I/O** - Read and write entire files into/from memory
//! - **Streaming I/O** - Shard-based lazy ingestion for large files
//! - **Parallel writers** - Deterministic parallel file writing
//! - **Runner integration** - `VecOps` adapters for execution engine support
//!
//! ## Supported Formats
//!
//! Each format is behind an optional feature flag and provides consistent APIs:
//!
//! ### JSON Lines (feature: `io-jsonl`)
//! - **Module**: [`jsonl`]
//! - **Format**: Newline-delimited JSON (one JSON object per line)
//! - **Vector I/O**: [`read_jsonl_vec`](jsonl::read_jsonl_vec), [`write_jsonl_vec`](jsonl::write_jsonl_vec)
//! - **Streaming**: [`JsonlShards`](jsonl::JsonlShards), [`build_jsonl_shards`](jsonl::build_jsonl_shards)
//! - **Parallel**: [`write_jsonl_par`](jsonl::write_jsonl_par) (requires `parallel-io`)
//!
//! ### CSV (feature: `io-csv`)
//! - **Module**: [`csv`]
//! - **Format**: Comma-separated values with optional headers
//! - **Vector I/O**: [`read_csv_vec`](csv::read_csv_vec), [`write_csv_vec`](csv::write_csv_vec)
//! - **Streaming**: [`CsvShards`](csv::CsvShards), [`build_csv_shards`](csv::build_csv_shards)
//! - **Parallel**: [`write_csv_par`](csv::write_csv_par) (requires `parallel-io`)
//!
//! ### Parquet (feature: `io-parquet`)
//! - **Module**: [`parquet`]
//! - **Format**: Apache Parquet columnar storage
//! - **Vector I/O**: [`read_parquet_vec`](parquet::read_parquet_vec), [`write_parquet_vec`](parquet::write_parquet_vec)
//! - **Streaming**: [`ParquetShards`](parquet::ParquetShards), [`build_parquet_shards`](parquet::build_parquet_shards)
//! - **Note**: Uses Arrow 56 and `serde_arrow` 0.13 for schema inference
//!
//! ## Architecture
//!
//! ### Vector I/O Pattern
//!
//! All formats follow the same pattern for in-memory I/O:
//! ```text
//! // Read entire file into Vec<T>
//! let data: Vec<MyRecord> = read_FORMAT_vec(path)?;
//!
//! // Write Vec<T> to file
//! write_FORMAT_vec(path, &data)?;
//! ```
//!
//! ### Streaming I/O Pattern
//!
//! For large files that don't fit in memory, use streaming ingestion:
//! 1. **Build shards** - Scan file metadata to create shard descriptors
//! 2. **Lazy reading** - Each shard is read-only when needed by the runner
//! 3. **Parallel execution** - Shards can be processed in parallel
//!
//! ```text
//! // Create shard descriptor (lightweight, just metadata)
//! let shards = build_FORMAT_shards(path, rows_per_shard)?;
//!
//! // Read a specific shard on demand
//! let data = read_FORMAT_range(&shards, start, end)?;
//! ```
//!
//! ### VecOps Integration
//!
//! Each format provides a `VecOps` adapter (`JsonlVecOps<T>`, `CsvVecOps<T>`,
//! `ParquetVecOps<T>`) that enables:
//! - **Length introspection** - Get total row count without reading data
//! - **Partitioning** - Split into concrete partitions for parallel execution
//! - **Sequential fallback** - Read entire dataset for sequential paths
//!
//! This integration allows streaming sources to work seamlessly with Rustflow's
//! execution engine.
//!
//! ## Parallel Writing
//!
//! When the `parallel-io` feature is enabled, each format provides a parallel
//! writer that:
//! 1. Splits the input into shards
//! 2. Serializes each shard in parallel (using Rayon)
//! 3. Concatenates results in **deterministic order**
//!
//! This preserves file ordering regardless of thread scheduling, making parallel
//! writes safe for deterministic pipelines.
//!
//! ## Design Decisions
//!
//! ### Serde-Based Type Safety
//! All I/O is type-safe through Serde's `Serialize`/`Deserialize` traits. This
//! means:
//! - No manual parsing or serialization code
//! - Compile-time type checking
//! - Automatic schema inference (for Parquet via Arrow)
//!
//! ### Row-Based Sharding
//! Sharding is based on **row/line counts**, not byte offsets. This provides:
//! - Predictable shard sizes
//! - Simpler implementation
//! - Works well with compressed files (though streaming reads from compressed
//!   files still need to decompress from the start)
//!
//! ### Error Context
//! All I/O operations use `anyhow::Context` to provide detailed error messages
//! including file paths, line/row numbers, and operation context.
//!
//! ## High-Level Helpers
//!
//! The [`helpers`](crate::helpers) module provides more ergonomic APIs that build
//! on these primitives:
//! - [`read_jsonl`](crate::read_jsonl) - Create a `PCollection` from JSONL
//! - [`read_csv`](crate::read_csv) - Create a `PCollection` from CSV
//! - [`read_parquet_streaming`](crate::read_parquet_streaming) - Streaming Parquet source
//! - [`PCollection::write_jsonl`](crate::PCollection::write_jsonl) - Write collection to JSONL
//! - [`PCollection::write_csv`](crate::PCollection::write_csv) - Write collection to CSV
//! - [`PCollection::write_parquet`](crate::PCollection::write_parquet) - Write collection to Parquet
//!
//! Most users should use the helper APIs rather than calling these low-level
//! functions directly.
//!
//! ## Feature Flags
//!
//! - `io-jsonl` - Enable JSON Lines I/O
//! - `io-csv` - Enable CSV I/O
//! - `io-parquet` - Enable Parquet I/O (adds Arrow dependencies)
//! - `parallel-io` - Enable parallel writers for all formats
//!
//! ## Examples
//!
//! ### Vector I/O (in-memory)
//! ```no_run
//! use rustflow::io::jsonl::{read_jsonl_vec, write_jsonl_vec};
//! use serde::{Deserialize, Serialize};
//! # fn main() -> anyhow::Result<()> {
//! #[derive(Serialize, Deserialize)]
//! struct Record { id: u32, name: String }
//!
//! // Read
//! let data: Vec<Record> = read_jsonl_vec("input.jsonl")?;
//!
//! // Process
//! let filtered: Vec<Record> = data.into_iter()
//!     .filter(|r| r.id > 100)
//!     .collect();
//!
//! // Write
//! write_jsonl_vec("output.jsonl", &filtered)?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Streaming I/O
//! ```no_run
//! use rustflow::io::csv::{build_csv_shards, read_csv_range};
//! use serde::Deserialize;
//! # fn main() -> anyhow::Result<()> {
//! #[derive(Deserialize)]
//! struct Row { k: String, v: u64 }
//!
//! // Build shard metadata (fast, doesn't read data)
//! let shards = build_csv_shards("large.csv", true, 10_000)?;
//!
//! // Process each shard independently
//! for &(start, end) in &shards.ranges {
//!     let data: Vec<Row> = read_csv_range(&shards, start, end)?;
//!     // ... process this chunk ...
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ### Parallel Writing
//! ```no_run
//! use rustflow::io::jsonl::write_jsonl_par;
//! use serde::Serialize;
//! # fn main() -> anyhow::Result<()> {
//! #[derive(Serialize)]
//! struct Record { id: u32 }
//!
//! let data: Vec<Record> = (0..100_000).map(|id| Record { id }).collect();
//!
//! // Write in parallel with 8 shards
//! write_jsonl_par("output.jsonl", &data, Some(8))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## See Also
//!
//! - [`helpers`](crate::helpers) - High-level I/O helpers for pipelines
//! - [`type_token::VecOps`](crate::type_token::VecOps) - Partition operations trait
//! - [`runner`](crate::runner) - Execution engine that consumes these primitives

#[cfg_attr(docsrs, doc(cfg(feature = "io-jsonl")))]
#[cfg(feature = "io-jsonl")]
pub mod jsonl;

#[cfg_attr(docsrs, doc(cfg(feature = "io-csv")))]
#[cfg(feature = "io-csv")]
pub mod csv;

#[cfg_attr(docsrs, doc(cfg(feature = "io-parquet")))]
#[cfg(feature = "io-parquet")]
pub mod parquet;

pub mod compression;
pub mod glob;
