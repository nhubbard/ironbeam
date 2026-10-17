//! Pluggable compression support for transparent I/O operations.
//!
//! This module provides a trait-based system for automatic compression/decompression
//! detection and handling, inspired by Apache Beam's compression support.
//!
//! ## Architecture
//!
//! The system is built around two core traits:
//! - [`CompressionCodec`] - Pluggable compression algorithm implementations
//! - Auto-detection via file extensions and magic bytes
//!
//! ## Built-in Codecs
//!
//! When enabled via feature flags, the following codecs are available:
//! - **Gzip** (`.gz`) - via `flate2` crate (feature: `compression-gzip`)
//! - **Zstd** (`.zst`) - via `zstd` crate (feature: `compression-zstd`)
//! - **Bzip2** (`.bz2`) - via `bzip2` crate (feature: `compression-bzip2`)
//! - **Xz** (`.xz`) - via `xz2` crate (feature: `compression-xz`)
//!
//! ## Usage Patterns
//!
//! ### Transparent Auto-Detection
//! ```no_run
//! use ironbeam_compression::{auto_detect_reader, auto_detect_writer};
//! use std::fs::File;
//! use anyhow::Result;
//! # fn main() -> Result<()> {
//!
//! // Automatically detects .gz and wraps with decompressor
//! let file = File::open("data.json.gz")?;
//! let reader = auto_detect_reader(file, "data.json.gz")?;
//!
//! // Automatically detects .zst and wraps with compressor
//! let file = File::create("output.csv.zst")?;
//! let writer = auto_detect_writer(file, "output.csv.zst")?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Custom Codec Implementation
//! ```
//! use ironbeam_compression::CompressionCodec;
//! use std::io::{Read, Write, Result};
//!
//! struct MyCodec;
//!
//! impl CompressionCodec for MyCodec {
//!     fn name(&self) -> &str { "mycodec" }
//!
//!     fn extensions(&self) -> &[&str] { &[".myext"] }
//!
//!     fn magic_bytes(&self) -> Option<&[u8]> { Some(&[0xAB, 0xCD]) }
//!
//!     fn wrap_reader_dyn(&self, reader: Box<dyn Read + Send>) -> Result<Box<dyn Read + Send>> {
//!         // Your decompression logic
//!         Ok(reader)
//!     }
//!
//!     fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> Result<Box<dyn Write>> {
//!         // Your compression logic
//!         Ok(writer)
//!     }
//! }
//! ```
//!
//! ## Design Decisions
//!
//! ### Extension-First Detection
//! File extensions are checked first for performance, falling back to magic bytes
//! only when necessary. This avoids reading file headers in the common case.
//!
//! ### Pluggable Architecture
//! The [`CompressionCodec`] trait allows users to implement custom codecs without
//! modifying Ironbeam's core. Codecs can be registered globally via [`register_codec`].
//!
//! ### Streaming Compatibility
//! Compressed streams don't support random access, which affects shard-based streaming:
//! - For reads: Auto-detection works transparently, but sharding requires full decompression
//! - For writes: Each shard can be compressed independently in parallel
//!
//! ### Zero-Cost Abstraction
//! When no compression features are enabled, the auto-detection functions become
//! simple pass-through operations with minimal overhead.

mod implementation;
pub use implementation::*;
