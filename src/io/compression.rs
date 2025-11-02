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
//! use rustflow::io::compression::{auto_detect_reader, auto_detect_writer};
//! use std::fs::File;
//! # fn main() -> anyhow::Result<()> {
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
//! use rustflow::io::compression::CompressionCodec;
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
//!     fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> Result<Box<dyn Read>> {
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
//! modifying Rustflow's core. Codecs can be registered globally via [`register_codec`].
//!
//! ### Streaming Compatibility
//! Compressed streams don't support random access, which affects shard-based streaming:
//! - For reads: Auto-detection works transparently, but sharding requires full decompression
//! - For writes: Each shard can be compressed independently in parallel
//!
//! ### Zero-Cost Abstraction
//! When no compression features are enabled, the auto-detection functions become
//! simple pass-throughs with minimal overhead.

use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Global codec registry for pluggable compression support.
static CODEC_REGISTRY: RwLock<Option<Vec<Arc<dyn CompressionCodec>>>> = RwLock::new(None);

/// Initialize the codec registry with built-in codecs.
fn init_registry() -> Vec<Arc<dyn CompressionCodec>> {
    vec![
        #[cfg(feature = "compression-gzip")]
        Arc::new(GzipCodec),
        #[cfg(feature = "compression-zstd")]
        Arc::new(ZstdCodec),
        #[cfg(feature = "compression-bzip2")]
        Arc::new(Bzip2Codec),
        #[cfg(feature = "compression-xz")]
        Arc::new(XzCodec),
    ]
}

/// Get or initialize the global codec registry.
fn get_registry() -> Vec<Arc<dyn CompressionCodec>> {
    let mut lock = CODEC_REGISTRY.write().unwrap();
    if lock.is_none() {
        *lock = Some(init_registry());
    }
    lock.as_ref().unwrap().clone()
}

/// Register a custom compression codec globally.
///
/// This allows users to add their own compression algorithms that will be
/// automatically detected alongside built-in codecs.
///
/// # Examples
/// ```
/// use rustflow::io::compression::{register_codec, CompressionCodec};
/// use std::io::{Read, Write};
/// # use std::sync::Arc;
///
/// struct MyCodec;
/// impl CompressionCodec for MyCodec {
///     fn name(&self) -> &str { "mycodec" }
///     fn extensions(&self) -> &[&str] { &[".myext"] }
///     fn magic_bytes(&self) -> Option<&[u8]> { None }
///     fn wrap_reader_dyn(&self, r: Box<dyn Read>) -> std::io::Result<Box<dyn Read>> {
///         Ok(r)
///     }
///     fn wrap_writer_dyn(&self, w: Box<dyn Write>) -> std::io::Result<Box<dyn Write>> {
///         Ok(w)
///     }
/// }
///
/// register_codec(Arc::new(MyCodec));
/// ```
pub fn register_codec(codec: Arc<dyn CompressionCodec>) {
    let mut lock = CODEC_REGISTRY.write().unwrap();
    if lock.is_none() {
        *lock = Some(init_registry());
    }
    lock.as_mut().unwrap().push(codec);
}

/// Pluggable compression codec trait.
///
/// Implement this trait to add custom compression algorithms to Rustflow's I/O system.
/// Codecs are detected via file extensions (fast path) or magic bytes (fallback).
///
/// # Thread Safety
/// Implementations must be `Send + Sync` as they're stored in a global registry
/// and may be accessed from multiple threads.
pub trait CompressionCodec: Send + Sync {
    /// Human-readable codec name (e.g., "gzip", "zstd").
    fn name(&self) -> &str;

    /// File extensions associated with this codec (e.g., `&[".gz", ".gzip"]`).
    ///
    /// Extensions should include the leading dot and be lowercase.
    fn extensions(&self) -> &[&str];

    /// Optional magic byte signature for content-based detection.
    ///
    /// Return `None` if the format has no reliable magic bytes.
    fn magic_bytes(&self) -> Option<&[u8]>;

    /// Wrap a reader with decompression.
    ///
    /// Takes ownership of the input reader and returns a boxed trait object
    /// that transparently decompresses the stream.
    fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> std::io::Result<Box<dyn Read>>;

    /// Wrap a writer with compression.
    ///
    /// Takes ownership of the input writer and returns a boxed trait object
    /// that transparently compresses the stream.
    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> std::io::Result<Box<dyn Write>>;
}

/// Detect compression codec from file path extension.
///
/// Returns the first registered codec whose extensions match the file path.
/// Matching is case-insensitive and handles multiple extensions (e.g., `.tar.gz`).
fn detect_from_extension(path: impl AsRef<Path>) -> Option<Arc<dyn CompressionCodec>> {
    let path = path.as_ref();
    let path_str = path.to_string_lossy().to_lowercase();

    for codec in get_registry() {
        for ext in codec.extensions() {
            if path_str.ends_with(ext) {
                return Some(codec.clone());
            }
        }
    }
    None
}

/// Detect compression codec from magic bytes at the start of a stream.
///
/// Peeks at the beginning of the buffered reader to match against registered
/// codec signatures. The reader is not advanced.
fn detect_from_magic<R: BufRead>(reader: &mut R) -> Option<Arc<dyn CompressionCodec>> {
    // Peek at up to 16 bytes for magic byte matching
    let buf = reader.fill_buf().ok()?;
    if buf.is_empty() {
        return None;
    }

    for codec in get_registry() {
        if let Some(magic) = codec.magic_bytes() {
            if buf.len() >= magic.len() && buf.starts_with(magic) {
                return Some(codec.clone());
            }
        }
    }
    None
}

/// Automatically detect and wrap a reader with decompression if needed.
///
/// Detection strategy:
/// 1. Check file path extension (fast path)
/// 2. Fall back to magic byte detection if extension not recognized
/// 3. Return unwrapped reader if no compression detected
///
/// # Examples
/// ```no_run
/// use rustflow::io::compression::auto_detect_reader;
/// use std::fs::File;
/// # fn main() -> anyhow::Result<()> {
///
/// let file = File::open("data.json.gz")?;
/// let reader = auto_detect_reader(file, "data.json.gz")?;
/// // reader now transparently decompresses gzip content
/// # Ok(())
/// # }
/// ```
pub fn auto_detect_reader<R: Read + 'static>(
    reader: R,
    path_hint: impl AsRef<Path>,
) -> Result<Box<dyn Read>> {
    // Try extension-based detection first
    if let Some(codec) = detect_from_extension(&path_hint) {
        return codec
            .wrap_reader_dyn(Box::new(reader))
            .with_context(|| format!("wrap reader with {} codec", codec.name()));
    }

    // Fall back to magic byte detection
    let mut buf_reader = BufReader::new(reader);
    if let Some(codec) = detect_from_magic(&mut buf_reader) {
        return codec
            .wrap_reader_dyn(Box::new(buf_reader))
            .with_context(|| format!("wrap reader with {} codec", codec.name()));
    }

    // No compression detected, return as-is
    Ok(Box::new(buf_reader))
}

/// Automatically detect and wrap a writer with compression if needed.
///
/// Detection is based solely on file path extension. If a matching codec is found,
/// the writer is wrapped with compression; otherwise it's returned unwrapped.
///
/// # Examples
/// ```no_run
/// use rustflow::io::compression::auto_detect_writer;
/// use std::fs::File;
/// # fn main() -> anyhow::Result<()> {
///
/// let file = File::create("output.csv.zst")?;
/// let writer = auto_detect_writer(file, "output.csv.zst")?;
/// // writer now transparently compresses with zstd
/// # Ok(())
/// # }
/// ```
pub fn auto_detect_writer<W: Write + 'static>(
    writer: W,
    path_hint: impl AsRef<Path>,
) -> Result<Box<dyn Write>> {
    if let Some(codec) = detect_from_extension(&path_hint) {
        return codec
            .wrap_writer_dyn(Box::new(writer))
            .with_context(|| format!("wrap writer with {} codec", codec.name()));
    }

    // No compression detected, return buffered writer
    Ok(Box::new(BufWriter::new(writer)))
}

// ============================================================================
// Built-in Codec Implementations
// ============================================================================

#[cfg(feature = "compression-gzip")]
struct GzipCodec;

#[cfg(feature = "compression-gzip")]
impl CompressionCodec for GzipCodec {
    fn name(&self) -> &str {
        "gzip"
    }

    fn extensions(&self) -> &[&str] {
        &[".gz", ".gzip"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        Some(&[0x1f, 0x8b])
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> std::io::Result<Box<dyn Read>> {
        use flate2::read::GzDecoder;
        Ok(Box::new(GzDecoder::new(reader)))
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> std::io::Result<Box<dyn Write>> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        Ok(Box::new(GzEncoder::new(writer, Compression::default())))
    }
}

#[cfg(feature = "compression-zstd")]
struct ZstdCodec;

#[cfg(feature = "compression-zstd")]
impl CompressionCodec for ZstdCodec {
    fn name(&self) -> &str {
        "zstd"
    }

    fn extensions(&self) -> &[&str] {
        &[".zst", ".zstd"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        Some(&[0x28, 0xb5, 0x2f, 0xfd])
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> std::io::Result<Box<dyn Read>> {
        zstd::stream::read::Decoder::new(reader)
            .map(|d| Box::new(d) as Box<dyn Read>)
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> std::io::Result<Box<dyn Write>> {
        zstd::stream::write::Encoder::new(writer, 3)
            .map(|e| Box::new(e.auto_finish()) as Box<dyn Write>)
    }
}

#[cfg(feature = "compression-bzip2")]
struct Bzip2Codec;

#[cfg(feature = "compression-bzip2")]
impl CompressionCodec for Bzip2Codec {
    fn name(&self) -> &str {
        "bzip2"
    }

    fn extensions(&self) -> &[&str] {
        &[".bz2", ".bzip2"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        Some(&[0x42, 0x5a])
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> std::io::Result<Box<dyn Read>> {
        use bzip2::read::BzDecoder;
        Ok(Box::new(BzDecoder::new(reader)))
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> std::io::Result<Box<dyn Write>> {
        use bzip2::write::BzEncoder;
        use bzip2::Compression;
        Ok(Box::new(BzEncoder::new(writer, Compression::default())))
    }
}

#[cfg(feature = "compression-xz")]
struct XzCodec;

#[cfg(feature = "compression-xz")]
impl CompressionCodec for XzCodec {
    fn name(&self) -> &str {
        "xz"
    }

    fn extensions(&self) -> &[&str] {
        &[".xz"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        Some(&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00])
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> std::io::Result<Box<dyn Read>> {
        use xz2::read::XzDecoder;
        Ok(Box::new(XzDecoder::new(reader)))
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> std::io::Result<Box<dyn Write>> {
        use xz2::write::XzEncoder;
        Ok(Box::new(XzEncoder::new(writer, 6)))
    }
}
