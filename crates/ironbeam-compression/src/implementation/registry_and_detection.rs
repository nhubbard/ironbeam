use anyhow::{Context, Result};
use std::io::{BufRead, BufReader, BufWriter, Read, Result as IoResult, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

#[cfg(feature = "compression-zstd")]
use zstd::stream::read::Decoder;
#[cfg(feature = "compression-zstd")]
use zstd::stream::write::Encoder;

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
/// use ironbeam_compression::{register_codec, CompressionCodec};
/// use std::io::{Read, Write};
/// # use std::sync::Arc;
///
/// struct MyCodec;
/// impl CompressionCodec for MyCodec {
///     fn name(&self) -> &str { "mycodec" }
///     fn extensions(&self) -> &[&str] { &[".myext"] }
///     fn magic_bytes(&self) -> Option<&[u8]> { None }
///     fn wrap_reader_dyn(&self, r: Box<dyn Read + Send>) -> std::io::Result<Box<dyn Read + Send>> {
///         Ok(r)
///     }
///     fn wrap_writer_dyn(&self, w: Box<dyn Write>) -> std::io::Result<Box<dyn Write>> {
///         Ok(w)
///     }
/// }
///
/// register_codec(Arc::new(MyCodec));
/// ```
///
/// # Panics
///
/// If the codec registry is unable to be written to, then this function will panic.
/// This situation is highly unlikely to happen.
pub fn register_codec(codec: Arc<dyn CompressionCodec>) {
    let mut lock = CODEC_REGISTRY.write().unwrap();
    if lock.is_none() {
        *lock = Some(init_registry());
    }
    lock.as_mut().unwrap().push(codec);
}

/// Pluggable compression codec trait.
///
/// Implement this trait to add custom compression algorithms to Ironbeam's I/O system.
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
    ///
    /// # Errors
    ///
    /// You must handle errors appropriately and return an [`std::io::Result`] accordingly.
    fn wrap_reader_dyn(&self, reader: Box<dyn Read + Send>) -> IoResult<Box<dyn Read + Send>>;

    /// Wrap a writer with compression.
    ///
    /// Takes ownership of the input writer and returns a boxed trait object
    /// that transparently compresses the stream.
    ///
    /// # Errors
    ///
    /// You must handle errors appropriately and return an [`std::io::Result`] accordingly.
    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> IoResult<Box<dyn Write>>;
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
        if let Some(magic) = codec.magic_bytes()
            && buf.len() >= magic.len()
            && buf.starts_with(magic)
        {
            return Some(codec.clone());
        }
    }
    None
}

/// Automatically detect and wrap a reader with decompression if needed.
///
/// Detection strategy:
/// 1. Check file path extension (fast path)
/// 2. Fall back to magic byte detection if the extension is not recognized
/// 3. Return an unwrapped reader if no compression is detected
///
/// # Examples
/// ```no_run
/// use ironbeam_compression::auto_detect_reader;
/// use std::fs::File;
/// use anyhow::Result;
/// # fn main() -> Result<()> {
///
/// let file = File::open("data.json.gz")?;
/// let reader = auto_detect_reader(file, "data.json.gz")?;
/// // reader now transparently decompresses gzip content
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// If an error occurs during detection, then a [`Result`] is returned.
pub fn auto_detect_reader<R: Read + Send + 'static>(
    reader: R,
    path_hint: impl AsRef<Path>,
) -> Result<Box<dyn Read + Send>> {
    if let Some(codec) = detect_from_extension(&path_hint) {
        return codec
            .wrap_reader_dyn(Box::new(reader))
            .with_context(|| format!("wrap reader with {} codec", codec.name()));
    }

    let mut buf_reader = BufReader::new(reader);
    if let Some(codec) = detect_from_magic(&mut buf_reader) {
        return codec
            .wrap_reader_dyn(Box::new(buf_reader))
            .with_context(|| format!("wrap reader with {} codec", codec.name()));
    }

    Ok(Box::new(buf_reader))
}

/// Automatically detect and wrap a writer with compression if needed.
///
/// Detection is based solely on file path extension. If a matching codec is found,
/// the writer is wrapped with compression; otherwise it's returned unwrapped.
///
/// # Examples
/// ```no_run
/// use ironbeam_compression::auto_detect_writer;
/// use std::fs::File;
/// use anyhow::Result;
/// # fn main() -> Result<()> {
///
/// let file = File::create("output.csv.zst")?;
/// let writer = auto_detect_writer(file, "output.csv.zst")?;
/// // writer now transparently compresses with zstd
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// If an error occurs during auto-detection, a [`Result`] is returned.
pub fn auto_detect_writer<W: Write + 'static>(
    writer: W,
    path_hint: impl AsRef<Path>,
) -> Result<Box<dyn Write>> {
    if let Some(codec) = detect_from_extension(&path_hint) {
        return codec
            .wrap_writer_dyn(Box::new(writer))
            .with_context(|| format!("wrap writer with {} codec", codec.name()));
    }

    Ok(Box::new(BufWriter::new(writer)))
}
