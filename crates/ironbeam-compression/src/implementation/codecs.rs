/// Write a bounded value directly into a finalized compression buffer.
///
/// The closure writes to the selected codec or to an uncompressed buffer for
/// unknown extensions. The codec is finalized before the bytes are returned.
///
/// # Errors
/// Returns an error when the requested codec is disabled, writing fails, or
/// compression cannot be finalized.
pub fn write_to_vec_for_path(
    path_hint: impl AsRef<Path>,
    write: impl FnOnce(&mut dyn Write) -> Result<()>,
) -> Result<Vec<u8>> {
    let extension = path_hint
        .as_ref()
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    if extension.eq_ignore_ascii_case("gz") || extension.eq_ignore_ascii_case("gzip") {
        #[cfg(feature = "compression-gzip")]
        {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            write(&mut encoder)?;
            return Ok(encoder.finish()?);
        }
        #[cfg(not(feature = "compression-gzip"))]
        anyhow::bail!("gzip compression not enabled (enable 'compression-gzip' feature)");
    }
    if extension.eq_ignore_ascii_case("zst") || extension.eq_ignore_ascii_case("zstd") {
        #[cfg(feature = "compression-zstd")]
        {
            let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3)?;
            write(&mut encoder)?;
            return Ok(encoder.finish()?);
        }
        #[cfg(not(feature = "compression-zstd"))]
        anyhow::bail!("zstd compression not enabled (enable 'compression-zstd' feature)");
    }
    if extension.eq_ignore_ascii_case("bz2") || extension.eq_ignore_ascii_case("bzip2") {
        #[cfg(feature = "compression-bzip2")]
        {
            let mut encoder =
                bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
            write(&mut encoder)?;
            return Ok(encoder.finish()?);
        }
        #[cfg(not(feature = "compression-bzip2"))]
        anyhow::bail!("bzip2 compression not enabled (enable 'compression-bzip2' feature)");
    }
    if extension.eq_ignore_ascii_case("xz") {
        #[cfg(feature = "compression-xz")]
        {
            let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 6);
            write(&mut encoder)?;
            return Ok(encoder.finish()?);
        }
        #[cfg(not(feature = "compression-xz"))]
        anyhow::bail!("xz compression not enabled (enable 'compression-xz' feature)");
    }
    let mut bytes = Vec::new();
    write(&mut bytes)?;
    Ok(bytes)
}

/// Compress a bounded byte buffer according to its destination extension.
///
/// Unknown extensions return the bytes unchanged. Recognized extensions
/// require their corresponding feature and finalize the compression stream.
///
/// # Errors
/// Returns an error when the requested codec is disabled or compression fails.
pub fn compress_bytes_for_path(bytes: &[u8], path_hint: impl AsRef<Path>) -> Result<Vec<u8>> {
    write_to_vec_for_path(path_hint, |writer| {
        writer.write_all(bytes)?;
        Ok(())
    })
}

// ============================================================================
// Built-in Codec Implementations
// ============================================================================

#[cfg(feature = "compression-gzip")]
struct GzipCodec;

#[cfg(feature = "compression-gzip")]
impl CompressionCodec for GzipCodec {
    fn name(&self) -> &'static str {
        "gzip"
    }

    fn extensions(&self) -> &[&str] {
        &[".gz", ".gzip"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        Some(&[0x1f, 0x8b])
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read + Send>) -> IoResult<Box<dyn Read + Send>> {
        use flate2::read::GzDecoder;
        Ok(Box::new(GzDecoder::new(reader)))
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> IoResult<Box<dyn Write>> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        Ok(Box::new(GzEncoder::new(writer, Compression::default())))
    }
}

#[cfg(feature = "compression-zstd")]
struct ZstdCodec;

#[cfg(feature = "compression-zstd")]
impl CompressionCodec for ZstdCodec {
    fn name(&self) -> &'static str {
        "zstd"
    }

    fn extensions(&self) -> &[&str] {
        &[".zst", ".zstd"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        Some(&[0x28, 0xb5, 0x2f, 0xfd])
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read + Send>) -> IoResult<Box<dyn Read + Send>> {
        Decoder::new(reader).map(|d| Box::new(d) as Box<dyn Read + Send>)
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> IoResult<Box<dyn Write>> {
        Encoder::new(writer, 3).map(|e| Box::new(e.auto_finish()) as Box<dyn Write>)
    }
}

#[cfg(feature = "compression-bzip2")]
struct Bzip2Codec;

#[cfg(feature = "compression-bzip2")]
impl CompressionCodec for Bzip2Codec {
    fn name(&self) -> &'static str {
        "bzip2"
    }

    fn extensions(&self) -> &[&str] {
        &[".bz2", ".bzip2"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        Some(&[0x42, 0x5a])
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read + Send>) -> IoResult<Box<dyn Read + Send>> {
        use bzip2::read::BzDecoder;
        Ok(Box::new(BzDecoder::new(reader)))
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> IoResult<Box<dyn Write>> {
        use bzip2::Compression;
        use bzip2::write::BzEncoder;
        Ok(Box::new(BzEncoder::new(writer, Compression::default())))
    }
}

#[cfg(feature = "compression-xz")]
struct XzCodec;

#[cfg(feature = "compression-xz")]
impl CompressionCodec for XzCodec {
    fn name(&self) -> &'static str {
        "xz"
    }

    fn extensions(&self) -> &[&str] {
        &[".xz"]
    }

    fn magic_bytes(&self) -> Option<&[u8]> {
        Some(&[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00])
    }

    fn wrap_reader_dyn(&self, reader: Box<dyn Read + Send>) -> IoResult<Box<dyn Read + Send>> {
        use xz2::read::XzDecoder;
        Ok(Box::new(XzDecoder::new(reader)))
    }

    fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> IoResult<Box<dyn Write>> {
        use xz2::write::XzEncoder;
        Ok(Box::new(XzEncoder::new(writer, 6)))
    }
}
