#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-bzip2",
    feature = "compression-xz"
))]
use ironbeam_compression::auto_detect_reader;
use ironbeam_compression::{
    CompressionCodec, auto_detect_writer, compress_bytes_for_path, register_codec,
};
#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-bzip2",
    feature = "compression-xz"
))]
use std::io::Cursor;
use std::io::{Read, Write};
use std::sync::Arc;

struct PassThroughCodec;
impl CompressionCodec for PassThroughCodec {
    fn name(&self) -> &'static str {
        "pass-through"
    }
    fn extensions(&self) -> &[&str] {
        &[".ibpass"]
    }
    fn magic_bytes(&self) -> Option<&[u8]> {
        None
    }
    fn wrap_reader_dyn(
        &self,
        reader: Box<dyn std::io::Read>,
    ) -> std::io::Result<Box<dyn std::io::Read>> {
        Ok(reader)
    }
    fn wrap_writer_dyn(
        &self,
        writer: Box<dyn std::io::Write>,
    ) -> std::io::Result<Box<dyn std::io::Write>> {
        Ok(writer)
    }
}

#[test]
fn custom_codec_registration_selects_its_extension() {
    register_codec(Arc::new(PassThroughCodec));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("custom.ibpass");
    let mut writer = auto_detect_writer(std::fs::File::create(&path).unwrap(), &path).unwrap();
    writer.write_all(b"custom\n").unwrap();
    drop(writer);
    assert_eq!(std::fs::read(path).unwrap(), b"custom\n");
}

#[test]
fn writer_without_codec_passes_bytes_through() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("plain.txt");
    let mut writer = auto_detect_writer(std::fs::File::create(&path).unwrap(), &path).unwrap();
    writer.write_all(b"plain\n").unwrap();
    writer.flush().unwrap();
    drop(writer);
    assert_eq!(std::fs::read(path).unwrap(), b"plain\n");
}

#[test]
fn reader_without_codec_handles_plain_and_empty_inputs() {
    let mut plain = String::new();
    ironbeam_compression::auto_detect_reader(std::io::Cursor::new(b"plain"), "plain.bin")
        .unwrap()
        .read_to_string(&mut plain)
        .unwrap();
    assert_eq!(plain, "plain");
    let mut empty = Vec::new();
    ironbeam_compression::auto_detect_reader(std::io::Cursor::new(Vec::<u8>::new()), "empty.bin")
        .unwrap()
        .read_to_end(&mut empty)
        .unwrap();
    assert_eq!(empty, []);
}

#[test]
fn write_callback_failure_does_not_return_a_partial_buffer() {
    let error = ironbeam_compression::write_to_vec_for_path("output.txt", |writer| {
        writer.write_all(b"partial")?;
        anyhow::bail!("encode failed")
    })
    .unwrap_err();
    assert!(error.to_string().contains("encode failed"));
}

#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-bzip2",
    feature = "compression-xz"
))]
fn writer_round_trip(extension: &str) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("encoded.{extension}"));
    let mut writer = auto_detect_writer(std::fs::File::create(&path).unwrap(), &path).unwrap();
    writer.write_all(b"streamed value\n").unwrap();
    drop(writer);
    let mut decoded = Vec::new();
    auto_detect_reader(std::fs::File::open(&path).unwrap(), &path)
        .unwrap()
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(decoded, b"streamed value\n");
}

#[test]
fn unknown_extension_preserves_bytes() {
    assert_eq!(
        compress_bytes_for_path(b"line\n", "data.txt").unwrap(),
        b"line\n"
    );
}

#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-bzip2",
    feature = "compression-xz"
))]
fn round_trip(path: &str) {
    let original = b"some repeatable line\nsome repeatable line\n";
    let compressed = compress_bytes_for_path(original, path).unwrap();
    assert_ne!(compressed, original);
    let mut decoded = Vec::new();
    auto_detect_reader(Cursor::new(compressed.clone()), path)
        .unwrap()
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(decoded, original);
    let mut decoded_magic = Vec::new();
    auto_detect_reader(Cursor::new(compressed), "unknown.bin")
        .unwrap()
        .read_to_end(&mut decoded_magic)
        .unwrap();
    assert_eq!(decoded_magic, original);
}

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_bytes_are_finalized() {
    round_trip("data.GZIP");
    writer_round_trip("gz");
}
#[cfg(not(feature = "compression-gzip"))]
#[test]
fn disabled_gzip_is_an_explicit_error() {
    assert!(compress_bytes_for_path(b"x", "data.gz").is_err());
}

#[cfg(feature = "compression-zstd")]
#[test]
fn zstd_bytes_are_finalized() {
    round_trip("data.zst");
    writer_round_trip("zst");
}
#[cfg(not(feature = "compression-zstd"))]
#[test]
fn disabled_zstd_is_an_explicit_error() {
    assert!(compress_bytes_for_path(b"x", "data.zst").is_err());
}

#[cfg(feature = "compression-bzip2")]
#[test]
fn bzip2_bytes_are_finalized() {
    round_trip("data.bz2");
    writer_round_trip("bz2");
}
#[cfg(not(feature = "compression-bzip2"))]
#[test]
fn disabled_bzip2_is_an_explicit_error() {
    assert!(compress_bytes_for_path(b"x", "data.bz2").is_err());
}

#[cfg(feature = "compression-xz")]
#[test]
fn xz_bytes_are_finalized() {
    round_trip("data.xz");
    writer_round_trip("xz");
}
#[cfg(not(feature = "compression-xz"))]
#[test]
fn disabled_xz_is_an_explicit_error() {
    assert!(compress_bytes_for_path(b"x", "data.xz").is_err());
}
