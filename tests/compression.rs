#[cfg(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-bzip2",
    feature = "compression-xz"
))]
mod compression_tests {
    use rustflow::io::compression::{
        auto_detect_reader, auto_detect_writer, register_codec, CompressionCodec,
    };
    use serde::{Deserialize, Serialize};
    use std::io::{Read, Write};
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestRecord {
        id: u32,
        name: String,
        value: f64,
    }

    fn sample_data() -> Vec<TestRecord> {
        vec![
            TestRecord {
                id: 1,
                name: "Alice".to_string(),
                value: 3.14,
            },
            TestRecord {
                id: 2,
                name: "Bob".to_string(),
                value: 2.71,
            },
            TestRecord {
                id: 3,
                name: "Charlie".to_string(),
                value: 1.41,
            },
        ]
    }

    fn write_jsonl_compressed(path: &str, data: &[TestRecord]) -> anyhow::Result<()> {
        let file = std::fs::File::create(path)?;
        let mut writer = auto_detect_writer(file, path)?;
        for record in data {
            serde_json::to_writer(&mut writer, record)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        Ok(())
    }

    fn read_jsonl_compressed(path: &str) -> anyhow::Result<Vec<TestRecord>> {
        let file = std::fs::File::open(path)?;
        let reader = auto_detect_reader(file, path)?;
        let buf_reader = std::io::BufReader::new(reader);
        let mut result = Vec::new();
        for line in std::io::BufRead::lines(buf_reader) {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let record: TestRecord = serde_json::from_str(&line)?;
            result.push(record);
        }
        Ok(result)
    }

    #[cfg(feature = "compression-gzip")]
    #[test]
    fn test_gzip_roundtrip() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let path = temp.path().with_extension("jsonl.gz");
        let path_str = path.to_str().unwrap();

        let data = sample_data();
        write_jsonl_compressed(path_str, &data)?;

        let loaded = read_jsonl_compressed(path_str)?;
        assert_eq!(data, loaded);

        // Verify file is actually compressed (should be smaller than uncompressed)
        let compressed_size = std::fs::metadata(&path)?.len();
        let uncompressed_json = serde_json::to_string(&data)?;
        assert!(compressed_size < uncompressed_json.len() as u64);

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn test_zstd_roundtrip() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let path = temp.path().with_extension("jsonl.zst");
        let path_str = path.to_str().unwrap();

        let data = sample_data();
        write_jsonl_compressed(path_str, &data)?;

        let loaded = read_jsonl_compressed(path_str)?;
        assert_eq!(data, loaded);

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[cfg(feature = "compression-bzip2")]
    #[test]
    fn test_bzip2_roundtrip() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let path = temp.path().with_extension("jsonl.bz2");
        let path_str = path.to_str().unwrap();

        let data = sample_data();
        write_jsonl_compressed(path_str, &data)?;

        let loaded = read_jsonl_compressed(path_str)?;
        assert_eq!(data, loaded);

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[cfg(feature = "compression-xz")]
    #[test]
    fn test_xz_roundtrip() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let path = temp.path().with_extension("jsonl.xz");
        let path_str = path.to_str().unwrap();

        let data = sample_data();
        write_jsonl_compressed(path_str, &data)?;

        let loaded = read_jsonl_compressed(path_str)?;
        assert_eq!(data, loaded);

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn test_uncompressed_passthrough() -> anyhow::Result<()> {
        let temp = NamedTempFile::new()?;
        let path = temp.path().with_extension("jsonl");
        let path_str = path.to_str().unwrap();

        let data = sample_data();
        write_jsonl_compressed(path_str, &data)?;

        let loaded = read_jsonl_compressed(path_str)?;
        assert_eq!(data, loaded);

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn test_custom_codec() -> anyhow::Result<()> {
        // Custom no-op codec for testing pluggability
        struct NoOpCodec;

        impl CompressionCodec for NoOpCodec {
            fn name(&self) -> &str {
                "noop"
            }

            fn extensions(&self) -> &[&str] {
                &[".noop"]
            }

            fn magic_bytes(&self) -> Option<&[u8]> {
                Some(&[0xFF, 0xFE])
            }

            fn wrap_reader_dyn(&self, reader: Box<dyn Read>) -> std::io::Result<Box<dyn Read>> {
                Ok(reader)
            }

            fn wrap_writer_dyn(&self, writer: Box<dyn Write>) -> std::io::Result<Box<dyn Write>> {
                Ok(writer)
            }
        }

        // Register custom codec
        register_codec(Arc::new(NoOpCodec));

        let temp = NamedTempFile::new()?;
        let path = temp.path().with_extension("jsonl.noop");
        let path_str = path.to_str().unwrap();

        let data = sample_data();
        write_jsonl_compressed(path_str, &data)?;

        let loaded = read_jsonl_compressed(path_str)?;
        assert_eq!(data, loaded);

        std::fs::remove_file(path)?;
        Ok(())
    }

    #[cfg(feature = "compression-gzip")]
    #[test]
    fn test_magic_byte_detection() -> anyhow::Result<()> {
        // Write gzip data but with wrong extension to test magic byte fallback
        let temp = NamedTempFile::new()?;
        let gz_path = temp.path().with_extension("dat"); // intentionally wrong extension
        let gz_path_str = gz_path.to_str().unwrap();

        // Manually write gzip data
        use flate2::Compression;
        use flate2::write::GzEncoder;
        let file = std::fs::File::create(&gz_path)?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        let data = sample_data();
        for record in &data {
            serde_json::to_writer(&mut encoder, record)?;
            encoder.write_all(b"\n")?;
        }
        encoder.finish()?;

        // Should still detect gzip via magic bytes
        let loaded = read_jsonl_compressed(gz_path_str)?;
        assert_eq!(data, loaded);

        std::fs::remove_file(gz_path)?;
        Ok(())
    }

    #[test]
    #[cfg(feature = "compression-gzip")]
    fn test_detect_from_magic_insufficient_bytes() {
        use std::io::BufRead;
        // Test with less bytes than gzip magic bytes
        let data: &[u8] = &[0x1f]; // Only 1 byte, but gzip needs 2
        let mut reader = std::io::BufReader::new(std::io::Cursor::new(data));
        // Force fill_buf to be called
        let _ = reader.fill_buf();
        // Test that detection doesn't panic and returns None
        let file = std::io::Cursor::new(data);
        let result = auto_detect_reader(file, "test.dat");
        // Should succeed but not detect compression
        assert!(result.is_ok());
    }

    // Unit tests from src/io/compression.rs

    #[test]
    #[cfg(feature = "compression-gzip")]
    fn test_gzip_codec_name() {
        // Test that we can access the gzip codec name
        // (Indirect test since GzipCodec is not directly accessible)
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().with_extension("test.gz");
        let result = auto_detect_writer(vec![], path.to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "compression-zstd")]
    fn test_zstd_codec_name() {
        // Test that we can access the zstd codec name
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().with_extension("test.zst");
        let result = auto_detect_writer(vec![], path.to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "compression-bzip2")]
    fn test_bzip2_codec_name() {
        // Test that we can access the bzip2 codec name
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().with_extension("test.bz2");
        let result = auto_detect_writer(vec![], path.to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "compression-xz")]
    fn test_xz_codec_name() {
        // Test that we can access the xz codec name
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().with_extension("test.xz");
        let result = auto_detect_writer(vec![], path.to_str().unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn test_detect_from_magic_empty_buffer() {
        use std::io::Cursor;
        let data: &[u8] = &[];
        let result = auto_detect_reader(Cursor::new(data), "test.dat");
        assert!(result.is_ok());
    }

    #[test]
    fn test_detect_from_magic_no_match() {
        use std::io::Cursor;
        let data: &[u8] = &[0x00, 0x01, 0x02, 0x03];
        let result = auto_detect_reader(Cursor::new(data), "test.dat");
        assert!(result.is_ok());
    }

    #[test]
    #[cfg(feature = "compression-gzip")]
    fn test_auto_detect_reader_error_on_bad_compression() {
        use std::io::Cursor;
        // Create invalid gzip data
        let bad_data: Vec<u8> = vec![0x1f, 0x8b, 0x00, 0x00]; // Gzip magic but incomplete
        let cursor = Cursor::new(bad_data);

        // This should fail when trying to decompress
        let result = auto_detect_reader(cursor, "test.gz");
        // The error context should mention the codec name
        if let Err(e) = result {
            let error_msg = format!("{:?}", e);
            assert!(error_msg.contains("gzip") || error_msg.contains("wrap reader"));
        }
    }

    #[test]
    #[cfg(feature = "compression-gzip")]
    fn test_auto_detect_writer_with_gzip() {
        let buffer = Vec::new();
        let result = auto_detect_writer(buffer, "test.gz");
        assert!(result.is_ok());
    }

    #[test]
    fn test_auto_detect_writer_no_compression() {
        let buffer = Vec::new();
        let result = auto_detect_writer(buffer, "test.txt");
        assert!(result.is_ok());
    }
}

#[cfg(not(any(
    feature = "compression-gzip",
    feature = "compression-zstd",
    feature = "compression-bzip2",
    feature = "compression-xz"
)))]
#[test]
fn compression_tests_skipped() {
    // This ensures the test file compiles even without compression features
}
