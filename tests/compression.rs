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
        use flate2::write::GzEncoder;
        use flate2::Compression;
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
