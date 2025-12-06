use ironbeam::io::cloud::ObjectIO;
use ironbeam::io::cloud::fake::FakeObjectIO;
use ironbeam::io::cloud::readers::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestRecord {
    id: u64,
    name: String,
}

#[test]
fn test_read_write_cloud_jsonl() {
    let storage = FakeObjectIO::new();

    // Write test data
    let records = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
        },
    ];

    let count = write_cloud_jsonl_vec(&storage, "test-bucket", "data.jsonl", &records).unwrap();
    assert_eq!(count, 2);

    // Read it back
    let read_records: Vec<TestRecord> =
        read_cloud_jsonl_vec(&storage, "test-bucket", "data.jsonl").unwrap();
    assert_eq!(read_records, records);
}

#[test]
fn test_expand_cloud_glob() {
    let storage = FakeObjectIO::new();

    // Add some test objects
    storage
        .put_object("bucket", "logs/2024-01-01/data.jsonl", b"test1")
        .unwrap();
    storage
        .put_object("bucket", "logs/2024-01-02/data.jsonl", b"test2")
        .unwrap();
    storage
        .put_object("bucket", "logs/2024-02-01/data.jsonl", b"test3")
        .unwrap();
    storage
        .put_object("bucket", "other/file.txt", b"test4")
        .unwrap();

    // Test glob matching
    let matches = expand_cloud_glob(&storage, "bucket", "logs/2024-01-*/data.jsonl").unwrap();
    assert_eq!(matches.len(), 2);
    assert!(matches.contains(&"logs/2024-01-01/data.jsonl".to_string()));
    assert!(matches.contains(&"logs/2024-01-02/data.jsonl".to_string()));

    // Test with wildcard at the end
    let matches = expand_cloud_glob(&storage, "bucket", "logs/*/*.jsonl").unwrap();
    assert_eq!(matches.len(), 3);
}

#[test]
fn test_read_cloud_jsonl_glob() {
    let storage = FakeObjectIO::new();

    // Create test data across multiple files
    let records1 = vec![TestRecord {
        id: 1,
        name: "Alice".to_string(),
    }];
    let records2 = vec![TestRecord {
        id: 2,
        name: "Bob".to_string(),
    }];

    write_cloud_jsonl_vec(&storage, "bucket", "data/file1.jsonl", &records1).unwrap();
    write_cloud_jsonl_vec(&storage, "bucket", "data/file2.jsonl", &records2).unwrap();

    // Read all matching files
    let all_records: Vec<TestRecord> =
        read_cloud_jsonl_glob(&storage, "bucket", "data/*.jsonl").unwrap();

    assert_eq!(all_records.len(), 2);
    assert!(all_records.contains(&records1[0]));
    assert!(all_records.contains(&records2[0]));
}

#[test]
fn test_expand_cloud_glob_required() {
    let storage = FakeObjectIO::new();

    // Should error when no matches
    let result = expand_cloud_glob_required(&storage, "bucket", "nonexistent/*.jsonl");
    assert!(result.is_err());

    // Should succeed when matches exist
    storage
        .put_object("bucket", "data/file.jsonl", b"test")
        .unwrap();
    let result = expand_cloud_glob_required(&storage, "bucket", "data/*.jsonl");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
}

#[test]
#[cfg(feature = "compression-gzip")]
fn test_write_compressed_gzip() {
    let storage = FakeObjectIO::new();

    // Use larger dataset to ensure compression benefits outweigh gzip header overhead
    let records: Vec<TestRecord> = (1..=100)
        .map(|i| TestRecord {
            id: i,
            name: format!("Name_{i}_with_some_longer_text_to_compress"),
        })
        .collect();

    // Write with gzip compression
    let count = write_cloud_jsonl_vec(&storage, "test-bucket", "data.jsonl.gz", &records).unwrap();
    assert_eq!(count, 100);

    // Verify the data is actually compressed (should be smaller than uncompressed for large data)
    let compressed_data = storage.get_object("test-bucket", "data.jsonl.gz").unwrap();
    let mut uncompressed_size = 0;
    for record in &records {
        uncompressed_size += serde_json::to_string(record).unwrap().len() + 1; // +1 for newline
    }
    // With sufficient data and repetition, compression should win
    assert!(
        compressed_data.len() < uncompressed_size,
        "Compressed size {} should be less than uncompressed size {}",
        compressed_data.len(),
        uncompressed_size
    );

    // Read it back - decompression should be automatic
    let read_records: Vec<TestRecord> =
        read_cloud_jsonl_vec(&storage, "test-bucket", "data.jsonl.gz").unwrap();
    assert_eq!(read_records, records);
}

#[test]
#[cfg(feature = "compression-zstd")]
fn test_write_compressed_zstd() {
    let storage = FakeObjectIO::new();

    let records = vec![
        TestRecord {
            id: 100,
            name: "Charlie".to_string(),
        },
        TestRecord {
            id: 200,
            name: "Diana".to_string(),
        },
    ];

    // Write with zstd compression
    write_cloud_jsonl_vec(&storage, "test-bucket", "data.jsonl.zst", &records).unwrap();

    // Read it back
    let read_records: Vec<TestRecord> =
        read_cloud_jsonl_vec(&storage, "test-bucket", "data.jsonl.zst").unwrap();
    assert_eq!(read_records, records);
}

#[test]
fn test_compression_feature_error() {
    let _storage = FakeObjectIO::new();
    let _records = [TestRecord {
        id: 1,
        name: "Test".to_string(),
    }];

    // Try to write with a compression format that's not enabled
    #[cfg(not(feature = "compression-gzip"))]
    {
        let result = write_cloud_jsonl_vec(&storage, "bucket", "data.jsonl.gz", &records);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("not enabled"));
    }
}
