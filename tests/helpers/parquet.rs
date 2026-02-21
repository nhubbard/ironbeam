//! Tests for Parquet helper functions with focus on error handling and edge cases.

#![cfg(feature = "io-parquet")]

use anyhow::Result;
use ironbeam::testing::*;
use ironbeam::{from_vec, read_parquet_streaming};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct TestRecord {
    id: u64,
    name: String,
}

#[test]
fn test_write_parquet_empty_collection() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.parquet");

    let p = TestPipeline::new();
    let empty: Vec<TestRecord> = vec![];
    let collection = from_vec(&p, empty);

    let count = collection.write_parquet(&path)?;
    assert_eq!(count, 0);
    assert!(path.exists());

    Ok(())
}

#[test]
fn test_write_parquet_single_record() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("single.parquet");

    let p = TestPipeline::new();
    let data = vec![TestRecord {
        id: 1,
        name: "test".to_string(),
    }];
    let collection = from_vec(&p, data.clone());

    let count = collection.write_parquet(&path)?;
    assert_eq!(count, 1);

    // Read it back to verify
    let p2 = TestPipeline::new();
    let read = read_parquet_streaming::<TestRecord>(&p2, &path, 1)?;
    let result = read.collect_seq_sorted()?;
    assert_eq!(result, data);

    Ok(())
}

#[test]
fn test_write_parquet_preserves_order() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("ordered.parquet");

    let p = TestPipeline::new();
    let data = vec![
        TestRecord {
            id: 3,
            name: "c".to_string(),
        },
        TestRecord {
            id: 1,
            name: "a".to_string(),
        },
        TestRecord {
            id: 2,
            name: "b".to_string(),
        },
    ];
    let collection = from_vec(&p, data.clone());

    let _ = collection.write_parquet(&path)?;

    // Read it back and verify order is preserved
    let p2 = TestPipeline::new();
    let read = read_parquet_streaming::<TestRecord>(&p2, &path, 1)?;
    let result = read.collect_seq()?;
    assert_eq!(result, data); // Order should match

    Ok(())
}

#[test]
fn test_read_parquet_streaming_glob_pattern() -> Result<()> {
    let tmp = tempfile::tempdir()?;

    // Create multiple parquet files
    let p1 = TestPipeline::new();
    let data1 = vec![
        TestRecord {
            id: 1,
            name: "file1".to_string(),
        },
        TestRecord {
            id: 2,
            name: "file1".to_string(),
        },
    ];
    from_vec(&p1, data1.clone()).write_parquet(tmp.path().join("file1.parquet"))?;

    let p2 = TestPipeline::new();
    let data2 = vec![
        TestRecord {
            id: 3,
            name: "file2".to_string(),
        },
        TestRecord {
            id: 4,
            name: "file2".to_string(),
        },
    ];
    from_vec(&p2, data2.clone()).write_parquet(tmp.path().join("file2.parquet"))?;

    // Read with glob pattern
    let p3 = TestPipeline::new();
    let glob_pattern = tmp.path().join("*.parquet");
    let collection = read_parquet_streaming::<TestRecord>(&p3, glob_pattern, 1)?;
    let result = collection.collect_seq_sorted()?;

    // Should contain all records from both files, sorted
    let mut expected = data1;
    expected.extend(data2);
    expected.sort();

    assert_eq!(result, expected);

    Ok(())
}

#[test]
fn test_read_parquet_streaming_glob_pattern_no_matches() {
    let tmp = tempfile::tempdir().unwrap();

    let p = TestPipeline::new();
    let glob_pattern = tmp.path().join("nonexistent*.parquet");
    let result = read_parquet_streaming::<TestRecord>(&p, glob_pattern, 1);

    // Should return an error when no files match
    assert!(result.is_err());
    if let Err(err) = result {
        assert!(err.to_string().contains("no files found"));
    }
}

#[test]
fn test_read_parquet_streaming_single_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("single.parquet");

    // Write test data
    let p1 = TestPipeline::new();
    let data = vec![
        TestRecord {
            id: 1,
            name: "test".to_string(),
        },
        TestRecord {
            id: 2,
            name: "test".to_string(),
        },
    ];
    from_vec(&p1, data.clone()).write_parquet(&path)?;

    // Read with single file path (not a glob)
    let p2 = TestPipeline::new();
    let collection = read_parquet_streaming::<TestRecord>(&p2, &path, 1)?;
    let result = collection.collect_seq_sorted()?;

    assert_eq!(result, data);

    Ok(())
}

#[test]
fn test_read_parquet_streaming_with_subdirectories() -> Result<()> {
    let tmp = tempfile::tempdir()?;

    // Create nested directory structure
    let year_dir = tmp.path().join("year=2024");
    let month_dir = year_dir.join("month=01");
    fs::create_dir_all(&month_dir)?;

    // Write files in nested directories
    let p1 = TestPipeline::new();
    let data1 = vec![TestRecord {
        id: 1,
        name: "nested".to_string(),
    }];
    from_vec(&p1, data1.clone()).write_parquet(month_dir.join("data.parquet"))?;

    let month_dir2 = year_dir.join("month=02");
    fs::create_dir_all(&month_dir2)?;

    let p2 = TestPipeline::new();
    let data2 = vec![TestRecord {
        id: 2,
        name: "nested2".to_string(),
    }];
    from_vec(&p2, data2.clone()).write_parquet(month_dir2.join("data.parquet"))?;

    // Read with nested glob pattern
    let p3 = TestPipeline::new();
    let pattern = format!("{}/year=2024/month=*/*.parquet", tmp.path().display());
    let collection = read_parquet_streaming::<TestRecord>(&p3, pattern, 1)?;
    let result = collection.collect_seq_sorted()?;

    let mut expected = data1;
    expected.extend(data2);
    expected.sort();

    assert_eq!(result, expected);

    Ok(())
}

#[test]
fn test_read_parquet_streaming_multiple_row_groups() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi_groups.parquet");

    // Create a larger dataset to ensure multiple row groups
    let p1 = TestPipeline::new();
    let data: Vec<TestRecord> = (0..1000)
        .map(|i| TestRecord {
            id: i,
            name: format!("record_{i}"),
        })
        .collect();
    from_vec(&p1, data.clone()).write_parquet(&path)?;

    // Read with different groups_per_shard values
    for groups_per_shard in [1, 2, 5, 10] {
        let p2 = TestPipeline::new();
        let collection = read_parquet_streaming::<TestRecord>(&p2, &path, groups_per_shard)?;
        let result = collection.collect_seq_sorted()?;

        assert_eq!(result, data);
    }

    Ok(())
}

#[test]
fn test_write_parquet_invalid_path() {
    let p = TestPipeline::new();
    let data = vec![TestRecord {
        id: 1,
        name: "test".to_string(),
    }];
    let collection = from_vec(&p, data);

    // Try to write to an invalid path (directory that doesn't exist)
    let invalid_path = "/nonexistent/directory/file.parquet";
    let result = collection.write_parquet(invalid_path);

    // Should return an error
    assert!(result.is_err());
}

#[test]
fn test_read_parquet_streaming_nonexistent_file() {
    let p = TestPipeline::new();
    let result = read_parquet_streaming::<TestRecord>(&p, "/nonexistent/file.parquet", 1);

    // Should return an error
    assert!(result.is_err());
}

#[test]
fn test_parquet_with_complex_types() -> Result<()> {
    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct ComplexRecord {
        id: u64,
        values: Vec<f64>,
        metadata: String,
    }

    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("complex.parquet");

    let p1 = TestPipeline::new();
    let data = vec![
        ComplexRecord {
            id: 1,
            values: vec![1.0, 2.0, 3.0],
            metadata: "test1".to_string(),
        },
        ComplexRecord {
            id: 2,
            values: vec![4.0, 5.0],
            metadata: "test2".to_string(),
        },
    ];
    from_vec(&p1, data.clone()).write_parquet(&path)?;

    let p2 = TestPipeline::new();
    let collection = read_parquet_streaming::<ComplexRecord>(&p2, &path, 1)?;
    let result = collection.collect_seq()?;

    assert_eq!(result, data);

    Ok(())
}

#[test]
fn test_parquet_roundtrip_with_special_characters() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("special_chars.parquet");

    let p1 = TestPipeline::new();
    let data = vec![
        TestRecord {
            id: 1,
            name: "test with spaces".to_string(),
        },
        TestRecord {
            id: 2,
            name: "test\nwith\nnewlines".to_string(),
        },
        TestRecord {
            id: 3,
            name: "test\twith\ttabs".to_string(),
        },
        TestRecord {
            id: 4,
            name: "test\"with\"quotes".to_string(),
        },
    ];
    from_vec(&p1, data.clone()).write_parquet(&path)?;

    let p2 = TestPipeline::new();
    let collection = read_parquet_streaming::<TestRecord>(&p2, &path, 1)?;
    let result = collection.collect_seq_sorted()?;

    assert_eq!(result, data);

    Ok(())
}

#[test]
fn test_parquet_glob_pattern_with_question_mark() -> Result<()> {
    let tmp = tempfile::tempdir()?;

    // Create files matching pattern
    let p1 = TestPipeline::new();
    let data1 = vec![TestRecord {
        id: 1,
        name: "a".to_string(),
    }];
    from_vec(&p1, data1.clone()).write_parquet(tmp.path().join("file1.parquet"))?;

    let p2 = TestPipeline::new();
    let data2 = vec![TestRecord {
        id: 2,
        name: "b".to_string(),
    }];
    from_vec(&p2, data2.clone()).write_parquet(tmp.path().join("file2.parquet"))?;

    // Use ? wildcard (matches single character)
    let p3 = TestPipeline::new();
    let pattern = tmp.path().join("file?.parquet");
    let collection = read_parquet_streaming::<TestRecord>(&p3, pattern, 1)?;
    let result = collection.collect_seq_sorted()?;

    let mut expected = data1;
    expected.extend(data2);
    expected.sort();

    assert_eq!(result, expected);

    Ok(())
}

#[test]
fn test_write_parquet_returns_correct_count() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("count_test.parquet");

    let p = TestPipeline::new();
    let data: Vec<TestRecord> = (0..42)
        .map(|i| TestRecord {
            id: i,
            name: format!("record_{i}"),
        })
        .collect();
    let collection = from_vec(&p, data);

    let count = collection.write_parquet(&path)?;
    assert_eq!(count, 42);

    Ok(())
}

#[test]
fn test_parquet_streaming_with_unicode() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("unicode.parquet");

    let p1 = TestPipeline::new();
    let data = vec![
        TestRecord {
            id: 1,
            name: "こんにちは".to_string(), // Japanese
        },
        TestRecord {
            id: 2,
            name: "مرحبا".to_string(), // Arabic
        },
        TestRecord {
            id: 3,
            name: "🎉🚀".to_string(), // Emojis
        },
    ];
    from_vec(&p1, data.clone()).write_parquet(&path)?;

    let p2 = TestPipeline::new();
    let collection = read_parquet_streaming::<TestRecord>(&p2, &path, 1)?;
    let result = collection.collect_seq_sorted()?;

    assert_eq!(result, data);

    Ok(())
}
