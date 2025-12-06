//! Integration tests for glob pattern support in file readers.

use anyhow::Result;
use ironbeam::testing::*;
use ironbeam::*;
use serde::{Deserialize, Serialize};
use std::fs::create_dir_all;
use tempfile::TempDir;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Record {
    id: u32,
    name: String,
}

#[cfg(feature = "io-jsonl")]
#[test]
fn test_jsonl_glob_pattern() -> Result<()> {
    let dir = TempDir::new()?;
    let base = dir.path();

    // Create multiple JSONL files
    let file1 = base.join("data1.jsonl");
    let file2 = base.join("data2.jsonl");

    let records1 = vec![
        Record {
            id: 1,
            name: "Alice".to_string(),
        },
        Record {
            id: 2,
            name: "Bob".to_string(),
        },
    ];
    let records2 = vec![
        Record {
            id: 3,
            name: "Charlie".to_string(),
        },
        Record {
            id: 4,
            name: "Diana".to_string(),
        },
    ];

    write_jsonl_vec(&file1, &records1)?;
    write_jsonl_vec(&file2, &records2)?;

    // Read with a glob pattern
    let p = TestPipeline::new();
    let pattern = format!("{}/*.jsonl", base.display());
    let pc: PCollection<Record> = read_jsonl(&p, &pattern)?;

    let mut result = pc.collect_seq()?;
    result.sort();

    assert_eq!(result.len(), 4);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 2);
    assert_eq!(result[2].id, 3);
    assert_eq!(result[3].id, 4);

    Ok(())
}

#[cfg(feature = "io-jsonl")]
#[test]
fn test_jsonl_single_file() -> Result<()> {
    let dir = TempDir::new()?;
    let file = dir.path().join("data.jsonl");

    let records = vec![
        Record {
            id: 1,
            name: "Alice".to_string(),
        },
        Record {
            id: 2,
            name: "Bob".to_string(),
        },
    ];

    write_jsonl_vec(&file, &records)?;

    // Read a single file (no glob pattern)
    let p = TestPipeline::new();
    let pc: PCollection<Record> = read_jsonl(&p, &file)?;

    let result = pc.collect_seq()?;

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 2);

    Ok(())
}

#[cfg(feature = "io-jsonl")]
#[test]
fn test_jsonl_no_matches() {
    let dir = TempDir::new().unwrap();
    let pattern = format!("{}/*.nonexistent", dir.path().display());

    let p = TestPipeline::new();
    let result: Result<PCollection<Record>, _> = read_jsonl(&p, &pattern);

    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(err.to_string().contains("no files found"));
}

#[cfg(feature = "io-csv")]
#[test]
fn test_csv_glob_pattern() -> Result<()> {
    let dir = TempDir::new()?;
    let base = dir.path();

    // Create multiple CSV files
    let file1 = base.join("data1.csv");
    let file2 = base.join("data2.csv");

    let records1 = vec![
        Record {
            id: 1,
            name: "Alice".to_string(),
        },
        Record {
            id: 2,
            name: "Bob".to_string(),
        },
    ];
    let records2 = vec![
        Record {
            id: 3,
            name: "Charlie".to_string(),
        },
        Record {
            id: 4,
            name: "Diana".to_string(),
        },
    ];

    write_csv_vec(&file1, true, &records1)?;
    write_csv_vec(&file2, true, &records2)?;

    // Read with a glob pattern
    let p = TestPipeline::new();
    let pattern = format!("{}/*.csv", base.display());
    let pc: PCollection<Record> = read_csv(&p, &pattern, true)?;

    let mut result = pc.collect_seq()?;
    result.sort();

    assert_eq!(result.len(), 4);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 2);
    assert_eq!(result[2].id, 3);
    assert_eq!(result[3].id, 4);

    Ok(())
}

#[cfg(feature = "io-csv")]
#[test]
fn test_csv_single_file() -> Result<()> {
    let dir = TempDir::new()?;
    let file = dir.path().join("data.csv");

    let records = vec![
        Record {
            id: 1,
            name: "Alice".to_string(),
        },
        Record {
            id: 2,
            name: "Bob".to_string(),
        },
    ];

    write_csv_vec(&file, true, &records)?;

    // Read a single file (no glob pattern)
    let p = TestPipeline::new();
    let pc: PCollection<Record> = read_csv(&p, &file, true)?;

    let result = pc.collect_seq()?;

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 2);

    Ok(())
}

#[cfg(feature = "io-parquet")]
#[test]
fn test_parquet_glob_pattern() -> Result<()> {
    let dir = TempDir::new()?;
    let base = dir.path();

    // Create multiple Parquet files
    let file1 = base.join("data1.parquet");
    let file2 = base.join("data2.parquet");

    let records1 = vec![
        Record {
            id: 1,
            name: "Alice".to_string(),
        },
        Record {
            id: 2,
            name: "Bob".to_string(),
        },
    ];
    let records2 = vec![
        Record {
            id: 3,
            name: "Charlie".to_string(),
        },
        Record {
            id: 4,
            name: "Diana".to_string(),
        },
    ];

    write_parquet_vec(&file1, &records1)?;
    write_parquet_vec(&file2, &records2)?;

    // Read with a glob pattern
    let p = TestPipeline::new();
    let pattern = format!("{}/*.parquet", base.display());
    let pc: PCollection<Record> = read_parquet_streaming(&p, &pattern, 1)?;

    let mut result = pc.collect_seq()?;
    result.sort();

    assert_eq!(result.len(), 4);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 2);
    assert_eq!(result[2].id, 3);
    assert_eq!(result[3].id, 4);

    Ok(())
}

#[cfg(feature = "io-parquet")]
#[test]
fn test_parquet_single_file() -> Result<()> {
    let dir = TempDir::new()?;
    let file = dir.path().join("data.parquet");

    let records = vec![
        Record {
            id: 1,
            name: "Alice".to_string(),
        },
        Record {
            id: 2,
            name: "Bob".to_string(),
        },
    ];

    write_parquet_vec(&file, &records)?;

    // Read a single file (no glob pattern)
    let p = TestPipeline::new();
    let pc: PCollection<Record> = read_parquet_streaming(&p, &file, 1)?;

    let result = pc.collect_seq()?;

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 2);

    Ok(())
}

#[cfg(feature = "io-jsonl")]
#[test]
fn test_jsonl_date_partitions() -> Result<()> {
    let dir = TempDir::new()?;
    let base = dir.path();

    // Create a date-partitioned structure
    let day1 = base.join("year=2024/month=01/day=01");
    let day2 = base.join("year=2024/month=01/day=02");
    create_dir_all(&day1)?;
    create_dir_all(&day2)?;

    let file1 = day1.join("data.jsonl");
    let file2 = day2.join("data.jsonl");

    let records1 = vec![Record {
        id: 1,
        name: "Day1".to_string(),
    }];
    let records2 = vec![Record {
        id: 2,
        name: "Day2".to_string(),
    }];

    write_jsonl_vec(&file1, &records1)?;
    write_jsonl_vec(&file2, &records2)?;

    // Read with date partition glob pattern
    let p = TestPipeline::new();
    let pattern = format!("{}/year=2024/month=*/day=*/data.jsonl", base.display());
    let pc: PCollection<Record> = read_jsonl(&p, &pattern)?;

    let mut result = pc.collect_seq()?;
    result.sort();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].id, 1);
    assert_eq!(result[1].id, 2);

    Ok(())
}

#[cfg(feature = "io-csv")]
#[test]
fn test_csv_deterministic_order() -> Result<()> {
    let dir = TempDir::new()?;
    let base = dir.path();

    // Create files with names that will sort lexicographically
    let file1 = base.join("a_data.csv");
    let file2 = base.join("b_data.csv");
    let file3 = base.join("c_data.csv");

    let records1 = vec![Record {
        id: 1,
        name: "A".to_string(),
    }];
    let records2 = vec![Record {
        id: 2,
        name: "B".to_string(),
    }];
    let records3 = vec![Record {
        id: 3,
        name: "C".to_string(),
    }];

    write_csv_vec(&file1, true, &records1)?;
    write_csv_vec(&file2, true, &records2)?;
    write_csv_vec(&file3, true, &records3)?;

    // Read with a glob pattern multiple times
    let p1 = TestPipeline::new();
    let pattern = format!("{}/*.csv", base.display());
    let pc1: PCollection<Record> = read_csv(&p1, &pattern, true)?;
    let result1 = pc1.collect_seq()?;

    let p2 = TestPipeline::new();
    let pc2: PCollection<Record> = read_csv(&p2, &pattern, true)?;
    let result2 = pc2.collect_seq()?;

    // Results should be identical due to sorted file order
    assert_eq!(result1, result2);
    assert_eq!(result1[0].id, 1);
    assert_eq!(result1[1].id, 2);
    assert_eq!(result1[2].id, 3);

    Ok(())
}

// Unit tests from src/io/glob.rs
mod glob_unit_tests {
    use anyhow::Result;
    use ironbeam::io::glob::{expand_glob, expand_glob_required};
    use std::fs::{File, create_dir_all};
    use tempfile::TempDir;

    #[test]
    fn test_expand_glob_basic() -> Result<()> {
        let dir = TempDir::new()?;
        let base = dir.path();

        // Create test files
        File::create(base.join("test1.txt"))?;
        File::create(base.join("test2.txt"))?;
        File::create(base.join("other.csv"))?;

        let pattern = format!("{}/*.txt", base.display());
        let files = expand_glob(&pattern)?;

        assert_eq!(files.len(), 2);
        assert!(files[0].to_string_lossy().ends_with("test1.txt"));
        assert!(files[1].to_string_lossy().ends_with("test2.txt"));
        Ok(())
    }

    #[test]
    fn test_expand_glob_nested() -> Result<()> {
        let dir = TempDir::new()?;
        let base = dir.path();

        // Create nested structure
        create_dir_all(base.join("year=2024/month=01"))?;
        create_dir_all(base.join("year=2024/month=02"))?;
        File::create(base.join("year=2024/month=01/data.json"))?;
        File::create(base.join("year=2024/month=02/data.json"))?;

        let pattern = format!("{}/year=2024/month=*/data.json", base.display());
        let files = expand_glob(&pattern)?;

        assert_eq!(files.len(), 2);
        Ok(())
    }

    #[test]
    fn test_expand_glob_empty() -> Result<()> {
        let dir = TempDir::new()?;
        let pattern = format!("{}/*.nonexistent", dir.path().display());
        let files = expand_glob(&pattern)?;
        assert_eq!(files.len(), 0);
        Ok(())
    }

    #[test]
    fn test_expand_glob_required_fails_on_empty() -> Result<()> {
        let dir = TempDir::new()?;
        let pattern = format!("{}/*.nonexistent", dir.path().display());
        let result = expand_glob_required(&pattern);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_expand_glob_sorted() -> Result<()> {
        let dir = TempDir::new()?;
        let base = dir.path();

        // Create files in non-sorted order
        File::create(base.join("c.txt"))?;
        File::create(base.join("a.txt"))?;
        File::create(base.join("b.txt"))?;

        let pattern = format!("{}/*.txt", base.display());
        let files = expand_glob(&pattern)?;

        assert_eq!(files.len(), 3);
        // Verify sorted order
        for i in 0..files.len() - 1 {
            assert!(files[i] < files[i + 1]);
        }
        Ok(())
    }

    #[test]
    fn test_expand_glob_excludes_directories() -> Result<()> {
        let dir = TempDir::new()?;
        let base = dir.path();

        // Create a file and a directory
        File::create(base.join("file.txt"))?;
        create_dir_all(base.join("subdir.txt"))?;

        let pattern = format!("{}/*.txt", base.display());
        let files = expand_glob(&pattern)?;

        // Should only match the file, not the directory
        assert_eq!(files.len(), 1);
        assert!(files[0].to_string_lossy().ends_with("file.txt"));
        Ok(())
    }
}
