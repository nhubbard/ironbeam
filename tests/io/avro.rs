//! Tests for Avro I/O functionality.
//!
//! These tests verify:
//! - Vector read/write operations
//! - Streaming read operations
//! - Parallel write operations
//! - Schema handling
//! - Compression support
//! - Error handling

use anyhow::{Result, anyhow};
use apache_avro::Schema;
use ironbeam::io::avro::{
    AvroVecOps, build_avro_shards, build_avro_shards_with_schema,
    build_avro_shards_with_schema_obj, read_avro_range, read_avro_vec_with_schema,
    read_avro_vec_with_schema_obj, write_avro_par, write_avro_vec_with_schema,
};
use ironbeam::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct TestRecord {
    id: u32,
    name: String,
    value: f64,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
struct SimpleRecord {
    k: String,
    v: u64,
}

// Avro schemas for test types
const TEST_RECORD_SCHEMA: &str = r#"{
  "type": "record",
  "name": "TestRecord",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "value", "type": "double"}
  ]
}"#;

const SIMPLE_RECORD_SCHEMA: &str = r#"{
  "type": "record",
  "name": "SimpleRecord",
  "fields": [
    {"name": "k", "type": "string"},
    {"name": "v", "type": "long"}
  ]
}"#;

#[cfg(feature = "io-avro")]
#[test]
fn test_read_write_avro_vec() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("input.avro");
    let _output_path = temp_dir.path().join("output.avro");

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 100.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 200.0,
        },
        TestRecord {
            id: 3,
            name: "Charlie".to_string(),
            value: 300.0,
        },
    ];

    // Write
    let written = write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;
    assert_eq!(written, 3);

    // Read
    let read_data: Vec<TestRecord> = read_avro_vec(&input_path)?;
    assert_eq!(read_data.len(), 3);
    assert_eq!(read_data[0], data[0]);
    assert_eq!(read_data[1], data[1]);
    assert_eq!(read_data[2], data[2]);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_write_avro_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("empty.avro");
    let _output_path = temp_dir.path().join("output.avro");

    let data: Vec<TestRecord> = vec![];

    // Write empty
    let written = write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;
    assert_eq!(written, 0);

    // Read empty
    let read_data: Vec<TestRecord> = read_avro_vec(&input_path)?;
    assert_eq!(read_data.len(), 0);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_write_avro_simple() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("simple.avro");
    let _output_path = temp_dir.path().join("output.avro");

    let data = vec![
        SimpleRecord {
            k: "a".to_string(),
            v: 1,
        },
        SimpleRecord {
            k: "b".to_string(),
            v: 2,
        },
        SimpleRecord {
            k: "c".to_string(),
            v: 3,
        },
    ];

    // Write
    let written = write_avro_vec(&input_path, &data, SIMPLE_RECORD_SCHEMA)?;
    assert_eq!(written, 3);

    // Read
    let read_data: Vec<SimpleRecord> = read_avro_vec(&input_path)?;
    assert_eq!(read_data.len(), 3);
    assert_eq!(read_data[0], data[0]);
    assert_eq!(read_data[1], data[1]);
    assert_eq!(read_data[2], data[2]);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_write_avro_pipeline() -> Result<()> {
    let p = Pipeline::default();

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 100.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 200.0,
        },
    ];

    let pc: PCollection<TestRecord> = from_vec(&p, data.clone());
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("pipeline_output.avro");

    let written = pc.write_avro_with_schema(&output_path, TEST_RECORD_SCHEMA)?;
    assert_eq!(written, 2);

    let read_data: Vec<TestRecord> = read_avro_vec(&output_path)?;
    assert_eq!(read_data.len(), 2);
    assert_eq!(read_data[0], data[0]);
    assert_eq!(read_data[1], data[1]);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_write_avro_pipeline_transform() -> Result<()> {
    let p = Pipeline::default();

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 100.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 200.0,
        },
    ];

    let pc: PCollection<TestRecord> = from_vec(&p, data);
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("transformed.avro");

    let doubled = pc.map(|r: &TestRecord| TestRecord {
        id: r.id,
        name: r.name.clone(),
        value: r.value * 2.0,
    });

    let written = doubled.write_avro_with_schema(&output_path, TEST_RECORD_SCHEMA)?;
    assert_eq!(written, 2);

    let read_data: Vec<TestRecord> = read_avro_vec(&output_path)?;
    assert_eq!(read_data.len(), 2);
    assert_approx_eq!(read_data[0].value, 200.0);
    assert_approx_eq!(read_data[1].value, 400.0);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_write_avro_parallel() -> Result<()> {
    #[cfg(feature = "parallel-io")]
    {
        let p = Pipeline::default();

        let data = vec![
            TestRecord {
                id: 1,
                name: "Alice".to_string(),
                value: 100.0,
            },
            TestRecord {
                id: 2,
                name: "Bob".to_string(),
                value: 200.0,
            },
            TestRecord {
                id: 3,
                name: "Charlie".to_string(),
                value: 300.0,
            },
        ];

        let pc: PCollection<TestRecord> = from_vec(&p, data.clone());
        let temp_dir = TempDir::new()?;
        let output_path = temp_dir.path().join("parallel_output.avro");

        let written = pc.write_avro_par(&output_path, Some(2), TEST_RECORD_SCHEMA)?;
        assert_eq!(written, 3);

        let read_data: Vec<TestRecord> = read_avro_vec(&output_path)?;
        assert_eq!(read_data.len(), 3);
        assert_eq!(read_data[0], data[0]);
        assert_eq!(read_data[1], data[1]);
        assert_eq!(read_data[2], data[2]);
    }

    #[cfg(not(feature = "parallel-io"))]
    {
        // Skip test if parallel-io is not enabled
    }

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_streaming() -> Result<()> {
    let p = Pipeline::default();

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 100.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 200.0,
        },
        TestRecord {
            id: 3,
            name: "Charlie".to_string(),
            value: 300.0,
        },
    ];

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("streaming_input.avro");

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Create streaming source
    let stream: PCollection<TestRecord> = read_avro_streaming(&p, &input_path, 2)?;

    // Collect results
    let results = stream.collect_seq()?;
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], data[0]);
    assert_eq!(results[1], data[1]);
    assert_eq!(results[2], data[2]);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_read_avro_streaming_large() -> Result<()> {
    let p = Pipeline::default();

    let data: Vec<TestRecord> = (0..1000)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("User{i}"),
            value: f64::from(i),
        })
        .collect();

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("large_input.avro");

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Create streaming source with small shards
    let stream: PCollection<TestRecord> = read_avro_streaming(&p, &input_path, 100)?;

    // Collect results
    let results = stream.collect_seq()?;
    assert_eq!(results.len(), 1000);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_streaming_empty() -> Result<()> {
    let p = Pipeline::default();

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("empty.avro");

    // Write empty file
    write_avro_vec(&input_path, &Vec::<TestRecord>::new(), TEST_RECORD_SCHEMA)?;

    // Create streaming source
    let stream: PCollection<TestRecord> = read_avro_streaming(&p, &input_path, 100)?;

    // Collect results
    let results = stream.collect_seq()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_streaming_pipeline() -> Result<()> {
    let p = Pipeline::default();

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 100.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 200.0,
        },
    ];

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("pipeline_input.avro");

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Create streaming source
    let stream: PCollection<TestRecord> = read_avro_streaming(&p, &input_path, 1)?;

    // Transform
    let doubled = stream.map(|r: &TestRecord| TestRecord {
        id: r.id,
        name: r.name.clone(),
        value: r.value * 2.0,
    });

    // Collect results
    let results = doubled.collect_seq()?;
    assert_eq!(results.len(), 2);
    assert_approx_eq!(results[0].value, 200.0);
    assert_approx_eq!(results[1].value, 400.0);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_read_avro_streaming_large_pipeline() -> Result<()> {
    let p = Pipeline::default();

    let data: Vec<TestRecord> = (0..1000)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("User{i}"),
            value: f64::from(i),
        })
        .collect();

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("large_pipeline_input.avro");

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Create streaming source
    let stream: PCollection<TestRecord> = read_avro_streaming(&p, &input_path, 100)?;

    // Filter and transform
    let filtered = stream.filter(|r: &TestRecord| r.id.is_multiple_of(2));

    // Collect results
    let results = filtered.collect_seq()?;
    assert_eq!(results.len(), 500);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_streaming_keyed() -> Result<()> {
    let p = Pipeline::default();

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 100.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 200.0,
        },
        TestRecord {
            id: 3,
            name: "Alice".to_string(),
            value: 300.0,
        },
    ];

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("keyed_input.avro");

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Create streaming source
    let stream: PCollection<TestRecord> = read_avro_streaming(&p, &input_path, 1)?;

    // Key by name
    let keyed = stream.key_by(|r: &TestRecord| r.name.clone());

    // Combine values - extract the value field and sum it
    let combined = keyed
        .map_values(|v: &TestRecord| v.value)
        .combine_values(Sum::<f64>::default());

    // Collect results
    let results = combined.collect_seq()?;
    assert_eq!(results.len(), 2);

    let alice_sum = results.iter().find(|(k, _)| k == "Alice").unwrap();
    assert_approx_eq!(alice_sum.1, 400.0);

    let bob_sum = results.iter().find(|(k, _)| k == "Bob").unwrap();
    assert_approx_eq!(bob_sum.1, 200.0);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_build_avro_shards() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("shards_input.avro");

    let data: Vec<TestRecord> = (0..100)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("User{i}"),
            value: f64::from(i),
        })
        .collect();

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Build shards
    let shards = build_avro_shards(&input_path, 25)?;

    assert_eq!(shards.total_records, 100);
    assert_eq!(shards.ranges.len(), 4);
    assert_eq!(shards.ranges[0], (0, 25));
    assert_eq!(shards.ranges[1], (25, 50));
    assert_eq!(shards.ranges[2], (50, 75));
    assert_eq!(shards.ranges[3], (75, 100));

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_build_avro_shards_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("empty.avro");

    // Write empty file
    write_avro_vec(&input_path, &Vec::<TestRecord>::new(), TEST_RECORD_SCHEMA)?;

    // Build shards
    let shards = build_avro_shards(&input_path, 25)?;

    assert_eq!(shards.total_records, 0);
    assert_eq!(shards.ranges.len(), 0);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_read_avro_range() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("range_input.avro");

    let data: Vec<TestRecord> = (0..100)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("User{i}"),
            value: f64::from(i),
        })
        .collect();

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Build shards
    let shards = build_avro_shards(&input_path, 25)?;

    // Read range 0-25
    let range1: Vec<TestRecord> = read_avro_range(&shards, 0, 25)?;
    assert_eq!(range1.len(), 25);

    // Read range 25-50
    let range2: Vec<TestRecord> = read_avro_range(&shards, 25, 50)?;
    assert_eq!(range2.len(), 25);

    // Read range 50-75
    let range3: Vec<TestRecord> = read_avro_range(&shards, 50, 75)?;
    assert_eq!(range3.len(), 25);

    // Read range 75-100
    let range4: Vec<TestRecord> = read_avro_range(&shards, 75, 100)?;
    assert_eq!(range4.len(), 25);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_range_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("empty.avro");

    // Write empty file
    write_avro_vec(&input_path, &Vec::<TestRecord>::new(), TEST_RECORD_SCHEMA)?;

    // Build shards
    let shards = build_avro_shards(&input_path, 25)?;

    // Read empty range
    let range: Vec<TestRecord> = read_avro_range(&shards, 0, 0)?;
    assert_eq!(range.len(), 0);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::or_fun_call)]
fn test_avro_vec_ops() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("vecops_input.avro");

    let data: Vec<TestRecord> = (0..100)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("User{i}"),
            value: f64::from(i),
        })
        .collect();

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Build shards
    let shards = build_avro_shards(&input_path, 25)?;

    // Create VecOps adapter
    let vec_ops = AvroVecOps::<TestRecord>::new();

    // Test len
    let len = vec_ops.len(&shards).ok_or(anyhow!("len failed"))?;
    assert_eq!(len, 100);

    // Test split
    let parts = vec_ops.split(&shards, 4).ok_or(anyhow!("split failed"))?;
    assert_eq!(parts.len(), 4);

    // Test clone_any
    let cloned = vec_ops
        .clone_any(&shards)
        .ok_or(anyhow!("clone_any failed"))?;
    let cloned_data: Vec<TestRecord> = *cloned.downcast::<Vec<TestRecord>>().unwrap();
    assert_eq!(cloned_data.len(), 100);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::or_fun_call)]
fn test_avro_vec_ops_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("empty.avro");

    // Write empty file
    write_avro_vec(&input_path, &Vec::<TestRecord>::new(), TEST_RECORD_SCHEMA)?;

    // Build shards
    let shards = build_avro_shards(&input_path, 25)?;

    // Create VecOps adapter
    let vec_ops = AvroVecOps::<TestRecord>::new();

    // Test len
    let len = vec_ops.len(&shards).ok_or(anyhow!("len failed"))?;
    assert_eq!(len, 0);

    // Test split
    let parts = vec_ops.split(&shards, 4).ok_or(anyhow!("split failed"))?;
    assert_eq!(parts.len(), 0);

    // Test clone_any
    let cloned = vec_ops
        .clone_any(&shards)
        .ok_or(anyhow!("clone_any failed"))?;
    let cloned_data: Vec<TestRecord> = *cloned.downcast::<Vec<TestRecord>>().unwrap();
    assert_eq!(cloned_data.len(), 0);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::or_fun_call)]
fn test_avro_vec_ops_partial_split() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("partial.avro");

    let data: Vec<TestRecord> = (0..100)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("User{i}"),
            value: f64::from(i),
        })
        .collect();

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Build shards
    let shards = build_avro_shards(&input_path, 25)?;

    // Create VecOps adapter
    let vec_ops = AvroVecOps::<TestRecord>::new();

    // Test split with fewer partitions than shards
    let parts = vec_ops.split(&shards, 2).ok_or(anyhow!("split failed"))?;
    assert_eq!(parts.len(), 4);

    // Test split with more partitions than shards
    let parts = vec_ops.split(&shards, 10).ok_or(anyhow!("split failed"))?;
    assert_eq!(parts.len(), 4);

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_avro_roundtrip() -> Result<()> {
    let p = Pipeline::default();

    let original_data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 100.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 200.0,
        },
        TestRecord {
            id: 3,
            name: "Charlie".to_string(),
            value: 300.0,
        },
    ];

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("roundtrip_input.avro");
    let output_path = temp_dir.path().join("roundtrip_output.avro");

    // Write input
    write_avro_vec(&input_path, &original_data, TEST_RECORD_SCHEMA)?;

    // Read into PCollection
    let pc: PCollection<TestRecord> = read_avro(&p, &input_path)?;

    // Write output
    pc.write_avro_with_schema(&output_path, TEST_RECORD_SCHEMA)?;

    // Read output
    let output_data: Vec<TestRecord> = read_avro_vec(&output_path)?;

    // Verify roundtrip
    assert_eq!(output_data.len(), original_data.len());
    for (original, output) in original_data.iter().zip(output_data.iter()) {
        assert_approx_eq!(original.value, output.value);
        assert_eq!(original.id, output.id);
        assert_eq!(original.name, output.name);
    }

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::items_after_statements)]
#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::cast_sign_loss)]
fn test_avro_complex_pipeline() -> Result<()> {
    let p = Pipeline::default();

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 100.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 200.0,
        },
        TestRecord {
            id: 3,
            name: "Charlie".to_string(),
            value: 300.0,
        },
    ];

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("complex_input.avro");
    let output_path = temp_dir.path().join("complex_output.avro");

    // Write input
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Create complex pipeline
    let pc: PCollection<TestRecord> = read_avro(&p, &input_path)?;

    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct OutputRecord {
        k: String,
        v: u64,
    }

    let processed = pc
        .filter(|r: &TestRecord| r.value > 150.0)
        .map(|r: &TestRecord| TestRecord {
            id: r.id,
            name: r.name.clone(),
            value: r.value * 1.1,
        })
        .key_by(|r: &TestRecord| r.name.clone())
        .map_values(|v: &TestRecord| (v.value as u64).saturating_sub(10))
        .combine_values(Sum::<u64>::default());

    // Write using kv_schema for OutputRecord output
    let kv_schema = r#"{
      "type": "record",
      "name": "KvRecord",
      "fields": [
        {"name": "k", "type": "string"},
        {"name": "v", "type": "long"}
      ]
    }"#;

    let processed_with_struct = processed.map(|(k, v): &(String, u64)| OutputRecord {
        k: k.clone(),
        v: *v,
    });

    processed_with_struct.write_avro_with_schema(&output_path, kv_schema)?;

    // Read and verify results
    let output_data: Vec<OutputRecord> = read_avro_vec(&output_path)?;
    assert_eq!(output_data.len(), 2);
    assert!(output_data.iter().any(|r| r.k == "Bob" && r.v == 210u64));
    assert!(
        output_data
            .iter()
            .any(|r| r.k == "Charlie" && r.v == 320u64)
    );

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_avro_error_handling() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("invalid.avro");

    // Create invalid Avro file
    let mut file = File::create(&input_path)?;
    file.write_all(b"not an avro file")?;
    drop(file);

    // Try to read - should fail
    let result: Result<Vec<TestRecord>, _> = read_avro_vec(&input_path);
    assert!(result.is_err());

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_avro_large_dataset() -> Result<()> {
    let p = Pipeline::default();

    let data: Vec<TestRecord> = (0..10_000)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("User{i}"),
            value: f64::from(i),
        })
        .collect();

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("large.avro");
    let output_path = temp_dir.path().join("large_output.avro");

    // Write large dataset
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Read into PCollection
    let pc: PCollection<TestRecord> = read_avro(&p, &input_path)?;

    // Filter and transform
    let filtered = pc.filter(|r: &TestRecord| r.id.is_multiple_of(2));

    // Write output
    filtered.write_avro_with_schema(&output_path, TEST_RECORD_SCHEMA)?;

    // Read output
    let output_data: Vec<TestRecord> = read_avro_vec(&output_path)?;

    // Verify
    assert_eq!(output_data.len(), 5_000);
    #[allow(clippy::cast_possible_truncation)]
    for (i, record) in output_data.iter().enumerate() {
        assert_eq!(record.id, (i * 2) as u32);
    }

    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_avro_streaming_large_dataset() -> Result<()> {
    let p = Pipeline::default();

    let data: Vec<TestRecord> = (0..10_000)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("User{i}"),
            value: f64::from(i),
        })
        .collect();

    let temp_dir = TempDir::new()?;
    let input_path = temp_dir.path().join("large_streaming.avro");

    // Write large dataset
    write_avro_vec(&input_path, &data, TEST_RECORD_SCHEMA)?;

    // Create streaming source
    let stream: PCollection<TestRecord> = read_avro_streaming(&p, &input_path, 1000)?;

    // Filter
    let filtered = stream.filter(|r: &TestRecord| r.id.is_multiple_of(2));

    // Collect
    let results = filtered.collect_seq()?;

    // Verify
    assert_eq!(results.len(), 5_000);
    #[allow(clippy::cast_possible_truncation)]
    for (i, record) in results.iter().enumerate() {
        assert_eq!(record.id, (i * 2) as u32);
    }

    Ok(())
}

// ── Schema-variant functions (previously uncovered) ───────────────────────────

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_vec_with_schema() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("schema.avro");

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".into(),
            value: 1.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".into(),
            value: 2.0,
        },
    ];
    write_avro_vec(&path, &data, TEST_RECORD_SCHEMA)?;

    let read: Vec<TestRecord> = read_avro_vec_with_schema(&path, TEST_RECORD_SCHEMA)?;
    assert_eq!(read, data);
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_vec_with_schema_obj() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("schema_obj.avro");

    let data = vec![
        TestRecord {
            id: 3,
            name: "Carol".into(),
            value: 3.0,
        },
        TestRecord {
            id: 4,
            name: "Dave".into(),
            value: 4.0,
        },
    ];
    write_avro_vec(&path, &data, TEST_RECORD_SCHEMA)?;

    let schema = Schema::parse_str(TEST_RECORD_SCHEMA)?;
    let read: Vec<TestRecord> = read_avro_vec_with_schema_obj(&path, &schema)?;
    assert_eq!(read, data);
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_write_avro_vec_with_schema_obj() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("write_schema_obj.avro");

    let data = vec![
        SimpleRecord {
            k: "x".into(),
            v: 10,
        },
        SimpleRecord {
            k: "y".into(),
            v: 20,
        },
    ];
    let schema = Schema::parse_str(SIMPLE_RECORD_SCHEMA)?;
    let n = write_avro_vec_with_schema(&path, &data, &schema)?;
    assert_eq!(n, 2);

    let read: Vec<SimpleRecord> = read_avro_vec(&path)?;
    assert_eq!(read, data);
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_write_avro_vec_with_schema_obj_nested_dir() -> Result<()> {
    let temp_dir = TempDir::new()?;
    // Write to a path whose parent dirs don't exist yet
    let path = temp_dir.path().join("a/b/c/nested.avro");

    let data = vec![SimpleRecord {
        k: "z".into(),
        v: 99,
    }];
    let schema = Schema::parse_str(SIMPLE_RECORD_SCHEMA)?;
    let n = write_avro_vec_with_schema(&path, &data, &schema)?;
    assert_eq!(n, 1);

    let read: Vec<SimpleRecord> = read_avro_vec(&path)?;
    assert_eq!(read, data);
    Ok(())
}

#[cfg(all(feature = "io-avro", feature = "parallel-io"))]
#[test]
fn test_write_avro_par_io_layer() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("par_io.avro");

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".into(),
            value: 1.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".into(),
            value: 2.0,
        },
        TestRecord {
            id: 3,
            name: "Carol".into(),
            value: 3.0,
        },
    ];
    let n = write_avro_par(&path, &data, Some(2), TEST_RECORD_SCHEMA)?;
    assert_eq!(n, 3);

    // Verify the output is a valid, readable Avro file
    let read: Vec<TestRecord> = read_avro_vec(&path)?;
    assert_eq!(read, data);
    Ok(())
}

#[cfg(all(feature = "io-avro", feature = "parallel-io"))]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_write_avro_par_io_layer_large() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("par_large.avro");

    let data: Vec<TestRecord> = (0..500)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("U{i}"),
            value: f64::from(i),
        })
        .collect();
    let n = write_avro_par(&path, &data, Some(4), TEST_RECORD_SCHEMA)?;
    assert_eq!(n, 500);

    let read: Vec<TestRecord> = read_avro_vec(&path)?;
    assert_eq!(read.len(), 500);
    assert_eq!(read[0], data[0]);
    assert_eq!(read[499], data[499]);
    Ok(())
}

#[cfg(all(feature = "io-avro", feature = "parallel-io"))]
#[test]
fn test_write_avro_par_io_layer_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("par_empty.avro");

    let data: Vec<TestRecord> = vec![];
    let n = write_avro_par(&path, &data, None, TEST_RECORD_SCHEMA)?;
    assert_eq!(n, 0);

    // The file should exist and be a valid (empty) Avro file
    let read: Vec<TestRecord> = read_avro_vec(&path)?;
    assert_eq!(read.len(), 0);
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_build_avro_shards_with_schema() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("schema_shards.avro");

    let data: Vec<TestRecord> = (0..50)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("U{i}"),
            value: f64::from(i),
        })
        .collect();
    write_avro_vec(&path, &data, TEST_RECORD_SCHEMA)?;

    let shards = build_avro_shards_with_schema(&path, TEST_RECORD_SCHEMA, 10)?;
    assert_eq!(shards.total_records, 50);
    assert_eq!(shards.ranges.len(), 5);
    assert_eq!(shards.ranges[0], (0, 10));
    assert_eq!(shards.ranges[4], (40, 50));
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_build_avro_shards_with_schema_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("schema_shards_empty.avro");
    write_avro_vec(&path, &Vec::<TestRecord>::new(), TEST_RECORD_SCHEMA)?;

    let shards = build_avro_shards_with_schema(&path, TEST_RECORD_SCHEMA, 10)?;
    assert_eq!(shards.total_records, 0);
    assert!(shards.ranges.is_empty());
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_build_avro_shards_with_schema_obj() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("schema_obj_shards.avro");

    let data: Vec<TestRecord> = (0..30)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("U{i}"),
            value: f64::from(i),
        })
        .collect();
    write_avro_vec(&path, &data, TEST_RECORD_SCHEMA)?;

    let schema = Schema::parse_str(TEST_RECORD_SCHEMA)?;
    let shards = build_avro_shards_with_schema_obj(&path, &schema, 10)?;
    assert_eq!(shards.total_records, 30);
    assert_eq!(shards.ranges.len(), 3);
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_build_avro_shards_with_schema_obj_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("schema_obj_shards_empty.avro");
    write_avro_vec(&path, &Vec::<TestRecord>::new(), TEST_RECORD_SCHEMA)?;

    let schema = Schema::parse_str(TEST_RECORD_SCHEMA)?;
    let shards = build_avro_shards_with_schema_obj(&path, &schema, 10)?;
    assert_eq!(shards.total_records, 0);
    assert!(shards.ranges.is_empty());
    Ok(())
}

// ── Partial-coverage gaps ─────────────────────────────────────────────────────

#[cfg(feature = "io-avro")]
#[test]
fn test_write_avro_vec_nested_dir() -> Result<()> {
    // Exercises the parent-directory creation branch in write_avro_vec
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("nested/deep/out.avro");

    let data = vec![SimpleRecord {
        k: "a".into(),
        v: 1,
    }];
    let n = write_avro_vec(&path, &data, SIMPLE_RECORD_SCHEMA)?;
    assert_eq!(n, 1);

    let read: Vec<SimpleRecord> = read_avro_vec(&path)?;
    assert_eq!(read, data);
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_read_avro_range_hits_end_break() -> Result<()> {
    // Exercises the `i >= end` break path in read_avro_range
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("range_break.avro");

    let data: Vec<TestRecord> = (0..20)
        .map(|i| TestRecord {
            id: i as u32,
            name: format!("U{i}"),
            value: f64::from(i),
        })
        .collect();
    write_avro_vec(&path, &data, TEST_RECORD_SCHEMA)?;

    let shards = build_avro_shards(&path, 20)?;
    // Read only records [5, 10) — the loop must break at i=10
    let range: Vec<TestRecord> = read_avro_range(&shards, 5, 10)?;
    assert_eq!(range.len(), 5);
    assert_eq!(range[0].id, 5);
    assert_eq!(range[4].id, 9);
    Ok(())
}

// ── Glob support in read_avro (helpers layer) ─────────────────────────────────

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_glob_multiple_files() -> Result<()> {
    // Verifies that read_avro with a glob pattern reads all matching files and
    // concatenates them in sorted (lexicographic) order.
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;

    let file_a = temp_dir.path().join("part-00000.avro");
    let file_b = temp_dir.path().join("part-00001.avro");
    let file_c = temp_dir.path().join("part-00002.avro");

    // Write three separate files in known order
    write_avro_vec(&file_a, &[SimpleRecord { k: "a".into(), v: 1 }], SIMPLE_RECORD_SCHEMA)?;
    write_avro_vec(&file_b, &[SimpleRecord { k: "b".into(), v: 2 }], SIMPLE_RECORD_SCHEMA)?;
    write_avro_vec(&file_c, &[SimpleRecord { k: "c".into(), v: 3 }], SIMPLE_RECORD_SCHEMA)?;

    let glob = format!("{}/*.avro", temp_dir.path().display());
    let pc: PCollection<SimpleRecord> = read_avro(&p, &glob)?;
    let mut results = pc.collect_seq()?;
    results.sort_by_key(|r: &SimpleRecord| r.v);

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], SimpleRecord { k: "a".into(), v: 1 });
    assert_eq!(results[1], SimpleRecord { k: "b".into(), v: 2 });
    assert_eq!(results[2], SimpleRecord { k: "c".into(), v: 3 });
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_glob_single_match() -> Result<()> {
    // A glob that matches exactly one file should behave like a direct path read.
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;

    let file = temp_dir.path().join("only.avro");
    write_avro_vec(
        &file,
        &[
            TestRecord { id: 1, name: "Alice".into(), value: 1.0 },
            TestRecord { id: 2, name: "Bob".into(), value: 2.0 },
        ],
        TEST_RECORD_SCHEMA,
    )?;

    let glob = format!("{}/*.avro", temp_dir.path().display());
    let pc: PCollection<TestRecord> = read_avro(&p, &glob)?;
    let results = pc.collect_seq()?;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, 1);
    assert_eq!(results[1].id, 2);
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_glob_no_match_is_error() -> Result<()> {
    // A glob that matches no files should return an error.
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;

    let glob = format!("{}/nonexistent/*.avro", temp_dir.path().display());
    let result = read_avro::<SimpleRecord>(&p, &glob);
    assert!(result.is_err());
    Ok(())
}

#[cfg(feature = "io-avro")]
#[test]
fn test_read_avro_glob_concatenation_order() -> Result<()> {
    // Files are concatenated in sorted lexicographic order; the test relies on
    // names that sort differently than creation order to verify this.
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;

    // Write files in reverse order so creation order ≠ sort order
    write_avro_vec(
        &temp_dir.path().join("c.avro"),
        &[SimpleRecord { k: "c".into(), v: 3 }],
        SIMPLE_RECORD_SCHEMA,
    )?;
    write_avro_vec(
        &temp_dir.path().join("a.avro"),
        &[SimpleRecord { k: "a".into(), v: 1 }],
        SIMPLE_RECORD_SCHEMA,
    )?;
    write_avro_vec(
        &temp_dir.path().join("b.avro"),
        &[SimpleRecord { k: "b".into(), v: 2 }],
        SIMPLE_RECORD_SCHEMA,
    )?;

    let glob = format!("{}/*.avro", temp_dir.path().display());
    let pc: PCollection<SimpleRecord> = read_avro(&p, &glob)?;
    let results = pc.collect_seq()?;

    // Sorted order: a.avro → b.avro → c.avro
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].k, "a");
    assert_eq!(results[1].k, "b");
    assert_eq!(results[2].k, "c");
    Ok(())
}
