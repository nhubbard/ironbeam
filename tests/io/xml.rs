//! Tests for XML I/O functionality.
//!
//! These tests verify:
//! - Vector read/write operations
//! - Streaming read operations (single-shard)
//! - Parallel write operations
//! - Glob pattern support
//! - Compression support
//! - Error handling

use anyhow::Result;
use ironbeam::io::xml::{XmlVecOps, build_xml_shards, read_xml_range, read_xml_vec, write_xml_vec};
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

// ── Vector I/O ────────────────────────────────────────────────────────────────

#[cfg(feature = "io-xml")]
#[test]
fn test_read_write_xml_vec() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("data.xml");

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

    let written = write_xml_vec(&path, &data)?;
    assert_eq!(written, 3);

    let read: Vec<TestRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 3);
    assert_eq!(read[0], data[0]);
    assert_eq!(read[1], data[1]);
    assert_eq!(read[2], data[2]);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_write_xml_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("empty.xml");

    let data: Vec<TestRecord> = vec![];
    let written = write_xml_vec(&path, &data)?;
    assert_eq!(written, 0);

    let read: Vec<TestRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 0);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_write_xml_simple_record() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("simple.xml");

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

    let written = write_xml_vec(&path, &data)?;
    assert_eq!(written, 3);

    let read: Vec<SimpleRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 3);
    assert_eq!(read[0], data[0]);
    assert_eq!(read[1], data[1]);
    assert_eq!(read[2], data[2]);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_write_xml_creates_nested_dirs() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("a/b/c/nested.xml");

    let data = vec![SimpleRecord {
        k: "x".to_string(),
        v: 99,
    }];
    let n = write_xml_vec(&path, &data)?;
    assert_eq!(n, 1);

    let read: Vec<SimpleRecord> = read_xml_vec(&path)?;
    assert_eq!(read, data);

    Ok(())
}

// ── Pipeline integration ───────────────────────────────────────────────────────

#[cfg(feature = "io-xml")]
#[test]
fn test_xml_pipeline_write() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("pipeline.xml");

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
    let written = pc.write_xml(&path)?;
    assert_eq!(written, 2);

    let read: Vec<TestRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 2);
    assert_eq!(read[0], data[0]);
    assert_eq!(read[1], data[1]);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_xml_pipeline_transform_write() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("transformed.xml");

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
    let doubled = pc.map(|r: &TestRecord| TestRecord {
        id: r.id,
        name: r.name.clone(),
        value: r.value * 2.0,
    });

    let written = doubled.write_xml(&path)?;
    assert_eq!(written, 2);

    let read: Vec<TestRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 2);
    assert_approx_eq!(read[0].value, 200.0);
    assert_approx_eq!(read[1].value, 400.0);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_xml_roundtrip_read_transform_write() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;
    let input = temp_dir.path().join("in.xml");
    let output = temp_dir.path().join("out.xml");

    let original = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 10.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 20.0,
        },
        TestRecord {
            id: 3,
            name: "Carol".to_string(),
            value: 30.0,
        },
    ];
    write_xml_vec(&input, &original)?;

    let pc: PCollection<TestRecord> = read_xml(&p, &input)?;
    pc.write_xml(&output)?;

    let result: Vec<TestRecord> = read_xml_vec(&output)?;
    assert_eq!(result.len(), original.len());
    for (a, b) in original.iter().zip(result.iter()) {
        assert_eq!(a.id, b.id);
        assert_eq!(a.name, b.name);
        assert_approx_eq!(a.value, b.value);
    }

    Ok(())
}

// ── Parallel write ─────────────────────────────────────────────────────────────

#[cfg(feature = "io-xml")]
#[test]
fn test_xml_parallel_write() -> Result<()> {
    #[cfg(feature = "parallel-io")]
    {
        use ironbeam::io::xml::write_xml_par;

        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().join("par.xml");

        let data = vec![
            TestRecord {
                id: 1,
                name: "Alice".to_string(),
                value: 1.0,
            },
            TestRecord {
                id: 2,
                name: "Bob".to_string(),
                value: 2.0,
            },
            TestRecord {
                id: 3,
                name: "Charlie".to_string(),
                value: 3.0,
            },
        ];

        let written = write_xml_par(&path, &data, Some(2))?;
        assert_eq!(written, 3);

        let read: Vec<TestRecord> = read_xml_vec(&path)?;
        assert_eq!(read.len(), 3);
        assert_eq!(read[0], data[0]);
        assert_eq!(read[1], data[1]);
        assert_eq!(read[2], data[2]);
    }
    Ok(())
}

#[cfg(all(feature = "io-xml", feature = "parallel-io"))]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_xml_parallel_write_large() -> Result<()> {
    use ironbeam::io::xml::write_xml_par;

    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("par_large.xml");

    let data: Vec<SimpleRecord> = (0..500)
        .map(|i| SimpleRecord {
            k: format!("key-{i:04}"),
            v: i,
        })
        .collect();

    let n = write_xml_par(&path, &data, Some(4))?;
    assert_eq!(n, 500);

    let read: Vec<SimpleRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 500);
    assert_eq!(read[0], data[0]);
    assert_eq!(read[499], data[499]);

    Ok(())
}

#[cfg(all(feature = "io-xml", feature = "parallel-io"))]
#[test]
fn test_xml_parallel_write_empty() -> Result<()> {
    use ironbeam::io::xml::write_xml_par;

    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("par_empty.xml");

    let data: Vec<SimpleRecord> = vec![];
    let n = write_xml_par(&path, &data, None)?;
    assert_eq!(n, 0);

    let read: Vec<SimpleRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 0);

    Ok(())
}

#[cfg(all(feature = "io-xml", feature = "parallel-io"))]
#[test]
fn test_xml_pipeline_write_par() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("pipeline_par.xml");

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 1.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 2.0,
        },
    ];

    let pc: PCollection<TestRecord> = from_vec(&p, data.clone());
    let written = pc.write_xml_par(&path, Some(2))?;
    assert_eq!(written, 2);

    let read: Vec<TestRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 2);
    assert_eq!(read[0], data[0]);
    assert_eq!(read[1], data[1]);

    Ok(())
}

/// Sequential and parallel writers must produce files that read identically.
#[cfg(all(feature = "io-xml", feature = "parallel-io"))]
#[test]
fn test_xml_par_matches_seq() -> Result<()> {
    use ironbeam::io::xml::write_xml_par;

    let temp_dir = TempDir::new()?;
    let seq_path = temp_dir.path().join("seq.xml");
    let par_path = temp_dir.path().join("par.xml");

    let data: Vec<SimpleRecord> = (0..20)
        .map(|i| SimpleRecord {
            k: format!("k{i}"),
            v: i,
        })
        .collect();

    write_xml_vec(&seq_path, &data)?;
    write_xml_par(&par_path, &data, Some(4))?;

    let seq_read: Vec<SimpleRecord> = read_xml_vec(&seq_path)?;
    let par_read: Vec<SimpleRecord> = read_xml_vec(&par_path)?;

    assert_eq!(seq_read, par_read);

    Ok(())
}

// ── Streaming ─────────────────────────────────────────────────────────────────

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_streaming() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("stream.xml");

    let data = vec![
        TestRecord {
            id: 1,
            name: "Alice".to_string(),
            value: 10.0,
        },
        TestRecord {
            id: 2,
            name: "Bob".to_string(),
            value: 20.0,
        },
        TestRecord {
            id: 3,
            name: "Carol".to_string(),
            value: 30.0,
        },
    ];
    write_xml_vec(&path, &data)?;

    let stream: PCollection<TestRecord> = read_xml_streaming(&p, &path, 100)?;
    let result = stream.collect_seq()?;

    assert_eq!(result.len(), 3);
    assert_eq!(result[0], data[0]);
    assert_eq!(result[2], data[2]);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
#[allow(clippy::cast_sign_loss)]
fn test_read_xml_streaming_large() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("large.xml");

    let data: Vec<SimpleRecord> = (0..1000)
        .map(|i| SimpleRecord {
            k: format!("key-{i:04}"),
            v: i,
        })
        .collect();
    write_xml_vec(&path, &data)?;

    let stream: PCollection<SimpleRecord> = read_xml_streaming(&p, &path, 100_000)?;
    let result = stream.collect_seq()?;

    assert_eq!(result.len(), 1000);
    assert_eq!(result[0].v, 0);
    assert_eq!(result[999].v, 999);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_streaming_empty() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("empty_stream.xml");

    write_xml_vec(&path, &Vec::<SimpleRecord>::new())?;

    let stream: PCollection<SimpleRecord> = read_xml_streaming(&p, &path, 10)?;
    let result = stream.collect_seq()?;
    assert_eq!(result.len(), 0);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_streaming_pipeline_transform() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("stream_transform.xml");

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
        SimpleRecord {
            k: "d".to_string(),
            v: 4,
        },
        SimpleRecord {
            k: "e".to_string(),
            v: 5,
        },
    ];
    write_xml_vec(&path, &data)?;

    let stream: PCollection<SimpleRecord> = read_xml_streaming(&p, &path, 100)?;
    let filtered = stream.filter(|r: &SimpleRecord| r.v % 2 == 1);
    let result = filtered.collect_seq()?;

    assert_eq!(result.len(), 3);
    assert!(result.iter().all(|r| r.v % 2 == 1));

    Ok(())
}

// ── Shards / VecOps ───────────────────────────────────────────────────────────

#[cfg(feature = "io-xml")]
#[test]
fn test_build_xml_shards() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("shards.xml");

    let data: Vec<SimpleRecord> = (0..10)
        .map(|i| SimpleRecord {
            k: format!("k{i}"),
            v: i,
        })
        .collect();
    write_xml_vec(&path, &data)?;

    // XML always produces at most one shard regardless of records_per_shard.
    let shards = build_xml_shards(&path, 3)?;
    assert_eq!(shards.total_records, 10);
    assert_eq!(shards.ranges.len(), 1);
    assert_eq!(shards.ranges[0], (0, 10));

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_build_xml_shards_empty() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("empty_shards.xml");

    write_xml_vec(&path, &Vec::<SimpleRecord>::new())?;

    let shards = build_xml_shards(&path, 10)?;
    assert_eq!(shards.total_records, 0);
    assert!(shards.ranges.is_empty());

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_xml_vec_ops_len() -> Result<()> {
    use anyhow::anyhow;

    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("vecops.xml");

    let data: Vec<SimpleRecord> = (0..7)
        .map(|i| SimpleRecord {
            k: format!("k{i}"),
            v: i,
        })
        .collect();
    write_xml_vec(&path, &data)?;

    let shards = build_xml_shards(&path, 10)?;
    let ops = XmlVecOps::<SimpleRecord>::new();
    let len = ops.len(&shards).ok_or(anyhow!("len failed"))?;
    assert_eq!(len, 7);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_xml_vec_ops_split() -> Result<()> {
    use anyhow::anyhow;

    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("split.xml");

    let data: Vec<SimpleRecord> = (0..5)
        .map(|i| SimpleRecord {
            k: format!("k{i}"),
            v: i,
        })
        .collect();
    write_xml_vec(&path, &data)?;

    let shards = build_xml_shards(&path, 10)?;
    let ops = XmlVecOps::<SimpleRecord>::new();
    let parts = ops.split(&shards, 4).ok_or(anyhow!("split failed"))?;
    // XML always produces exactly one shard.
    assert_eq!(parts.len(), 1);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_range() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("range.xml");

    let data: Vec<SimpleRecord> = (0..10)
        .map(|i| SimpleRecord {
            k: format!("k{i}"),
            v: i,
        })
        .collect();
    write_xml_vec(&path, &data)?;

    let shards = build_xml_shards(&path, 10)?;
    let range: Vec<SimpleRecord> = read_xml_range(&shards, 3, 7)?;
    assert_eq!(range.len(), 4);
    assert_eq!(range[0].v, 3);
    assert_eq!(range[3].v, 6);

    Ok(())
}

// ── Glob support ──────────────────────────────────────────────────────────────

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_glob_multiple_files() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;

    let file_a = temp_dir.path().join("part-00000.xml");
    let file_b = temp_dir.path().join("part-00001.xml");
    let file_c = temp_dir.path().join("part-00002.xml");

    write_xml_vec(
        &file_a,
        &[SimpleRecord {
            k: "a".into(),
            v: 1,
        }],
    )?;
    write_xml_vec(
        &file_b,
        &[SimpleRecord {
            k: "b".into(),
            v: 2,
        }],
    )?;
    write_xml_vec(
        &file_c,
        &[SimpleRecord {
            k: "c".into(),
            v: 3,
        }],
    )?;

    let glob = format!("{}/*.xml", temp_dir.path().display());
    let pc: PCollection<SimpleRecord> = read_xml(&p, &glob)?;
    let mut results = pc.collect_seq()?;
    results.sort_by_key(|r: &SimpleRecord| r.v);

    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0],
        SimpleRecord {
            k: "a".into(),
            v: 1
        }
    );
    assert_eq!(
        results[1],
        SimpleRecord {
            k: "b".into(),
            v: 2
        }
    );
    assert_eq!(
        results[2],
        SimpleRecord {
            k: "c".into(),
            v: 3
        }
    );

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_glob_single_match() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;

    let file = temp_dir.path().join("only.xml");
    write_xml_vec(
        &file,
        &[
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
        ],
    )?;

    let glob = format!("{}/*.xml", temp_dir.path().display());
    let pc: PCollection<TestRecord> = read_xml(&p, &glob)?;
    let results = pc.collect_seq()?;

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, 1);
    assert_eq!(results[1].id, 2);

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_glob_no_match_is_error() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;

    let glob = format!("{}/nonexistent/*.xml", temp_dir.path().display());
    let result = read_xml::<SimpleRecord>(&p, &glob);
    assert!(result.is_err());

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_glob_concatenation_order() -> Result<()> {
    let p = Pipeline::default();
    let temp_dir = TempDir::new()?;

    // Write in reverse order to verify lexicographic sorting wins.
    write_xml_vec(
        temp_dir.path().join("c.xml"),
        &[SimpleRecord {
            k: "c".into(),
            v: 3,
        }],
    )?;
    write_xml_vec(
        temp_dir.path().join("a.xml"),
        &[SimpleRecord {
            k: "a".into(),
            v: 1,
        }],
    )?;
    write_xml_vec(
        temp_dir.path().join("b.xml"),
        &[SimpleRecord {
            k: "b".into(),
            v: 2,
        }],
    )?;

    let glob = format!("{}/*.xml", temp_dir.path().display());
    let pc: PCollection<SimpleRecord> = read_xml(&p, &glob)?;
    let results = pc.collect_seq()?;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].k, "a");
    assert_eq!(results[1].k, "b");
    assert_eq!(results[2].k, "c");

    Ok(())
}

// ── Compression ───────────────────────────────────────────────────────────────

#[cfg(all(feature = "io-xml", feature = "compression-gzip"))]
#[test]
fn test_xml_gzip_roundtrip() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("data.xml.gz");

    let data = vec![
        SimpleRecord {
            k: "a".into(),
            v: 1,
        },
        SimpleRecord {
            k: "b".into(),
            v: 2,
        },
    ];

    let written = write_xml_vec(&path, &data)?;
    assert_eq!(written, 2);

    let read: Vec<SimpleRecord> = read_xml_vec(&path)?;
    assert_eq!(read, data);

    Ok(())
}

#[cfg(all(feature = "io-xml", feature = "compression-zstd"))]
#[test]
fn test_xml_zstd_roundtrip() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("data.xml.zst");

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

    let written = write_xml_vec(&path, &data)?;
    assert_eq!(written, 2);

    let read: Vec<SimpleRecord> = read_xml_vec(&path)?;
    assert_eq!(read, data);

    Ok(())
}

// ── Error handling ────────────────────────────────────────────────────────────

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_missing_file_is_error() -> Result<()> {
    let result = read_xml_vec::<SimpleRecord>("/nonexistent/path/to/file.xml");
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("nonexistent") || msg.contains("No such file") || msg.contains("open"));

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_malformed_is_error() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("bad.xml");

    // Write garbage that is not valid XML.
    let mut f = File::create(&path)?;
    f.write_all(b"this is not xml at all <<<<<")?;
    drop(f);

    let result = read_xml_vec::<SimpleRecord>(&path);
    assert!(result.is_err());

    Ok(())
}

#[cfg(feature = "io-xml")]
#[test]
fn test_read_xml_single_record() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let path = temp_dir.path().join("single.xml");

    let data = vec![SimpleRecord {
        k: "only".to_string(),
        v: 42,
    }];
    write_xml_vec(&path, &data)?;

    let read: Vec<SimpleRecord> = read_xml_vec(&path)?;
    assert_eq!(read.len(), 1);
    assert_eq!(read[0], data[0]);

    Ok(())
}
