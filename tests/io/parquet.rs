#![cfg(feature = "io-parquet")]

use anyhow::Result;
use ironbeam::from_vec;
use ironbeam::io::parquet::*;
use ironbeam::testing::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Row {
    id: u32,
    name: String,
    score: Option<f64>,
    tags: Vec<String>,
}

#[test]
fn parquet_roundtrip_typed() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("rows.parquet");

    let data = vec![
        Row {
            id: 1,
            name: "a".into(),
            score: Some(1.5),
            tags: vec!["x".into()],
        },
        Row {
            id: 2,
            name: "b".into(),
            score: None,
            tags: vec!["y".into(), "z".into()],
        },
    ];

    // Write directly
    let n = write_parquet_vec(&path, &data)?;
    assert_eq!(n, 2);

    // Read back
    let back: Vec<Row> = read_parquet_vec(&path)?;
    assert_eq!(back, data);

    // Also via pipeline
    let p = TestPipeline::new();
    let col = from_vec(&p, back);
    let out_path = tmp.path().join("out.parquet");
    let m = col.write_parquet(&out_path)?;
    assert_eq!(m, 2);
    let back2: Vec<Row> = read_parquet_vec(&out_path)?;
    assert_eq!(back2, data);
    Ok(())
}

#[test]
fn write_parquet_vec_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.parquet");
    let data: Vec<Row> = vec![];

    let n = write_parquet_vec(&path, &data)?;
    assert_eq!(n, 0);
    assert!(path.exists());

    // Read back the empty file
    let back: Vec<Row> = read_parquet_vec(&path)?;
    assert_eq!(back.len(), 0);
    Ok(())
}

#[test]
fn build_parquet_shards_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.parquet");
    let data: Vec<Row> = vec![];

    write_parquet_vec(&path, &data)?;

    let shards = build_parquet_shards(&path, 1)?;
    assert_eq!(shards.total_rows, 0);
    assert_eq!(shards.group_ranges.len(), 0);
    Ok(())
}

#[test]
fn build_parquet_shards_multiple_groups() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi_group.parquet");
    let mut data = vec![];
    for i in 0..100 {
        data.push(Row {
            id: i,
            name: format!("name{i}"),
            score: Some(f64::from(i)),
            tags: vec!["tag".into()],
        });
    }

    write_parquet_vec(&path, &data)?;

    let shards = build_parquet_shards(&path, 2)?;
    assert_eq!(shards.total_rows, 100);
    assert!(!shards.group_ranges.is_empty());
    Ok(())
}

#[test]
fn read_parquet_row_group_range_test() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range_test.parquet");
    let mut data = vec![];
    for i in 0..50 {
        data.push(Row {
            id: i,
            name: format!("name{i}"),
            score: Some(f64::from(i)),
            tags: vec!["tag".into()],
        });
    }

    write_parquet_vec(&path, &data)?;

    let shards = build_parquet_shards(&path, 1)?;
    assert!(!shards.group_ranges.is_empty());

    // Read the first group
    let (start, end) = shards.group_ranges[0];
    let subset: Vec<Row> = read_parquet_row_group_range(&shards, start, end)?;
    assert!(!subset.is_empty());
    Ok(())
}

#[test]
fn read_parquet_vec_file_not_found() {
    let result: Result<Vec<Row>> = read_parquet_vec("nonexistent_file.parquet");
    assert!(result.is_err());
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.contains("open") || err_msg.contains("No such file"));
}

#[test]
fn build_parquet_shards_file_not_found() {
    let result = build_parquet_shards("nonexistent_file.parquet", 10);
    assert!(result.is_err());
    if let Err(e) = result {
        let err_msg = format!("{e:?}");
        assert!(err_msg.contains("open") || err_msg.contains("No such file"));
    }
}

#[test]
fn read_parquet_row_group_range_file_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("test.parquet");
    let data = vec![Row {
        id: 1,
        name: "test".into(),
        score: Some(1.0),
        tags: vec!["tag".into()],
    }];
    write_parquet_vec(&path, &data)?;

    let shards = build_parquet_shards(&path, 1)?;

    // Delete the file before reading
    std::fs::remove_file(&path)?;

    let result: Result<Vec<Row>> = read_parquet_row_group_range(&shards, 0, 1);
    assert!(result.is_err());
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.contains("open") || err_msg.contains("No such file"));
    Ok(())
}
