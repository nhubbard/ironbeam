#![cfg(feature = "io-parquet")]

use anyhow::Result;
use ironbeam::from_vec;
use ironbeam::helpers::parquet::read_parquet;
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

// ── read_parquet (eager glob helper) ─────────────────────────────────────────

#[test]
fn read_parquet_single_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("input.parquet");

    let data = vec![
        Row {
            id: 1,
            name: "alice".into(),
            score: Some(9.5),
            tags: vec!["a".into()],
        },
        Row {
            id: 2,
            name: "bob".into(),
            score: None,
            tags: vec![],
        },
    ];
    write_parquet_vec(&path, &data)?;

    let p = TestPipeline::new();
    let col = read_parquet::<Row>(&p, &path)?;
    let out = col.collect_seq()?;
    assert_eq!(out, data);
    Ok(())
}

#[test]
fn read_parquet_glob_concat_sorted() -> Result<()> {
    let tmp = tempfile::tempdir()?;

    let rows_a = vec![
        Row {
            id: 1,
            name: "a".into(),
            score: Some(1.0),
            tags: vec![],
        },
        Row {
            id: 2,
            name: "b".into(),
            score: Some(2.0),
            tags: vec![],
        },
    ];
    let rows_b = vec![Row {
        id: 3,
        name: "c".into(),
        score: Some(3.0),
        tags: vec![],
    }];
    write_parquet_vec(tmp.path().join("part-0.parquet"), &rows_a)?;
    write_parquet_vec(tmp.path().join("part-1.parquet"), &rows_b)?;

    let glob = format!("{}/*.parquet", tmp.path().display());
    let p = TestPipeline::new();
    let col = read_parquet::<Row>(&p, &glob)?;
    let out = col.collect_seq()?;

    // Both files must be included (sorted glob order: part-0 before part-1)
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].id, 1);
    assert_eq!(out[2].id, 3);
    Ok(())
}

#[test]
fn read_parquet_glob_no_match() {
    let tmp = tempfile::tempdir().unwrap();
    let glob = format!("{}/no_match_*.parquet", tmp.path().display());
    let p = TestPipeline::new();
    let result = read_parquet::<Row>(&p, &glob);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("no files found") || msg.contains("pattern"));
}

// ── write_parquet_par ─────────────────────────────────────────────────────────

#[cfg(feature = "parallel-io")]
mod par {
    use super::*;
    use ironbeam::helpers::parquet::read_parquet;

    #[test]
    fn write_parquet_par_empty() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("empty.parquet");

        let p = TestPipeline::new();
        let col = from_vec::<Row>(&p, vec![]);
        let n = col.write_parquet_par(&path, None)?;
        assert_eq!(n, 0);
        assert!(path.exists());

        let back: Vec<Row> = read_parquet_vec(&path)?;
        assert_eq!(back.len(), 0);
        Ok(())
    }

    #[test]
    fn write_parquet_par_single_shard() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("single.parquet");

        let data = vec![
            Row {
                id: 1,
                name: "x".into(),
                score: Some(1.0),
                tags: vec!["t".into()],
            },
            Row {
                id: 2,
                name: "y".into(),
                score: None,
                tags: vec![],
            },
        ];
        let p = TestPipeline::new();
        let col = from_vec(&p, data.clone());
        let n = col.write_parquet_par(&path, Some(1))?;
        assert_eq!(n, 2);

        let back: Vec<Row> = read_parquet_vec(&path)?;
        assert_eq!(back, data);
        Ok(())
    }

    #[test]
    fn write_parquet_par_multi_shard_deterministic() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("multi.parquet");

        let data: Vec<Row> = (0..12)
            .map(|i| Row {
                id: i,
                name: format!("item{i}"),
                score: Some(f64::from(i)),
                tags: vec![],
            })
            .collect();

        let p = TestPipeline::new();
        let col = from_vec(&p, data.clone());
        let n = col.write_parquet_par(&path, Some(4))?;
        assert_eq!(n, 12);

        let back: Vec<Row> = read_parquet_vec(&path)?;
        assert_eq!(back, data, "element order must be preserved across shards");
        Ok(())
    }

    #[test]
    fn write_parquet_par_pipeline_roundtrip() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src.parquet");
        let dst = tmp.path().join("dst.parquet");

        let data: Vec<Row> = (0..6)
            .map(|i| Row {
                id: i,
                name: format!("r{i}"),
                score: None,
                tags: vec![],
            })
            .collect();
        write_parquet_vec(&src, &data)?;

        let p = TestPipeline::new();
        let col = read_parquet::<Row>(&p, &src)?;
        let n = col.write_parquet_par(&dst, Some(3))?;
        assert_eq!(n, 6);

        let back: Vec<Row> = read_parquet_vec(&dst)?;
        assert_eq!(back, data);
        Ok(())
    }

    #[test]
    fn write_parquet_par_mkdir() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("new_dir").join("output.parquet");

        let data = vec![Row {
            id: 1,
            name: "n".into(),
            score: None,
            tags: vec![],
        }];
        let p = TestPipeline::new();
        let col = from_vec(&p, data.clone());
        let n = col.write_parquet_par(&path, None)?;
        assert_eq!(n, 1);
        assert!(path.exists());

        let back: Vec<Row> = read_parquet_vec(&path)?;
        assert_eq!(back, data);
        Ok(())
    }
}
