//! Tests for the Arrow IPC I/O connector (feature `io-arrow`).

#![cfg(feature = "io-arrow")]

use anyhow::Result;
use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use ironbeam::io::arrow_ipc::{
    ArrowBatch, ArrowBatchVecOps, ArrowRowVecOps, ArrowShards, build_arrow_shards,
    read_arrow_ipc_range, read_arrow_ipc_rows_range, read_arrow_ipc_rows_vec, read_arrow_ipc_vec,
    write_arrow_ipc_rows_vec, write_arrow_ipc_vec,
};
use ironbeam::testing::*;
use ironbeam::type_token::VecOps;
use ironbeam::{from_vec, read_arrow_ipc, read_arrow_ipc_batches, read_arrow_ipc_streaming};
use serde::{Deserialize, Serialize};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use serde_arrow::to_record_batch;

// ── Test helpers ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Row {
    id: u32,
    name: String,
    score: f64,
}

fn sample_rows(n: u32) -> Vec<Row> {
    (0..n)
        .map(|i| Row {
            id: i,
            name: format!("item{i}"),
            score: f64::from(i) * 1.5,
        })
        .collect()
}

fn make_batch(rows: &[Row]) -> RecordBatch {
    let fields: Vec<FieldRef> =
        Vec::<FieldRef>::from_type::<Row>(TracingOptions::default()).unwrap();
    to_record_batch(&fields, &rows).unwrap()
}

// ── Row-level vector I/O ──────────────────────────────────────────────────────

#[test]
fn write_rows_vec_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("rows.arrow");
    let rows = sample_rows(10);
    let n = write_arrow_ipc_rows_vec(&path, &rows)?;
    assert_eq!(n, 10);
    let back: Vec<Row> = read_arrow_ipc_rows_vec(&path)?;
    assert_eq!(back, rows);
    Ok(())
}

#[test]
fn write_rows_vec_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.arrow");
    let n = write_arrow_ipc_rows_vec(&path, &Vec::<Row>::new())?;
    assert_eq!(n, 0);
    let back: Vec<Row> = read_arrow_ipc_rows_vec(&path)?;
    assert!(back.is_empty());
    Ok(())
}

#[test]
fn write_rows_vec_creates_parent_dir() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("subdir").join("nested.arrow");
    write_arrow_ipc_rows_vec(&path, &sample_rows(5))?;
    assert!(path.exists());
    Ok(())
}

#[test]
fn read_rows_vec_file_not_found() {
    let result = read_arrow_ipc_rows_vec::<Row>("nonexistent_rustflow_arrow.arrow");
    assert!(result.is_err());
}

// ── Batch-level vector I/O ────────────────────────────────────────────────────

#[test]
fn write_ipc_vec_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("batches.arrow");
    let batch = make_batch(&sample_rows(8));
    let n = write_arrow_ipc_vec(&path, &[batch])?;
    assert_eq!(n, 8);
    let back = read_arrow_ipc_vec(&path)?;
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].num_rows(), 8);
    Ok(())
}

#[test]
fn write_ipc_vec_empty_produces_valid_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty_batches.arrow");
    let n = write_arrow_ipc_vec(&path, &[])?;
    assert_eq!(n, 0);
    let back = read_arrow_ipc_vec(&path)?;
    assert!(back.is_empty());
    Ok(())
}

#[test]
fn write_ipc_vec_multi_batch() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi.arrow");
    let batch1 = make_batch(&sample_rows(4));
    let batch2 = make_batch(&sample_rows(3));
    let n = write_arrow_ipc_vec(&path, &[batch1, batch2])?;
    assert_eq!(n, 7);
    let back = read_arrow_ipc_vec(&path)?;
    assert_eq!(back.len(), 2);
    assert_eq!(back[0].num_rows(), 4);
    assert_eq!(back[1].num_rows(), 3);
    Ok(())
}

#[test]
fn read_ipc_vec_file_not_found() {
    let result = read_arrow_ipc_vec("nonexistent_rustflow_arrow.arrow");
    assert!(result.is_err());
}

#[test]
fn write_ipc_vec_creates_parent_dir() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("deep").join("nested.arrow");
    let batch = make_batch(&sample_rows(2));
    write_arrow_ipc_vec(&path, &[batch])?;
    assert!(path.exists());
    Ok(())
}

// ── ArrowBatch serde roundtrip (via serde_json) ───────────────────────────────

#[test]
fn arrow_batch_serde_json_roundtrip() -> Result<()> {
    let rows = sample_rows(5);
    let batch = make_batch(&rows);
    let wrapped = ArrowBatch(batch);

    let json = serde_json::to_string(&wrapped)?;
    let back: ArrowBatch = serde_json::from_str(&json)?;
    assert_eq!(back.0.num_rows(), 5);
    Ok(())
}

// ── Sharding ──────────────────────────────────────────────────────────────────

#[test]
fn build_shards_single_shard() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("file.arrow");
    let batch = make_batch(&sample_rows(10));
    write_arrow_ipc_vec(&path, &[batch])?;

    let shards = build_arrow_shards(&path, 10)?;
    assert_eq!(shards.total_batches, 1);
    assert_eq!(shards.total_rows, 10);
    assert_eq!(shards.ranges, vec![(0, 1)]);
    Ok(())
}

#[test]
fn build_shards_multiple_batches() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi.arrow");
    let b1 = make_batch(&sample_rows(3));
    let b2 = make_batch(&sample_rows(4));
    let b3 = make_batch(&sample_rows(5));
    write_arrow_ipc_vec(&path, &[b1, b2, b3])?;

    // shard every 2 batches
    let shards = build_arrow_shards(&path, 2)?;
    assert_eq!(shards.total_batches, 3);
    assert_eq!(shards.total_rows, 12);
    assert_eq!(shards.ranges, vec![(0, 2), (2, 3)]);
    Ok(())
}

#[test]
fn build_shards_empty_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.arrow");
    write_arrow_ipc_vec(&path, &[])?;

    let shards = build_arrow_shards(&path, 5)?;
    assert_eq!(shards.total_batches, 0);
    assert_eq!(shards.total_rows, 0);
    assert!(shards.ranges.is_empty());
    Ok(())
}

#[test]
fn build_shards_zero_bps_treated_as_one() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("file.arrow");
    let b1 = make_batch(&sample_rows(2));
    let b2 = make_batch(&sample_rows(2));
    write_arrow_ipc_vec(&path, &[b1, b2])?;

    // 0 treated as 1: one batch per shard -> two shards
    let shards = build_arrow_shards(&path, 0)?;
    assert_eq!(shards.ranges.len(), 2);
    Ok(())
}

#[test]
fn read_range_full() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range.arrow");
    let b1 = make_batch(&sample_rows(3));
    let b2 = make_batch(&sample_rows(4));
    write_arrow_ipc_vec(&path, &[b1, b2])?;

    let shards = build_arrow_shards(&path, 10)?;
    let batches = read_arrow_ipc_range(&shards, 0, 2)?;
    assert_eq!(batches.len(), 2);
    Ok(())
}

#[test]
fn read_range_sub() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range.arrow");
    let b1 = make_batch(&sample_rows(3));
    let b2 = make_batch(&sample_rows(4));
    let b3 = make_batch(&sample_rows(5));
    write_arrow_ipc_vec(&path, &[b1, b2, b3])?;

    let shards = build_arrow_shards(&path, 10)?;
    let batches = read_arrow_ipc_range(&shards, 1, 3)?;
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), 4);
    assert_eq!(batches[1].num_rows(), 5);
    Ok(())
}

#[test]
fn read_rows_range() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range.arrow");
    let all_rows = sample_rows(6);
    let b1 = make_batch(&all_rows[..3]);
    let b2 = make_batch(&all_rows[3..]);
    write_arrow_ipc_vec(&path, &[b1, b2])?;

    let shards = build_arrow_shards(&path, 10)?;
    let back: Vec<Row> = read_arrow_ipc_rows_range(&shards, 0, 2)?;
    assert_eq!(back.len(), 6);
    Ok(())
}

// ── VecOps adapters ───────────────────────────────────────────────────────────

#[test]
fn batch_vec_ops_len() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("ops.arrow");
    let b1 = make_batch(&sample_rows(3));
    let b2 = make_batch(&sample_rows(4));
    write_arrow_ipc_vec(&path, &[b1, b2])?;
    let shards = build_arrow_shards(&path, 10)?;

    let ops = ArrowBatchVecOps::new();
    assert_eq!(ops.len(&shards), Some(2)); // 2 batches
    Ok(())
}

#[test]
fn batch_vec_ops_split() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("ops.arrow");
    let b1 = make_batch(&sample_rows(3));
    let b2 = make_batch(&sample_rows(4));
    write_arrow_ipc_vec(&path, &[b1, b2])?;
    let shards = build_arrow_shards(&path, 1)?; // 1 batch per shard -> 2 shards

    let ops = ArrowBatchVecOps::new();
    let parts = ops.split(&shards, 2).expect("split should succeed");
    assert_eq!(parts.len(), 2);
    let p0 = parts[0].downcast_ref::<Vec<ArrowBatch>>().unwrap();
    assert_eq!(p0.len(), 1);
    assert_eq!(p0[0].0.num_rows(), 3);
    Ok(())
}

#[test]
fn batch_vec_ops_clone_any() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("ops.arrow");
    let batch = make_batch(&sample_rows(5));
    write_arrow_ipc_vec(&path, &[batch])?;
    let shards = build_arrow_shards(&path, 10)?;

    let ops = ArrowBatchVecOps::new();
    let cloned = ops.clone_any(&shards).expect("clone_any should succeed");
    let v = cloned.downcast_ref::<Vec<ArrowBatch>>().unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].0.num_rows(), 5);
    Ok(())
}

#[test]
fn row_vec_ops_len() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("ops.arrow");
    let b1 = make_batch(&sample_rows(3));
    let b2 = make_batch(&sample_rows(4));
    write_arrow_ipc_vec(&path, &[b1, b2])?;
    let shards = build_arrow_shards(&path, 10)?;

    let ops = ArrowRowVecOps::<Row>::new();
    assert_eq!(ops.len(&shards), Some(7)); // 3 + 4 rows
    Ok(())
}

#[test]
fn row_vec_ops_split() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("ops.arrow");
    let b1 = make_batch(&sample_rows(3));
    let b2 = make_batch(&sample_rows(4));
    write_arrow_ipc_vec(&path, &[b1, b2])?;
    let shards = build_arrow_shards(&path, 1)?; // 1 batch per shard

    let ops = ArrowRowVecOps::<Row>::new();
    let parts = ops.split(&shards, 2).expect("split should succeed");
    assert_eq!(parts.len(), 2);
    let p0 = parts[0].downcast_ref::<Vec<Row>>().unwrap();
    assert_eq!(p0.len(), 3);
    let p1 = parts[1].downcast_ref::<Vec<Row>>().unwrap();
    assert_eq!(p1.len(), 4);
    Ok(())
}

#[test]
fn row_vec_ops_clone_any() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("ops.arrow");
    let batch = make_batch(&sample_rows(6));
    write_arrow_ipc_vec(&path, &[batch])?;
    let shards = build_arrow_shards(&path, 10)?;

    let ops = ArrowRowVecOps::<Row>::new();
    let cloned = ops.clone_any(&shards).expect("clone_any should succeed");
    let v = cloned.downcast_ref::<Vec<Row>>().unwrap();
    assert_eq!(v.len(), 6);
    Ok(())
}

#[test]
fn vec_ops_empty_shards_len_zero() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.arrow");
    write_arrow_ipc_vec(&path, &[])?;
    let shards = build_arrow_shards(&path, 5)?;

    let batch_ops = ArrowBatchVecOps::new();
    assert_eq!(batch_ops.len(&shards), Some(0));

    let row_ops = ArrowRowVecOps::<Row>::new();
    assert_eq!(row_ops.len(&shards), Some(0));
    Ok(())
}

// ── High-level pipeline helpers ───────────────────────────────────────────────

#[test]
fn pipeline_row_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("pipeline.arrow");
    let rows = sample_rows(5);
    write_arrow_ipc_rows_vec(&path, &rows)?;

    let p = TestPipeline::new();
    let pc = read_arrow_ipc::<Row>(&p, &path)?;
    let back: Vec<Row> = pc.collect_seq()?;
    assert_eq!(back, rows);
    Ok(())
}

#[test]
fn pipeline_batch_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("pipeline_batches.arrow");
    let batch = make_batch(&sample_rows(4));
    write_arrow_ipc_vec(&path, &[batch])?;

    let p = TestPipeline::new();
    let pc = read_arrow_ipc_batches(&p, &path)?;
    let back: Vec<ArrowBatch> = pc.collect_seq()?;
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].0.num_rows(), 4);
    Ok(())
}

#[test]
fn pipeline_write_rows() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("write.arrow");
    let rows = sample_rows(6);
    let p = TestPipeline::new();
    let pc = from_vec(&p, rows.clone());
    let n = pc.write_arrow_ipc_rows(&path)?;
    assert_eq!(n, 6);
    let back: Vec<Row> = read_arrow_ipc_rows_vec(&path)?;
    assert_eq!(back, rows);
    Ok(())
}

#[test]
fn pipeline_write_batches() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("write_batches.arrow");
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![ArrowBatch(make_batch(&sample_rows(3)))]);
    let n = pc.write_arrow_ipc_batches(&path)?;
    assert_eq!(n, 3);
    let back = read_arrow_ipc_vec(&path)?;
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].num_rows(), 3);
    Ok(())
}

#[test]
fn read_arrow_ipc_glob_concat() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path_a = tmp.path().join("a.arrow");
    let path_b = tmp.path().join("b.arrow");
    write_arrow_ipc_rows_vec(&path_a, &sample_rows(3))?;
    write_arrow_ipc_rows_vec(
        &path_b,
        &[Row {
            id: 99,
            name: "z".into(),
            score: 0.0,
        }],
    )?;

    let pattern = format!("{}/*.arrow", tmp.path().display());
    let p = TestPipeline::new();
    let pc = read_arrow_ipc::<Row>(&p, &pattern)?;
    let back: Vec<Row> = pc.collect_seq()?;
    assert_eq!(back.len(), 4); // a.arrow (3) + b.arrow (1)
    Ok(())
}

#[test]
fn read_arrow_ipc_glob_no_match() {
    let p = TestPipeline::new();
    let result = read_arrow_ipc::<Row>(&p, "/tmp/nonexistent_rustflow_arrow_glob_x/*.arrow");
    assert!(result.is_err());
}

#[test]
fn read_arrow_ipc_streaming_collect() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("stream.arrow");
    let b1 = make_batch(&sample_rows(3));
    let b2 = make_batch(&sample_rows(4));
    write_arrow_ipc_vec(&path, &[b1, b2])?;

    let p = TestPipeline::new();
    let pc = read_arrow_ipc_streaming::<Row>(&p, &path, 1)?; // 1 batch per shard
    let back: Vec<Row> = pc.collect_seq()?;
    assert_eq!(back.len(), 7);
    Ok(())
}

#[test]
fn read_arrow_ipc_streaming_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty_stream.arrow");
    write_arrow_ipc_vec(&path, &[])?;

    let p = TestPipeline::new();
    let pc = read_arrow_ipc_streaming::<Row>(&p, &path, 1)?;
    let back: Vec<Row> = pc.collect_seq()?;
    assert!(back.is_empty());
    Ok(())
}

// ── Parallel writing ──────────────────────────────────────────────────────────

#[cfg(feature = "parallel-io")]
mod par {
    use super::*;
    use ironbeam::io::arrow_ipc::{write_arrow_ipc_par, write_arrow_ipc_rows_par};

    #[test]
    fn write_ipc_par_empty() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("par_empty.arrow");
        let n = write_arrow_ipc_par(&path, &[], None)?;
        assert_eq!(n, 0);
        let back = read_arrow_ipc_vec(&path)?;
        assert!(back.is_empty());
        Ok(())
    }

    #[test]
    fn write_ipc_par_single_shard() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("par_single.arrow");
        let batch = make_batch(&sample_rows(5));
        let n = write_arrow_ipc_par(&path, &[batch], Some(1))?;
        assert_eq!(n, 5);
        let back = read_arrow_ipc_vec(&path)?;
        assert_eq!(back[0].num_rows(), 5);
        Ok(())
    }

    #[test]
    fn write_ipc_par_multi_shard_row_count() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("par_multi.arrow");
        let batches: Vec<RecordBatch> = sample_rows(12).chunks(4).map(make_batch).collect();
        let n = write_arrow_ipc_par(&path, &batches, Some(3))?;
        assert_eq!(n, 12);
        let back = read_arrow_ipc_vec(&path)?;
        let total_rows: usize = back.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 12);
        Ok(())
    }

    #[test]
    fn write_ipc_par_creates_parent_dir() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("sub").join("par.arrow");
        let batch = make_batch(&sample_rows(3));
        write_arrow_ipc_par(&path, &[batch], Some(1))?;
        assert!(path.exists());
        Ok(())
    }

    #[test]
    fn write_ipc_rows_par_roundtrip() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("rows_par.arrow");
        let rows = sample_rows(10);
        let n = write_arrow_ipc_rows_par(&path, &rows, Some(3))?;
        assert_eq!(n, 10);
        let back: Vec<Row> = read_arrow_ipc_rows_vec(&path)?;
        assert_eq!(back.len(), 10);
        Ok(())
    }

    #[test]
    fn write_ipc_rows_par_empty() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("rows_par_empty.arrow");
        let n = write_arrow_ipc_rows_par(&path, &Vec::<Row>::new(), None)?;
        assert_eq!(n, 0);
        let back: Vec<Row> = read_arrow_ipc_rows_vec(&path)?;
        assert!(back.is_empty());
        Ok(())
    }

    #[test]
    fn pipeline_write_rows_par() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("pipeline_par.arrow");
        let rows = sample_rows(8);
        let p = TestPipeline::new();
        let pc = from_vec(&p, rows);
        let n = pc.write_arrow_ipc_rows_par(&path, Some(2))?;
        assert_eq!(n, 8);
        let back: Vec<Row> = read_arrow_ipc_rows_vec(&path)?;
        assert_eq!(back.len(), 8);
        Ok(())
    }

    #[test]
    fn pipeline_write_batches_par() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("pipeline_batches_par.arrow");
        let p = TestPipeline::new();
        let pc = from_vec(
            &p,
            vec![
                ArrowBatch(make_batch(&sample_rows(3))),
                ArrowBatch(make_batch(&sample_rows(4))),
            ],
        );
        let n = pc.write_arrow_ipc_batches_par(&path, Some(2))?;
        assert_eq!(n, 7);
        Ok(())
    }
}
