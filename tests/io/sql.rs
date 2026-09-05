//! Tests for the SQL database I/O connector (feature `io-sql`).
//!
//! All tests use a real SQLite database file in a temp directory (via `?mode=rwc` to
//! auto-create it) rather than `sqlite::memory:` — each low-level function in
//! `crate::io::sql` opens and drops its own connection pool per call, and a bare
//! in-memory SQLite database only lives as long as a single connection stays open, so a
//! file-backed database is the simplest way to let multiple independent calls observe
//! the same data. No Docker or external process is required.

#![cfg(feature = "io-sql")]
#![allow(clippy::assert_is_empty)]

use anyhow::Result;
use ironbeam::io::sql::*;
use ironbeam::testing::*;
use ironbeam::type_token::VecOps;
use ironbeam::{from_vec, read_sql, read_sql_streaming};

#[derive(Clone, Debug, PartialEq, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i64,
    name: String,
    value: f64,
}

#[derive(Clone, Debug, PartialEq, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
struct RowOpt {
    id: i64,
    name: Option<String>,
}

/// Build a file-backed SQLite URL inside `tmp`, auto-creating the database file.
fn db_url(tmp: &tempfile::TempDir) -> String {
    format!("sqlite://{}?mode=rwc", tmp.path().join("test.db").display())
}

/// Create `t (id INTEGER, name TEXT, value REAL)` at `url` via a throwaway connection.
fn create_rows_table(url: &str) -> Result<()> {
    create_table(url, "CREATE TABLE t (id INTEGER, name TEXT, value REAL)")
}

/// Create `t (id INTEGER, name TEXT)` at `url` via a throwaway connection.
fn create_opt_table(url: &str) -> Result<()> {
    create_table(url, "CREATE TABLE t (id INTEGER, name TEXT)")
}

/// Execute arbitrary DDL against `url` using a standalone runtime, independent of the
/// crate's internal `SQL_RUNTIME`.
fn create_table(url: &str, ddl: &str) -> Result<()> {
    sqlx::any::install_default_drivers();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let pool = sqlx::AnyPool::connect(url).await?;
        sqlx::query(sqlx::AssertSqlSafe(ddl.to_owned()))
            .execute(&pool)
            .await?;
        Ok::<(), sqlx::Error>(())
    })?;
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
fn sample_rows(n: i64) -> Vec<Row> {
    (0..n)
        .map(|i| Row {
            id: i,
            name: format!("row-{i}"),
            value: f64::from(i as i32) * 1.5,
        })
        .collect()
}

fn bind_row(mut sep: sqlx::query_builder::Separated<'_, sqlx::Any, &'static str>, r: &Row) {
    sep.push_bind(r.id)
        .push_bind(r.name.clone())
        .push_bind(r.value);
}

fn bind_row_opt(mut sep: sqlx::query_builder::Separated<'_, sqlx::Any, &'static str>, r: &RowOpt) {
    sep.push_bind(r.id).push_bind(r.name.clone());
}

// ── Low-level module ────────────────────────────────────────────────────────────

#[test]
fn read_sql_vec_basic() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(3);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;

    let back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t ORDER BY id")?;
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn read_sql_vec_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;

    let back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t")?;
    assert!(back.is_empty());
    Ok(())
}

#[test]
fn read_sql_vec_bad_url() {
    let result: Result<Vec<Row>> = read_sql_vec("not-a-real-scheme://nope", "SELECT 1");
    assert!(result.is_err());
}

#[test]
fn write_sql_with_basic() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(4);

    let n = write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;
    assert_eq!(n, 4);
    Ok(())
}

#[test]
fn write_sql_with_empty() -> Result<()> {
    let n = write_sql_with(
        "sqlite::memory:",
        "INSERT INTO t (id, name, value)",
        &Vec::<Row>::new(),
        bind_row,
    )?;
    assert_eq!(n, 0);
    Ok(())
}

#[test]
fn build_sql_shards_basic() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(10);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;

    let shards = build_sql_shards(&url, "SELECT id, name, value FROM t", 4)?;
    assert_eq!(shards.total_rows, 10);
    assert_eq!(shards.ranges, vec![(0, 4), (4, 4), (8, 2)]);
    Ok(())
}

#[test]
fn build_sql_shards_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;

    let shards = build_sql_shards(&url, "SELECT id, name, value FROM t", 4)?;
    assert_eq!(shards.total_rows, 0);
    assert!(shards.ranges.is_empty());
    Ok(())
}

#[test]
fn read_sql_range_basic() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(6);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;
    let shards = build_sql_shards(&url, "SELECT id, name, value FROM t ORDER BY id", 2)?;

    let mid: Vec<Row> = read_sql_range(&shards, 2, 2)?;
    assert_eq!(mid, data[2..4].to_vec());
    Ok(())
}

#[test]
fn read_sql_range_zero_limit() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(4);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;
    let shards = build_sql_shards(&url, "SELECT id, name, value FROM t ORDER BY id", 2)?;

    let empty: Vec<Row> = read_sql_range(&shards, 0, 0)?;
    assert!(empty.is_empty());
    Ok(())
}

#[test]
fn read_sql_range_beyond_end() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(6);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;
    let shards = build_sql_shards(&url, "SELECT id, name, value FROM t ORDER BY id", 2)?;

    let tail: Vec<Row> = read_sql_range(&shards, 4, 100)?;
    assert_eq!(tail, data[4..6].to_vec());
    Ok(())
}

// ── VecOps adapter ──────────────────────────────────────────────────────────────

#[test]
fn sql_vec_ops_len() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(7);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;
    let shards = build_sql_shards(&url, "SELECT id, name, value FROM t", 3)?;

    let vec_ops = SqlVecOps::<Row>::new();
    let len = vec_ops
        .len(&shards)
        .ok_or_else(|| anyhow::anyhow!("len failed"))?;
    assert_eq!(len, 7);
    Ok(())
}

#[test]
fn sql_vec_ops_split() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(7);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;
    let shards = build_sql_shards(&url, "SELECT id, name, value FROM t ORDER BY id", 3)?;

    let vec_ops = SqlVecOps::<Row>::new();
    let parts = vec_ops
        .split(&shards, 4)
        .ok_or_else(|| anyhow::anyhow!("split failed"))?;
    assert_eq!(parts.len(), shards.ranges.len());

    let total: usize = parts
        .iter()
        .map(|p| p.downcast_ref::<Vec<Row>>().unwrap().len())
        .sum();
    assert_eq!(total, 7);
    Ok(())
}

#[test]
fn sql_vec_ops_clone_any() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(7);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;
    let shards = build_sql_shards(&url, "SELECT id, name, value FROM t ORDER BY id", 3)?;

    let vec_ops = SqlVecOps::<Row>::new();
    let cloned = vec_ops
        .clone_any(&shards)
        .ok_or_else(|| anyhow::anyhow!("clone_any failed"))?;
    let cloned_data: Vec<Row> = *cloned.downcast::<Vec<Row>>().unwrap();
    assert_eq!(cloned_data, data);
    Ok(())
}

#[test]
fn sql_vec_ops_wrong_type() {
    let vec_ops = SqlVecOps::<Row>::new();
    let wrong: i32 = 42;
    assert!(vec_ops.len(&wrong).is_none());
    assert!(vec_ops.split(&wrong, 4).is_none());
    assert!(vec_ops.clone_any(&wrong).is_none());
}

// ── Parallel writer (feature `parallel-io`) ──────────────────────────────────────

#[cfg(feature = "parallel-io")]
#[test]
fn write_sql_par_empty() -> Result<()> {
    let n = write_sql_par_with(
        "sqlite::memory:",
        "INSERT INTO t (id, name, value)",
        &Vec::<Row>::new(),
        &bind_row,
        Some(4),
    )?;
    assert_eq!(n, 0);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_sql_par_basic() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(20);

    let n = write_sql_par_with(
        &url,
        "INSERT INTO t (id, name, value)",
        &data,
        &bind_row,
        Some(4),
    )?;
    assert_eq!(n, 20);

    let mut back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t")?;
    back.sort_by_key(|r| r.id);
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_sql_par_single_shard() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(5);

    let n = write_sql_par_with(
        &url,
        "INSERT INTO t (id, name, value)",
        &data,
        &bind_row,
        Some(1),
    )?;
    assert_eq!(n, 5);

    let mut back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t")?;
    back.sort_by_key(|r| r.id);
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_sql_par_more_shards_than_rows() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(3);

    let n = write_sql_par_with(
        &url,
        "INSERT INTO t (id, name, value)",
        &data,
        &bind_row,
        Some(10),
    )?;
    assert_eq!(n, 3);

    let mut back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t")?;
    back.sort_by_key(|r| r.id);
    assert_eq!(back, data);
    Ok(())
}

// ── Helpers layer ─────────────────────────────────────────────────────────────

#[test]
fn read_sql_helper_basic() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(3);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;

    let p = TestPipeline::new();
    let pc = read_sql::<Row>(&p, &url, "SELECT id, name, value FROM t ORDER BY id")?;
    assert_eq!(pc.collect_seq()?, data);
    Ok(())
}

#[test]
fn read_sql_streaming_helper_basic() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(5);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;

    let p = TestPipeline::new();
    let pc = read_sql_streaming::<Row>(&p, &url, "SELECT id, name, value FROM t ORDER BY id", 2)?;
    assert_eq!(pc.collect_seq()?, data);
    Ok(())
}

#[test]
fn write_sql_with_helper() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(4);

    let p = TestPipeline::new();
    let pc = from_vec(&p, data.clone());
    let n = pc.write_sql_with(&url, "INSERT INTO t (id, name, value)", bind_row)?;
    assert_eq!(n, 4);

    let back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t ORDER BY id")?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_sql_par_with_helper() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(12);

    let p = TestPipeline::new();
    let pc = from_vec(&p, data.clone());
    let n = pc.write_sql_par_with(&url, "INSERT INTO t (id, name, value)", bind_row, Some(3))?;
    assert_eq!(n, 12);

    let mut back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t")?;
    back.sort_by_key(|r| r.id);
    assert_eq!(back, data);
    Ok(())
}

// ── Edge cases ────────────────────────────────────────────────────────────────

#[test]
fn roundtrip_vec_io() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(8);

    let p = TestPipeline::new();
    from_vec(&p, data.clone()).write_sql_with(&url, "INSERT INTO t (id, name, value)", bind_row)?;

    let p2 = TestPipeline::new();
    let back = read_sql::<Row>(&p2, &url, "SELECT id, name, value FROM t ORDER BY id")?;
    assert_eq!(back.collect_seq()?, data);
    Ok(())
}

#[test]
fn roundtrip_streaming() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = sample_rows(9);

    let p = TestPipeline::new();
    from_vec(&p, data.clone()).write_sql_with(&url, "INSERT INTO t (id, name, value)", bind_row)?;

    let p2 = TestPipeline::new();
    let stream =
        read_sql_streaming::<Row>(&p2, &url, "SELECT id, name, value FROM t ORDER BY id", 3)?;
    assert_eq!(stream.collect_seq()?, data);
    Ok(())
}

#[test]
fn unicode_values() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_rows_table(&url)?;
    let data = vec![
        Row {
            id: 1,
            name: "日本語".into(),
            value: 1.0,
        },
        Row {
            id: 2,
            name: "Ünïcödé — emoji 🚀🎉".into(),
            value: 2.0,
        },
        Row {
            id: 3,
            name: "Кириллица".into(),
            value: 3.0,
        },
    ];
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;

    let back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t ORDER BY id")?;
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn null_values() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let url = db_url(&tmp);
    create_opt_table(&url)?;
    let data = vec![
        RowOpt {
            id: 1,
            name: Some("present".into()),
        },
        RowOpt { id: 2, name: None },
    ];
    write_sql_with(&url, "INSERT INTO t (id, name)", &data, bind_row_opt)?;

    let back: Vec<RowOpt> = read_sql_vec(&url, "SELECT id, name FROM t ORDER BY id")?;
    assert_eq!(back, data);
    Ok(())
}
