//! Postgres integration tests for the SQL I/O connector (feature `io-sql`), run against
//! a real PostgreSQL server started via Testcontainers.
//!
//! Unlike `tests/io/sql.rs` (SQLite, no external process needed), these tests validate
//! the `sqlx::Any` driver's cross-database dispatch against a real network database.
//!
//! **Requires a working Docker (or Docker-compatible) daemon.** If Docker is
//! unavailable, container startup fails and these tests report as failed — this is an
//! accepted limitation for local development without Docker, matching the convention
//! used for the MongoDB Testcontainers suite (see `tests/io/mongodb.rs`). CI runs these
//! with Docker available as a service.

#![cfg(feature = "io-sql")]

use anyhow::Result;
use ironbeam::io::sql::*;
use ironbeam::testing::*;
use ironbeam::{from_vec, read_sql, read_sql_streaming};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::postgres::Postgres;

#[derive(Clone, Debug, PartialEq, sqlx::FromRow, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i64,
    name: String,
    value: f64,
}

fn bind_row(mut sep: sqlx::query_builder::Separated<'_, sqlx::Any, &'static str>, r: &Row) {
    sep.push_bind(r.id)
        .push_bind(r.name.clone())
        .push_bind(r.value);
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

/// Start a Postgres container and return it (kept alive for the container's lifetime)
/// along with a connection URL, with `t (id, name, value)` already created.
fn start_postgres_with_table() -> Result<(testcontainers::Container<Postgres>, String)> {
    let node = Postgres::default().with_host_auth().start()?;
    let url = format!(
        "postgres://postgres@{}:{}/postgres",
        node.get_host()?,
        node.get_host_port_ipv4(5432)?
    );

    sqlx::any::install_default_drivers();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let pool = sqlx::AnyPool::connect(&url).await?;
        sqlx::query(sqlx::AssertSqlSafe(
            "CREATE TABLE t (id BIGINT, name TEXT, value DOUBLE PRECISION)".to_owned(),
        ))
        .execute(&pool)
        .await?;
        Ok::<(), sqlx::Error>(())
    })?;

    Ok((node, url))
}

#[test]
fn postgres_write_then_read_roundtrip() -> Result<()> {
    let (_node, url) = start_postgres_with_table()?;
    let data = sample_rows(5);

    let n = write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;
    assert_eq!(n, 5);

    let back: Vec<Row> = read_sql_vec(&url, "SELECT id, name, value FROM t ORDER BY id")?;
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn postgres_streaming_helper_roundtrip() -> Result<()> {
    let (_node, url) = start_postgres_with_table()?;
    let data = sample_rows(7);
    write_sql_with(&url, "INSERT INTO t (id, name, value)", &data, bind_row)?;

    let p = TestPipeline::new();
    let stream =
        read_sql_streaming::<Row>(&p, &url, "SELECT id, name, value FROM t ORDER BY id", 3)?;
    assert_eq!(stream.collect_seq()?, data);
    Ok(())
}

#[test]
fn postgres_read_sql_helper_roundtrip() -> Result<()> {
    let (_node, url) = start_postgres_with_table()?;
    let data = sample_rows(4);

    let p = TestPipeline::new();
    from_vec(&p, data.clone()).write_sql_with(&url, "INSERT INTO t (id, name, value)", bind_row)?;

    let p2 = TestPipeline::new();
    let pc = read_sql::<Row>(&p2, &url, "SELECT id, name, value FROM t ORDER BY id")?;
    assert_eq!(pc.collect_seq()?, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn postgres_write_par_roundtrip() -> Result<()> {
    let (_node, url) = start_postgres_with_table()?;
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
