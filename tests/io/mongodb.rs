//! Integration tests for the MongoDB I/O connector (feature `io-mongodb`), run against a
//! real MongoDB server started via Testcontainers.
//!
//! The default `mongo:5.0.6` image (pulled by `testcontainers_modules::mongo::Mongo`)
//! publishes a multi-arch manifest covering `linux/amd64` and `linux/arm64`, so these tests
//! run natively on both Apple Silicon and `x86_64` CI runners without Rosetta/QEMU emulation.
//!
//! **Requires a working Docker (or Docker-compatible, e.g., Rancher Desktop/Podman) daemon.**
//! If unavailable, container startup fails, and these tests report as failed — this is an
//! accepted limitation for local development without a running daemon, matching the
//! convention used for the SQL Testcontainers suite (see `tests/io/sql_containers.rs`). CI
//! runs these with Docker available as a service.
//!
//! Each test starts its own container, so no test shares state with another. MongoDB has no
//! DDL step: databases and collections are created implicitly on first insert.

#![cfg(feature = "io-mongodb")]

use anyhow::Result;
use ironbeam::io::mongodb::*;
use ironbeam::testing::*;
use ironbeam::type_token::VecOps;
use ironbeam::{from_vec, read_mongodb, read_mongodb_streaming};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::mongo::Mongo;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Doc {
    id: i64,
    name: String,
    value: f64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct DocOpt {
    id: i64,
    name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Address {
    city: String,
    zip: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Person {
    id: i64,
    address: Address,
}

/// Start a MongoDB container (kept alive for the container's lifetime) and return a
/// connection URI. There is no table/collection to pre-create — MongoDB creates both
/// implicitly on first insert.
fn start_mongo() -> Result<(testcontainers::Container<Mongo>, String)> {
    let node = Mongo::default().start()?;
    let uri = format!(
        "mongodb://{}:{}/",
        node.get_host()?,
        node.get_host_port_ipv4(27017)?
    );
    Ok((node, uri))
}

#[allow(clippy::cast_possible_truncation)]
fn sample_docs(n: i64) -> Vec<Doc> {
    (0..n)
        .map(|i| Doc {
            id: i,
            name: format!("doc-{i}"),
            value: f64::from(i as i32) * 1.5,
        })
        .collect()
}

fn sorted_by_id(mut v: Vec<Doc>) -> Vec<Doc> {
    v.sort_by_key(|d| d.id);
    v
}

// ── Low-level module ────────────────────────────────────────────────────────────

#[test]
fn read_mongodb_vec_basic() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(5);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;

    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(back), data);
    Ok(())
}

#[test]
fn read_mongodb_vec_empty() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert!(back.is_empty());
    Ok(())
}

#[test]
fn read_mongodb_vec_with_filter() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(5);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;

    let back: Vec<Doc> =
        read_mongodb_vec(&uri, "testdb", "docs", bson::doc! { "id": { "$gte": 2 } })?;
    assert_eq!(sorted_by_id(back), data[2..].to_vec());
    Ok(())
}

#[test]
fn read_mongodb_vec_bad_uri() {
    let result: Result<Vec<Doc>> =
        read_mongodb_vec("not-a-real-scheme://nope", "testdb", "docs", bson::doc! {});
    assert!(result.is_err());
}

#[test]
fn write_mongodb_vec_basic() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(4);
    let n = write_mongodb_vec(&uri, "testdb", "docs", &data)?;
    assert_eq!(n, 4);
    Ok(())
}

#[test]
fn write_mongodb_vec_empty() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let n = write_mongodb_vec(&uri, "testdb", "docs", &Vec::<Doc>::new())?;
    assert_eq!(n, 0);
    Ok(())
}

#[test]
fn write_mongodb_vec_large_batch() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(100_005);
    let n = write_mongodb_vec(&uri, "testdb", "docs", &data)?;
    assert_eq!(n, 100_005);

    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(back.len(), 100_005);
    Ok(())
}

#[test]
fn build_mongodb_shards_basic() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(10);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;

    let shards = build_mongodb_shards(&uri, "testdb", "docs", bson::doc! {}, 4)?;
    assert_eq!(shards.total_docs, 10);
    assert_eq!(shards.ranges, vec![(0, 4), (4, 4), (8, 2)]);
    Ok(())
}

#[test]
fn build_mongodb_shards_empty() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let shards = build_mongodb_shards(&uri, "testdb", "docs", bson::doc! {}, 4)?;
    assert_eq!(shards.total_docs, 0);
    assert!(shards.ranges.is_empty());
    Ok(())
}

#[test]
fn build_mongodb_shards_with_filter() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(10);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;

    let shards = build_mongodb_shards(
        &uri,
        "testdb",
        "docs",
        bson::doc! { "id": { "$gte": 5 } },
        2,
    )?;
    assert_eq!(shards.total_docs, 5);
    assert_eq!(shards.ranges, vec![(0, 2), (2, 2), (4, 1)]);
    Ok(())
}

#[test]
fn read_mongodb_range_basic() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(6);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;
    let shards = build_mongodb_shards(&uri, "testdb", "docs", bson::doc! {}, 2)?;

    let mid: Vec<Doc> = read_mongodb_range(&shards, 2, 2)?;
    assert_eq!(sorted_by_id(mid), data[2..4].to_vec());
    Ok(())
}

#[test]
fn read_mongodb_range_zero_limit() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(4);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;
    let shards = build_mongodb_shards(&uri, "testdb", "docs", bson::doc! {}, 2)?;

    let empty: Vec<Doc> = read_mongodb_range(&shards, 0, 0)?;
    assert!(empty.is_empty());
    Ok(())
}

#[test]
fn read_mongodb_range_beyond_end() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(6);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;
    let shards = build_mongodb_shards(&uri, "testdb", "docs", bson::doc! {}, 2)?;

    let tail: Vec<Doc> = read_mongodb_range(&shards, 4, 100)?;
    assert_eq!(sorted_by_id(tail), data[4..6].to_vec());
    Ok(())
}

// ── VecOps adapter ──────────────────────────────────────────────────────────────

#[test]
fn mongo_vec_ops_len() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(7);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;
    let shards = build_mongodb_shards(&uri, "testdb", "docs", bson::doc! {}, 3)?;

    let vec_ops = MongoVecOps::<Doc>::new();
    let len = vec_ops
        .len(&shards)
        .ok_or_else(|| anyhow::anyhow!("len failed"))?;
    assert_eq!(len, 7);
    Ok(())
}

#[test]
fn mongo_vec_ops_split() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(7);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;
    let shards = build_mongodb_shards(&uri, "testdb", "docs", bson::doc! {}, 3)?;

    let vec_ops = MongoVecOps::<Doc>::new();
    let parts = vec_ops
        .split(&shards, 4)
        .ok_or_else(|| anyhow::anyhow!("split failed"))?;
    assert_eq!(parts.len(), shards.ranges.len());

    let total: usize = parts
        .iter()
        .map(|p| p.downcast_ref::<Vec<Doc>>().unwrap().len())
        .sum();
    assert_eq!(total, 7);
    Ok(())
}

#[test]
fn mongo_vec_ops_clone_any() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(7);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;
    let shards = build_mongodb_shards(&uri, "testdb", "docs", bson::doc! {}, 3)?;

    let vec_ops = MongoVecOps::<Doc>::new();
    let cloned = vec_ops
        .clone_any(&shards)
        .ok_or_else(|| anyhow::anyhow!("clone_any failed"))?;
    let cloned_data: Vec<Doc> = *cloned.downcast::<Vec<Doc>>().unwrap();
    assert_eq!(sorted_by_id(cloned_data), data);
    Ok(())
}

#[test]
fn mongo_vec_ops_wrong_type() {
    let vec_ops = MongoVecOps::<Doc>::new();
    let wrong: i32 = 42;
    assert!(vec_ops.len(&wrong).is_none());
    assert!(vec_ops.split(&wrong, 4).is_none());
    assert!(vec_ops.clone_any(&wrong).is_none());
}

// ── Parallel writer (feature `parallel-io`) ──────────────────────────────────────

#[cfg(feature = "parallel-io")]
#[test]
fn write_mongodb_par_empty() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let n = write_mongodb_par(&uri, "testdb", "docs", &Vec::<Doc>::new(), Some(4))?;
    assert_eq!(n, 0);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_mongodb_par_basic() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(20);

    let n = write_mongodb_par(&uri, "testdb", "docs", &data, Some(4))?;
    assert_eq!(n, 20);

    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(back), data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_mongodb_par_single_shard() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(5);

    let n = write_mongodb_par(&uri, "testdb", "docs", &data, Some(1))?;
    assert_eq!(n, 5);

    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(back), data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_mongodb_par_over_sharded() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(3);

    let n = write_mongodb_par(&uri, "testdb", "docs", &data, Some(10))?;
    assert_eq!(n, 3);

    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(back), data);
    Ok(())
}

// ── Helpers layer ─────────────────────────────────────────────────────────────

#[test]
fn read_mongodb_helper_basic() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(3);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;

    let p = TestPipeline::new();
    let pc = read_mongodb::<Doc>(&p, &uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(pc.collect_seq()?), data);
    Ok(())
}

#[test]
fn read_mongodb_streaming_helper_basic() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(5);
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;

    let p = TestPipeline::new();
    let pc = read_mongodb_streaming::<Doc>(&p, &uri, "testdb", "docs", bson::doc! {}, 2)?;
    assert_eq!(pc.collect_seq()?, data);
    Ok(())
}

#[test]
fn write_mongodb_helper() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(4);

    let p = TestPipeline::new();
    let pc = from_vec(&p, data.clone());
    let n = pc.write_mongodb(&uri, "testdb", "docs")?;
    assert_eq!(n, 4);

    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(back), data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_mongodb_par_helper() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(12);

    let p = TestPipeline::new();
    let pc = from_vec(&p, data.clone());
    let n = pc.write_mongodb_par(&uri, "testdb", "docs", Some(3))?;
    assert_eq!(n, 12);

    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(back), data);
    Ok(())
}

// ── Edge cases ────────────────────────────────────────────────────────────────

#[test]
fn roundtrip_vec_io() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(8);

    let p = TestPipeline::new();
    from_vec(&p, data.clone()).write_mongodb(&uri, "testdb", "docs")?;

    let p2 = TestPipeline::new();
    let back = read_mongodb::<Doc>(&p2, &uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(back.collect_seq()?), data);
    Ok(())
}

#[test]
fn roundtrip_streaming() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = sample_docs(9);

    let p = TestPipeline::new();
    from_vec(&p, data.clone()).write_mongodb(&uri, "testdb", "docs")?;

    let p2 = TestPipeline::new();
    let stream = read_mongodb_streaming::<Doc>(&p2, &uri, "testdb", "docs", bson::doc! {}, 3)?;
    assert_eq!(stream.collect_seq()?, data);
    Ok(())
}

#[test]
fn unicode_fields() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = vec![
        Doc {
            id: 1,
            name: "日本語".into(),
            value: 1.0,
        },
        Doc {
            id: 2,
            name: "Ünïcödé — emoji 🚀🎉".into(),
            value: 2.0,
        },
        Doc {
            id: 3,
            name: "Кириллица".into(),
            value: 3.0,
        },
    ];
    write_mongodb_vec(&uri, "testdb", "docs", &data)?;

    let back: Vec<Doc> = read_mongodb_vec(&uri, "testdb", "docs", bson::doc! {})?;
    assert_eq!(sorted_by_id(back), data);
    Ok(())
}

#[test]
fn nested_documents() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = vec![
        Person {
            id: 1,
            address: Address {
                city: "Springfield".into(),
                zip: "00000".into(),
            },
        },
        Person {
            id: 2,
            address: Address {
                city: "Shelbyville".into(),
                zip: "11111".into(),
            },
        },
    ];
    write_mongodb_vec(&uri, "testdb", "people", &data)?;

    let mut back: Vec<Person> = read_mongodb_vec(&uri, "testdb", "people", bson::doc! {})?;
    back.sort_by_key(|p| p.id);
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn optional_fields() -> Result<()> {
    let (_node, uri) = start_mongo()?;
    let data = vec![
        DocOpt {
            id: 1,
            name: Some("present".into()),
        },
        DocOpt { id: 2, name: None },
    ];
    write_mongodb_vec(&uri, "testdb", "opt_docs", &data)?;

    let mut back: Vec<DocOpt> = read_mongodb_vec(&uri, "testdb", "opt_docs", bson::doc! {})?;
    back.sort_by_key(|d| d.id);
    assert_eq!(back, data);
    Ok(())
}
