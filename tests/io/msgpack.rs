//! Tests for the `MessagePack` I/O connector (feature `io-msgpack`).

#![cfg(feature = "io-msgpack")]

use anyhow::{Result, anyhow};
use ironbeam::io::msgpack::*;
use ironbeam::testing::*;
use ironbeam::type_token::VecOps;
use ironbeam::{Count, from_vec, read_msgpack, read_msgpack_streaming};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct Rec {
    id: u32,
    word: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Line {
    line: String,
}

fn sample(n: u32) -> Vec<Rec> {
    (0..n)
        .map(|i| Rec {
            id: i,
            word: format!("word{i}"),
        })
        .collect()
}

// ── Vector I/O ───────────────────────────────────────────────────────────────

#[test]
fn write_then_read_vec_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("out.msgpack");
    let data = sample(5);

    let n = write_msgpack_vec(&path, &data)?;
    assert_eq!(n, 5);

    let back: Vec<Rec> = read_msgpack_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn write_vec_empty_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.msgpack");

    let n = write_msgpack_vec(&path, &Vec::<Rec>::new())?;
    assert_eq!(n, 0);
    assert!(path.exists());

    let back: Vec<Rec> = read_msgpack_vec(&path)?;
    assert!(back.is_empty());
    Ok(())
}

#[test]
fn write_vec_creates_parent_dirs() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("sub").join("dir").join("out.msgpack");
    write_msgpack_vec(&path, &sample(2))?;
    assert!(path.exists());
    Ok(())
}

#[test]
fn read_vec_file_not_found() {
    let result: Result<Vec<Rec>> = read_msgpack_vec("definitely_missing.msgpack");
    assert!(result.is_err());
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("open") || msg.contains("No such file"));
}

#[test]
fn read_vec_corrupt_propagates_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("corrupt.msgpack");

    // Valid records followed by the reserved marker 0xc1, which is never a valid
    // MessagePack value and must surface as a decode error (not a clean EOF).
    write_msgpack_vec(&path, &sample(2))?;
    let mut bytes = fs::read(&path)?;
    bytes.push(0xc1);
    fs::write(&path, &bytes)?;

    let result: Result<Vec<Rec>> = read_msgpack_vec(&path);
    assert!(result.is_err());
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("deserialize MessagePack record #3"), "{msg}");
    Ok(())
}

#[cfg(feature = "compression-gzip")]
#[test]
fn write_read_vec_gzip_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("out.msgpack.gz");
    let data = sample(20);

    write_msgpack_vec(&path, &data)?;
    let back: Vec<Rec> = read_msgpack_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

// ── Error-path coverage ────────────────────────────────────────────────────────

#[test]
fn write_vec_mkdir_failure_when_parent_is_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    // `blocker` is a regular file, so treating it as a parent directory must fail.
    let blocker = tmp.path().join("blocker");
    fs::write(&blocker, b"x")?;
    let path = blocker.join("child.msgpack");

    let result = write_msgpack_vec(&path, &sample(1));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("mkdir -p"), "{msg}");
    Ok(())
}

#[test]
fn write_vec_create_failure_when_path_is_dir() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("a_directory");
    fs::create_dir(&dir)?;

    // The parent exists, but creating a file at an existing directory path fails.
    let result = write_msgpack_vec(&dir, &sample(1));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("create"), "{msg}");
    Ok(())
}

#[test]
fn build_shards_file_not_found() {
    let result = build_msgpack_shards("missing_shards.msgpack", 4);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(
        msg.contains("open") || msg.contains("No such file"),
        "{msg}"
    );
}

#[test]
fn read_range_file_not_found() {
    let shards = MsgpackShards {
        path: "missing_range.msgpack".into(),
        ranges: vec![(0, 1)],
        total_records: 1,
    };
    let result: Result<Vec<Rec>> = read_msgpack_range(&shards, 0, 1);
    assert!(result.is_err());
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("open") || msg.contains("No such file"),
        "{msg}"
    );
}

#[test]
fn read_glob_propagates_file_read_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    // One good file and one corrupt file: the glob read must surface the failure
    // with the offending file in context.
    write_msgpack_vec(dir.join("a.msgpack"), &sample(1))?;
    let bad = dir.join("b.msgpack");
    write_msgpack_vec(&bad, &sample(1))?;
    let mut bytes = fs::read(&bad)?;
    bytes.push(0xc1);
    fs::write(&bad, &bytes)?;

    let p = TestPipeline::new();
    let result = read_msgpack::<Rec>(&p, dir.join("*.msgpack"));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(
        msg.contains("reading") && msg.contains("b.msgpack"),
        "{msg}"
    );
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_mkdir_failure_when_parent_is_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let blocker = tmp.path().join("blocker_par");
    fs::write(&blocker, b"x")?;
    let path = blocker.join("child.msgpack");

    let result = write_msgpack_par(&path, &sample(2), Some(2));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("mkdir -p"), "{msg}");
    Ok(())
}

// ── Parallel writer ────────────────────────────────────────────────────────────

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty_par.msgpack");

    let n = write_msgpack_par(&path, &Vec::<Rec>::new(), None)?;
    assert_eq!(n, 0);
    assert!(path.exists());

    let back: Vec<Rec> = read_msgpack_vec(&path)?;
    assert!(back.is_empty());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_single_shard() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("single_par.msgpack");
    let data = sample(3);

    let n = write_msgpack_par(&path, &data, Some(1))?;
    assert_eq!(n, 3);

    let back: Vec<Rec> = read_msgpack_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_multiple_shards() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi_par.msgpack");
    let data = sample(50);

    let n = write_msgpack_par(&path, &data, Some(4))?;
    assert_eq!(n, 50);

    let back: Vec<Rec> = read_msgpack_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_auto_shards() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("auto_par.msgpack");
    let data = sample(100);

    let n = write_msgpack_par(&path, &data, None)?;
    assert_eq!(n, 100);

    let back: Vec<Rec> = read_msgpack_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

// ── Sharding / ranges ──────────────────────────────────────────────────────────

#[test]
fn build_shards_non_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("shards.msgpack");
    write_msgpack_vec(&path, &sample(10))?;

    let shards = build_msgpack_shards(&path, 4)?;
    assert_eq!(shards.total_records, 10);
    assert_eq!(shards.ranges, vec![(0, 4), (4, 8), (8, 10)]);
    Ok(())
}

#[test]
fn build_shards_zero_per_shard_clamps_to_one() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("clamp.msgpack");
    write_msgpack_vec(&path, &sample(3))?;

    // records_per_shard = 0 should clamp to 1 → one range per record.
    let shards = build_msgpack_shards(&path, 0)?;
    assert_eq!(shards.total_records, 3);
    assert_eq!(shards.ranges, vec![(0, 1), (1, 2), (2, 3)]);
    Ok(())
}

#[test]
fn build_shards_empty_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.msgpack");
    fs::write(&path, [])?;

    let shards = build_msgpack_shards(&path, 10)?;
    assert_eq!(shards.total_records, 0);
    assert!(shards.ranges.is_empty());
    Ok(())
}

#[test]
fn build_shards_corrupt_propagates_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("corrupt_count.msgpack");
    write_msgpack_vec(&path, &sample(2))?;
    let mut bytes = fs::read(&path)?;
    bytes.push(0xc1);
    fs::write(&path, &bytes)?;

    let result = build_msgpack_shards(&path, 4);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("count MessagePack record #3"), "{msg}");
    Ok(())
}

#[test]
fn read_range_full_and_subranges() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range.msgpack");
    let data = sample(6);
    write_msgpack_vec(&path, &data)?;

    let shards = build_msgpack_shards(&path, 2)?;

    // Full range.
    let all: Vec<Rec> = read_msgpack_range(&shards, 0, shards.total_records)?;
    assert_eq!(all, data);

    // Interior subrange (start > 0, end < total).
    let mid: Vec<Rec> = read_msgpack_range(&shards, 2, 4)?;
    assert_eq!(mid, data[2..4].to_vec());

    // Empty range: end == 0 returns immediately without decoding.
    let none: Vec<Rec> = read_msgpack_range(&shards, 0, 0)?;
    assert!(none.is_empty());
    Ok(())
}

#[test]
fn read_range_corrupt_propagates_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range_corrupt.msgpack");
    write_msgpack_vec(&path, &sample(2))?;
    let mut bytes = fs::read(&path)?;
    bytes.push(0xc1);
    fs::write(&path, &bytes)?;

    // Build a shard descriptor by hand so the bad record falls inside the range.
    let shards = MsgpackShards {
        path,
        ranges: vec![(0, 3)],
        total_records: 3,
    };
    let result: Result<Vec<Rec>> = read_msgpack_range(&shards, 0, 3);
    assert!(result.is_err());
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("deserialize MessagePack record #3"), "{msg}");
    Ok(())
}

// ── VecOps adapter ───────────────────────────────────────────────────────────

#[test]
#[allow(clippy::or_fun_call)]
fn vec_ops_len_split_clone() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("vecops.msgpack");
    write_msgpack_vec(&path, &sample(100))?;

    let shards = build_msgpack_shards(&path, 25)?;
    let vec_ops = MsgpackVecOps::<Rec>::new();

    let len = vec_ops.len(&shards).ok_or(anyhow!("len failed"))?;
    assert_eq!(len, 100);

    let parts = vec_ops.split(&shards, 4).ok_or(anyhow!("split failed"))?;
    assert_eq!(parts.len(), 4);

    let cloned = vec_ops
        .clone_any(&shards)
        .ok_or(anyhow!("clone_any failed"))?;
    let cloned_data: Vec<Rec> = *cloned.downcast::<Vec<Rec>>().unwrap();
    assert_eq!(cloned_data.len(), 100);
    Ok(())
}

#[test]
#[allow(clippy::or_fun_call)]
fn vec_ops_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("vecops_empty.msgpack");
    write_msgpack_vec(&path, &Vec::<Rec>::new())?;

    let shards = build_msgpack_shards(&path, 25)?;
    let vec_ops = MsgpackVecOps::<Rec>::new();

    assert_eq!(vec_ops.len(&shards).ok_or(anyhow!("len"))?, 0);
    assert_eq!(vec_ops.split(&shards, 4).ok_or(anyhow!("split"))?.len(), 0);
    let cloned = vec_ops.clone_any(&shards).ok_or(anyhow!("clone_any"))?;
    assert!(cloned.downcast::<Vec<Rec>>().unwrap().is_empty());
    Ok(())
}

// ── High-level helpers ─────────────────────────────────────────────────────────

#[test]
fn pipeline_roundtrip_transform() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("pipeline.msgpack");

    let p = TestPipeline::new();
    let input = from_vec(
        &p,
        vec![
            Rec {
                id: 1,
                word: "hi".into(),
            },
            Rec {
                id: 2,
                word: "there".into(),
            },
        ],
    );
    let upper = input.map(|r: &Rec| Rec {
        id: r.id,
        word: r.word.to_uppercase(),
    });
    let n = upper.write_msgpack(&path)?;
    assert_eq!(n, 2);

    let p2 = TestPipeline::new();
    let back = read_msgpack::<Rec>(&p2, &path)?;
    let v = back.collect_seq()?;
    assert_eq!(
        v,
        vec![
            Rec {
                id: 1,
                word: "HI".into()
            },
            Rec {
                id: 2,
                word: "THERE".into()
            },
        ]
    );
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn pipeline_write_par() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("pipeline_par.msgpack");

    let p = TestPipeline::new();
    let input = from_vec(&p, sample(30));
    let n = input.write_msgpack_par(&path, Some(4))?;
    assert_eq!(n, 30);

    let back: Vec<Rec> = read_msgpack_vec(&path)?;
    assert_eq!(back, sample(30));
    Ok(())
}

#[test]
fn read_glob_concatenates_sorted() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    write_msgpack_vec(
        dir.join("a.msgpack"),
        &[Rec {
            id: 1,
            word: "a".into(),
        }],
    )?;
    write_msgpack_vec(
        dir.join("b.msgpack"),
        &[Rec {
            id: 2,
            word: "b".into(),
        }],
    )?;

    let pattern = dir.join("*.msgpack");
    let p = TestPipeline::new();
    let pc = read_msgpack::<Rec>(&p, &pattern)?;
    let v = pc.collect_seq()?;
    assert_eq!(
        v,
        vec![
            Rec {
                id: 1,
                word: "a".into()
            },
            Rec {
                id: 2,
                word: "b".into()
            },
        ]
    );
    Ok(())
}

#[test]
fn read_glob_no_match_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let pattern = tmp.path().join("*.msgpack");
    let p = TestPipeline::new();
    let result = read_msgpack::<Rec>(&p, &pattern);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("no files found matching pattern"), "{msg}");
}

#[test]
fn streaming_pipeline_wordcount() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("stream.msgpack");
    write_msgpack_vec(
        &path,
        &[
            Line {
                line: "The quick brown fox".into(),
            },
            Line {
                line: "jumps over the lazy dog".into(),
            },
        ],
    )?;

    let p = TestPipeline::new();
    let input = read_msgpack_streaming::<Line>(&p, &path, 1)?;
    let words = input.flat_map(|l: &Line| {
        l.line
            .split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>()
    });
    let counts = words
        .key_by(|w: &String| w.clone())
        .map_values(|_v: &String| 1u64)
        .combine_values(Count);

    let mut m = std::collections::HashMap::<String, u64>::new();
    for (k, v) in counts.collect_seq()? {
        m.insert(k, v);
    }
    assert_eq!(m.get("the"), Some(&2));
    assert_eq!(m.get("quick"), Some(&1));
    assert_eq!(m.get("lazy"), Some(&1));
    Ok(())
}

#[test]
fn streaming_empty_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("stream_empty.msgpack");
    write_msgpack_vec(&path, &Vec::<Rec>::new())?;

    let p = TestPipeline::new();
    let stream = read_msgpack_streaming::<Rec>(&p, &path, 10)?;
    let out = stream.collect_seq()?;
    assert!(out.is_empty());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn streaming_parallel_collect() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("stream_par.msgpack");
    write_msgpack_vec(&path, &sample(200))?;

    let p = TestPipeline::new();
    let stream = read_msgpack_streaming::<Rec>(&p, &path, 16)?;
    let mut out = stream.collect_par(None, None)?;
    out.sort_by_key(|r| r.id);
    assert_eq!(out, sample(200));
    Ok(())
}
