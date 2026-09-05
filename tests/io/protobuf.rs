//! Tests for the Protocol Buffers I/O connector (feature `io-protobuf`).

#![cfg(feature = "io-protobuf")]
#![allow(clippy::assert_is_empty)]

use anyhow::{Result, anyhow};
use ironbeam::io::protobuf::*;
use ironbeam::testing::*;
use ironbeam::type_token::VecOps;
use ironbeam::{Count, from_vec, read_proto, read_proto_streaming};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;

/// Minimal prost-derived test record.
///
/// Also derives serde so it satisfies `Element` when the `coders` feature is on
/// (which requires `Serialize + DeserializeOwned`). prost automatically implements
/// `Default`, so it must not be in the derive list.
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
struct Rec {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub word: String,
}

/// Minimal prost record used for streaming word-count tests.
#[derive(Clone, PartialEq, prost::Message, Serialize, Deserialize)]
struct Line {
    #[prost(string, tag = "1")]
    pub line: String,
}

fn sample(n: u32) -> Vec<Rec> {
    (0..n)
        .map(|i| Rec {
            id: i,
            word: format!("word{i}"),
        })
        .collect()
}

// ── Vector I/O ────────────────────────────────────────────────────────────────

#[test]
fn write_then_read_vec_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("out.proto");
    let data = sample(5);

    let n = write_proto_vec(&path, &data)?;
    assert_eq!(n, 5);

    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn write_vec_empty_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.proto");

    let n = write_proto_vec(&path, &Vec::<Rec>::new())?;
    assert_eq!(n, 0);
    assert!(path.exists());

    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert!(back.is_empty());
    Ok(())
}

#[test]
fn write_vec_creates_parent_dirs() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("sub").join("dir").join("out.proto");
    write_proto_vec(&path, &sample(2))?;
    assert!(path.exists());
    Ok(())
}

#[test]
fn read_vec_file_not_found() {
    let result: Result<Vec<Rec>> = read_proto_vec("definitely_missing.proto");
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(
        msg.contains("open") || msg.contains("No such file"),
        "{msg}"
    );
}

#[test]
fn read_vec_corrupt_propagates_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("corrupt.proto");

    // Write valid records then append a rogue varint (claims 20 bytes of payload
    // follow, but none are present). read_proto_vec will hit UnexpectedEof while
    // trying to read the claimed payload bytes.
    write_proto_vec(&path, &sample(3))?;
    let mut f = fs::OpenOptions::new().append(true).open(&path)?;
    f.write_all(&[0x14])?; // varint 20, no payload

    let result: Result<Vec<Rec>> = read_proto_vec(&path);
    assert!(result.is_err(), "corrupt data must not silently succeed");
    Ok(())
}

/// Verifies the clean-EOF path: after N records the read loop terminates cleanly.
#[test]
fn clean_eof_at_record_boundary() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("eof.proto");
    let data = sample(10);
    write_proto_vec(&path, &data)?;

    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert_eq!(back.len(), 10);
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "compression-gzip")]
#[test]
fn write_read_vec_gzip_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("out.proto.gz");
    let data = sample(20);

    write_proto_vec(&path, &data)?;
    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

// ── Error-path coverage ───────────────────────────────────────────────────────

#[test]
fn write_vec_mkdir_failure_when_parent_is_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let blocker = tmp.path().join("blocker");
    fs::write(&blocker, b"x")?;
    let path = blocker.join("child.proto");

    let result = write_proto_vec(&path, &sample(1));
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

    let result = write_proto_vec(&dir, &sample(1));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("create"), "{msg}");
    Ok(())
}

#[test]
fn build_shards_file_not_found() {
    let result = build_proto_shards("missing_shards.proto", 4);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(
        msg.contains("open") || msg.contains("No such file"),
        "{msg}"
    );
}

#[test]
fn read_range_file_not_found() {
    let shards = ProtoShards {
        path: "missing_range.proto".into(),
        ranges: vec![(0, 1)],
        total_records: 1,
    };
    let result: Result<Vec<Rec>> = read_proto_range(&shards, 0, 1);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(
        msg.contains("open") || msg.contains("No such file"),
        "{msg}"
    );
}

#[test]
fn read_glob_propagates_file_read_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    write_proto_vec(dir.join("a.proto"), &sample(1))?;

    // Write a second file then append a rogue varint with no payload.
    let bad = dir.join("b.proto");
    write_proto_vec(&bad, &sample(1))?;
    {
        use std::io::Write;
        let mut f = fs::OpenOptions::new().append(true).open(&bad)?;
        f.write_all(&[0x14])?; // varint 20, no payload
    }

    let p = TestPipeline::new();
    let result = read_proto::<Rec>(&p, dir.join("*.proto"));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("reading") && msg.contains("b.proto"), "{msg}");
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_mkdir_failure_when_parent_is_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let blocker = tmp.path().join("blocker_par");
    fs::write(&blocker, b"x")?;
    let path = blocker.join("child.proto");

    let result = write_proto_par(&path, &sample(2), Some(2));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("mkdir -p"), "{msg}");
    Ok(())
}

// ── Parallel writer ───────────────────────────────────────────────────────────

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty_par.proto");

    let n = write_proto_par(&path, &Vec::<Rec>::new(), None)?;
    assert_eq!(n, 0);
    assert!(path.exists());

    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert!(back.is_empty());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_single_shard() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("single_par.proto");
    let data = sample(3);

    let n = write_proto_par(&path, &data, Some(1))?;
    assert_eq!(n, 3);

    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_multiple_shards() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi_par.proto");
    let data = sample(50);

    let n = write_proto_par(&path, &data, Some(4))?;
    assert_eq!(n, 50);

    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_auto_shards() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("auto_par.proto");
    let data = sample(100);

    let n = write_proto_par(&path, &data, None)?;
    assert_eq!(n, 100);

    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

// ── Sharding / ranges ─────────────────────────────────────────────────────────

#[test]
fn build_shards_non_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("shards.proto");
    write_proto_vec(&path, &sample(10))?;

    let shards = build_proto_shards(&path, 4)?;
    assert_eq!(shards.total_records, 10);
    assert_eq!(shards.ranges, vec![(0, 4), (4, 8), (8, 10)]);
    Ok(())
}

#[test]
fn build_shards_zero_per_shard_clamps_to_one() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("clamp.proto");
    write_proto_vec(&path, &sample(3))?;

    let shards = build_proto_shards(&path, 0)?;
    assert_eq!(shards.total_records, 3);
    assert_eq!(shards.ranges, vec![(0, 1), (1, 2), (2, 3)]);
    Ok(())
}

#[test]
fn build_shards_empty_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.proto");
    write_proto_vec::<Rec>(&path, &[])?;

    let shards = build_proto_shards(&path, 10)?;
    assert_eq!(shards.total_records, 0);
    assert!(shards.ranges.is_empty());
    Ok(())
}

#[test]
fn build_shards_corrupt_propagates_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("corrupt_count.proto");
    write_proto_vec(&path, &sample(2))?;

    // Append a rogue varint (value 20) after the two valid records, but provide
    // no payload bytes. proto_count_records will read the varint successfully
    // then call read_exact(20 bytes) and hit UnexpectedEof — an error.
    let mut f = fs::OpenOptions::new().append(true).open(&path)?;
    f.write_all(&[0x14])?; // varint 20

    let result = build_proto_shards(&path, 4);
    assert!(result.is_err());
    Ok(())
}

#[test]
fn read_range_full_and_subranges() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range.proto");
    let data = sample(6);
    write_proto_vec(&path, &data)?;

    let shards = build_proto_shards(&path, 2)?;

    // Full range.
    let all: Vec<Rec> = read_proto_range(&shards, 0, shards.total_records)?;
    assert_eq!(all, data);

    // Interior subrange.
    let mid: Vec<Rec> = read_proto_range(&shards, 2, 4)?;
    assert_eq!(mid, data[2..4].to_vec());

    // Empty range.
    let none: Vec<Rec> = read_proto_range(&shards, 0, 0)?;
    assert!(none.is_empty());
    Ok(())
}

#[test]
fn read_range_corrupt_propagates_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range_corrupt.proto");
    write_proto_vec(&path, &sample(2))?;

    // Append a rogue varint claiming 20 bytes of payload, but provide none.
    // The range reader will read it as a third record and hit UnexpectedEof.
    let mut f = fs::OpenOptions::new().append(true).open(&path)?;
    f.write_all(&[0x14])?; // varint 20

    let shards = ProtoShards {
        path,
        ranges: vec![(0, 3)],
        total_records: 3,
    };
    let result: Result<Vec<Rec>> = read_proto_range(&shards, 0, 3);
    assert!(result.is_err());
    Ok(())
}

// ── VecOps adapter ────────────────────────────────────────────────────────────

#[test]
#[allow(clippy::or_fun_call)]
fn vec_ops_len_split_clone() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("vecops.proto");
    write_proto_vec(&path, &sample(100))?;

    let shards = build_proto_shards(&path, 25)?;
    let vec_ops = ProtoVecOps::<Rec>::new();

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
    let path = tmp.path().join("vecops_empty.proto");
    write_proto_vec::<Rec>(&path, &[])?;

    let shards = build_proto_shards(&path, 25)?;
    let vec_ops = ProtoVecOps::<Rec>::new();

    assert_eq!(vec_ops.len(&shards).ok_or(anyhow!("len"))?, 0);
    assert_eq!(vec_ops.split(&shards, 4).ok_or(anyhow!("split"))?.len(), 0);
    let cloned = vec_ops.clone_any(&shards).ok_or(anyhow!("clone_any"))?;
    assert!(cloned.downcast::<Vec<Rec>>().unwrap().is_empty());
    Ok(())
}

// ── High-level helpers ────────────────────────────────────────────────────────

#[test]
fn pipeline_roundtrip_transform() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("pipeline.proto");

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
    let doubled = input.map(|r: &Rec| Rec {
        id: r.id * 10,
        word: r.word.repeat(2),
    });
    let n = doubled.write_proto(&path)?;
    assert_eq!(n, 2);

    let p2 = TestPipeline::new();
    let back = read_proto::<Rec>(&p2, &path)?;
    let v = back.collect_seq()?;
    assert_eq!(
        v,
        vec![
            Rec {
                id: 10,
                word: "hihi".into()
            },
            Rec {
                id: 20,
                word: "therethere".into()
            },
        ]
    );
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn pipeline_write_par() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("pipeline_par.proto");

    let p = TestPipeline::new();
    let input = from_vec(&p, sample(30));
    let n = input.write_proto_par(&path, Some(4))?;
    assert_eq!(n, 30);

    let back: Vec<Rec> = read_proto_vec(&path)?;
    assert_eq!(back, sample(30));
    Ok(())
}

#[test]
fn read_glob_concatenates_sorted() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    write_proto_vec(
        dir.join("a.proto"),
        &[Rec {
            id: 1,
            word: "a".into(),
        }],
    )?;
    write_proto_vec(
        dir.join("b.proto"),
        &[Rec {
            id: 2,
            word: "b".into(),
        }],
    )?;

    let pattern = dir.join("*.proto");
    let p = TestPipeline::new();
    let pc = read_proto::<Rec>(&p, &pattern)?;
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
    let pattern = tmp.path().join("*.proto");
    let p = TestPipeline::new();
    let result = read_proto::<Rec>(&p, &pattern);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("no files found matching pattern"), "{msg}");
}

#[test]
fn streaming_pipeline_wordcount() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("stream.proto");
    write_proto_vec(
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
    let input = read_proto_streaming::<Line>(&p, &path, 1)?;
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
    let path = tmp.path().join("stream_empty.proto");
    write_proto_vec::<Rec>(&path, &[])?;

    let p = TestPipeline::new();
    let stream = read_proto_streaming::<Rec>(&p, &path, 10)?;
    let out = stream.collect_seq()?;
    assert!(out.is_empty());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn streaming_parallel_collect() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("stream_par.proto");
    write_proto_vec(&path, &sample(200))?;

    let p = TestPipeline::new();
    let stream = read_proto_streaming::<Rec>(&p, &path, 16)?;
    let mut out = stream.collect_par(None, None)?;
    out.sort_by_key(|r| r.id);
    assert_eq!(out, sample(200));
    Ok(())
}
