//! Tests for the `TFRecord` I/O connector (feature `io-tfrecord`).

#![cfg(feature = "io-tfrecord")]

use anyhow::Result;
use ironbeam::io::tfrecord::*;
use ironbeam::testing::*;
use ironbeam::type_token::VecOps;
use ironbeam::{Count, from_vec, read_tfrecord, read_tfrecord_streaming};
use std::fs;
use std::io::Write;

fn sample(n: u32) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("record{i}").into_bytes()).collect()
}

// ── CRC helpers (always compiled) ─────────────────────────────────────────────

#[test]
fn mask_unmask_crc_roundtrip() {
    for crc in [0u32, 1, 0x1234_abcd, u32::MAX] {
        assert_eq!(
            unmask_crc(mask_crc(crc)),
            crc,
            "roundtrip failed for {crc:#010x}"
        );
    }
}

#[test]
fn mask_crc_known_value() {
    // TensorFlow's masking formula applied to CRC=0 → 0xa282ead8
    assert_eq!(mask_crc(0), 0xa282_ead8);
}

// ── Vector I/O ────────────────────────────────────────────────────────────────

#[test]
fn write_then_read_vec_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("out.tfrecord");
    let data = sample(5);

    let n = write_tfrecord_vec(&path, &data)?;
    assert_eq!(n, 5);

    let back = read_tfrecord_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn write_vec_empty_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.tfrecord");

    let n = write_tfrecord_vec(&path, &[])?;
    assert_eq!(n, 0);
    assert!(path.exists());

    let back = read_tfrecord_vec(&path)?;
    assert!(back.is_empty());
    Ok(())
}

#[test]
fn write_vec_creates_parent_dirs() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("sub").join("dir").join("out.tfrecord");
    write_tfrecord_vec(&path, &sample(2))?;
    assert!(path.exists());
    Ok(())
}

#[test]
fn read_vec_file_not_found() {
    let result = read_tfrecord_vec("definitely_missing.tfrecord");
    assert!(result.is_err());
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("open") || msg.contains("No such file"),
        "{msg}"
    );
}

#[test]
fn clean_eof_at_record_boundary() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("eof.tfrecord");
    let data = sample(10);
    write_tfrecord_vec(&path, &data)?;

    let back = read_tfrecord_vec(&path)?;
    assert_eq!(back.len(), 10);
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "compression-gzip")]
#[test]
fn write_read_vec_gzip_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("out.tfrecord.gz");
    let data = sample(20);

    write_tfrecord_vec(&path, &data)?;
    let back = read_tfrecord_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

// ── CRC corruption tests ───────────────────────────────────────────────────────

#[test]
fn read_vec_length_crc_mismatch_errors() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("len_crc.tfrecord");
    write_tfrecord_vec(&path, &sample(1))?;

    // Flip a bit in the 4-byte length CRC (bytes 8..12 of the file).
    let mut bytes = fs::read(&path)?;
    bytes[8] ^= 0xFF;
    fs::write(&path, &bytes)?;

    let result = read_tfrecord_vec(&path);
    assert!(result.is_err());
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("CRC"), "{msg}");
    Ok(())
}

#[test]
fn read_vec_data_crc_mismatch_errors() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("data_crc.tfrecord");
    write_tfrecord_vec(&path, &sample(1))?;

    // Flip a byte in the data CRC (last 4 bytes of the record).
    let mut bytes = fs::read(&path)?;
    let len = bytes.len();
    bytes[len - 1] ^= 0xFF;
    fs::write(&path, &bytes)?;

    let result = read_tfrecord_vec(&path);
    assert!(result.is_err());
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("CRC"), "{msg}");
    Ok(())
}

#[test]
fn read_vec_truncated_record_errors() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("truncated.tfrecord");
    write_tfrecord_vec(&path, &sample(1))?;

    // Remove the last 5 bytes so the data-CRC field is incomplete.
    let mut bytes = fs::read(&path)?;
    let new_len = bytes.len().saturating_sub(5);
    bytes.truncate(new_len);
    fs::write(&path, &bytes)?;

    let result = read_tfrecord_vec(&path);
    assert!(result.is_err());
    Ok(())
}

#[test]
fn read_vec_corrupt_partial_header_errors() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("partial.tfrecord");

    // Write valid records then append only 5 bytes (partial 8-byte length header).
    write_tfrecord_vec(&path, &sample(2))?;
    let mut f = fs::OpenOptions::new().append(true).open(&path)?;
    f.write_all(&[0x01, 0x02, 0x03, 0x04, 0x05])?;
    drop(f);

    let result = read_tfrecord_vec(&path);
    assert!(result.is_err());
    Ok(())
}

// ── Error-path coverage ────────────────────────────────────────────────────────

#[test]
fn write_vec_mkdir_failure_when_parent_is_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let blocker = tmp.path().join("blocker");
    fs::write(&blocker, b"x")?;
    let path = blocker.join("child.tfrecord");

    let result = write_tfrecord_vec(&path, &sample(1));
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

    let result = write_tfrecord_vec(&dir, &sample(1));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("create"), "{msg}");
    Ok(())
}

#[test]
fn build_shards_file_not_found() {
    let result = build_tfrecord_shards("missing_shards.tfrecord", 4);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(
        msg.contains("open") || msg.contains("No such file"),
        "{msg}"
    );
}

#[test]
fn read_range_file_not_found() {
    let shards = TFRecordShards {
        path: "missing_range.tfrecord".into(),
        ranges: vec![(0, 1)],
        total_records: 1,
    };
    let result = read_tfrecord_range(&shards, 0, 1);
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
    write_tfrecord_vec(dir.join("a.tfrecord"), &sample(1))?;

    let bad = dir.join("b.tfrecord");
    write_tfrecord_vec(&bad, &sample(1))?;
    // Corrupt the data CRC of the second file.
    let mut bytes = fs::read(&bad)?;
    let len = bytes.len();
    bytes[len - 1] ^= 0xFF;
    fs::write(&bad, &bytes)?;

    let p = TestPipeline::new();
    let result = read_tfrecord(&p, dir.join("*.tfrecord"));
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(
        msg.contains("reading") && msg.contains("b.tfrecord"),
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
    let path = blocker.join("child.tfrecord");

    let result = write_tfrecord_par(&path, &sample(2), Some(2));
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
    let path = tmp.path().join("empty_par.tfrecord");

    let n = write_tfrecord_par(&path, &[], None)?;
    assert_eq!(n, 0);
    assert!(path.exists());

    let back = read_tfrecord_vec(&path)?;
    assert!(back.is_empty());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_single_shard() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("single_par.tfrecord");
    let data = sample(3);

    let n = write_tfrecord_par(&path, &data, Some(1))?;
    assert_eq!(n, 3);

    let back = read_tfrecord_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_multiple_shards() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi_par.tfrecord");
    let data = sample(50);

    let n = write_tfrecord_par(&path, &data, Some(4))?;
    assert_eq!(n, 50);

    let back = read_tfrecord_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_par_auto_shards() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("auto_par.tfrecord");
    let data = sample(100);

    let n = write_tfrecord_par(&path, &data, None)?;
    assert_eq!(n, 100);

    let back = read_tfrecord_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

// ── Sharding / ranges ─────────────────────────────────────────────────────────

#[test]
fn build_shards_non_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("shards.tfrecord");
    write_tfrecord_vec(&path, &sample(10))?;

    let shards = build_tfrecord_shards(&path, 4)?;
    assert_eq!(shards.total_records, 10);
    assert_eq!(shards.ranges, vec![(0, 4), (4, 8), (8, 10)]);
    Ok(())
}

#[test]
fn build_shards_zero_per_shard_clamps_to_one() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("clamp.tfrecord");
    write_tfrecord_vec(&path, &sample(3))?;

    let shards = build_tfrecord_shards(&path, 0)?;
    assert_eq!(shards.total_records, 3);
    assert_eq!(shards.ranges, vec![(0, 1), (1, 2), (2, 3)]);
    Ok(())
}

#[test]
fn build_shards_empty_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.tfrecord");
    write_tfrecord_vec(&path, &[])?;

    let shards = build_tfrecord_shards(&path, 10)?;
    assert_eq!(shards.total_records, 0);
    assert!(shards.ranges.is_empty());
    Ok(())
}

#[test]
fn build_shards_corrupt_propagates_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("corrupt_count.tfrecord");
    write_tfrecord_vec(&path, &sample(2))?;

    // Corrupt the length-CRC of the second record (bytes 16+8=24..28).
    let mut bytes = fs::read(&path)?;
    bytes[24] ^= 0xFF;
    fs::write(&path, &bytes)?;

    let result = build_tfrecord_shards(&path, 4);
    assert!(result.is_err());
    Ok(())
}

#[test]
fn read_range_full_and_subranges() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range.tfrecord");
    let data = sample(6);
    write_tfrecord_vec(&path, &data)?;

    let shards = build_tfrecord_shards(&path, 2)?;

    let all = read_tfrecord_range(&shards, 0, shards.total_records)?;
    assert_eq!(all, data);

    let mid = read_tfrecord_range(&shards, 2, 4)?;
    assert_eq!(mid, data[2..4].to_vec());

    let none = read_tfrecord_range(&shards, 0, 0)?;
    assert!(none.is_empty());
    Ok(())
}

#[test]
fn read_range_corrupt_propagates_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range_corrupt.tfrecord");
    write_tfrecord_vec(&path, &sample(2))?;

    // Corrupt the data CRC of the first record (last 4 bytes of its entry).
    // First record: 8 (len) + 4 (len-crc) + len("record0"=7) + 4 (data-crc) = 23 bytes.
    // Data CRC is at bytes 19..23.
    let mut bytes = fs::read(&path)?;
    bytes[22] ^= 0xFF;
    fs::write(&path, &bytes)?;

    let shards = TFRecordShards {
        path,
        ranges: vec![(0, 2)],
        total_records: 2,
    };
    let result = read_tfrecord_range(&shards, 0, 2);
    assert!(result.is_err());
    Ok(())
}

// ── VecOps adapter ────────────────────────────────────────────────────────────

#[test]
#[allow(clippy::or_fun_call)]
fn vec_ops_len_split_clone() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("vecops.tfrecord");
    let data = sample(100);
    write_tfrecord_vec(&path, &data)?;

    let shards = build_tfrecord_shards(&path, 25)?;
    let vec_ops = TFRecordVecOps::new();

    let len = vec_ops.len(&shards).ok_or(anyhow::anyhow!("len failed"))?;
    assert_eq!(len, 100);

    let parts = vec_ops
        .split(&shards, 4)
        .ok_or(anyhow::anyhow!("split failed"))?;
    assert_eq!(parts.len(), 4);

    let cloned = vec_ops
        .clone_any(&shards)
        .ok_or(anyhow::anyhow!("clone_any failed"))?;
    let cloned_data: Vec<Vec<u8>> = *cloned.downcast::<Vec<Vec<u8>>>().unwrap();
    assert_eq!(cloned_data.len(), 100);
    Ok(())
}

#[test]
#[allow(clippy::or_fun_call)]
fn vec_ops_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("vecops_empty.tfrecord");
    write_tfrecord_vec(&path, &[])?;

    let shards = build_tfrecord_shards(&path, 25)?;
    let vec_ops = TFRecordVecOps::new();

    assert_eq!(vec_ops.len(&shards).ok_or(anyhow::anyhow!("len"))?, 0);
    assert_eq!(
        vec_ops
            .split(&shards, 4)
            .ok_or(anyhow::anyhow!("split"))?
            .len(),
        0
    );
    let cloned = vec_ops
        .clone_any(&shards)
        .ok_or(anyhow::anyhow!("clone_any"))?;
    assert!(cloned.downcast::<Vec<Vec<u8>>>().unwrap().is_empty());
    Ok(())
}

// ── High-level helpers ─────────────────────────────────────────────────────────

#[test]
fn pipeline_roundtrip_transform() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("pipeline.tfrecord");

    let p = TestPipeline::new();
    let input = from_vec(
        &p,
        vec![b"hello".to_vec(), b"world".to_vec(), b"foo".to_vec()],
    );
    let upper = input.map(|r: &Vec<u8>| r.to_ascii_uppercase());
    let n = upper.write_tfrecord(&path)?;
    assert_eq!(n, 3);

    let p2 = TestPipeline::new();
    let back = read_tfrecord(&p2, &path)?;
    let v = back.collect_seq()?;
    assert_eq!(
        v,
        vec![b"HELLO".to_vec(), b"WORLD".to_vec(), b"FOO".to_vec()]
    );
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn pipeline_write_par() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("pipeline_par.tfrecord");

    let p = TestPipeline::new();
    let data = sample(30);
    let input = from_vec(&p, data.clone());
    let n = input.write_tfrecord_par(&path, Some(4))?;
    assert_eq!(n, 30);

    let back = read_tfrecord_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn read_glob_concatenates_sorted() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    write_tfrecord_vec(dir.join("a.tfrecord"), &[b"aaa".to_vec()])?;
    write_tfrecord_vec(dir.join("b.tfrecord"), &[b"bbb".to_vec()])?;

    let p = TestPipeline::new();
    let pc = read_tfrecord(&p, dir.join("*.tfrecord"))?;
    let v = pc.collect_seq()?;
    assert_eq!(v, vec![b"aaa".to_vec(), b"bbb".to_vec()]);
    Ok(())
}

#[test]
fn read_glob_no_match_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let pattern = tmp.path().join("*.tfrecord");
    let p = TestPipeline::new();
    let result = read_tfrecord(&p, &pattern);
    assert!(result.is_err());
    let msg = format!("{:?}", result.err().unwrap());
    assert!(msg.contains("no files found matching pattern"), "{msg}");
}

#[test]
fn streaming_pipeline_wordcount() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("stream.tfrecord");
    let records: Vec<Vec<u8>> = vec![
        b"the quick brown fox".to_vec(),
        b"jumps over the lazy dog".to_vec(),
    ];
    write_tfrecord_vec(&path, &records)?;

    let p = TestPipeline::new();
    let input = read_tfrecord_streaming(&p, &path, 1)?;
    let words = input.flat_map(|r: &Vec<u8>| {
        String::from_utf8_lossy(r)
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
    let path = tmp.path().join("stream_empty.tfrecord");
    write_tfrecord_vec(&path, &[])?;

    let p = TestPipeline::new();
    let stream = read_tfrecord_streaming(&p, &path, 10)?;
    let out = stream.collect_seq()?;
    assert!(out.is_empty());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn streaming_parallel_collect() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("stream_par.tfrecord");
    let data = sample(200);
    write_tfrecord_vec(&path, &data)?;

    let p = TestPipeline::new();
    let stream = read_tfrecord_streaming(&p, &path, 16)?;
    let mut out = stream.collect_par(None, None)?;
    out.sort();
    let mut expected = data;
    expected.sort();
    assert_eq!(out, expected);
    Ok(())
}

// ── tf.Example roundtrip ──────────────────────────────────────────────────────

#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
#[test]
fn read_tfrecord_examples_roundtrip() -> Result<()> {
    use ironbeam::io::tfrecord_proto::{BytesList, Example, Feature, Features, feature};
    use prost::Message;
    use std::collections::BTreeMap;

    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("examples.tfrecord");

    // Build two Example records and encode them to raw bytes.
    let make_example = |label: &str| Example {
        features: Some(Features {
            feature: BTreeMap::from([(
                "label".to_string(),
                Feature {
                    kind: Some(feature::Kind::BytesList(BytesList {
                        value: vec![label.as_bytes().to_vec()],
                    })),
                },
            )]),
        }),
    };
    let ex1 = make_example("cat");
    let ex2 = make_example("dog");

    let raw: Vec<Vec<u8>> = vec![ex1.encode_to_vec(), ex2.encode_to_vec()];
    write_tfrecord_vec(&path, &raw)?;

    let decoded = ironbeam::io::tfrecord::read_tfrecord_examples_vec(&path)?;
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0], ex1);
    assert_eq!(decoded[1], ex2);
    Ok(())
}

#[cfg(all(feature = "io-tfrecord", feature = "io-protobuf"))]
#[test]
fn read_tfrecord_examples_helper_roundtrip() -> Result<()> {
    use ironbeam::io::tfrecord_proto::{BytesList, Example, Feature, Features, feature};
    use ironbeam::read_tfrecord_examples;
    use prost::Message;
    use std::collections::BTreeMap;

    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("examples2.tfrecord");

    let ex = Example {
        features: Some(Features {
            feature: BTreeMap::from([(
                "x".to_string(),
                Feature {
                    kind: Some(feature::Kind::BytesList(BytesList {
                        value: vec![b"hello".to_vec()],
                    })),
                },
            )]),
        }),
    };
    write_tfrecord_vec(&path, &[ex.encode_to_vec()])?;

    let p = TestPipeline::new();
    let pc = read_tfrecord_examples(&p, &path)?;
    let v = pc.collect_seq()?;
    assert_eq!(v.len(), 1);
    assert_eq!(v[0], ex);
    Ok(())
}
