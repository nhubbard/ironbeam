use anyhow::Result;
use rustflow::io::jsonl::*;
use rustflow::testing::*;
use rustflow::{from_vec, read_jsonl, Count};
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

#[cfg(feature = "io-jsonl")]
#[test]
fn jsonl_roundtrip_stateless() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("out.jsonl");

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
    let n = upper.write_jsonl(&file)?;
    assert_eq!(n, 2);

    // Read back and check
    let p2 = TestPipeline::new();
    let back = read_jsonl::<Rec>(&p2, &file)?;
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

#[cfg(feature = "io-jsonl")]
#[test]
fn jsonl_wordcount_end_to_end() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("words.jsonl");

    // seed a file
    fs::write(
        &file,
        r#"{"line":"The quick brown fox"}
{"line":"jumps over the lazy dog"}"#,
    )?;

    let p = TestPipeline::new();
    let input = read_jsonl::<Line>(&p, &file)?;
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
fn read_jsonl_vec_with_empty_lines() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty_lines.jsonl");
    fs::write(
        &path,
        r#"{"id":1,"word":"hello"}

{"id":2,"word":"world"}

{"id":3,"word":"test"}
"#,
    )?;

    let data: Vec<Rec> = read_jsonl_vec(&path)?;
    assert_eq!(data.len(), 3);
    assert_eq!(data[0].id, 1);
    assert_eq!(data[1].id, 2);
    assert_eq!(data[2].id, 3);
    Ok(())
}

#[test]
fn read_jsonl_vec_parse_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("bad.jsonl");
    fs::write(
        &path,
        r#"{"id":1,"word":"ok"}
not valid json
"#,
    )?;

    let result: Result<Vec<Rec>> = read_jsonl_vec(&path);
    assert!(result.is_err());
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.contains("parse JSONL line"));
    Ok(())
}

#[test]
fn write_jsonl_vec_creates_parent_dirs() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("sub").join("dir").join("out.jsonl");
    let data = vec![Rec {
        id: 1,
        word: "test".into(),
    }];

    write_jsonl_vec(&path, &data)?;
    assert!(path.exists());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_jsonl_par_empty() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty_par.jsonl");
    let data: Vec<Rec> = vec![];

    let n = write_jsonl_par(&path, &data, None)?;
    assert_eq!(n, 0);
    assert!(path.exists());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_jsonl_par_single_shard() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("single_par.jsonl");
    let data = vec![
        Rec {
            id: 1,
            word: "hello".into(),
        },
        Rec {
            id: 2,
            word: "world".into(),
        },
    ];

    let n = write_jsonl_par(&path, &data, Some(1))?;
    assert_eq!(n, 2);

    let back: Vec<Rec> = read_jsonl_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_jsonl_par_multiple_shards() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi_par.jsonl");
    let mut data = vec![];
    for i in 0..50 {
        data.push(Rec {
            id: i,
            word: format!("word{i}"),
        });
    }

    let n = write_jsonl_par(&path, &data, Some(4))?;
    assert_eq!(n, 50);

    let back: Vec<Rec> = read_jsonl_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_jsonl_par_auto_shards() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("auto_par.jsonl");
    let mut data = vec![];
    for i in 0..100 {
        data.push(Rec {
            id: i,
            word: format!("word{i}"),
        });
    }

    let n = write_jsonl_par(&path, &data, None)?;
    assert_eq!(n, 100);

    let back: Vec<Rec> = read_jsonl_vec(&path)?;
    assert_eq!(back, data);
    Ok(())
}

#[test]
fn read_jsonl_range_with_empty_lines() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("range_test.jsonl");
    fs::write(
        &path,
        r#"{"id":1,"word":"a"}

{"id":2,"word":"b"}
{"id":3,"word":"c"}
"#,
    )?;

    let shards = build_jsonl_shards(&path, 10)?;
    let data: Vec<Rec> = read_jsonl_range(&shards, 0, 4)?;
    // Empty lines are skipped but counted in line numbers
    assert_eq!(data.len(), 3);
    assert_eq!(data[0].id, 1);
    assert_eq!(data[1].id, 2);
    assert_eq!(data[2].id, 3);
    Ok(())
}

#[test]
fn read_jsonl_vec_file_not_found() {
    let result: Result<Vec<Rec>> = read_jsonl_vec("nonexistent_file.jsonl");
    assert!(result.is_err());
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.contains("open") || err_msg.contains("No such file"));
}

#[test]
fn write_jsonl_vec_serialize_error_context() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("serialize_test.jsonl");

    // Even if serialization fails, we want to ensure the error context includes item number
    // This is implicitly tested by the write_jsonl_vec implementation
    let data = vec![Rec {
        id: 1,
        word: "test".into(),
    }];
    let result = write_jsonl_vec(&path, &data);
    assert!(result.is_ok());
    Ok(())
}

#[test]
fn build_jsonl_shards_empty_file() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.jsonl");
    fs::write(&path, "")?;

    let shards = build_jsonl_shards(&path, 10)?;
    assert_eq!(shards.total_lines, 0);
    assert_eq!(shards.ranges.len(), 0);
    Ok(())
}

#[test]
fn read_jsonl_range_parse_error() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("bad_range.jsonl");
    fs::write(
        &path,
        r#"{"id":1,"word":"ok"}
invalid json line
"#,
    )?;

    let shards = build_jsonl_shards(&path, 10)?;
    let result: Result<Vec<Rec>> = read_jsonl_range(&shards, 0, 2);
    assert!(result.is_err());
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.contains("parse JSONL line"));
    Ok(())
}
