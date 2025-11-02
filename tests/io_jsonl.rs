use anyhow::Result;
use rustflow::io::jsonl::*;
use rustflow::{from_vec, read_jsonl, Count, Pipeline};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct Rec {
    id: u32,
    word: String,
}

#[cfg(feature = "io-jsonl")]
#[test]
fn jsonl_roundtrip_stateless() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("out.jsonl");

    let p = Pipeline::default();
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
    let n = upper.clone().write_jsonl(&file)?;
    assert_eq!(n, 2);

    // Read back and check
    let p2 = Pipeline::default();
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

    #[derive(Clone, Serialize, Deserialize, Debug)]
    struct Line {
        line: String,
    }

    let p = Pipeline::default();
    let input = read_jsonl::<Line>(&p, &file)?;
    let words = input.flat_map(|l: &Line| {
        l.line
            .split_whitespace()
            .map(|w| w.to_lowercase())
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
    fs::write(&path, r#"{"id":1,"word":"ok"}
not valid json
"#)?;

    let result: anyhow::Result<Vec<Rec>> = read_jsonl_vec(&path);
    assert!(result.is_err());
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.contains("parse JSONL line"));
    Ok(())
}

#[test]
fn write_jsonl_vec_creates_parent_dirs() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("sub").join("dir").join("out.jsonl");
    let data = vec![Rec { id: 1, word: "test".into() }];

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
        Rec { id: 1, word: "hello".into() },
        Rec { id: 2, word: "world".into() },
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
        data.push(Rec { id: i, word: format!("word{}", i) });
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
        data.push(Rec { id: i, word: format!("word{}", i) });
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
