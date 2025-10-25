use serde::{Serialize, Deserialize};
use rustflow::{
    Pipeline, read_jsonl_streaming, read_csv_streaming, Count,
};
use std::collections::HashMap;
use std::fs;
use anyhow::Result;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Line { line: String }

#[test]
fn jsonl_streaming_wordcount() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("words.jsonl");
    fs::write(&file, concat!(
        r#"{"line":"a a b"}"#, "\n",
        r#"{"line":"c b a"}"#, "\n",
        r#"{"line":"c c a"}"#, "\n",
    ))?;

    let p = Pipeline::default();
    let input = read_jsonl_streaming::<Line>(&p, &file, 1)?; // 1 line/shard
    let words = input.flat_map(|l: &Line| l.line.split_whitespace().map(|w| w.to_string()).collect::<Vec<_>>());
    let counts = words.key_by(|w: &String| w.clone()).map_values(|_v: &String| 1u64).combine_values(Count);
    let mut m = HashMap::<String, u64>::new();
    for (k, v) in counts.collect_par(Some(4), None)? { m.insert(k, v); }
    assert_eq!(m.get("a"), Some(&4));
    assert_eq!(m.get("b"), Some(&2));
    assert_eq!(m.get("c"), Some(&3));
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Rec { id: u32, name: String }

#[test]
fn csv_streaming_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("data.csv");
    // write a small CSV with header
    rustflow::write_csv_vec(&file, true, &[
        Rec { id:1, name:"a".into() },
        Rec { id:2, name:"b".into() },
        Rec { id:3, name:"c".into() }
    ])?;

    let p = Pipeline::default();
    let input = read_csv_streaming::<Rec>(&p, &file, true, 2)?;
    let upper = input.map(|r: &Rec| Rec { id: r.id, name: r.name.to_uppercase() });
    let out = upper.collect_par(Some(3), None)?;
    assert_eq!(out.len(), 3);
    assert!(out.iter().any(|r| r.name == "A"));
    Ok(())
}