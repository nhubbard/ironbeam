#![cfg(feature = "io-jsonl")]

use anyhow::Result;
use ironbeam::testing::*;
use ironbeam::{Count, read_jsonl_streaming};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Line {
    line: String,
}

#[test]
fn jsonl_streaming_wordcount() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("words.jsonl");
    fs::write(
        &file,
        concat!(
            r#"{"line":"a a b"}"#,
            "\n",
            r#"{"line":"c b a"}"#,
            "\n",
            r#"{"line":"c c a"}"#,
            "\n",
        ),
    )?;

    let p = TestPipeline::new();
    let input = read_jsonl_streaming::<Line>(&p, &file, 1)?; // 1 line/shard
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
    let mut m = HashMap::<String, u64>::new();
    for (k, v) in counts.collect_par(Some(4), None)? {
        m.insert(k, v);
    }
    assert_eq!(m.get("a"), Some(&4));
    assert_eq!(m.get("b"), Some(&2));
    assert_eq!(m.get("c"), Some(&3));
    Ok(())
}
