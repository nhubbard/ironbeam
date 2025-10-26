use anyhow::Result;
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
