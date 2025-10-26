#![cfg(feature = "io-csv")]

use anyhow::Result;
use rustflow::{read_csv_streaming, Pipeline};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct Rec {
    id: u32,
    name: String,
}

#[test]
fn csv_streaming_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("data.csv");
    // write a small CSV with header
    rustflow::write_csv_vec(
        &file,
        true,
        &[
            Rec {
                id: 1,
                name: "a".into(),
            },
            Rec {
                id: 2,
                name: "b".into(),
            },
            Rec {
                id: 3,
                name: "c".into(),
            },
        ],
    )?;

    let p = Pipeline::default();
    let input = read_csv_streaming::<Rec>(&p, &file, true, 2)?;
    let upper = input.map(|r: &Rec| Rec {
        id: r.id,
        name: r.name.to_uppercase(),
    });
    let out = upper.collect_par(Some(3), None)?;
    assert_eq!(out.len(), 3);
    assert!(out.iter().any(|r| r.name == "A"));
    Ok(())
}
