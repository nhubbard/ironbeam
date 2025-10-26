#![cfg(feature = "io-parquet")]
use rustflow::{from_vec, read_parquet_streaming, Count, Pipeline};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct Rec {
    id: u32,
    word: String,
}

#[test]
fn parquet_streaming_roundtrip() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("rows.parquet");

    // seed a file via pipeline
    let p = Pipeline::default();
    let rows: Vec<Rec> = (0..1000)
        .map(|i| Rec {
            id: i,
            word: format!("w{}", i % 7),
        })
        .collect();
    let col = from_vec(&p, rows.clone());
    let _ = col.write_parquet(&path)?;

    // read in streaming mode (1 row group per shard)
    let p2 = Pipeline::default();
    let s = read_parquet_streaming::<Rec>(&p2, &path, 1)?;
    let words = s
        .key_by(|r: &Rec| r.word.clone())
        .map_values(|_r: &Rec| 1u64)
        .combine_values(Count);
    let out = words.collect_par(Some(4), None)?;
    assert!(out.iter().any(|(k, _v)| k == "w0"));
    Ok(())
}
