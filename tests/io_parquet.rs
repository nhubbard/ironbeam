#![cfg(feature = "io-parquet")]

use rustflow::{from_vec, Pipeline};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Row {
    id: u32,
    name: String,
    score: Option<f64>,
    tags: Vec<String>,
}

#[test]
fn parquet_roundtrip_typed() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("rows.parquet");

    let data = vec![
        Row {
            id: 1,
            name: "a".into(),
            score: Some(1.5),
            tags: vec!["x".into()],
        },
        Row {
            id: 2,
            name: "b".into(),
            score: None,
            tags: vec!["y".into(), "z".into()],
        },
    ];

    // Write directly
    let n = rustflow::write_parquet_vec(&path, &data)?;
    assert_eq!(n, 2);

    // Read back
    let back: Vec<Row> = rustflow::read_parquet_vec(&path)?;
    assert_eq!(back, data);

    // Also via pipeline
    let p = Pipeline::default();
    let col = from_vec(&p, back.clone());
    let out_path = tmp.path().join("out.parquet");
    let m = col.write_parquet(&out_path)?;
    assert_eq!(m, 2);
    let back2: Vec<Row> = rustflow::read_parquet_vec(&out_path)?;
    assert_eq!(back2, data);
    Ok(())
}
