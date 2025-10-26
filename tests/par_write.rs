#![cfg(all(feature = "io-jsonl", feature = "parallel-io"))]

use rustflow::{from_vec, Pipeline};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct R(u32);

#[test]
fn write_jsonl_par_is_stable() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("out.jsonl");

    let p = Pipeline::default();
    let data: Vec<R> = (0..1000).map(R).collect();
    let col = from_vec(&p, data.clone());
    let n = col.write_jsonl_par(&file, Some(8))?;
    assert_eq!(n, 1000);

    let back: Vec<R> = rustflow::read_jsonl_vec(&file)?;
    assert_eq!(back, data);
    Ok(())
}
