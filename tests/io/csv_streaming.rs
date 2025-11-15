#![cfg(feature = "io-csv")]

use anyhow::Result;
use ironbeam::testing::*;
use ironbeam::{from_vec, read_csv, read_csv_streaming, read_csv_vec, write_csv_vec};
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
    write_csv_vec(
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

    let p = TestPipeline::new();
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

#[test]
fn read_csv_helper_function() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("helper_test.csv");
    // Write test data
    write_csv_vec(
        &file,
        true,
        &[
            Rec {
                id: 10,
                name: "test".into(),
            },
            Rec {
                id: 20,
                name: "data".into(),
            },
        ],
    )?;

    // Test read_csv helper
    let p = TestPipeline::new();
    let input = read_csv::<Rec>(&p, &file, true)?;
    let out = input.collect_seq()?;
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].id, 10);
    assert_eq!(out[0].name, "test");
    assert_eq!(out[1].id, 20);
    assert_eq!(out[1].name, "data");
    Ok(())
}

#[test]
#[cfg(feature = "parallel-io")]
fn write_csv_par_helper_function() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let file = tmp.path().join("par_helper_test.csv");

    let p = TestPipeline::new();
    let data = vec![
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
        Rec {
            id: 4,
            name: "d".into(),
        },
    ];
    let input = from_vec(&p, data.clone());

    // Test write_csv_par helper
    let n = input.write_csv_par(&file, Some(2), true)?;
    assert_eq!(n, 4);

    // Verify data can be read back
    let back: Vec<Rec> = read_csv_vec(&file, true)?;
    assert_eq!(back, data);
    Ok(())
}
