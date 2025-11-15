use ironbeam::io::csv::*;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Record {
    id: u32,
    name: String,
}

#[test]
fn write_csv_roundtrip() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("test_out.csv");
    let data = vec![
        Record {
            id: 1,
            name: "A".into(),
        },
        Record {
            id: 2,
            name: "B".into(),
        },
    ];

    write_csv(&path, true, &data)?;
    let contents = fs::read_to_string(&path)?;
    assert!(contents.contains("id,name"));
    assert!(contents.contains("1,A"));
    Ok(())
}

#[test]
fn read_csv_vec_with_headers() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("read_test.csv");
    fs::write(&path, "id,name\n10,Alice\n20,Bob\n")?;

    let data: Vec<Record> = read_csv_vec(&path, true)?;
    assert_eq!(data.len(), 2);
    assert_eq!(data[0].id, 10);
    assert_eq!(data[0].name, "Alice");
    assert_eq!(data[1].id, 20);
    assert_eq!(data[1].name, "Bob");
    Ok(())
}

#[test]
fn read_csv_vec_without_headers() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("no_header.csv");
    fs::write(&path, "10,Alice\n20,Bob\n")?;

    let data: Vec<Record> = read_csv_vec(&path, false)?;
    assert_eq!(data.len(), 2);
    assert_eq!(data[0].id, 10);
    Ok(())
}

#[test]
fn read_csv_vec_parse_error() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("bad.csv");
    fs::write(&path, "id,name\nbad,Alice\n")?;

    let result: anyhow::Result<Vec<Record>> = read_csv_vec(&path, true);
    assert!(result.is_err());
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.contains("parse CSV record"));
    Ok(())
}

#[test]
fn write_csv_vec_creates_parent_dirs() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("sub").join("dir").join("out.csv");
    let data = vec![Record {
        id: 1,
        name: "test".into(),
    }];

    write_csv_vec(&path, true, &data)?;
    assert!(path.exists());
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_csv_par_empty() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("empty.csv");
    let data: Vec<Record> = vec![];

    let n = write_csv_par(&path, &data, None, true)?;
    assert_eq!(n, 0);
    assert!(path.exists());
    let contents = fs::read_to_string(&path)?;
    assert_eq!(contents.len(), 0);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_csv_par_single_shard() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("single.csv");
    let data = vec![
        Record {
            id: 1,
            name: "A".into(),
        },
        Record {
            id: 2,
            name: "B".into(),
        },
    ];

    let n = write_csv_par(&path, &data, Some(1), true)?;
    assert_eq!(n, 2);

    let back: Vec<Record> = read_csv_vec(&path, true)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_csv_par_multiple_shards() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("multi.csv");
    let data = vec![
        Record {
            id: 1,
            name: "A".into(),
        },
        Record {
            id: 2,
            name: "B".into(),
        },
        Record {
            id: 3,
            name: "C".into(),
        },
        Record {
            id: 4,
            name: "D".into(),
        },
    ];

    let n = write_csv_par(&path, &data, Some(2), true)?;
    assert_eq!(n, 4);

    let back: Vec<Record> = read_csv_vec(&path, true)?;
    assert_eq!(back, data);
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_csv_par_no_headers() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("no_header_par.csv");
    let data = vec![
        Record {
            id: 1,
            name: "A".into(),
        },
        Record {
            id: 2,
            name: "B".into(),
        },
    ];

    let n = write_csv_par(&path, &data, Some(2), false)?;
    assert_eq!(n, 2);

    let contents = fs::read_to_string(&path)?;
    assert!(!contents.contains("id,name"));
    Ok(())
}

#[cfg(feature = "parallel-io")]
#[test]
fn write_csv_par_auto_shards() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().join("auto.csv");
    let mut data = vec![];
    for i in 0..100 {
        data.push(Record {
            id: i,
            name: format!("name{i}"),
        });
    }

    let n = write_csv_par(&path, &data, None, true)?;
    assert_eq!(n, 100);

    let back: Vec<Record> = read_csv_vec(&path, true)?;
    assert_eq!(back, data);
    Ok(())
}
