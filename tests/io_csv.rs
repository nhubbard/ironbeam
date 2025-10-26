use serde::{Serialize, Deserialize};
use std::fs;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct Record { id: u32, name: String }

#[test]
fn write_csv_roundtrip() -> anyhow::Result<()> {
    let path = "test_out.csv";
    let data = vec![
        Record { id: 1, name: "A".into() },
        Record { id: 2, name: "B".into() },
    ];

    rustflow::write_csv(path, true, &data)?;
    let contents = fs::read_to_string(path)?;
    assert!(contents.contains("id,name"));
    assert!(contents.contains("1,A"));
    Ok(())
}