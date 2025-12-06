use ironbeam::testing::*;

#[test]
fn test_temp_file_path() {
    let temp = TempFilePath::new().unwrap();
    assert!(temp.path().exists());
}

#[test]
fn test_temp_file_path_with_extension() {
    let temp = TempFilePath::with_extension("csv").unwrap();
    assert_eq!(temp.path().extension().unwrap(), "csv");
}

#[test]
fn test_temp_dir_path() {
    let temp_dir = TempDirPath::new().unwrap();
    assert!(temp_dir.path().exists());
    assert!(temp_dir.path().is_dir());
}

#[test]
fn test_temp_dir_file_path() {
    let temp_dir = TempDirPath::new().unwrap();
    let file_path = temp_dir.file_path("test.txt");
    assert!(file_path.starts_with(temp_dir.path()));
    assert!(file_path.ends_with("test.txt"));
}

#[cfg(feature = "io-jsonl")]
#[test]
fn test_mock_jsonl_file() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestRecord {
        id: u32,
        value: String,
    }

    let data = vec![
        TestRecord {
            id: 1,
            value: "foo".to_string(),
        },
        TestRecord {
            id: 2,
            value: "bar".to_string(),
        },
    ];

    let temp_file = mock_jsonl_file(&data).unwrap();
    let read_data: Vec<TestRecord> = read_jsonl_output(temp_file.path()).unwrap();

    assert_eq!(read_data, data);
}

#[cfg(feature = "io-csv")]
#[test]
fn test_mock_csv_file() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestRecord {
        name: String,
        age: u32,
    }

    let data = vec![
        TestRecord {
            name: "Alice".to_string(),
            age: 30,
        },
        TestRecord {
            name: "Bob".to_string(),
            age: 25,
        },
    ];

    let temp_file = mock_csv_file(&data, true).unwrap();
    let read_data: Vec<TestRecord> = read_csv_output(temp_file.path()).unwrap();

    assert_eq!(read_data, data);
}
