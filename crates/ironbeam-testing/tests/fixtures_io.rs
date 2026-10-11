use ironbeam_testing::{
    numeric_data_with_outliers, product_metadata, sample_emails, sample_log_entries,
    sensor_readings, skewed_key_value_data, time_series_data, transaction_data,
    user_product_interactions, word_count_data,
};

#[test]
fn fixtures_have_the_shapes_needed_by_joins_windows_and_aggregation() {
    let logs = sample_log_entries();
    assert!(logs.iter().any(|row| row.status >= 400));
    assert!(logs.iter().any(|row| row.status < 400));
    assert!(
        word_count_data()
            .iter()
            .flat_map(|line| line.split_whitespace())
            .filter(|word| *word == "hello")
            .count()
            > 1
    );
    let numbers = numeric_data_with_outliers();
    assert!(numbers.iter().any(|value| *value < 0.0));
    assert!(numbers.iter().any(|value| *value > 100.0));
    let skewed = skewed_key_value_data();
    assert!(skewed.iter().filter(|(key, _)| key == "hot_key").count() > skewed.len() / 3);
    let times = time_series_data();
    assert!(times.windows(2).all(|pair| pair[0].0 < pair[1].0));
    let readings = sensor_readings();
    assert!(readings.iter().all(|(_, time, temp, humidity)| *time > 0
        && *temp > 0.0
        && (0.0..=100.0).contains(humidity)));
    assert!(sample_emails().iter().all(|mail| mail.contains('@')));
    assert!(
        transaction_data()
            .iter()
            .any(|(_, _, _, currency)| currency == "EUR")
    );

    let interactions = user_product_interactions();
    let products = product_metadata();
    assert!(
        interactions
            .iter()
            .all(|(_, product, _)| products.iter().any(|(id, _, _, _)| id == product))
    );
    assert!(
        products
            .iter()
            .any(|(id, _, _, _)| !interactions.iter().any(|(_, product, _)| product == id))
    );
}

#[cfg(any(feature = "io-csv", feature = "io-jsonl"))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    id: u32,
    label: String,
}

#[cfg(any(feature = "io-csv", feature = "io-jsonl"))]
fn rows() -> Vec<Row> {
    vec![
        Row {
            id: 1,
            label: "Ada".into(),
        },
        Row {
            id: 2,
            label: "Lin".into(),
        },
    ]
}

#[test]
fn temporary_paths_are_removed_when_dropped() {
    let default_file = ironbeam_testing::TempFilePath::default();
    assert!(default_file.path().exists());
    let plain_file = ironbeam_testing::TempFilePath::new().unwrap();
    assert!(plain_file.path().exists());
    let file = ironbeam_testing::TempFilePath::with_extension("data").unwrap();
    assert_eq!(file.path().extension().unwrap(), "data");
    let file_path = file.path().to_path_buf();
    assert!(file_path.exists());
    drop(file);
    assert!(!file_path.exists());

    let default_dir = ironbeam_testing::TempDirPath::default();
    assert!(default_dir.path().exists());
    let dir = ironbeam_testing::TempDirPath::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    let nested = dir.file_path("rows.jsonl");
    std::fs::write(&nested, b"data").unwrap();
    assert!(nested.exists());
    drop(dir);
    assert!(!dir_path.exists());
}

#[cfg(feature = "io-csv")]
#[test]
fn csv_mock_helpers_round_trip_and_reject_bad_rows() {
    use ironbeam_testing::{assert_csv_equals, mock_csv_file, read_csv_output};
    let data = rows();
    let file = mock_csv_file(&data, true).unwrap();
    assert_eq!(read_csv_output::<Row, _>(file.path()).unwrap(), data);
    assert_csv_equals(file.path(), &data);
    assert!(std::panic::catch_unwind(|| assert_csv_equals(file.path(), &data[..1])).is_err());
    let no_header = mock_csv_file(&data, false).unwrap();
    assert_eq!(
        std::fs::read_to_string(no_header.path())
            .unwrap()
            .lines()
            .count(),
        2
    );
    let bad = ironbeam_testing::TempFilePath::with_extension("csv").unwrap();
    std::fs::write(bad.path(), b"id,label\nnot-an-int,Ada\n").unwrap();
    assert!(read_csv_output::<Row, _>(bad.path()).is_err());
}

#[cfg(feature = "io-jsonl")]
#[test]
fn jsonl_mock_helpers_skip_blanks_and_report_bad_rows() {
    use ironbeam_testing::{assert_jsonl_equals, mock_jsonl_file, read_jsonl_output};
    let data = rows();
    let file = mock_jsonl_file(&data).unwrap();
    assert_eq!(read_jsonl_output::<Row, _>(file.path()).unwrap(), data);
    assert_jsonl_equals(file.path(), &data);
    assert!(std::panic::catch_unwind(|| assert_jsonl_equals(file.path(), &data[..1])).is_err());
    std::fs::OpenOptions::new()
        .append(true)
        .open(file.path())
        .unwrap()
        .write_all(b"\n  \n")
        .unwrap();
    assert_eq!(read_jsonl_output::<Row, _>(file.path()).unwrap(), data);
    std::fs::OpenOptions::new()
        .append(true)
        .open(file.path())
        .unwrap()
        .write_all(b"broken\n")
        .unwrap();
    assert!(read_jsonl_output::<Row, _>(file.path()).is_err());
}

#[cfg(feature = "io-jsonl")]
use std::io::Write;
