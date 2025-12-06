use ironbeam::testing::*;

#[test]
fn test_sample_log_entries() {
    let logs = sample_log_entries();
    assert_eq!(logs.len(), 5);
    assert!(logs.iter().all(|l| l.timestamp > 0));
}

#[test]
fn test_word_count_data() {
    let words = word_count_data();
    assert!(!words.is_empty());
    assert!(words.iter().any(|s| s.contains("hello")));
}

#[test]
fn test_numeric_data_with_outliers() {
    let data = numeric_data_with_outliers();
    let has_outliers = data.iter().any(|&x| !(0.0..=100.0).contains(&x));
    assert!(has_outliers);
}

#[test]
fn test_skewed_key_value_data() {
    let kvs = skewed_key_value_data();
    let hot_key_count = kvs.iter().filter(|(k, _)| k == "hot_key").count();
    assert_eq!(hot_key_count, 50); // Should be exactly 50
}

#[test]
fn test_time_series_data() {
    let ts = time_series_data();
    assert!(!ts.is_empty());
    // Verify timestamps are increasing
    for i in 1..ts.len() {
        assert!(ts[i].0 > ts[i - 1].0);
    }
}
