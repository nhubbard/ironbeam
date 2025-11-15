//! Pre-built test datasets and fixtures for common testing scenarios.

use serde::{Deserialize, Serialize};

/// Sample log entry structure for testing ETL pipelines.
///
/// This mimics a typical web server log entry and is useful for
/// testing data processing pipelines.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SampleLogEntry {
    pub timestamp: u64,
    pub ip: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub bytes: u64,
}

/// Generate sample log entries for testing.
///
/// # Example
///
/// ```
/// use ironbeam::testing::sample_log_entries;
///
/// let logs = sample_log_entries();
/// assert!(!logs.is_empty());
/// ```
#[must_use]
pub fn sample_log_entries() -> Vec<SampleLogEntry> {
    vec![
        SampleLogEntry {
            timestamp: 1_000_000,
            ip: "192.168.1.100".to_string(),
            method: "GET".to_string(),
            path: "/api/users".to_string(),
            status: 200,
            bytes: 1024,
        },
        SampleLogEntry {
            timestamp: 1_000_100,
            ip: "192.168.1.101".to_string(),
            method: "POST".to_string(),
            path: "/api/users".to_string(),
            status: 201,
            bytes: 512,
        },
        SampleLogEntry {
            timestamp: 1_000_200,
            ip: "192.168.1.102".to_string(),
            method: "GET".to_string(),
            path: "/api/posts".to_string(),
            status: 200,
            bytes: 2048,
        },
        SampleLogEntry {
            timestamp: 1_000_300,
            ip: "192.168.1.100".to_string(),
            method: "GET".to_string(),
            path: "/api/users".to_string(),
            status: 404,
            bytes: 256,
        },
        SampleLogEntry {
            timestamp: 1_000_400,
            ip: "192.168.1.103".to_string(),
            method: "DELETE".to_string(),
            path: "/api/posts".to_string(),
            status: 500,
            bytes: 128,
        },
    ]
}

/// Generate sample word count data (classic example).
///
/// Returns a vector of sentences suitable for word counting tests.
///
/// # Example
///
/// ```
/// use ironbeam::testing::word_count_data;
///
/// let sentences = word_count_data();
/// assert!(!sentences.is_empty());
/// ```
#[must_use]
pub fn word_count_data() -> Vec<String> {
    vec![
        "hello world".to_string(),
        "hello rust".to_string(),
        "world of data".to_string(),
        "rust programming".to_string(),
        "hello data world".to_string(),
    ]
}

/// Generate numeric data with outliers for testing statistical operations.
///
/// # Example
///
/// ```
/// use ironbeam::testing::numeric_data_with_outliers;
///
/// let data = numeric_data_with_outliers();
/// assert!(data.iter().any(|&x| x > 100.0)); // Has outliers
/// ```
#[must_use]
pub fn numeric_data_with_outliers() -> Vec<f64> {
    vec![
        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,  // Normal range
        150.0, // Outlier
        11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,  // Normal range
        -50.0, // Outlier
    ]
}

/// Generate key-value data with a skewed distribution.
///
/// This mimics real-world scenarios where some keys appear much more
/// frequently than others (e.g., user activity, product views).
///
/// # Example
///
/// ```
/// use ironbeam::testing::skewed_key_value_data;
///
/// let kvs = skewed_key_value_data();
/// let hot_key_count = kvs.iter().filter(|(k, _)| k == "hot_key").count();
/// assert!(hot_key_count > 10); // Hot key appears frequently
/// ```
#[must_use]
pub fn skewed_key_value_data() -> Vec<(String, i32)> {
    let mut data = Vec::new();

    // Hot key - appears 50% of the time
    for i in 0..50 {
        data.push(("hot_key".to_string(), i));
    }

    // Warm keys - appear 30% of the time
    for i in 0..30 {
        data.push((format!("warm_key_{}", i % 3), i));
    }

    // Cold keys - appear 20% of the time (unique keys)
    for i in 0..20 {
        data.push((format!("cold_key_{i}"), i));
    }

    data
}

/// Generate time-series data for testing windowing and temporal operations.
///
/// Returns `(timestamp, value)` pairs representing measurements over time.
///
/// # Example
///
/// ```
/// use ironbeam::testing::time_series_data;
///
/// let ts = time_series_data();
/// assert!(!ts.is_empty());
/// assert!(ts.iter().all(|(t, _)| *t > 0));
/// ```
#[must_use]
pub fn time_series_data() -> Vec<(u64, f64)> {
    vec![
        (1_000, 10.5),
        (1_100, 12.3),
        (1_200, 11.8),
        (1_300, 15.2),
        (1_400, 14.7),
        (1_500, 16.1),
        (1_600, 13.9),
        (1_700, 17.3),
        (1_800, 18.5),
        (1_900, 16.8),
        (2_000, 19.2),
    ]
}

/// Generate user-product interaction data for testing joins.
///
/// Returns `(user_id, product_id, rating)` tuples.
///
/// # Example
///
/// ```
/// use ironbeam::testing::user_product_interactions;
///
/// let interactions = user_product_interactions();
/// assert!(!interactions.is_empty());
/// ```
#[must_use]
pub fn user_product_interactions() -> Vec<(String, String, u8)> {
    vec![
        ("user1".to_string(), "product_a".to_string(), 5),
        ("user1".to_string(), "product_b".to_string(), 4),
        ("user2".to_string(), "product_a".to_string(), 3),
        ("user2".to_string(), "product_c".to_string(), 5),
        ("user3".to_string(), "product_b".to_string(), 4),
        ("user3".to_string(), "product_c".to_string(), 2),
        ("user4".to_string(), "product_a".to_string(), 5),
    ]
}

/// Generate product metadata for testing joins with user interactions.
///
/// Returns `(product_id, name, category, price)` tuples.
///
/// # Example
///
/// ```
/// use ironbeam::testing::product_metadata;
///
/// let products = product_metadata();
/// assert!(!products.is_empty());
/// ```
#[must_use]
pub fn product_metadata() -> Vec<(String, String, String, f64)> {
    vec![
        (
            "product_a".to_string(),
            "Widget A".to_string(),
            "Electronics".to_string(),
            29.99,
        ),
        (
            "product_b".to_string(),
            "Gadget B".to_string(),
            "Electronics".to_string(),
            49.99,
        ),
        (
            "product_c".to_string(),
            "Tool C".to_string(),
            "Hardware".to_string(),
            19.99,
        ),
        (
            "product_d".to_string(),
            "Device D".to_string(),
            "Electronics".to_string(),
            99.99,
        ),
    ]
}

/// Generate sensor readings for testing aggregation and grouping.
///
/// Returns `(sensor_id, timestamp, temperature, humidity)` tuples.
///
/// # Example
///
/// ```
/// use ironbeam::testing::sensor_readings;
///
/// let readings = sensor_readings();
/// assert!(!readings.is_empty());
/// ```
#[must_use]
pub fn sensor_readings() -> Vec<(String, u64, f64, f64)> {
    vec![
        ("sensor_1".to_string(), 1000, 20.5, 45.0),
        ("sensor_1".to_string(), 1100, 21.0, 46.5),
        ("sensor_1".to_string(), 1200, 21.5, 47.0),
        ("sensor_2".to_string(), 1000, 19.8, 50.0),
        ("sensor_2".to_string(), 1100, 20.2, 51.0),
        ("sensor_2".to_string(), 1200, 20.5, 52.0),
        ("sensor_3".to_string(), 1000, 22.0, 40.0),
        ("sensor_3".to_string(), 1100, 22.5, 39.5),
        ("sensor_3".to_string(), 1200, 23.0, 39.0),
    ]
}

/// Generate email addresses for testing string operations.
///
/// # Example
///
/// ```
/// use ironbeam::testing::sample_emails;
///
/// let emails = sample_emails();
/// assert!(emails.iter().all(|e| e.contains('@')));
/// ```
#[must_use]
pub fn sample_emails() -> Vec<String> {
    vec![
        "alice@example.com".to_string(),
        "bob@test.org".to_string(),
        "charlie@example.com".to_string(),
        "diana@example.net".to_string(),
        "eve@test.org".to_string(),
        "frank@demo.io".to_string(),
    ]
}

/// Generate sample transaction data for testing financial pipelines.
///
/// Returns `(transaction_id, user_id, amount, currency)` tuples.
///
/// # Example
///
/// ```
/// use ironbeam::testing::transaction_data;
///
/// let transactions = transaction_data();
/// assert!(!transactions.is_empty());
/// ```
#[must_use]
pub fn transaction_data() -> Vec<(String, String, f64, String)> {
    vec![
        (
            "txn_1".to_string(),
            "user1".to_string(),
            100.50,
            "USD".to_string(),
        ),
        (
            "txn_2".to_string(),
            "user2".to_string(),
            250.75,
            "USD".to_string(),
        ),
        (
            "txn_3".to_string(),
            "user1".to_string(),
            50.25,
            "EUR".to_string(),
        ),
        (
            "txn_4".to_string(),
            "user3".to_string(),
            1000.00,
            "USD".to_string(),
        ),
        (
            "txn_5".to_string(),
            "user2".to_string(),
            75.50,
            "EUR".to_string(),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
