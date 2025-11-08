use anyhow::Result;
use rustflow::combiners::{ApproxMedian, ApproxQuantiles, TDigest};
use rustflow::{from_vec, Pipeline};

#[test]
fn test_tdigest_basic() {
    let mut digest = TDigest::new(100.0);
    for i in 1..=100 {
        digest.add(i as f64);
    }

    // Test known quantiles
    assert!((digest.quantile(0.0) - 1.0).abs() < 2.0);
    assert!((digest.quantile(0.25) - 25.0).abs() < 5.0);
    assert!((digest.quantile(0.5) - 50.0).abs() < 5.0);
    assert!((digest.quantile(0.75) - 75.0).abs() < 5.0);
    assert!((digest.quantile(1.0) - 100.0).abs() < 2.0);
}

#[test]
fn test_tdigest_merge() {
    let mut d1 = TDigest::new(100.0);
    let mut d2 = TDigest::new(100.0);

    for i in 1..=50 {
        d1.add(i as f64);
    }
    for i in 51..=100 {
        d2.add(i as f64);
    }

    d1.merge(&d2);
    assert!((d1.quantile(0.5) - 50.0).abs() < 5.0);
    assert_eq!(d1.count(), 100.0);
}

#[test]
fn test_tdigest_empty() {
    let digest = TDigest::new(100.0);
    assert!(digest.quantile(0.5).is_nan());
    assert!(digest.is_empty());
}

#[test]
fn test_tdigest_cdf() {
    let mut digest = TDigest::new(100.0);
    for i in 1..=100 {
        digest.add(i as f64);
    }

    assert!((digest.cdf(50.0) - 0.5).abs() < 0.1);
    assert!(digest.cdf(0.0) < 0.05);
    assert!(digest.cdf(100.0) >= 0.95);
}

#[test]
fn approx_quantiles_basic() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=100).map(|i| ("key".to_string(), i as f64)).collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.0, 0.25, 0.5, 0.75, 1.0], 100.0))
        .collect_par_sorted_by_key(Some(1), None)?;

    assert_eq!(result.len(), 1);
    let (key, quantiles) = &result[0];
    assert_eq!(key, "key");
    assert_eq!(quantiles.len(), 5);

    // Check approximate accuracy (within 5% for this dataset)
    assert!((quantiles[0] - 1.0).abs() < 5.0); // min
    assert!((quantiles[1] - 25.0).abs() < 5.0); // 25th percentile
    assert!((quantiles[2] - 50.0).abs() < 5.0); // median
    assert!((quantiles[3] - 75.0).abs() < 5.0); // 75th percentile
    assert!((quantiles[4] - 100.0).abs() < 5.0); // max

    Ok(())
}

#[test]
fn approx_quantiles_lifted() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=100).map(|i| ("key".to_string(), i as f64)).collect();

    let result_direct = from_vec(&p, data.clone())
        .combine_values(ApproxQuantiles::new(vec![0.5], 100.0))
        .collect_par_sorted_by_key(Some(1), None)?;

    let result_lifted = from_vec(&p, data)
        .group_by_key()
        .combine_values_lifted(ApproxQuantiles::new(vec![0.5], 100.0))
        .collect_par_sorted_by_key(Some(1), None)?;

    assert_eq!(result_direct.len(), 1);
    assert_eq!(result_lifted.len(), 1);

    let (_, q_direct) = &result_direct[0];
    let (_, q_lifted) = &result_lifted[0];

    // Both should give similar median estimates
    assert!((q_direct[0] - q_lifted[0]).abs() < 5.0);
    assert!((q_direct[0] - 50.0).abs() < 5.0);

    Ok(())
}

#[test]
fn approx_quantiles_multiple_keys() -> Result<()> {
    let p = Pipeline::default();
    let mut data = Vec::new();

    // Key "a": values 1-50
    for i in 1..=50 {
        data.push(("a".to_string(), i as f64));
    }
    // Key "b": values 51-100
    for i in 51..=100 {
        data.push(("b".to_string(), i as f64));
    }

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.5], 100.0))
        .collect_par_sorted_by_key(Some(2), None)?;

    assert_eq!(result.len(), 2);

    // Find results for each key
    let (key_a, median_a) = result.iter().find(|(k, _)| k == "a").unwrap();
    let (key_b, median_b) = result.iter().find(|(k, _)| k == "b").unwrap();

    assert_eq!(key_a, "a");
    assert_eq!(key_b, "b");

    // Key "a" median should be around 25
    assert!((median_a[0] - 25.0).abs() < 5.0);

    // Key "b" median should be around 75
    assert!((median_b[0] - 75.0).abs() < 5.0);

    Ok(())
}

#[test]
fn approx_median_basic() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=100).map(|i| ("key".to_string(), i as f64)).collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxMedian::default())
        .collect_par_sorted_by_key(Some(1), None)?;

    assert_eq!(result.len(), 1);
    let (key, median) = &result[0];
    assert_eq!(key, "key");

    // Check approximate accuracy
    assert!((median - 50.0).abs() < 5.0);

    Ok(())
}

#[test]
fn approx_median_lifted() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=100).map(|i| ("key".to_string(), i as f64)).collect();

    let result_direct = from_vec(&p, data.clone())
        .combine_values(ApproxMedian::default())
        .collect_par_sorted_by_key(Some(1), None)?;

    let result_lifted = from_vec(&p, data)
        .group_by_key()
        .combine_values_lifted(ApproxMedian::default())
        .collect_par_sorted_by_key(Some(1), None)?;

    assert_eq!(result_direct.len(), 1);
    assert_eq!(result_lifted.len(), 1);

    let (_, median_direct) = &result_direct[0];
    let (_, median_lifted) = &result_lifted[0];

    // Both should give similar median estimates
    assert!((median_direct - median_lifted).abs() < 5.0);
    assert!((median_direct - 50.0).abs() < 5.0);

    Ok(())
}

#[test]
fn approx_quantiles_percentiles() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=1000).map(|i| ("key".to_string(), i as f64)).collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::percentiles(200.0))
        .collect_par_sorted_by_key(Some(1), None)?;

    assert_eq!(result.len(), 1);
    let (_, percentiles) = &result[0];

    // Should have 9 percentiles: 1, 5, 10, 25, 50, 75, 90, 95, 99
    assert_eq!(percentiles.len(), 9);

    // Check approximate values (within 2% for this larger dataset)
    let expected = vec![10.0, 50.0, 100.0, 250.0, 500.0, 750.0, 900.0, 950.0, 990.0];
    for (actual, exp) in percentiles.iter().zip(expected.iter()) {
        assert!(
            (actual - exp).abs() < 20.0,
            "Expected ~{}, got {}",
            exp,
            actual
        );
    }

    Ok(())
}

#[test]
fn approx_quantiles_five_number_summary() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=100).map(|i| ("key".to_string(), i as f64)).collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::five_number_summary(100.0))
        .collect_par_sorted_by_key(Some(1), None)?;

    assert_eq!(result.len(), 1);
    let (_, summary) = &result[0];

    // Should have 5 values: min, Q1, median, Q3, max
    assert_eq!(summary.len(), 5);

    assert!((summary[0] - 1.0).abs() < 2.0); // min
    assert!((summary[1] - 25.0).abs() < 5.0); // Q1
    assert!((summary[2] - 50.0).abs() < 5.0); // median
    assert!((summary[3] - 75.0).abs() < 5.0); // Q3
    assert!((summary[4] - 100.0).abs() < 2.0); // max

    Ok(())
}

#[test]
fn approx_quantiles_with_integers() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, i32)> = (1..=100).map(|i| ("key".to_string(), i)).collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.5], 100.0))
        .collect_par_sorted_by_key(Some(1), None)?;

    assert_eq!(result.len(), 1);
    let (_, median) = &result[0];

    // Median should be around 50
    assert!((median[0] - 50.0).abs() < 5.0);

    Ok(())
}

#[test]
fn approx_quantiles_skewed_distribution() -> Result<()> {
    let p = Pipeline::default();
    let mut data = Vec::new();

    // Skewed distribution: many low values, and few high values
    // 90 values between 1-10
    for i in 1..=90 {
        data.push(("key".to_string(), (i % 10 + 1) as f64));
    }
    // 10 values between 91-100
    for i in 91..=100 {
        data.push(("key".to_string(), i as f64));
    }

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.5, 0.9, 0.95, 0.99], 100.0))
        .collect_par_sorted_by_key(Some(1), None)?;

    assert_eq!(result.len(), 1);
    let (_, quantiles) = &result[0];

    // Median should be in the low range
    assert!(quantiles[0] < 20.0);

    // 90th percentile should be near the transition
    assert!(quantiles[1] > 10.0 && quantiles[1] < 95.0);

    // 95th and 99th should be in the high range
    assert!(quantiles[2] > 90.0);
    assert!(quantiles[3] > 95.0);

    Ok(())
}

#[test]
fn approx_quantiles_parallel_execution() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(i32, f64)> = (0..1000)
        .flat_map(|i| (1..=100).map(move |v| (i % 10, v as f64)))
        .collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::median(100.0))
        .collect_par_sorted_by_key(Some(8), None)?;

    // Should have 10 keys (0-9)
    assert_eq!(result.len(), 10);

    // Each key should have a median around 50
    for (_, median) in &result {
        assert!((median[0] - 50.0).abs() < 10.0);
    }

    Ok(())
}

// ===== Edge Case Tests =====

#[test]
fn approx_quantiles_single_value() -> Result<()> {
    let p = Pipeline::default();
    let data = vec![("a".to_string(), 42.0)];

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.0, 0.5, 1.0], 100.0))
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    let (_, quantiles) = &result[0];

    // All quantiles should be the single value
    assert!((quantiles[0] - 42.0).abs() < 1.0);
    assert!((quantiles[1] - 42.0).abs() < 1.0);
    assert!((quantiles[2] - 42.0).abs() < 1.0);

    Ok(())
}

#[test]
fn approx_quantiles_all_same_values() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = vec![("a".to_string(), 5.0); 100];

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.25, 0.5, 0.75], 100.0))
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    let (_, quantiles) = &result[0];

    // All quantiles should be the same value
    for q in quantiles {
        assert!((q - 5.0).abs() < 1.0);
    }

    Ok(())
}

#[test]
fn approx_median_single_value() -> Result<()> {
    let p = Pipeline::default();
    let data = vec![("a".to_string(), 99.0)];

    let result = from_vec(&p, data)
        .combine_values(ApproxMedian::default())
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    assert!((result[0].1 - 99.0).abs() < 1.0);

    Ok(())
}

#[test]
fn approx_quantiles_two_values() -> Result<()> {
    let p = Pipeline::default();
    let data = vec![("a".to_string(), 10.0), ("a".to_string(), 20.0)];

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.5], 100.0))
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    let median = result[0].1[0];

    // Median should be between the two values (t-digest approximation)
    assert!(median >= 10.0 && median <= 20.0);

    Ok(())
}

#[test]
fn approx_quantiles_extreme_values() -> Result<()> {
    let p = Pipeline::default();
    let data = vec![
        ("a".to_string(), 0.0),
        ("a".to_string(), 1e10),
        ("a".to_string(), 1e-10),
    ];

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.0, 0.5, 1.0], 100.0))
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    let (_, quantiles) = &result[0];

    // Should handle extreme values
    assert!(quantiles[0] < 1.0); // min close to 0
    assert!(quantiles[2] > 1e9); // max close to 1e10

    Ok(())
}

#[test]
fn approx_quantiles_with_negatives() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (-50..=50).map(|i| ("a".to_string(), i as f64)).collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.0, 0.5, 1.0], 100.0))
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    let (_, quantiles) = &result[0];

    assert!((quantiles[0] - (-50.0)).abs() < 5.0); // min
    assert!((quantiles[1] - 0.0).abs() < 5.0); // median
    assert!((quantiles[2] - 50.0).abs() < 5.0); // max

    Ok(())
}

#[test]
fn approx_quantiles_different_compression() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=1000).map(|i| ("a".to_string(), i as f64)).collect();

    // Test with different compression parameters
    for compression in [20.0, 50.0, 100.0, 200.0] {
        let result = from_vec(&p, data.clone())
            .combine_values(ApproxQuantiles::new(vec![0.5], compression))
            .collect_seq()?;

        assert_eq!(result.len(), 1);
        let median = result[0].1[0];

        // All should give reasonable median estimates
        assert!(
            (median - 500.0).abs() < 50.0,
            "Compression {} gave median {}",
            compression,
            median
        );
    }

    Ok(())
}

#[test]
fn approx_median_large_dataset() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=10_000).map(|i| ("a".to_string(), i as f64)).collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxMedian::new(200.0))
        .collect_par(Some(4), Some(16))?;

    assert_eq!(result.len(), 1);
    let median = result[0].1;

    // Median should be around 5000
    assert!((median - 5000.0).abs() < 100.0);

    Ok(())
}

#[test]
fn approx_quantiles_multiple_quantiles() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=100).map(|i| ("a".to_string(), i as f64)).collect();

    // Request many quantiles
    let quantiles_requested: Vec<f64> = (0..=10).map(|i| i as f64 / 10.0).collect();

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(quantiles_requested.clone(), 100.0))
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    let (_, quantiles) = &result[0];

    // Should have 11 quantiles (0.0, 0.1, 0.2, ..., 1.0)
    assert_eq!(quantiles.len(), 11);

    // Quantiles should be monotonically increasing
    for i in 0..quantiles.len() - 1 {
        assert!(
            quantiles[i] <= quantiles[i + 1],
            "Quantiles not monotonic at index {}",
            i
        );
    }

    Ok(())
}

#[test]
fn approx_quantiles_boundary_values() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = (1..=100).map(|i| ("a".to_string(), i as f64)).collect();

    // Test exact boundary quantiles
    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.0, 0.01, 0.99, 1.0], 100.0))
        .collect_seq()?;

    assert_eq!(result.len(), 1);
    let (_, quantiles) = &result[0];

    assert!((quantiles[0] - 1.0).abs() < 2.0); // 0th percentile (min)
    assert!(quantiles[1] < 5.0); // 1st percentile
    assert!(quantiles[2] > 95.0); // 99th percentile
    assert!((quantiles[3] - 100.0).abs() < 2.0); // 100th percentile (max)

    Ok(())
}

#[test]
fn approx_quantiles_empty_key() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<(String, f64)> = vec![];

    let result = from_vec(&p, data)
        .combine_values(ApproxQuantiles::new(vec![0.5], 100.0))
        .collect_seq()?;

    // Should have no results for empty input
    assert_eq!(result.len(), 0);

    Ok(())
}
