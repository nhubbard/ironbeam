use std::collections::HashMap;
use anyhow::Result;
use rustflow::{from_vec, AverageF64, DistinctCount, Max, Min, Pipeline, Sum, TopK};

#[test]
fn sum_min_max_average_basic_and_lifted() -> Result<()> {
    let p = Pipeline::default();
    let vals: Vec<i32> = (0..100).collect();

    // Sum
    let sum_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 5)
        .combine_values(Sum::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;

    let sum_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 5)
        .group_by_key()
        .combine_values_lifted(Sum::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;

    assert_eq!(sum_direct, sum_lifted);

    // Min
    let min_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 7)
        .combine_values(Min::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    let min_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 7)
        .group_by_key()
        .combine_values_lifted(Min::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    assert_eq!(min_direct, min_lifted);

    // Max
    let max_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 9)
        .combine_values(Max::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    let max_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 9)
        .group_by_key()
        .combine_values_lifted(Max::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    assert_eq!(max_direct, max_lifted);

    // AverageF64
    let avg_direct = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .key_by(|x| x % 4)
        .combine_values(AverageF64)
        .collect_par_sorted_by_key(Some(4), None)?;
    let avg_lifted = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .key_by(|x| x % 4)
        .group_by_key()
        .combine_values_lifted(AverageF64)
        .collect_par_sorted_by_key(Some(4), None)?;
    assert_eq!(avg_direct, avg_lifted);

    Ok(())
}

#[test]
fn distinct_count_basic_and_lifted() -> Result<()> {
    let p = Pipeline::default();
    // keys: 0..5, values: repeated pattern to ensure duplicates
    let vals: Vec<u32> = (0..500).map(|i| (i % 25) as u32).collect();

    let dc_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 5) // 5 buckets
        .combine_values(DistinctCount::<u32>::new())
        .collect_par_sorted_by_key(Some(6), None)?;

    let dc_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 5)
        .group_by_key()
        .combine_values_lifted(DistinctCount::<u32>::new())
        .collect_par_sorted_by_key(Some(6), None)?;

    assert_eq!(dc_direct, dc_lifted);
    Ok(())
}

#[test]
fn topk_basic_and_lifted() -> Result<()> {
    let p = Pipeline::default();
    let vals: Vec<i32> = (0..200).collect();

    // Top 3 per key (keyed by mod 7)
    let k = 3usize;

    let top_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 7)
        .combine_values(TopK::<i32>::new(k))
        .collect_par_sorted_by_key(Some(6), None)?;

    let top_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 7)
        .group_by_key()
        .combine_values_lifted(TopK::<i32>::new(k))
        .collect_par_sorted_by_key(Some(6), None)?;

    assert_eq!(top_direct, top_lifted);

    // sanity: each bucket should have descending order and len ≤ k
    for (_k, v) in top_direct {
        assert!(v.windows(2).all(|w| w[0] >= w[1]));
        assert!(v.len() <= 3);
    }
    Ok(())
}

// ===== Edge Case Tests =====

#[test]
fn sum_edge_cases() -> Result<()> {
    let p = Pipeline::default();

    // Empty group
    let empty_data: Vec<(String, u64)> = vec![];
    let result = from_vec(&p, empty_data)
        .combine_values(Sum::<u64>::default())
        .collect_seq()?;
    assert_eq!(result.len(), 0);

    // Single element
    let single_data = vec![("a".to_string(), 42u64)];
    let result = from_vec(&p, single_data)
        .combine_values(Sum::<u64>::default())
        .collect_seq()?;
    assert_eq!(result, vec![("a".to_string(), 42)]);

    // Large values
    let large_data = vec![
        ("a".to_string(), u64::MAX / 2),
        ("a".to_string(), u64::MAX / 2),
    ];
    let result = from_vec(&p, large_data)
        .combine_values(Sum::<u64>::default())
        .collect_seq()?;
    assert_eq!(result[0].1, u64::MAX - 1);

    Ok(())
}

#[test]
fn min_max_edge_cases() -> Result<()> {
    let p = Pipeline::default();

    // Single element
    let single_data = vec![("a".to_string(), 42)];
    let result_min = from_vec(&p, single_data.clone())
        .combine_values(Min::<i32>::default())
        .collect_seq()?;
    let result_max = from_vec(&p, single_data)
        .combine_values(Max::<i32>::default())
        .collect_seq()?;

    assert_eq!(result_min[0].1, 42);
    assert_eq!(result_max[0].1, 42);

    // All same values
    let same_data: Vec<(String, i32)> = vec![("a".to_string(), 5); 100];
    let result_min = from_vec(&p, same_data.clone())
        .combine_values(Min::<i32>::default())
        .collect_seq()?;
    let result_max = from_vec(&p, same_data)
        .combine_values(Max::<i32>::default())
        .collect_seq()?;

    assert_eq!(result_min[0].1, 5);
    assert_eq!(result_max[0].1, 5);

    // Extreme values
    let extreme_data = vec![
        ("a".to_string(), i32::MIN),
        ("a".to_string(), i32::MAX),
        ("a".to_string(), 0),
    ];
    let result_min = from_vec(&p, extreme_data.clone())
        .combine_values(Min::<i32>::default())
        .collect_seq()?;
    let result_max = from_vec(&p, extreme_data)
        .combine_values(Max::<i32>::default())
        .collect_seq()?;

    assert_eq!(result_min[0].1, i32::MIN);
    assert_eq!(result_max[0].1, i32::MAX);

    Ok(())
}

#[test]
fn average_edge_cases() -> Result<()> {
    let p = Pipeline::default();

    // Single value
    let single_data = vec![("a".to_string(), 42u32)];
    let result = from_vec(&p, single_data)
        .combine_values(AverageF64)
        .collect_seq()?;
    assert!((result[0].1 - 42.0).abs() < 1e-12);

    // All same values
    let same_data: Vec<(String, u32)> = vec![("a".to_string(), 10); 100];
    let result = from_vec(&p, same_data)
        .combine_values(AverageF64)
        .collect_seq()?;
    assert!((result[0].1 - 10.0).abs() < 1e-12);

    // Very small values
    let small_data: Vec<(String, u32)> = vec![
        ("a".to_string(), 1),
        ("a".to_string(), 1),
        ("a".to_string(), 1),
    ];
    let result = from_vec(&p, small_data)
        .combine_values(AverageF64)
        .collect_seq()?;
    assert!((result[0].1 - 1.0).abs() < 1e-12);

    Ok(())
}

#[test]
fn topk_edge_cases() -> Result<()> {
    let p = Pipeline::default();

    // K = 0
    let data = vec![("a".to_string(), 5), ("a".to_string(), 3)];
    let result = from_vec(&p, data)
        .combine_values(TopK::<i32>::new(0))
        .collect_seq()?;
    assert_eq!(result[0].1.len(), 0);

    // K larger than data
    let data = vec![("a".to_string(), 5), ("a".to_string(), 3)];
    let result = from_vec(&p, data.clone())
        .combine_values(TopK::<i32>::new(10))
        .collect_seq()?;
    assert_eq!(result[0].1.len(), 2);

    // Duplicate values
    let data: Vec<(String, i32)> = vec![("a".to_string(), 5); 10];
    let result = from_vec(&p, data)
        .combine_values(TopK::<i32>::new(3))
        .collect_seq()?;
    assert_eq!(result[0].1.len(), 3);
    assert!(result[0].1.iter().all(|&x| x == 5));

    // Single element
    let data = vec![("a".to_string(), 42)];
    let result = from_vec(&p, data)
        .combine_values(TopK::<i32>::new(5))
        .collect_seq()?;
    assert_eq!(result[0].1, vec![42]);

    Ok(())
}

#[test]
fn distinct_count_edge_cases() -> Result<()> {
    let p = Pipeline::default();

    // Single element
    let data = vec![("a".to_string(), 1u32)];
    let result = from_vec(&p, data)
        .combine_values(DistinctCount::<u32>::default())
        .collect_seq()?;
    assert_eq!(result[0].1, 1);

    // All same values
    let data: Vec<(String, u32)> = vec![("a".to_string(), 5); 100];
    let result = from_vec(&p, data)
        .combine_values(DistinctCount::<u32>::default())
        .collect_seq()?;
    assert_eq!(result[0].1, 1);

    // All distinct values
    let data: Vec<(String, u32)> = (1..=100).map(|i| ("a".to_string(), i)).collect();
    let result = from_vec(&p, data)
        .combine_values(DistinctCount::<u32>::default())
        .collect_seq()?;
    assert_eq!(result[0].1, 100);

    Ok(())
}

#[test]
fn combiners_with_negative_values() -> Result<()> {
    let p = Pipeline::default();

    // Sum with negatives
    let data = vec![
        ("a".to_string(), -10),
        ("a".to_string(), 5),
        ("a".to_string(), -3),
    ];
    let result = from_vec(&p, data)
        .combine_values(Sum::<i32>::default())
        .collect_seq()?;
    assert_eq!(result[0].1, -8);

    // Min/Max with negatives
    let data = vec![
        ("a".to_string(), -10),
        ("a".to_string(), 5),
        ("a".to_string(), -20),
    ];
    let result_min = from_vec(&p, data.clone())
        .combine_values(Min::<i32>::default())
        .collect_seq()?;
    let result_max = from_vec(&p, data)
        .combine_values(Max::<i32>::default())
        .collect_seq()?;

    assert_eq!(result_min[0].1, -20);
    assert_eq!(result_max[0].1, 5);

    Ok(())
}

#[test]
fn combiners_multiple_keys() -> Result<()> {
    let p = Pipeline::default();

    let data = vec![
        ("a".to_string(), 1),
        ("b".to_string(), 2),
        ("a".to_string(), 3),
        ("c".to_string(), 4),
        ("b".to_string(), 5),
    ];

    let result = from_vec(&p, data)
        .combine_values(Sum::<i32>::default())
        .collect_par_sorted_by_key(Some(4), None)?;

    assert_eq!(result.len(), 3);

    let map: HashMap<_, _> = result.into_iter().collect();
    assert_eq!(map.get("a"), Some(&4)); // 1 + 3
    assert_eq!(map.get("b"), Some(&7)); // 2 + 5
    assert_eq!(map.get("c"), Some(&4));

    Ok(())
}
