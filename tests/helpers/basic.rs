use anyhow::Result;
use ironbeam::*;

// ─────────────────────────────── sum_globally ────────────────────────────────

#[test]
fn sum_globally_integers() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1u64, 2, 3, 4, 5])
        .sum_globally()
        .collect_seq()?;
    assert_eq!(result, vec![15u64]);
    Ok(())
}

#[test]
fn sum_globally_single_element() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![42i32]).sum_globally().collect_seq()?;
    assert_eq!(result, vec![42i32]);
    Ok(())
}

#[test]
fn sum_globally_empty() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<i32>(&p, vec![]).sum_globally().collect_seq()?;
    // Empty collection → identity (Default::default())
    assert_eq!(result, vec![0i32]);
    Ok(())
}

#[test]
fn sum_globally_negatives() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![-3i32, 5, -2])
        .sum_globally()
        .collect_seq()?;
    assert_eq!(result, vec![0i32]);
    Ok(())
}

// ─────────────────────────────── sum_per_key ─────────────────────────────────

#[test]
fn sum_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![("a", 1u64), ("a", 2), ("b", 10)])
        .sum_per_key()
        .collect_seq_sorted()?;
    assert_eq!(result, vec![("a", 3u64), ("b", 10u64)]);
    Ok(())
}

#[test]
fn sum_per_key_single_entry_per_key() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("x", 7i32), ("y", -3)])
        .sum_per_key()
        .collect_seq()?;
    result.sort_by_key(|kv| kv.0);
    assert_eq!(result, vec![("x", 7i32), ("y", -3i32)]);
    Ok(())
}

#[test]
fn sum_per_key_empty() -> Result<()> {
    let p = Pipeline::default();
    let result: Vec<(&str, i32)> = from_vec(&p, vec![]).sum_per_key().collect_seq()?;
    assert!(result.is_empty());
    Ok(())
}

// ─────────────────────────────── min_globally ────────────────────────────────

#[test]
fn min_globally_basic() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![3i32, 1, 4, 1, 5])
        .min_globally()
        .collect_seq()?;
    assert_eq!(result, vec![1i32]);
    Ok(())
}

#[test]
fn min_globally_single_element() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![99u32]).min_globally().collect_seq()?;
    assert_eq!(result, vec![99u32]);
    Ok(())
}

#[test]
fn min_globally_negative_values() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![-100i64, 0, 50, -200])
        .min_globally()
        .collect_seq()?;
    assert_eq!(result, vec![-200i64]);
    Ok(())
}

// ─────────────────────────────── min_per_key ─────────────────────────────────

#[test]
fn min_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("a", 5i32), ("a", 2), ("b", 8)])
        .min_per_key()
        .collect_seq()?;
    result.sort_by_key(|kv| kv.0);
    assert_eq!(result, vec![("a", 2i32), ("b", 8i32)]);
    Ok(())
}

#[test]
fn min_per_key_single_entry_per_key() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("x", 42u64), ("y", 1)])
        .min_per_key()
        .collect_seq()?;
    result.sort_by_key(|kv| kv.0);
    assert_eq!(result, vec![("x", 42u64), ("y", 1u64)]);
    Ok(())
}

// ─────────────────────────────── max_globally ────────────────────────────────

#[test]
fn max_globally_basic() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![3i32, 1, 4, 1, 5])
        .max_globally()
        .collect_seq()?;
    assert_eq!(result, vec![5i32]);
    Ok(())
}

#[test]
fn max_globally_all_same() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![7u32; 100]).max_globally().collect_seq()?;
    assert_eq!(result, vec![7u32]);
    Ok(())
}

#[test]
fn max_globally_negative_values() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![-5i32, -1, -100])
        .max_globally()
        .collect_seq()?;
    assert_eq!(result, vec![-1i32]);
    Ok(())
}

// ─────────────────────────────── max_per_key ─────────────────────────────────

#[test]
fn max_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("a", 5i32), ("a", 2), ("b", 8)])
        .max_per_key()
        .collect_seq()?;
    result.sort_by_key(|kv| kv.0);
    assert_eq!(result, vec![("a", 5i32), ("b", 8i32)]);
    Ok(())
}

#[test]
fn max_per_key_single_entry_per_key() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("x", 42u64), ("y", 1)])
        .max_per_key()
        .collect_seq()?;
    result.sort_by_key(|kv| kv.0);
    assert_eq!(result, vec![("x", 42u64), ("y", 1u64)]);
    Ok(())
}

// ─────────────────────────────── average_globally ────────────────────────────

#[test]
fn average_globally_basic() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .average_globally()
        .collect_seq()?;
    assert!((result[0] - 3.0).abs() < 1e-12);
    Ok(())
}

#[test]
fn average_globally_single_element() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![42u32]).average_globally().collect_seq()?;
    assert!((result[0] - 42.0).abs() < 1e-12);
    Ok(())
}

#[test]
fn average_globally_all_same() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![10u32; 100])
        .average_globally()
        .collect_seq()?;
    assert!((result[0] - 10.0).abs() < 1e-12);
    Ok(())
}

#[test]
fn average_globally_empty_returns_zero() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<u32>(&p, vec![])
        .average_globally()
        .collect_seq()?;
    // AverageF64 returns 0.0 for empty groups
    assert!((result[0] - 0.0).abs() < 1e-12);
    Ok(())
}

// ─────────────────────────────── average_per_key ─────────────────────────────

#[test]
fn average_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("a", 1u32), ("a", 3), ("b", 10)])
        .average_per_key()
        .collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(b.0));
    assert_eq!(result[0].0, "a");
    assert!((result[0].1 - 2.0).abs() < 1e-12);
    assert_eq!(result[1].0, "b");
    assert!((result[1].1 - 10.0).abs() < 1e-12);
    Ok(())
}

#[test]
fn average_per_key_single_entry_per_key() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("x", 5u32), ("y", 20)])
        .average_per_key()
        .collect_seq()?;
    result.sort_by(|a, b| a.0.cmp(b.0));
    assert!((result[0].1 - 5.0).abs() < 1e-12);
    assert!((result[1].1 - 20.0).abs() < 1e-12);
    Ok(())
}

// ─────────────────────────────── distinct_count ──────────────────────────────

#[test]
fn distinct_count_globally_basic() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1u32, 1, 2, 3, 3, 3])
        .distinct_count_globally()
        .collect_seq()?;
    assert_eq!(result, vec![3u64]);
    Ok(())
}

#[test]
fn distinct_count_globally_all_unique() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .distinct_count_globally()
        .collect_seq()?;
    assert_eq!(result, vec![5u64]);
    Ok(())
}

#[test]
fn distinct_count_globally_all_same() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![7u32; 100])
        .distinct_count_globally()
        .collect_seq()?;
    assert_eq!(result, vec![1u64]);
    Ok(())
}

#[test]
fn distinct_count_globally_empty() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<u32>(&p, vec![])
        .distinct_count_globally()
        .collect_seq()?;
    assert_eq!(result, vec![0u64]);
    Ok(())
}

#[test]
fn distinct_count_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(
        &p,
        vec![("a", 1u32), ("a", 1), ("a", 2), ("b", 7u32), ("b", 7)],
    )
    .distinct_count_per_key()
    .collect_seq()?;
    result.sort_by_key(|kv| kv.0);
    assert_eq!(result, vec![("a", 2u64), ("b", 1u64)]);
    Ok(())
}

#[test]
fn distinct_count_per_key_all_unique_per_key() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("a", 1u32), ("a", 2), ("a", 3)])
        .distinct_count_per_key()
        .collect_seq()?;
    result.sort_by_key(|kv| kv.0);
    assert_eq!(result, vec![("a", 3u64)]);
    Ok(())
}

// ─────────────────────────────── to_list_globally ────────────────────────────

#[test]
fn to_list_globally_basic() -> Result<()> {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![3u32, 1, 2])
        .to_list_globally()
        .collect_seq()?;
    assert_eq!(result.len(), 1);
    result[0].sort_unstable();
    assert_eq!(result[0], vec![1u32, 2, 3]);
    Ok(())
}

#[test]
fn to_list_globally_empty() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<u32>(&p, vec![])
        .to_list_globally()
        .collect_seq()?;
    assert_eq!(result, vec![Vec::<u32>::new()]);
    Ok(())
}

#[test]
fn to_list_globally_single_element() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![42u32]).to_list_globally().collect_seq()?;
    assert_eq!(result, vec![vec![42u32]]);
    Ok(())
}

// ─────────────────────────────── to_set_globally ─────────────────────────────

#[test]
fn to_set_globally_deduplicates() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1u32, 1, 2, 3, 3])
        .to_set_globally()
        .collect_seq()?;
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].len(), 3);
    assert!(result[0].contains(&1));
    assert!(result[0].contains(&2));
    assert!(result[0].contains(&3));
    Ok(())
}

#[test]
fn to_set_globally_all_unique() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![10u32, 20, 30])
        .to_set_globally()
        .collect_seq()?;
    assert_eq!(result[0].len(), 3);
    Ok(())
}

#[test]
fn to_set_globally_empty() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec::<u32>(&p, vec![])
        .to_set_globally()
        .collect_seq()?;
    assert_eq!(result.len(), 1);
    assert!(result[0].is_empty());
    Ok(())
}

// ─────────────────────────────── top_k_globally ──────────────────────────────

#[test]
fn top_k_globally_basic() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![5i32, 2, 8, 1, 9, 3])
        .top_k_globally(3)
        .collect_seq()?;
    assert_eq!(result[0], vec![9i32, 8, 5]);
    Ok(())
}

#[test]
fn top_k_globally_k_larger_than_collection() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![3i32, 1, 2])
        .top_k_globally(10)
        .collect_seq()?;
    // All three, sorted descending
    assert_eq!(result[0], vec![3i32, 2, 1]);
    Ok(())
}

#[test]
fn top_k_globally_k_zero() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![5i32, 2, 8])
        .top_k_globally(0)
        .collect_seq()?;
    assert_eq!(result[0], Vec::<i32>::new());
    Ok(())
}

#[test]
fn top_k_globally_single_element() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![42i32]).top_k_globally(3).collect_seq()?;
    assert_eq!(result[0], vec![42i32]);
    Ok(())
}

#[test]
fn top_k_globally_matches_manual() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<u32> = (0..100).collect();
    let result = from_vec(&p, data).top_k_globally(5).collect_seq()?;
    // Top 5 should be 99, 98, 97, 96, 95
    assert_eq!(result[0], vec![99u32, 98, 97, 96, 95]);
    Ok(())
}

// ─────────────────────────────── consistency checks ──────────────────────────

#[test]
fn sum_globally_matches_per_key_with_single_key() -> Result<()> {
    // sum_globally and sum_per_key with one key should produce the same total
    let p = Pipeline::default();
    let data: Vec<u64> = (1..=10).collect();
    let global = from_vec(&p, data.clone()).sum_globally().collect_seq()?[0];
    let per_key = from_vec(&p, data)
        .key_by(|_| "k")
        .sum_per_key()
        .collect_seq()?[0]
        .1;
    assert_eq!(global, per_key);
    Ok(())
}

#[test]
fn min_max_globally_consistent() -> Result<()> {
    let p = Pipeline::default();
    let data: Vec<i32> = (-50..=50).collect();
    let min = from_vec(&p, data.clone()).min_globally().collect_seq()?[0];
    let max = from_vec(&p, data).max_globally().collect_seq()?[0];
    assert_eq!(min, -50);
    assert_eq!(max, 50);
    Ok(())
}
