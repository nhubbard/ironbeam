//! Integration tests combining multiple combiners.
//!
//! This module tests using `Count`, `ToList`, and `ToSet` combiners
//! in combination with other transforms.

use anyhow::Result;
use ironbeam::*;

#[test]
fn test_count_then_filter() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
            ("c".to_string(), 4),
            ("c".to_string(), 5),
            ("c".to_string(), 6),
        ],
    );

    // Count per key, then filter for keys with count > 1
    let counts = data.count_per_key();
    let filtered = counts.filter(|(_, count)| *count > 1);
    let result = filtered.collect_seq_sorted()?;

    assert_eq!(result, vec![("a".to_string(), 2), ("c".to_string(), 3)]);
    Ok(())
}

#[test]
fn test_to_list_then_map() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    );

    // Collect to list, then sum the list
    let lists = data.to_list_per_key();
    let sums = lists.map(|(k, v)| (k.clone(), v.iter().sum::<i32>()));
    let result = sums.collect_seq_sorted()?;

    assert_eq!(result, vec![("a".to_string(), 3), ("b".to_string(), 3)]);
    Ok(())
}

#[test]
fn test_to_set_then_count() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    );

    // Collect to set, then count unique values
    let sets = data.to_set_per_key();
    let unique_counts = sets.map(|(k, v)| (k.clone(), v.len() as u64));
    let result = unique_counts.collect_seq_sorted()?;

    assert_eq!(result, vec![("a".to_string(), 2), ("b".to_string(), 1)]);
    Ok(())
}
