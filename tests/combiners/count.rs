//! Tests for the `Count` combiner.
//!
//! This module tests the `Count` combiner functionality including:
//! - Basic counting per key
//! - Global counting
//! - Convenience methods (`count_globally`, `count_per_key`, `count_per_element`)
//! - Edge cases (empty collections, large datasets)
//! - Integration with liftable combiner API

use anyhow::Result;
use ironbeam::combiners::Count;
use ironbeam::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct ComplexValue {
    field: i32,
}

#[test]
fn test_count_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
            ("a".to_string(), 4),
        ],
    );

    let counts = data.combine_values(Count::new()).collect_seq_sorted()?;

    assert_eq!(counts, vec![("a".to_string(), 3), ("b".to_string(), 1)]);
    Ok(())
}

#[test]
fn test_count_per_key_empty() -> Result<()> {
    let p = Pipeline::default();
    let data: PCollection<(String, i32)> = from_vec(&p, vec![]);

    let counts = data.combine_values(Count::new()).collect_seq()?;

    assert_eq!(counts, vec![]);
    Ok(())
}

#[test]
fn test_count_per_key_single_key() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("only".to_string(), 1),
            ("only".to_string(), 2),
            ("only".to_string(), 3),
        ],
    );

    let counts = data.combine_values(Count::new()).collect_seq()?;

    assert_eq!(counts, vec![("only".to_string(), 3)]);
    Ok(())
}

#[test]
fn test_count_per_key_many_keys() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4),
            ("e".to_string(), 5),
            ("a".to_string(), 6),
            ("c".to_string(), 7),
            ("e".to_string(), 8),
            ("e".to_string(), 9),
        ],
    );

    let mut counts = data.combine_values(Count::new()).collect_seq()?;
    counts.sort_unstable();

    assert_eq!(
        counts,
        vec![
            ("a".to_string(), 2),
            ("b".to_string(), 1),
            ("c".to_string(), 2),
            ("d".to_string(), 1),
            ("e".to_string(), 3)
        ]
    );
    Ok(())
}

#[test]
fn test_count_globally_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1, 2, 3, 4, 5]);

    let count = data.combine_globally(Count::new(), None).collect_seq()?;

    assert_eq!(count, vec![5]);
    Ok(())
}

#[test]
fn test_count_globally_empty() -> Result<()> {
    let p = Pipeline::default();
    let data: PCollection<i32> = from_vec(&p, vec![]);

    let count = data.combine_globally(Count::new(), None).collect_seq()?;

    assert_eq!(count, vec![0]);
    Ok(())
}

#[test]
fn test_count_globally_large() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (0..10000).collect::<Vec<_>>());

    let count = data.combine_globally(Count::new(), None).collect_seq()?;

    assert_eq!(count, vec![10000]);
    Ok(())
}

#[test]
fn test_count_globally_convenience_method() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1, 2, 3, 4, 5]);

    let count = data.count_globally().collect_seq()?;

    assert_eq!(count, vec![5]);
    Ok(())
}

#[test]
fn test_count_per_key_convenience_method() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    );

    let counts = data.count_per_key().collect_seq_sorted()?;

    assert_eq!(counts, vec![("a".to_string(), 2), ("b".to_string(), 1)]);
    Ok(())
}

#[test]
fn test_count_per_element() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "c".to_string(),
            "a".to_string(),
            "b".to_string(),
        ],
    );

    let counts = data.count_per_element().collect_seq_sorted()?;

    assert_eq!(
        counts,
        vec![
            ("a".to_string(), 3),
            ("b".to_string(), 2),
            ("c".to_string(), 1)
        ]
    );
    Ok(())
}

#[test]
fn test_count_per_element_numeric() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1, 2, 1, 3, 1, 2, 4]);

    let mut counts = data.count_per_element().collect_seq()?;
    counts.sort_unstable();

    assert_eq!(counts, vec![(1, 3), (2, 2), (3, 1), (4, 1)]);
    Ok(())
}

#[test]
fn test_count_different_value_types() -> Result<()> {
    let p = Pipeline::default();

    // Test with strings
    let string_data = from_vec(
        &p,
        vec![
            ("k".to_string(), "v1".to_string()),
            ("k".to_string(), "v2".to_string()),
        ],
    );
    let string_count = string_data.combine_values(Count::new()).collect_seq()?;
    assert_eq!(string_count, vec![("k".to_string(), 2)]);

    // Test with complex structs
    let complex_data = from_vec(
        &p,
        vec![
            ("k".to_string(), ComplexValue { field: 1 }),
            ("k".to_string(), ComplexValue { field: 2 }),
        ],
    );
    let complex_count = complex_data.combine_values(Count::new()).collect_seq()?;
    assert_eq!(complex_count, vec![("k".to_string(), 2)]);

    Ok(())
}

#[test]
fn test_count_with_zero_length_key() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("".to_string(), 1),
            ("".to_string(), 2),
            ("".to_string(), 3),
        ],
    );

    let counts = data.count_per_key().collect_seq()?;

    assert_eq!(counts, vec![("".to_string(), 3)]);
    Ok(())
}

#[test]
fn test_count_with_numeric_keys() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            (1, "a".to_string()),
            (2, "b".to_string()),
            (1, "c".to_string()),
            (2, "d".to_string()),
        ],
    );

    let counts = data.count_per_key().collect_seq_sorted()?;

    assert_eq!(counts, vec![(1, 2), (2, 2)]);
    Ok(())
}

#[test]
fn test_very_large_count() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        (0..100_000)
            .map(|i| ("key".to_string(), i))
            .collect::<Vec<_>>(),
    );

    let counts = data.count_per_key().collect_seq()?;

    assert_eq!(counts, vec![("key".to_string(), 100_000)]);
    Ok(())
}

#[test]
fn test_count_with_combine_values_lifted() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    );

    let grouped = data.group_by_key();
    let counts = grouped.combine_values_lifted(Count::new());
    let result = counts.collect_seq_sorted()?;

    assert_eq!(result, vec![("a".to_string(), 2), ("b".to_string(), 1)]);
    Ok(())
}
