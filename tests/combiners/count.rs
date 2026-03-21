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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ComplexValue {
    field: i32,
}

#[test]
fn test_count_per_key_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3), ("a", 4)]);

    let counts = data.combine_values(Count::new()).collect_seq_sorted()?;

    assert_eq!(counts, vec![("a", 3), ("b", 1)]);
    Ok(())
}

#[test]
fn test_count_per_key_empty() -> Result<()> {
    let p = Pipeline::default();
    let data: PCollection<(&str, i32)> = from_vec(&p, vec![]);

    let counts = data.combine_values(Count::new()).collect_seq()?;

    assert_eq!(counts, vec![]);
    Ok(())
}

#[test]
fn test_count_per_key_single_key() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("only", 1), ("only", 2), ("only", 3)]);

    let counts = data.combine_values(Count::new()).collect_seq()?;

    assert_eq!(counts, vec![("only", 3)]);
    Ok(())
}

#[test]
fn test_count_per_key_many_keys() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", 1),
            ("b", 2),
            ("c", 3),
            ("d", 4),
            ("e", 5),
            ("a", 6),
            ("c", 7),
            ("e", 8),
            ("e", 9),
        ],
    );

    let mut counts = data.combine_values(Count::new()).collect_seq()?;
    counts.sort_unstable();

    assert_eq!(
        counts,
        vec![("a", 2), ("b", 1), ("c", 2), ("d", 1), ("e", 3)]
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
    let data = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3)]);

    let counts = data.count_per_key().collect_seq_sorted()?;

    assert_eq!(counts, vec![("a", 2), ("b", 1)]);
    Ok(())
}

#[test]
fn test_count_per_element() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["a", "b", "a", "c", "a", "b"]);

    let counts = data.count_per_element().collect_seq_sorted()?;

    assert_eq!(counts, vec![("a", 3), ("b", 2), ("c", 1)]);
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
    let string_data = from_vec(&p, vec![("k", "v1"), ("k", "v2")]);
    let string_count = string_data.combine_values(Count::new()).collect_seq()?;
    assert_eq!(string_count, vec![("k", 2)]);

    // Test with complex structs
    let complex_data = from_vec(
        &p,
        vec![
            ("k", ComplexValue { field: 1 }),
            ("k", ComplexValue { field: 2 }),
        ],
    );
    let complex_count = complex_data.combine_values(Count::new()).collect_seq()?;
    assert_eq!(complex_count, vec![("k", 2)]);

    Ok(())
}

#[test]
fn test_count_with_zero_length_key() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("", 1), ("", 2), ("", 3)]);

    let counts = data.count_per_key().collect_seq()?;

    assert_eq!(counts, vec![("", 3)]);
    Ok(())
}

#[test]
fn test_count_with_numeric_keys() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![(1, "a"), (2, "b"), (1, "c"), (2, "d")]);

    let counts = data.count_per_key().collect_seq_sorted()?;

    assert_eq!(counts, vec![(1, 2), (2, 2)]);
    Ok(())
}

#[test]
fn test_very_large_count() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (0..100_000).map(|i| ("key", i)).collect::<Vec<_>>());

    let counts = data.count_per_key().collect_seq()?;

    assert_eq!(counts, vec![("key", 100_000)]);
    Ok(())
}

#[test]
fn test_count_with_combine_values_lifted() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3)]);

    let grouped = data.group_by_key();
    let counts = grouped.combine_values_lifted(Count::new());
    let result = counts.collect_seq_sorted()?;

    assert_eq!(result, vec![("a", 2), ("b", 1)]);
    Ok(())
}
