//! Tests for the `ToSet` combiner.
//!
//! This module tests the `ToSet` combiner functionality including:
//! - Basic set collection per key
//! - Automatic deduplication
//! - Convenience methods (`to_set_per_key`)
//! - Various value types
//! - Integration with liftable combiner API

use anyhow::Result;
use ironbeam::combiners::ToSet;
use ironbeam::*;
use std::collections::HashSet;

#[test]
fn test_to_set_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3)]);

    let mut sets = data.combine_values(ToSet::new()).collect_seq()?;
    sets.sort_by_key(|x| x.0);

    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].0, "a");
    assert_eq!(sets[0].1.len(), 2);
    assert!(sets[0].1.contains(&1));
    assert!(sets[0].1.contains(&2));
    assert_eq!(sets[1].0, "b");
    assert_eq!(sets[1].1.len(), 1);
    assert!(sets[1].1.contains(&3));
    Ok(())
}

#[test]
fn test_to_set_empty() -> Result<()> {
    let p = Pipeline::default();
    let data: PCollection<(&str, i32)> = from_vec(&p, vec![]);

    let sets = data.combine_values(ToSet::new()).collect_seq()?;

    assert_eq!(sets, vec![]);
    Ok(())
}

#[test]
fn test_to_set_deduplicates() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 1), ("a", 2), ("a", 1)]);

    let sets = data.combine_values(ToSet::new()).collect_seq()?;

    assert_eq!(sets.len(), 1);
    assert_eq!(sets[0].0, "a");
    assert_eq!(sets[0].1.len(), 2); // Only unique values: 1 and 2
    assert!(sets[0].1.contains(&1));
    assert!(sets[0].1.contains(&2));
    Ok(())
}

#[test]
fn test_to_set_all_duplicates() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 5), ("a", 5), ("a", 5), ("a", 5)]);

    let sets = data.combine_values(ToSet::new()).collect_seq()?;

    assert_eq!(sets.len(), 1);
    assert_eq!(sets[0].0, "a");
    assert_eq!(sets[0].1.len(), 1);
    assert!(sets[0].1.contains(&5));
    Ok(())
}

#[test]
fn test_to_set_many_unique_values() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (1..=100).map(|i| ("key", i)).collect::<Vec<_>>());

    let sets = data.combine_values(ToSet::new()).collect_seq()?;

    assert_eq!(sets.len(), 1);
    assert_eq!(sets[0].0, "key");
    assert_eq!(sets[0].1.len(), 100);
    for i in 1..=100 {
        assert!(sets[0].1.contains(&i));
    }
    Ok(())
}

#[test]
fn test_to_set_per_key_convenience_method() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 1), ("a", 2), ("b", 3)]);

    let mut sets = data.to_set_per_key().collect_seq()?;
    sets.sort_by_key(|x| x.0);

    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].0, "a");
    assert_eq!(sets[0].1.len(), 2);
    assert_eq!(sets[1].0, "b");
    assert_eq!(sets[1].1.len(), 1);
    Ok(())
}

#[test]
fn test_to_set_string_values() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", "foo".to_string()),
            ("a", "bar".to_string()),
            ("a", "foo".to_string()),
            ("b", "baz".to_string()),
        ],
    );

    let mut sets = data.combine_values(ToSet::new()).collect_seq()?;
    sets.sort_by_key(|x| x.0);

    assert_eq!(sets.len(), 2);
    assert_eq!(sets[0].0, "a");
    assert_eq!(sets[0].1.len(), 2);
    assert!(sets[0].1.contains("foo"));
    assert!(sets[0].1.contains("bar"));
    assert_eq!(sets[1].0, "b");
    assert_eq!(sets[1].1.len(), 1);
    assert!(sets[1].1.contains("baz"));
    Ok(())
}

#[test]
fn test_to_set_returns_hashset() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 2), ("a", 1)]);

    let sets = data.combine_values(ToSet::new()).collect_seq()?;

    // Verify the type is HashSet
    assert_eq!(sets.len(), 1);
    let (_key, set): (&str, HashSet<i32>) = sets[0].clone();
    assert_eq!(set.len(), 2);
    Ok(())
}

#[test]
fn test_to_set_with_combine_values_lifted() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 1), ("a", 2)]);

    let grouped = data.group_by_key();
    let sets = grouped.combine_values_lifted(ToSet::new());
    let result = sets.collect_seq()?;

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, "a");
    assert_eq!(result[0].1.len(), 2);
    Ok(())
}
