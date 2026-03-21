//! Tests for the `ToList` combiner.
//!
//! This module tests the `ToList` combiner functionality including:
//! - Basic list collection per key
//! - Convenience methods (`to_list_per_key`)
//! - Duplicate preservation
//! - Complex value types
//! - Integration with liftable combiner API

use anyhow::Result;
use ironbeam::combiners::ToList;
use ironbeam::*;

#[test]
fn test_to_list_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3)]);

    let lists = data.combine_values(ToList::new()).collect_seq_sorted()?;

    assert_eq!(lists.len(), 2);
    assert_eq!(lists[0].0, "a");
    assert_eq!(lists[0].1.len(), 2);
    assert!(lists[0].1.contains(&1));
    assert!(lists[0].1.contains(&2));
    assert_eq!(lists[1].0, "b");
    assert_eq!(lists[1].1, vec![3]);
    Ok(())
}

#[test]
fn test_to_list_empty() -> Result<()> {
    let p = Pipeline::default();
    let data: PCollection<(&str, i32)> = from_vec(&p, vec![]);

    let lists = data.combine_values(ToList::new()).collect_seq()?;

    assert_eq!(lists, vec![]);
    Ok(())
}

#[test]
fn test_to_list_single_value_per_key() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("b", 2), ("c", 3)]);

    let mut lists = data.combine_values(ToList::new()).collect_seq()?;
    lists.sort_by_key(|x| x.0);

    assert_eq!(
        lists,
        vec![("a", vec![1]), ("b", vec![2]), ("c", vec![3])]
    );
    Ok(())
}

#[test]
fn test_to_list_many_values_per_key() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("a", 4),
            ("a", 5),
            ("a", 6),
            ("a", 7),
            ("a", 8),
        ],
    );

    let lists = data.combine_values(ToList::new()).collect_seq()?;

    assert_eq!(lists.len(), 1);
    assert_eq!(lists[0].0, "a");
    assert_eq!(lists[0].1.len(), 8);
    // Check all values are present
    for i in 1..=8 {
        assert!(lists[0].1.contains(&i));
    }
    Ok(())
}

#[test]
fn test_to_list_preserves_duplicates() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 1), ("a", 2), ("a", 1)]);

    let lists = data.combine_values(ToList::new()).collect_seq()?;

    assert_eq!(lists.len(), 1);
    assert_eq!(lists[0].0, "a");
    assert_eq!(lists[0].1.len(), 4);
    assert_eq!(lists[0].1.iter().filter(|&&x| x == 1).count(), 3);
    assert_eq!(lists[0].1.iter().filter(|&&x| x == 2).count(), 1);
    Ok(())
}

#[test]
fn test_to_list_per_key_convenience_method() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3)]);

    let lists = data.to_list_per_key().collect_seq_sorted()?;

    assert_eq!(lists.len(), 2);
    assert_eq!(lists[0].0, "a");
    assert_eq!(lists[0].1.len(), 2);
    assert_eq!(lists[1].0, "b");
    assert_eq!(lists[1].1, vec![3]);
    Ok(())
}

#[test]
fn test_to_list_complex_values() -> Result<()> {
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct Value {
        id: i32,
        name: String,
    }

    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            (
                "key1",
                Value {
                    id: 1,
                    name: "first".to_string(),
                },
            ),
            (
                "key1",
                Value {
                    id: 2,
                    name: "second".to_string(),
                },
            ),
            (
                "key2",
                Value {
                    id: 3,
                    name: "third".to_string(),
                },
            ),
        ],
    );

    let mut lists = data.combine_values(ToList::new()).collect_seq()?;
    lists.sort_by_key(|x| x.0);

    assert_eq!(lists.len(), 2);
    assert_eq!(lists[0].0, "key1");
    assert_eq!(lists[0].1.len(), 2);
    assert_eq!(lists[1].0, "key2");
    assert_eq!(lists[1].1.len(), 1);
    Ok(())
}

#[test]
fn test_to_list_with_unit_values() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", ()), ("a", ()), ("b", ())]);

    let lists = data.combine_values(ToList::new()).collect_seq_sorted()?;

    assert_eq!(lists.len(), 2);
    assert_eq!(lists[0].0, "a");
    assert_eq!(lists[0].1.len(), 2);
    assert_eq!(lists[1].0, "b");
    assert_eq!(lists[1].1.len(), 1);
    Ok(())
}

#[test]
fn test_to_list_returns_vec() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 2)]);

    let lists = data.combine_values(ToList::new()).collect_seq()?;

    // Verify the type is Vec
    assert_eq!(lists.len(), 1);
    let (_key, list): (&str, Vec<i32>) = lists[0].clone();
    assert_eq!(list.len(), 2);
    Ok(())
}

#[test]
fn test_to_list_with_combine_values_lifted() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![("a", 1), ("a", 2), ("b", 3)]);

    let grouped = data.group_by_key();
    let lists = grouped.combine_values_lifted(ToList::new());
    let result = lists.collect_seq_sorted()?;

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].0, "a");
    assert_eq!(result[0].1.len(), 2);
    assert_eq!(result[1].0, "b");
    assert_eq!(result[1].1, vec![3]);
    Ok(())
}
