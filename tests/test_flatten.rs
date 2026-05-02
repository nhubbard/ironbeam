//! Comprehensive tests for the flatten transform.

use anyhow::Result;
use ironbeam::*;

/// Test basic 2-way flatten with simple integers
#[test]
fn test_flatten_two_collections() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2, 3]);
    let pc2 = from_vec(&p, vec![4, 5, 6]);

    let merged = flatten(&[&pc1, &pc2]);
    let mut result = merged.collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    Ok(())
}

/// Test 3-way flatten
#[test]
fn test_flatten_three_collections() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2]);
    let pc2 = from_vec(&p, vec![3, 4]);
    let pc3 = from_vec(&p, vec![5, 6]);

    let merged = flatten(&[&pc1, &pc2, &pc3]);
    let mut result = merged.collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    Ok(())
}

/// Test N-way flatten with many collections
#[test]
fn test_flatten_many_collections() -> Result<()> {
    let p = Pipeline::default();
    let collections: Vec<_> = (0..10)
        .map(|i| from_vec(&p, vec![i * 10, i * 10 + 1]))
        .collect();

    let refs: Vec<_> = collections.iter().collect();
    let merged = flatten(&refs);
    let mut result = merged.collect_seq()?;
    result.sort_unstable();

    let expected: Vec<i32> = (0..10).flat_map(|i| vec![i * 10, i * 10 + 1]).collect();
    assert_eq!(result, expected);
    Ok(())
}

/// Test flatten with empty collections
#[test]
fn test_flatten_with_empty_collections() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2, 3]);
    let pc2: PCollection<i32> = from_vec(&p, vec![]);
    let pc3 = from_vec(&p, vec![4, 5]);

    let merged = flatten(&[&pc1, &pc2, &pc3]);
    let mut result = merged.collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![1, 2, 3, 4, 5]);
    Ok(())
}

/// Test flatten with all empty collections
#[test]
fn test_flatten_all_empty() -> Result<()> {
    let p = Pipeline::default();
    let pc1: PCollection<i32> = from_vec(&p, vec![]);
    let pc2: PCollection<i32> = from_vec(&p, vec![]);

    let merged = flatten(&[&pc1, &pc2]);
    let result = merged.collect_seq()?;

    assert_eq!(result, Vec::<i32>::new());
    Ok(())
}

/// Test flatten with strings
#[test]
fn test_flatten_strings() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec!["hello".to_string(), "world".to_string()]);
    let pc2 = from_vec(&p, vec!["foo".to_string(), "bar".to_string()]);

    let merged = flatten(&[&pc1, &pc2]);
    let mut result = merged.collect_seq()?;
    result.sort();

    assert_eq!(result, vec!["bar", "foo", "hello", "world"]);
    Ok(())
}

/// Test flatten with keyed collections (tuples)
#[test]
fn test_flatten_keyed_collections() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    let pc2 = from_vec(&p, vec![("c".to_string(), 3), ("d".to_string(), 4)]);

    let merged = flatten(&[&pc1, &pc2]);
    let mut result = merged.collect_seq()?;
    result.sort_by_key(|x| x.0.clone());

    assert_eq!(
        result,
        vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4),
        ]
    );
    Ok(())
}

/// Test flatten preserves keys for `group_by_key`
#[test]
fn test_flatten_then_group_by_key() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![("a".to_string(), 1), ("b".to_string(), 2)]);
    let pc2 = from_vec(&p, vec![("a".to_string(), 3), ("c".to_string(), 4)]);

    let merged = flatten(&[&pc1, &pc2]);
    let grouped = merged.group_by_key();
    let mut result = grouped.collect_seq()?;

    // Sort groups for deterministic comparison
    for (_, vals) in &mut result {
        vals.sort_unstable();
    }
    result.sort_by_key(|x| x.0.clone());

    assert_eq!(
        result,
        vec![
            ("a".to_string(), vec![1, 3]),
            ("b".to_string(), vec![2]),
            ("c".to_string(), vec![4]),
        ]
    );
    Ok(())
}

/// Test flatten with transformed collections
#[test]
fn test_flatten_after_transformations() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2, 3]).map(|x| x * 2);
    let pc2 = from_vec(&p, vec![4, 5, 6]).map(|x| x * 3);

    let merged = flatten(&[&pc1, &pc2]);
    let mut result = merged.collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![2, 4, 6, 12, 15, 18]);
    Ok(())
}

/// Test flatten followed by filter
#[test]
fn test_flatten_then_filter() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2, 3, 4]);
    let pc2 = from_vec(&p, vec![5, 6, 7, 8]);

    let merged = flatten(&[&pc1, &pc2]);
    let filtered = merged.filter(|x| x % 2 == 0);
    let mut result = filtered.collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![2, 4, 6, 8]);
    Ok(())
}

/// Test flatten with parallel execution
#[test]
fn test_flatten_parallel() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2, 3]);
    let pc2 = from_vec(&p, vec![4, 5, 6]);
    let pc3 = from_vec(&p, vec![7, 8, 9]);

    let merged = flatten(&[&pc1, &pc2, &pc3]);
    let mut result = merged.collect_par(None, None)?;
    result.sort_unstable();

    assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    Ok(())
}

/// Test flatten with single collection (edge case)
#[test]
fn test_flatten_single_collection() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2, 3]);

    let merged = flatten(&[&pc1]);
    let mut result = merged.collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![1, 2, 3]);
    Ok(())
}

/// Test flatten with large collections
#[test]
fn test_flatten_large_collections() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, (0..1000).collect::<Vec<_>>());
    let pc2 = from_vec(&p, (1000..2000).collect::<Vec<_>>());

    let merged = flatten(&[&pc1, &pc2]);
    let result = merged.collect_seq()?;

    assert_eq!(result.len(), 2000);
    // Verify all elements are present (they should be in order already)
    let expected: Vec<_> = (0..2000).collect();
    assert_eq!(result, expected);
    Ok(())
}

/// Test type safety - this should compile because all collections are the same type
#[test]
fn test_flatten_type_safety() -> Result<()> {
    let p = Pipeline::default();
    let pc1: PCollection<u64> = from_vec(&p, vec![1u64, 2, 3]);
    let pc2: PCollection<u64> = from_vec(&p, vec![4, 5, 6]);

    let merged = flatten(&[&pc1, &pc2]);
    let mut result = merged.collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![1u64, 2, 3, 4, 5, 6]);
    Ok(())
}

/// Test flatten with structs
#[test]
fn test_flatten_with_structs() -> Result<()> {
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Record {
        id: u32,
        name: String,
    }

    let p = Pipeline::default();
    let pc1 = from_vec(
        &p,
        vec![
            Record {
                id: 1,
                name: "Alice".to_string(),
            },
            Record {
                id: 2,
                name: "Bob".to_string(),
            },
        ],
    );
    let pc2 = from_vec(
        &p,
        vec![
            Record {
                id: 3,
                name: "Charlie".to_string(),
            },
            Record {
                id: 4,
                name: "David".to_string(),
            },
        ],
    );

    let merged = flatten(&[&pc1, &pc2]);
    let mut result = merged.collect_seq()?;
    result.sort_by_key(|r| r.id);

    assert_eq!(
        result,
        vec![
            Record {
                id: 1,
                name: "Alice".to_string()
            },
            Record {
                id: 2,
                name: "Bob".to_string()
            },
            Record {
                id: 3,
                name: "Charlie".to_string()
            },
            Record {
                id: 4,
                name: "David".to_string()
            },
        ]
    );
    Ok(())
}

/// Test flatten with combine operation
#[test]
fn test_flatten_with_combine() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2, 3]);
    let pc2 = from_vec(&p, vec![4, 5, 6]);

    let merged = flatten(&[&pc1, &pc2]);
    let sum_collection = merged
        .key_by(|_| "all".to_string())
        .map_values(|x| *x)
        .combine_values(Sum::default());

    let result = sum_collection.collect_seq()?;
    assert_eq!(result, vec![("all".to_string(), 21)]);
    Ok(())
}

/// Verify that flatten branches run correctly when branches execute concurrently.
/// This specifically exercises the `par_iter` path in `exec_par`'s Flatten arm.
#[test]
fn flatten_branches_produce_correct_result_in_parallel() -> Result<()> {
    let p = Pipeline::default();
    let a = from_vec(&p, vec![1u32, 2, 3]);
    let b = from_vec(&p, vec![4u32, 5, 6]);
    let c = from_vec(&p, vec![7u32, 8, 9]);
    let mut out = flatten(&[&a, &b, &c]).collect_par(None, Some(4))?;
    out.sort_unstable();
    assert_eq!(out, (1u32..=9).collect::<Vec<_>>());
    Ok(())
}

/// Test flatten followed by distinct
#[test]
fn test_flatten_then_distinct() -> Result<()> {
    let p = Pipeline::default();
    let pc1 = from_vec(&p, vec![1, 2, 3, 2]);
    let pc2 = from_vec(&p, vec![3, 4, 5, 4]);

    let merged = flatten(&[&pc1, &pc2]);
    let distinct = merged.distinct();
    let mut result = distinct.collect_seq()?;
    result.sort_unstable();

    assert_eq!(result, vec![1, 2, 3, 4, 5]);
    Ok(())
}
