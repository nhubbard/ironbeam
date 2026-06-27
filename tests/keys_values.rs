//! Comprehensive tests for feature 4.1: `Keys` / `Values` transforms.
//!
//! This test suite validates:
//! - `keys()` — extracts the key component from `PCollection<(K, V)>`
//! - `values()` — extracts the value component from `PCollection<(K, V)>`

use ironbeam::*;
use serde::{Deserialize, Serialize};

// ── keys() ───────────────────────────────────────────────────────────────────

/// Basic: extract string keys from `(String, u32)` pairs.
#[test]
fn test_keys_basic() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("alpha".to_string(), 1u32),
            ("beta".to_string(), 2),
            ("gamma".to_string(), 3),
        ],
    );

    let mut out = pairs.keys().collect_seq().unwrap();
    out.sort();

    assert_eq!(
        out,
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
    );
}

/// Basic: extract integer keys from `(i32, String)` pairs.
#[test]
fn test_keys_integer_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            (10i32, "x".to_string()),
            (20, "y".to_string()),
            (30, "z".to_string()),
        ],
    );

    let mut out = pairs.keys().collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![10i32, 20, 30]);
}

/// Edge case: empty collection produces no keys.
#[test]
fn test_keys_empty() {
    let p = Pipeline::default();
    let pairs: Vec<(String, u32)> = vec![];
    let result = from_vec(&p, pairs).keys().collect_seq().unwrap();
    assert!(result.is_empty());
}

/// Edge case: single-element collection.
#[test]
fn test_keys_single_element() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![("only".to_string(), 42u32)]);
    let out = pairs.keys().collect_seq().unwrap();
    assert_eq!(out, vec!["only".to_string()]);
}

/// Parallel execution produces the same keys (unordered).
#[test]
fn test_keys_parallel() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, (0..20u32).map(|i| (i, i * 2)).collect::<Vec<_>>());

    let mut out = pairs.keys().collect_par(Some(4), Some(4)).unwrap();
    out.sort_unstable();

    let expected: Vec<u32> = (0..20).collect();
    assert_eq!(out, expected);
}

/// Keys should preserve duplicates when multiple pairs share the same key.
#[test]
fn test_keys_duplicates_preserved() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    );

    let mut out = pairs.keys().collect_seq().unwrap();
    out.sort();

    assert_eq!(out, vec!["a".to_string(), "a".to_string(), "b".to_string()]);
}

/// `keys()` chained with `map` on the resulting collection.
#[test]
fn test_keys_chained_with_map() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![("hello".to_string(), 1u32), ("world".to_string(), 2)],
    );

    let mut out = pairs
        .keys()
        .map(|s: &String| s.len())
        .collect_seq()
        .unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![5usize, 5]);
}

/// `keys()` chained with `filter`.
#[test]
fn test_keys_chained_with_filter() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            (1i32, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
            (4, "four".to_string()),
        ],
    );

    let mut out = pairs.keys().filter_gt(&2i32).collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![3i32, 4]);
}

/// `keys()` followed by a secondary `key_by` and `group_by_key`.
#[test]
fn test_keys_then_rekey_and_group() {
    let p = Pipeline::default();
    // pairs: (even/odd bucket, value)
    let pairs = from_vec(
        &p,
        vec![
            (0u32, "a".to_string()),
            (1, "b".to_string()),
            (0, "c".to_string()),
            (1, "d".to_string()),
        ],
    );

    // Extract keys (the bucket numbers), then re-key by parity and group
    let mut out = pairs
        .keys()
        .key_by(|k: &u32| k % 2)
        .group_by_key()
        .collect_seq()
        .unwrap();
    out.sort_by_key(|(k, _)| *k);

    assert_eq!(out.len(), 2);
    let mut even = out[0].1.clone();
    even.sort_unstable();
    assert_eq!(even, vec![0u32, 0]);
    let mut odd = out[1].1.clone();
    odd.sort_unstable();
    assert_eq!(odd, vec![1u32, 1]);
}

/// Struct value type — `keys()` discards the struct, keeping only the key.
#[test]
fn test_keys_struct_value_discarded() {
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Payload {
        _data: Vec<u8>,
    }

    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("k1".to_string(), Payload { _data: vec![1, 2] }),
            ("k2".to_string(), Payload { _data: vec![3, 4] }),
        ],
    );

    let mut out = pairs.keys().collect_seq().unwrap();
    out.sort();

    assert_eq!(out, vec!["k1".to_string(), "k2".to_string()]);
}

// ── values() ─────────────────────────────────────────────────────────────────

/// Basic: extract u32 values from `(String, u32)` pairs.
#[test]
fn test_values_basic() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("x".to_string(), 10u32),
            ("y".to_string(), 20),
            ("z".to_string(), 30),
        ],
    );

    let mut out = pairs.values().collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![10u32, 20, 30]);
}

/// Values with string type.
#[test]
fn test_values_string_values() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            (1u32, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ],
    );

    let mut out = pairs.values().collect_seq().unwrap();
    out.sort();

    assert_eq!(
        out,
        vec!["one".to_string(), "three".to_string(), "two".to_string()]
    );
}

/// Edge case: empty collection produces no values.
#[test]
fn test_values_empty() {
    let p = Pipeline::default();
    let pairs: Vec<(u32, String)> = vec![];
    let result = from_vec(&p, pairs).values().collect_seq().unwrap();
    assert!(result.is_empty());
}

/// Edge case: single-element collection.
#[test]
fn test_values_single_element() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![(99u32, "sole".to_string())]);
    let out = pairs.values().collect_seq().unwrap();
    assert_eq!(out, vec!["sole".to_string()]);
}

/// Parallel execution produces the same values (unordered).
#[test]
fn test_values_parallel() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, (0..20u32).map(|i| (i * 2, i)).collect::<Vec<_>>());

    let mut out = pairs.values().collect_par(Some(4), Some(4)).unwrap();
    out.sort_unstable();

    let expected: Vec<u32> = (0..20).collect();
    assert_eq!(out, expected);
}

/// Values preserves duplicates when multiple pairs share the same value.
#[test]
fn test_values_duplicates_preserved() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 42u32),
            ("b".to_string(), 42),
            ("c".to_string(), 7),
        ],
    );

    let mut out = pairs.values().collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![7u32, 42, 42]);
}

/// `values()` chained with `map`.
#[test]
fn test_values_chained_with_map() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 3u32),
            ("b".to_string(), 5),
            ("c".to_string(), 7),
        ],
    );

    let mut out = pairs.values().map(|v: &u32| v * 2).collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![6u32, 10, 14]);
}

/// `values()` chained with `filter`.
#[test]
fn test_values_chained_with_filter() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("p".to_string(), 1u32),
            ("q".to_string(), 2),
            ("r".to_string(), 3),
            ("s".to_string(), 4),
            ("t".to_string(), 5),
        ],
    );

    let mut out = pairs.values().filter_gt(&2u32).collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![3u32, 4, 5]);
}

/// `values()` fed into an aggregation (sum).
#[test]
fn test_values_aggregation_sum() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 10u32),
            ("b".to_string(), 20),
            ("c".to_string(), 30),
        ],
    );

    let out = pairs
        .values()
        .with_constant_key(())
        .combine_values(Sum::default())
        .collect_seq()
        .unwrap();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].1, 60u32);
}

/// `values()` then re-key and group — verifies the full pipeline composes correctly.
#[test]
fn test_values_then_rekey_and_group() {
    let p = Pipeline::default();
    // Key: ignored label; Value: number; group values by parity
    let pairs = from_vec(
        &p,
        vec![
            ("lbl".to_string(), 1u32),
            ("lbl".to_string(), 2),
            ("lbl".to_string(), 3),
            ("lbl".to_string(), 4),
        ],
    );

    let mut out = pairs
        .values()
        .key_by(|v: &u32| v % 2)
        .group_by_key()
        .collect_seq()
        .unwrap();
    out.sort_by_key(|(k, _)| *k);

    assert_eq!(out.len(), 2);
    let mut even = out[0].1.clone();
    even.sort_unstable();
    assert_eq!(even, vec![2u32, 4]);
    let mut odd = out[1].1.clone();
    odd.sort_unstable();
    assert_eq!(odd, vec![1u32, 3]);
}

/// Struct key type — `values()` discards the struct, keeping only the value.
#[test]
fn test_values_struct_key_discarded() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct CompositeKey {
        region: String,
        shard: u32,
    }

    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            (
                CompositeKey {
                    region: "us-east".into(),
                    shard: 0,
                },
                100u64,
            ),
            (
                CompositeKey {
                    region: "eu-west".into(),
                    shard: 1,
                },
                200u64,
            ),
        ],
    );

    let mut out = pairs.values().collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![100u64, 200]);
}

// ── combined: keys() and values() together ────────────────────────────────────

/// Splitting a collection via `keys()` and `values()` yields independent results.
#[test]
fn test_keys_and_values_independent() {
    let data = vec![
        ("a".to_string(), 10u32),
        ("b".to_string(), 20),
        ("c".to_string(), 30),
    ];

    let p1 = Pipeline::default();
    let p2 = Pipeline::default();

    let mut keys_out = from_vec(&p1, data.clone()).keys().collect_seq().unwrap();
    let mut vals_out = from_vec(&p2, data).values().collect_seq().unwrap();

    keys_out.sort();
    vals_out.sort_unstable();

    assert_eq!(
        keys_out,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(vals_out, vec![10u32, 20, 30]);
}

/// Large collection: `keys()` and `values()` preserve element count.
#[test]
fn test_keys_values_large_collection_count() {
    const N: u32 = 1000;
    let data: Vec<(u32, u32)> = (0..N).map(|i| (i, i * 10)).collect();

    let p1 = Pipeline::default();
    let p2 = Pipeline::default();

    let keys_out = from_vec(&p1, data.clone()).keys().collect_seq().unwrap();
    let vals_out = from_vec(&p2, data).values().collect_seq().unwrap();

    assert_eq!(keys_out.len(), N as usize);
    assert_eq!(vals_out.len(), N as usize);
}
