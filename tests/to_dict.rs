//! Comprehensive tests for feature 4.5: `ToDict` combiner + `to_dict` helper.
//!
//! This test suite validates:
//! - `ToDict<K, V>` as a `CombineFn` used directly with `combine_globally`.
//! - The `combine_globally_lifted` path.
//! - The `to_dict()` convenience helper on `PCollection<(K, V)>`.
//! - Edge cases: empty / single / parallel / duplicates / large input.
//! - Composition: chaining `to_dict()` with downstream transforms.

use ironbeam::combiners::ToDict;
use ironbeam::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ── ToDict via combine_globally ──────────────────────────────────────────────

/// Basic: distinct keys produce a complete map.
#[test]
fn test_to_dict_combiner_basic() {
    let p = Pipeline::default();
    let dict = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ],
    )
    .combine_globally(ToDict::new(), None)
    .collect_seq()
    .unwrap();

    assert_eq!(dict.len(), 1);
    let expected: HashMap<String, u32> = [
        ("a".to_string(), 1u32),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
    ]
    .into_iter()
    .collect();
    assert_eq!(dict[0], expected);
}

/// Empty input produces a single empty `HashMap`.
#[test]
fn test_to_dict_combiner_empty() {
    let p = Pipeline::default();
    let input: Vec<(String, u32)> = vec![];
    let dict = from_vec(&p, input)
        .combine_globally(ToDict::new(), None)
        .collect_seq()
        .unwrap();
    assert_eq!(dict.len(), 1);
    assert!(dict[0].is_empty());
}

/// Single pair yields a single-entry map.
#[test]
fn test_to_dict_combiner_single_pair() {
    let p = Pipeline::default();
    let dict = from_vec(&p, vec![("only".to_string(), 42u32)])
        .combine_globally(ToDict::new(), None)
        .collect_seq()
        .unwrap();
    assert_eq!(dict.len(), 1);
    assert_eq!(dict[0].len(), 1);
    assert_eq!(dict[0].get("only"), Some(&42));
}

/// Parallel: distinct keys → all present in the final map.
#[test]
fn test_to_dict_combiner_parallel_distinct_keys() {
    let p = Pipeline::default();
    let data: Vec<(u32, u32)> = (0..100u32).map(|i| (i, i * 10)).collect();
    let dict = from_vec(&p, data)
        .combine_globally(ToDict::new(), Some(4))
        .collect_par(Some(4), Some(4))
        .unwrap();

    assert_eq!(dict.len(), 1);
    assert_eq!(dict[0].len(), 100);
    for i in 0..100u32 {
        assert_eq!(dict[0].get(&i), Some(&(i * 10)));
    }
}

/// Duplicates: with a single key inserted twice, the surviving value matches one
/// of the inputs (last-value-wins per partition; partition order is unspecified).
#[test]
fn test_to_dict_combiner_duplicates_unspecified() {
    let p = Pipeline::default();
    let dict = from_vec(
        &p,
        vec![
            ("k".to_string(), 1u32),
            ("k".to_string(), 2),
            ("k".to_string(), 3),
        ],
    )
    .combine_globally(ToDict::new(), None)
    .collect_seq()
    .unwrap();

    assert_eq!(dict.len(), 1);
    assert_eq!(dict[0].len(), 1);
    let v = dict[0].get("k").copied().expect("key 'k' missing");
    assert!(
        [1u32, 2, 3].contains(&v),
        "value {v} is not one of the inputs"
    );
}

/// Owned string keys + numeric values.
#[test]
fn test_to_dict_combiner_string_keys() {
    let p = Pipeline::default();
    let dict = from_vec(
        &p,
        vec![
            ("alpha".to_string(), 1u64),
            ("beta".into(), 2),
            ("gamma".into(), 3),
        ],
    )
    .combine_globally(ToDict::new(), None)
    .collect_seq()
    .unwrap();

    assert_eq!(dict[0].len(), 3);
    assert_eq!(dict[0].get("alpha"), Some(&1));
    assert_eq!(dict[0].get("beta"), Some(&2));
    assert_eq!(dict[0].get("gamma"), Some(&3));
}

/// Numeric keys + owned string values.
#[test]
fn test_to_dict_combiner_numeric_keys_string_values() {
    let p = Pipeline::default();
    let dict = from_vec(
        &p,
        vec![
            (1u32, "one".to_string()),
            (2, "two".into()),
            (3, "three".into()),
        ],
    )
    .combine_globally(ToDict::new(), None)
    .collect_seq()
    .unwrap();

    assert_eq!(dict[0].len(), 3);
    assert_eq!(dict[0].get(&1).map(String::as_str), Some("one"));
    assert_eq!(dict[0].get(&2).map(String::as_str), Some("two"));
    assert_eq!(dict[0].get(&3).map(String::as_str), Some("three"));
}

// ── ToDict via combine_globally_lifted ────────────────────────────────────────

/// Lifted combiner produces the same map.
#[test]
fn test_to_dict_combiner_lifted() {
    let p = Pipeline::default();
    let dict = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ],
    )
    .combine_globally_lifted(ToDict::new(), None)
    .collect_seq()
    .unwrap();
    let expected: HashMap<String, u32> = [
        ("a".to_string(), 1u32),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
    ]
    .into_iter()
    .collect();
    assert_eq!(dict[0], expected);
}

/// Lifted path with fanout.
#[test]
fn test_to_dict_combiner_lifted_with_fanout() {
    let p = Pipeline::default();
    let data: Vec<(u32, u32)> = (0..50u32).map(|i| (i, i)).collect();
    let dict = from_vec(&p, data)
        .combine_globally_lifted(ToDict::new(), Some(8))
        .collect_seq()
        .unwrap();
    assert_eq!(dict[0].len(), 50);
    for i in 0..50u32 {
        assert_eq!(dict[0].get(&i), Some(&i));
    }
}

// ── to_dict helper ───────────────────────────────────────────────────────────

/// Helper: produces the same result as the explicit combiner form.
#[test]
fn test_to_dict_helper_basic() {
    let p = Pipeline::default();
    let dict = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ],
    )
    .to_dict()
    .collect_seq()
    .unwrap();

    let expected: HashMap<String, u32> = [
        ("a".to_string(), 1u32),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
    ]
    .into_iter()
    .collect();
    assert_eq!(dict[0], expected);
}

/// Helper: empty input produces an empty map.
#[test]
fn test_to_dict_helper_empty() {
    let p = Pipeline::default();
    let input: Vec<(String, u32)> = vec![];
    let dict = from_vec(&p, input).to_dict().collect_seq().unwrap();
    assert_eq!(dict.len(), 1);
    assert!(dict[0].is_empty());
}

/// Helper: parallel execution.
#[test]
fn test_to_dict_helper_parallel() {
    let p = Pipeline::default();
    let data: Vec<(u32, u32)> = (0..200u32).map(|i| (i, i + 1000)).collect();
    let dict = from_vec(&p, data)
        .to_dict()
        .collect_par(Some(4), Some(4))
        .unwrap();
    assert_eq!(dict[0].len(), 200);
    for i in 0..200u32 {
        assert_eq!(dict[0].get(&i), Some(&(i + 1000)));
    }
}

/// Helper: large input — element count is preserved (no collisions in this set).
#[test]
fn test_to_dict_helper_large() {
    const N: u32 = 5_000;
    let data: Vec<(u32, u64)> = (0..N).map(|i| (i, u64::from(i) * 2)).collect();
    let p = Pipeline::default();
    let dict = from_vec(&p, data).to_dict().collect_seq().unwrap();
    assert_eq!(dict[0].len(), N as usize);
    assert_eq!(dict[0].get(&0), Some(&0));
    assert_eq!(dict[0].get(&(N - 1)), Some(&(u64::from(N - 1) * 2)));
}

/// Helper: struct key + primitive value.
#[test]
fn test_to_dict_helper_struct_key() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    struct CompositeKey {
        region: String,
        shard: u32,
    }

    let p = Pipeline::default();
    let dict = from_vec(
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
    )
    .to_dict()
    .collect_seq()
    .unwrap();

    assert_eq!(dict[0].len(), 2);
    let k_east = CompositeKey {
        region: "us-east".into(),
        shard: 0,
    };
    let k_west = CompositeKey {
        region: "eu-west".into(),
        shard: 1,
    };
    assert_eq!(dict[0].get(&k_east), Some(&100));
    assert_eq!(dict[0].get(&k_west), Some(&200));
}

// ── Composition ──────────────────────────────────────────────────────────────

/// `to_dict()` after a deterministic per-key fold gives a stable result for
/// duplicate keys (the `sum_per_key` step removes the non-determinism).
#[test]
fn test_to_dict_after_sum_per_key_is_stable() {
    let p = Pipeline::default();
    let data = vec![
        ("a".to_string(), 1u64),
        ("a".to_string(), 2),
        ("b".to_string(), 10),
        ("b".to_string(), 20),
        ("c".to_string(), 100),
    ];
    let dict = from_vec(&p, data)
        .sum_per_key()
        .to_dict()
        .collect_seq()
        .unwrap();

    let expected: HashMap<String, u64> = [
        ("a".to_string(), 3u64),
        ("b".to_string(), 30),
        ("c".to_string(), 100),
    ]
    .into_iter()
    .collect();
    assert_eq!(dict[0], expected);
}

/// `to_dict()` chained with a `map` to extract a single key's value.
#[test]
fn test_to_dict_then_map_extract_key() {
    let p = Pipeline::default();
    let out = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ],
    )
    .to_dict()
    .map(|m: &HashMap<String, u32>| m.get("b").copied().unwrap_or_default())
    .collect_seq()
    .unwrap();
    assert_eq!(out, vec![2u32]);
}

/// `to_dict()` chained with a `map` to extract map size.
#[test]
fn test_to_dict_then_map_size() {
    let p = Pipeline::default();
    let out = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4),
        ],
    )
    .to_dict()
    .map(|m: &HashMap<String, u32>| m.len())
    .collect_seq()
    .unwrap();
    assert_eq!(out, vec![4usize]);
}

// ── ToDict<K, V> construction ────────────────────────────────────────────────

/// `ToDict::<K, V>::default()` and `::new()` produce equivalent combiners.
#[test]
fn test_to_dict_default_equals_new() {
    let p1 = Pipeline::default();
    let p2 = Pipeline::default();

    let from_new = from_vec(&p1, vec![("a".to_string(), 1u32), ("b".to_string(), 2)])
        .combine_globally(ToDict::<String, u32>::new(), None)
        .collect_seq()
        .unwrap();
    let from_default = from_vec(&p2, vec![("a".to_string(), 1u32), ("b".to_string(), 2)])
        .combine_globally(ToDict::<String, u32>::default(), None)
        .collect_seq()
        .unwrap();

    assert_eq!(from_new[0], from_default[0]);
}

/// `ToDict<K, V>` satisfies the expected auto traits + `Debug`.
#[test]
fn test_to_dict_trait_obligations() {
    fn assert_copy<T: Copy>() {}
    fn assert_clone<T: Clone>() {}
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_copy::<ToDict<&str, u32>>();
    assert_clone::<ToDict<&str, u32>>();
    assert_debug::<ToDict<&str, u32>>();

    let td: ToDict<&str, u32> = ToDict::new();
    let td_clone = td;
    let dbg = format!("{td_clone:?}");
    assert!(dbg.contains("ToDict"));
}
