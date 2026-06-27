//! Comprehensive tests for feature 4.4: `count_per_element`.
//!
//! The `count_per_element` helper has existed since v2.4.0 (originally added
//! alongside the `Count` combiner). This file adds exhaustive coverage of the
//! helper API at the `PCollection` level: edge cases, parallel execution,
//! various element types, chaining, and large inputs.

use ironbeam::*;
use serde::{Deserialize, Serialize};

// ── Basic correctness ───────────────────────────────────────────────────────

/// Basic: distinct count of `&str` elements.
#[test]
fn test_count_per_element_str_basic() {
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
    let counts = data.count_per_element().collect_seq_sorted().unwrap();
    assert_eq!(
        counts,
        vec![
            ("a".to_string(), 3u64),
            ("b".to_string(), 2),
            ("c".to_string(), 1)
        ]
    );
}

/// Basic: distinct count of `u32` elements.
#[test]
fn test_count_per_element_u32_basic() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 2, 1, 3, 1, 2, 4]);
    let mut counts = data.count_per_element().collect_seq().unwrap();
    counts.sort_unstable_by_key(|(k, _)| *k);
    assert_eq!(counts, vec![(1u32, 3u64), (2, 2), (3, 1), (4, 1)]);
}

/// Basic: distinct count with owned `String` elements (requires `Clone + Hash + Eq`).
#[test]
fn test_count_per_element_string() {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            "hello".to_string(),
            "world".into(),
            "hello".into(),
            "rust".into(),
            "hello".into(),
        ],
    );
    let mut counts = data.count_per_element().collect_seq().unwrap();
    counts.sort();
    assert_eq!(
        counts,
        vec![
            ("hello".to_string(), 3u64),
            ("rust".into(), 1),
            ("world".into(), 1),
        ]
    );
}

// ── Edge cases ──────────────────────────────────────────────────────────────

/// Empty input produces empty output.
#[test]
fn test_count_per_element_empty() {
    let p = Pipeline::default();
    let data: Vec<String> = vec![];
    let counts = from_vec(&p, data)
        .count_per_element()
        .collect_seq()
        .unwrap();
    assert!(counts.is_empty());
}

/// Single element produces a single `(elem, 1)` pair.
#[test]
fn test_count_per_element_single_element() {
    let p = Pipeline::default();
    let counts = from_vec(&p, vec!["only".to_string()])
        .count_per_element()
        .collect_seq()
        .unwrap();
    assert_eq!(counts, vec![("only".to_string(), 1u64)]);
}

/// All distinct elements: each has count 1.
#[test]
fn test_count_per_element_all_distinct() {
    let p = Pipeline::default();
    let counts = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .count_per_element()
        .collect_seq_sorted()
        .unwrap();
    assert_eq!(counts, vec![(1u32, 1u64), (2, 1), (3, 1), (4, 1), (5, 1)]);
}

/// All elements identical: a single entry with the full count.
#[test]
fn test_count_per_element_all_same() {
    let p = Pipeline::default();
    let counts = from_vec(&p, vec!["x".to_string(); 100])
        .count_per_element()
        .collect_seq()
        .unwrap();
    assert_eq!(counts, vec![("x".to_string(), 100u64)]);
}

// ── Parallel execution ──────────────────────────────────────────────────────

/// Parallel execution produces the same counts.
#[test]
fn test_count_per_element_parallel() {
    let p = Pipeline::default();
    // 0..50 appears once, then 0..50 again — each value should appear twice.
    let mut data: Vec<u32> = (0..50).collect();
    data.extend(0..50);
    let counts = from_vec(&p, data)
        .count_per_element()
        .collect_par(Some(4), Some(4))
        .unwrap();

    assert_eq!(counts.len(), 50);
    for (_, c) in &counts {
        assert_eq!(*c, 2);
    }
}

/// Parallel: skewed key distribution (one hot key dominates).
#[test]
fn test_count_per_element_parallel_skewed() {
    let p = Pipeline::default();
    let mut data: Vec<u32> = vec![0u32; 1_000]; // hot key
    data.extend(1..=50); // cold keys
    let mut counts = from_vec(&p, data)
        .count_per_element()
        .collect_par(Some(8), Some(8))
        .unwrap();
    counts.sort_unstable_by_key(|(k, _)| *k);

    assert_eq!(counts.len(), 51);
    assert_eq!(counts[0], (0u32, 1_000u64));
    for (i, (k, c)) in counts.iter().skip(1).enumerate() {
        let expected_key = u32::try_from(i).unwrap() + 1;
        assert_eq!(*k, expected_key);
        assert_eq!(*c, 1);
    }
}

// ── Element types ───────────────────────────────────────────────────────────

/// Counts over tuple elements (`(K, V)`-typed input, but counted as opaque
/// elements rather than per-key).
#[test]
fn test_count_per_element_tuple_elements() {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 1), // duplicate
            ("a".to_string(), 2),
            ("b".to_string(), 1),
            ("b".to_string(), 1), // duplicate
        ],
    );
    let mut counts = data.count_per_element().collect_seq().unwrap();
    counts.sort_unstable();
    assert_eq!(
        counts,
        vec![
            (("a".to_string(), 1u32), 2u64),
            (("a".to_string(), 2), 1),
            (("b".to_string(), 1), 2),
        ]
    );
}

/// Counts over a custom struct (verifies `Hash + Eq` on derived types).
#[test]
fn test_count_per_element_struct_elements() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    struct Tag {
        scope: String,
        id: u32,
    }

    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            Tag {
                scope: "x".to_string(),
                id: 1,
            },
            Tag {
                scope: "x".to_string(),
                id: 1,
            },
            Tag {
                scope: "y".to_string(),
                id: 2,
            },
        ],
    );
    let mut counts = data.count_per_element().collect_seq().unwrap();
    counts.sort();
    assert_eq!(
        counts,
        vec![
            (
                Tag {
                    scope: "x".to_string(),
                    id: 1
                },
                2u64,
            ),
            (
                Tag {
                    scope: "y".to_string(),
                    id: 2
                },
                1,
            ),
        ]
    );
}

/// Counts over `Option<T>` (verifies hashing through `Option`).
#[test]
fn test_count_per_element_option_elements() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![Some(1u32), None, Some(1), Some(2), None, None]);
    let mut counts = data.count_per_element().collect_seq().unwrap();
    counts.sort();
    assert_eq!(counts, vec![(None, 3u64), (Some(1), 2), (Some(2), 1)]);
}

// ── Composition / chaining ──────────────────────────────────────────────────

/// `count_per_element` chained with `filter` to retain only frequent elements.
#[test]
fn test_count_per_element_then_filter_frequent() {
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
            "d".to_string(),
        ], // counts: a=3, b=2, c=1, d=1
    );
    let mut frequent = data
        .count_per_element()
        .filter(|(_, c): &(String, u64)| *c >= 2)
        .collect_seq()
        .unwrap();
    frequent.sort_unstable();
    assert_eq!(
        frequent,
        vec![("a".to_string(), 3u64), ("b".to_string(), 2)]
    );
}

/// `count_per_element` chained with `keys()` returns the distinct values
/// (equivalent to `distinct`).
#[test]
fn test_count_per_element_then_keys_yields_distinct() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 1, 2, 3, 3, 3, 4]);
    let mut distinct = data.count_per_element().keys().collect_seq().unwrap();
    distinct.sort_unstable();
    assert_eq!(distinct, vec![1u32, 2, 3, 4]);
}

/// `count_per_element` chained with `values()` returns just the counts.
#[test]
fn test_count_per_element_then_values_yields_counts() {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            "a".to_string(),
            "b".to_string(),
            "a".to_string(),
            "a".to_string(),
            "c".to_string(),
        ],
    );
    let mut counts = data.count_per_element().values().collect_seq().unwrap();
    counts.sort_unstable();
    assert_eq!(counts, vec![1u64, 1, 3]);
}

/// `count_per_element` followed by `sum_per_key` over the counts is a no-op
/// (because each element is already distinct after `count_per_element`).
#[test]
fn test_count_per_element_total_equals_input_size() {
    let p = Pipeline::default();
    let input: Vec<u32> = vec![1, 2, 1, 3, 1, 2, 4];
    let expected_total: u64 = input.len() as u64;

    let total = from_vec(&p, input)
        .count_per_element()
        .values()
        .sum_globally()
        .collect_seq()
        .unwrap();
    assert_eq!(total, vec![expected_total]);
}

// ── Large input ─────────────────────────────────────────────────────────────

/// Large input with many distinct elements and known counts.
#[test]
fn test_count_per_element_large() {
    const N: u32 = 100;
    let p = Pipeline::default();
    // Element `i` appears `i + 1` times: 1 -> 2 -> ... -> N+1 occurrences.
    let mut data = Vec::new();
    for i in 0..N {
        for _ in 0..=i {
            data.push(i);
        }
    }
    let mut counts = from_vec(&p, data)
        .count_per_element()
        .collect_seq()
        .unwrap();
    counts.sort_unstable_by_key(|(k, _)| *k);

    assert_eq!(counts.len(), N as usize);
    for (k, c) in &counts {
        assert_eq!(*c, u64::from(*k) + 1);
    }
}
