//! Comprehensive tests for feature 4.2: `KvSwap` transform.
//!
//! This test suite validates `kv_swap()` — swaps the key and value of each
//! `(K, V)` pair, producing `PCollection<(V, K)>`.

use ironbeam::*;

/// Basic: swap `(String, u32)` -> `(u32, String)`.
#[test]
fn test_kv_swap_basic() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("alpha".to_string(), 1u32),
            ("beta".to_string(), 2),
            ("gamma".to_string(), 3),
        ],
    );

    let mut out = pairs.kv_swap().collect_seq().unwrap();
    out.sort_by_key(|(k, _)| *k);

    assert_eq!(
        out,
        vec![
            (1u32, "alpha".to_string()),
            (2, "beta".to_string()),
            (3, "gamma".to_string()),
        ]
    );
}

/// Swap `(i32, String)` -> `(String, i32)`.
#[test]
fn test_kv_swap_integer_to_string() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            (10i32, "x".to_string()),
            (20, "y".to_string()),
            (30, "z".to_string()),
        ],
    );

    let mut out = pairs.kv_swap().collect_seq().unwrap();
    out.sort();

    assert_eq!(
        out,
        vec![
            ("x".to_string(), 10i32),
            ("y".to_string(), 20),
            ("z".to_string(), 30),
        ]
    );
}

/// Edge case: empty collection produces no pairs.
#[test]
fn test_kv_swap_empty() {
    let p = Pipeline::default();
    let pairs: Vec<(String, u32)> = vec![];
    let result = from_vec(&p, pairs).kv_swap().collect_seq().unwrap();
    assert!(result.is_empty());
}

/// Edge case: single-element collection.
#[test]
fn test_kv_swap_single_element() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![("only".to_string(), 42u32)]);
    let out = pairs.kv_swap().collect_seq().unwrap();
    assert_eq!(out, vec![(42u32, "only".to_string())]);
}

/// Parallel execution preserves all swapped pairs (unordered).
#[test]
fn test_kv_swap_parallel() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, (0..20u32).map(|i| (i, i * 2)).collect::<Vec<_>>());

    let mut out = pairs.kv_swap().collect_par(Some(4), Some(4)).unwrap();
    out.sort_unstable();

    let expected: Vec<(u32, u32)> = (0..20).map(|i| (i * 2, i)).collect();
    assert_eq!(out, expected);
}

/// Duplicates are preserved across the swap.
#[test]
fn test_kv_swap_duplicates_preserved() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 1),
            ("b".to_string(), 2),
        ],
    );

    let mut out = pairs.kv_swap().collect_seq().unwrap();
    out.sort();

    assert_eq!(
        out,
        vec![
            (1u32, "a".to_string()),
            (1, "a".to_string()),
            (2, "b".to_string()),
        ]
    );
}

/// Double swap returns the original pairs.
#[test]
fn test_kv_swap_double_is_identity() {
    let p = Pipeline::default();
    let original = vec![
        ("a".to_string(), 1u32),
        ("b".to_string(), 2),
        ("c".to_string(), 3),
    ];

    let mut out = from_vec(&p, original.clone())
        .kv_swap()
        .kv_swap()
        .collect_seq()
        .unwrap();
    out.sort();

    let mut expected = original;
    expected.sort();

    assert_eq!(out, expected);
}

/// `kv_swap()` chained with `keys()` extracts the original values.
#[test]
fn test_kv_swap_then_keys_yields_original_values() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 10u32),
            ("b".to_string(), 20),
            ("c".to_string(), 30),
        ],
    );

    let mut out = pairs.kv_swap().keys().collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![10u32, 20, 30]);
}

/// `kv_swap()` chained with `values()` extracts the original keys.
#[test]
fn test_kv_swap_then_values_yields_original_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 10u32),
            ("b".to_string(), 20),
            ("c".to_string(), 30),
        ],
    );

    let mut out = pairs.kv_swap().values().collect_seq().unwrap();
    out.sort();

    assert_eq!(out, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
}

/// `kv_swap()` chained with `group_by_key` groups by the original *values*.
#[test]
fn test_kv_swap_then_group_by_key() {
    let p = Pipeline::default();
    // Original: (label, bucket). After swap: (bucket, label).
    // Grouping by bucket then yields the labels per bucket.
    let pairs = from_vec(
        &p,
        vec![
            ("alpha".to_string(), 0u32),
            ("beta".to_string(), 1),
            ("gamma".to_string(), 0),
            ("delta".to_string(), 1),
        ],
    );

    let mut out = pairs.kv_swap().group_by_key().collect_seq().unwrap();
    out.sort_by_key(|(k, _)| *k);

    assert_eq!(out.len(), 2);
    let mut bucket_zero = out[0].1.clone();
    bucket_zero.sort();
    assert_eq!(bucket_zero, vec!["alpha".to_string(), "gamma".to_string()]);
    let mut bucket_one = out[1].1.clone();
    bucket_one.sort();
    assert_eq!(bucket_one, vec!["beta".to_string(), "delta".to_string()]);
}

/// `kv_swap()` followed by `map_values` operates on the post-swap value (i.e. original key).
#[test]
fn test_kv_swap_then_map_values() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("hello".to_string(), 1u32),
            ("world".to_string(), 2),
            ("rust".to_string(), 3),
        ],
    );

    let mut out = pairs
        .kv_swap()
        .map_values(std::string::String::len)
        .collect_seq()
        .unwrap();
    out.sort_by_key(|(k, _)| *k);

    assert_eq!(out, vec![(1u32, 5), (2, 5), (3, 4)]);
}

/// `kv_swap()` followed by a filter on the new key (original value).
#[test]
fn test_kv_swap_then_filter_by_new_key() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
            ("d".to_string(), 4),
        ],
    );

    let mut out = pairs
        .kv_swap()
        .filter(|(k, _): &(u32, String)| *k > 2)
        .collect_seq()
        .unwrap();
    out.sort();

    assert_eq!(out, vec![(3u32, "c".to_string()), (4, "d".to_string())]);
}

/// Struct key, primitive value — verifies swap works with non-trivial types.
#[test]
fn test_kv_swap_struct_key() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

    let mut out = pairs.kv_swap().collect_seq().unwrap();
    out.sort_by_key(|(k, _)| *k);

    assert_eq!(
        out,
        vec![
            (
                100u64,
                CompositeKey {
                    region: "us-east".into(),
                    shard: 0,
                },
            ),
            (
                200u64,
                CompositeKey {
                    region: "eu-west".into(),
                    shard: 1,
                },
            ),
        ]
    );
}

/// Primitive key, struct value — swap moves the struct into the key position.
#[test]
fn test_kv_swap_struct_value() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
    struct Payload {
        kind: String,
        size: u32,
    }

    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            (
                1u32,
                Payload {
                    kind: "a".into(),
                    size: 10,
                },
            ),
            (
                2u32,
                Payload {
                    kind: "b".into(),
                    size: 20,
                },
            ),
        ],
    );

    let mut out = pairs.kv_swap().collect_seq().unwrap();
    out.sort();

    assert_eq!(
        out,
        vec![
            (
                Payload {
                    kind: "a".into(),
                    size: 10,
                },
                1u32,
            ),
            (
                Payload {
                    kind: "b".into(),
                    size: 20,
                },
                2u32,
            ),
        ]
    );
}

/// Same-type key and value: `(u32, u32)` -> `(u32, u32)`.
#[test]
fn test_kv_swap_homogeneous_pair() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![(1u32, 100u32), (2, 200), (3, 300)]);

    let mut out = pairs.kv_swap().collect_seq().unwrap();
    out.sort_unstable();

    assert_eq!(out, vec![(100u32, 1u32), (200, 2), (300, 3)]);
}

/// Swap is order-preserving when run with a single shard.
#[test]
fn test_kv_swap_order_preserved_sequential() {
    let p = Pipeline::default();
    let input: Vec<(u32, String)> = (0..10u32).map(|i| (i, format!("v{i}"))).collect();
    let pairs = from_vec(&p, input.clone());

    let out = pairs.kv_swap().collect_seq().unwrap();
    let expected: Vec<(String, u32)> = input.into_iter().map(|(k, v)| (v, k)).collect();

    assert_eq!(out, expected);
}

/// Large collection: element count is preserved after swap.
#[test]
fn test_kv_swap_large_collection_count() {
    const N: u32 = 1000;
    let data: Vec<(u32, u32)> = (0..N).map(|i| (i, i * 10)).collect();
    let p = Pipeline::default();

    let out = from_vec(&p, data).kv_swap().collect_seq().unwrap();
    assert_eq!(out.len(), N as usize);
}

/// Swap followed by aggregation across the new keys.
#[test]
fn test_kv_swap_then_aggregate() {
    let p = Pipeline::default();
    // Pairs (label, group_id). After swap, group_id becomes the key.
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 1),
            ("c".to_string(), 2),
            ("d".to_string(), 2),
            ("e".to_string(), 2),
        ],
    );

    let mut out = pairs.kv_swap().count_per_key().collect_seq().unwrap();
    out.sort_by_key(|(k, _)| *k);

    assert_eq!(out, vec![(1u32, 2u64), (2, 3)]);
}

/// Key type that is not `Hash` is allowed for `kv_swap` itself.
///
/// This is the key differentiator over `keys()` / `values()` (which both
/// require `K: Hash` because they live in the keyed impl block). `kv_swap`
/// only requires `RFBound`, so a non-hashable key type works.
#[test]
fn test_kv_swap_non_hash_key() {
    #[derive(Clone, Debug, PartialEq, PartialOrd)]
    struct NonHashKey(f64);

    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            (NonHashKey(1.0), "a".to_string()),
            (NonHashKey(2.0), "b".to_string()),
        ],
    );

    // After swap, pairs are `(String, NonHashKey)`.
    let out = pairs.kv_swap().collect_seq().unwrap();
    assert_eq!(out.len(), 2);
    // Find each by string identity (NonHashKey is not Hash/Ord, so sort manually).
    let a = out.iter().find(|(s, _)| s == "a").unwrap();
    let b = out.iter().find(|(s, _)| s == "b").unwrap();
    assert_eq!(a.1, NonHashKey(1.0));
    assert_eq!(b.1, NonHashKey(2.0));
}
