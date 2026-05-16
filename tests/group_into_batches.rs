//! Comprehensive tests for feature 4.6: `group_into_batches`.
//!
//! Validates that per-key values are chunked into fixed-size `Vec` batches:
//! - Total elements emitted per key match the input.
//! - Each emitted batch is ≤ `batch_size`.
//! - All values for a key are accounted for across the emitted batches.
//! - Edge cases (empty / single / `batch_size` == values / large) behave correctly.
//! - `batch_size == 0` panics.

use ironbeam::*;
use std::collections::HashMap;

/// Helper: collect output into `HashMap<K, Vec<Vec<V>>>` so tests can assert on
/// batch counts and contents regardless of partition order.
fn group_by_key_batches<K, V>(out: Vec<(K, Vec<V>)>) -> HashMap<K, Vec<Vec<V>>>
where
    K: Eq + std::hash::Hash,
{
    let mut h: HashMap<K, Vec<Vec<V>>> = HashMap::new();
    for (k, batch) in out {
        h.entry(k).or_default().push(batch);
    }
    h
}

// ── Basic batching behavior ──────────────────────────────────────────────────

/// Basic: 5 values in one key chunked into batches of 2 → batches of [2, 2, 1].
#[test]
fn test_group_into_batches_basic() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![("a", 1u32), ("a", 2), ("a", 3), ("a", 4), ("a", 5)],
    );

    let out = pairs.group_into_batches(2).collect_seq().unwrap();
    let grouped = group_by_key_batches(out);

    let batches = grouped.get("a").expect("key 'a' missing");
    assert_eq!(batches.len(), 3, "expected 3 batches for key 'a'");

    let mut sizes: Vec<usize> = batches.iter().map(Vec::len).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![1, 2, 2]);

    // All values must be present across batches.
    let mut all_values: Vec<u32> = batches.iter().flatten().copied().collect();
    all_values.sort_unstable();
    assert_eq!(all_values, vec![1u32, 2, 3, 4, 5]);
}

/// Multiple keys with different per-key totals.
#[test]
fn test_group_into_batches_multiple_keys() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a", 1u32),
            ("a", 2),
            ("a", 3),
            ("a", 4),
            ("a", 5),
            ("b", 10),
            ("b", 20),
            ("b", 30),
            ("c", 100),
        ],
    );

    let out = pairs.group_into_batches(2).collect_seq().unwrap();
    let grouped = group_by_key_batches(out);

    assert_eq!(grouped.get("a").map(Vec::len), Some(3));
    assert_eq!(grouped.get("b").map(Vec::len), Some(2));
    assert_eq!(grouped.get("c").map(Vec::len), Some(1));

    // c has just one element so its single batch should have len 1.
    assert_eq!(grouped["c"][0].len(), 1);
    assert_eq!(grouped["c"][0][0], 100);

    // b has 3 values in batches of 2 → [2, 1].
    let mut b_sizes: Vec<usize> = grouped["b"].iter().map(Vec::len).collect();
    b_sizes.sort_unstable();
    assert_eq!(b_sizes, vec![1, 2]);
}

/// Every batch must be ≤ `batch_size`.
#[test]
fn test_group_into_batches_size_bound() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, (0..17u32).map(|i| ("k", i)).collect::<Vec<_>>());

    let batch_size = 4;
    let out = pairs.group_into_batches(batch_size).collect_seq().unwrap();
    for (_, b) in &out {
        assert!(
            b.len() <= batch_size,
            "batch len {} exceeded batch_size {batch_size}",
            b.len()
        );
        assert!(!b.is_empty(), "batches should never be empty");
    }

    // 17 / 4 = 4 batches of 4 + 1 batch of 1 = 5 batches.
    assert_eq!(out.len(), 5);
}

// ── Edge cases ──────────────────────────────────────────────────────────────

/// Empty input → empty output.
#[test]
fn test_group_into_batches_empty() {
    let p = Pipeline::default();
    let pairs: Vec<(&str, u32)> = vec![];
    let out = from_vec(&p, pairs)
        .group_into_batches(5)
        .collect_seq()
        .unwrap();
    assert!(out.is_empty());
}

/// Single value per key with `batch_size` > 1 → one batch of size 1.
#[test]
fn test_group_into_batches_single_value() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![("a", 42u32)]);
    let out = pairs.group_into_batches(10).collect_seq().unwrap();
    assert_eq!(out, vec![("a", vec![42u32])]);
}

/// `batch_size` == 1 → one batch per value.
#[test]
fn test_group_into_batches_size_one() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![("a", 1u32), ("a", 2), ("a", 3)]);
    let out = pairs.group_into_batches(1).collect_seq().unwrap();
    assert_eq!(out.len(), 3);
    for (k, batch) in &out {
        assert_eq!(*k, "a");
        assert_eq!(batch.len(), 1);
    }
    let mut values: Vec<u32> = out.into_iter().flat_map(|(_, b)| b).collect();
    values.sort_unstable();
    assert_eq!(values, vec![1u32, 2, 3]);
}

/// `batch_size` larger than total values per key → one batch with everything.
#[test]
fn test_group_into_batches_size_larger_than_input() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![("a", 1u32), ("a", 2), ("a", 3)]);
    let out = pairs.group_into_batches(100).collect_seq().unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].0, "a");
    let mut batch = out[0].1.clone();
    batch.sort_unstable();
    assert_eq!(batch, vec![1u32, 2, 3]);
}

/// `batch_size` exactly equals number of values per key → exactly one full
/// batch per key.
#[test]
fn test_group_into_batches_size_equal_to_input() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a", 1u32),
            ("a", 2),
            ("a", 3),
            ("b", 10),
            ("b", 20),
            ("b", 30),
        ],
    );

    let out = pairs.group_into_batches(3).collect_seq().unwrap();
    let grouped = group_by_key_batches(out);

    assert_eq!(grouped["a"].len(), 1);
    assert_eq!(grouped["a"][0].len(), 3);
    assert_eq!(grouped["b"].len(), 1);
    assert_eq!(grouped["b"][0].len(), 3);
}

/// `batch_size` == 0 must panic.
#[test]
#[should_panic(expected = "group_into_batches requires batch_size > 0")]
fn test_group_into_batches_zero_size_panics() {
    let p = Pipeline::default();
    let _ = from_vec(&p, vec![("a", 1u32)]).group_into_batches(0);
}

// ── Parallel and large inputs ───────────────────────────────────────────────

/// Parallel execution preserves the total value count.
#[test]
fn test_group_into_batches_parallel() {
    let p = Pipeline::default();
    // 10 keys * 50 values each = 500 values
    let mut data = Vec::with_capacity(500);
    for k in 0u32..10 {
        for v in 0u32..50 {
            data.push((k, v));
        }
    }
    let pairs = from_vec(&p, data);

    let out = pairs
        .group_into_batches(7)
        .collect_par(Some(4), Some(4))
        .unwrap();

    // For each key, the total values across batches must be 50, and every
    // batch must be ≤ 7 elements.
    let grouped = group_by_key_batches(out);
    assert_eq!(grouped.len(), 10);
    for batches in grouped.values() {
        let total: usize = batches.iter().map(Vec::len).sum();
        assert_eq!(total, 50);
        for b in batches {
            assert!(b.len() <= 7);
            assert!(!b.is_empty());
        }
        // 50 / 7 = 7 batches of 7 + 1 batch of 1 = 8 batches.
        assert_eq!(batches.len(), 8);
    }
}

/// Large input: total batches = ceil(N / `batch_size`) per key.
#[test]
fn test_group_into_batches_large() {
    const N: u32 = 1_000;
    let p = Pipeline::default();
    let data: Vec<(&str, u32)> = (0..N).map(|i| ("k", i)).collect();
    let out = from_vec(&p, data)
        .group_into_batches(33)
        .collect_seq()
        .unwrap();

    // Expected: ceil(1000 / 33) = 31 batches (30 of size 33, 1 of size 10).
    assert_eq!(out.len(), 31);

    let total_values: usize = out.iter().map(|(_, b)| b.len()).sum();
    assert_eq!(total_values, N as usize);

    let mut sizes: Vec<usize> = out.iter().map(|(_, b)| b.len()).collect();
    sizes.sort_unstable();
    assert_eq!(sizes[0], 10);
    assert!(sizes.iter().skip(1).all(|&s| s == 33));
}

// ── Type variety ────────────────────────────────────────────────────────────

/// Integer keys + `String` values.
#[test]
fn test_group_into_batches_integer_keys_string_values() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            (1u32, "a".to_string()),
            (1, "b".into()),
            (1, "c".into()),
            (2, "x".into()),
            (2, "y".into()),
        ],
    );

    let out = pairs.group_into_batches(2).collect_seq().unwrap();
    let grouped = group_by_key_batches(out);

    // Key 1: 3 values → 2 batches; Key 2: 2 values → 1 batch.
    assert_eq!(grouped[&1].len(), 2);
    assert_eq!(grouped[&2].len(), 1);

    let mut k1_sizes: Vec<usize> = grouped[&1].iter().map(Vec::len).collect();
    k1_sizes.sort_unstable();
    assert_eq!(k1_sizes, vec![1, 2]);
    assert_eq!(grouped[&2][0].len(), 2);
}

/// Struct values pass through.
#[test]
fn test_group_into_batches_struct_values() {
    #[derive(Clone, Debug, PartialEq)]
    struct Event {
        kind: &'static str,
        id: u32,
    }

    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("svc", Event { kind: "a", id: 1 }),
            ("svc", Event { kind: "b", id: 2 }),
            ("svc", Event { kind: "c", id: 3 }),
        ],
    );

    let out = pairs.group_into_batches(2).collect_seq().unwrap();
    assert_eq!(out.len(), 2);
    let total_events: usize = out.iter().map(|(_, b)| b.len()).sum();
    assert_eq!(total_events, 3);
    for (k, _) in &out {
        assert_eq!(*k, "svc");
    }
}

// ── Composition ──────────────────────────────────────────────────────────────

/// `group_into_batches` chained with `map` over the batches.
#[test]
fn test_group_into_batches_then_map_sum_per_batch() {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![("a", 1u32), ("a", 2), ("a", 3), ("a", 4), ("a", 5)],
    );

    // Per batch, compute the sum of values.
    let sums: Vec<(&str, u32)> = pairs
        .group_into_batches(2)
        .map(|(k, batch): &(&str, Vec<u32>)| (*k, batch.iter().sum()))
        .collect_seq()
        .unwrap();

    assert_eq!(sums.len(), 3); // 3 batches → 3 partial sums

    // Their total should equal 1+2+3+4+5 = 15
    let total: u32 = sums.iter().map(|(_, s)| s).sum();
    assert_eq!(total, 15);
}

/// `group_into_batches` followed by `values()` flattens to per-batch lists.
#[test]
fn test_group_into_batches_then_values() {
    let p = Pipeline::default();
    let pairs = from_vec(&p, vec![("a", 1u32), ("a", 2), ("a", 3)]);
    let mut batches = pairs.group_into_batches(2).values().collect_seq().unwrap();
    batches.sort_by_key(Vec::len);
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].len(), 1);
    assert_eq!(batches[1].len(), 2);
}
