//! Comprehensive tests for feature 4.7: `batch_elements` + `batch_by_size`.
//!
//! Validates that elements within each partition are grouped into `Vec<T>`
//! batches by either element count or estimated byte size, with predictable
//! batch boundaries, invariants under parallel execution, and well-defined
//! handling for oversized single elements.

use ironbeam::*;

// ── batch_elements (count-based) ─────────────────────────────────────────────

/// Basic: 10 elements in batches of 3 (sequential) → [3, 3, 3, 1].
#[test]
fn test_batch_elements_basic_sequential() {
    let p = Pipeline::default();
    let data = from_vec(&p, (0u32..10).collect::<Vec<_>>());
    let batches = data.batch_elements(3).collect_seq().unwrap();

    assert_eq!(batches.len(), 4);
    assert_eq!(batches[0], vec![0u32, 1, 2]);
    assert_eq!(batches[1], vec![3u32, 4, 5]);
    assert_eq!(batches[2], vec![6u32, 7, 8]);
    assert_eq!(batches[3], vec![9u32]);
}

/// Empty input produces no batches.
#[test]
fn test_batch_elements_empty() {
    let p = Pipeline::default();
    let data: Vec<u32> = vec![];
    let batches = from_vec(&p, data).batch_elements(5).collect_seq().unwrap();
    assert!(batches.is_empty());
}

/// Single element produces a single batch of size 1.
#[test]
fn test_batch_elements_single() {
    let p = Pipeline::default();
    let batches = from_vec(&p, vec![42u32])
        .batch_elements(10)
        .collect_seq()
        .unwrap();
    assert_eq!(batches, vec![vec![42u32]]);
}

/// `batch_size == 1` → one batch per element.
#[test]
fn test_batch_elements_size_one() {
    let p = Pipeline::default();
    let batches = from_vec(&p, vec![1u32, 2, 3])
        .batch_elements(1)
        .collect_seq()
        .unwrap();
    assert_eq!(batches, vec![vec![1u32], vec![2], vec![3]]);
}

/// `batch_size == 0` is silently clamped to 1 (no panic).
#[test]
fn test_batch_elements_size_zero_clamped_to_one() {
    let p = Pipeline::default();
    let batches = from_vec(&p, vec![1u32, 2, 3])
        .batch_elements(0)
        .collect_seq()
        .unwrap();
    // With clamp, batch_size = 1 → 3 single-element batches.
    assert_eq!(batches.len(), 3);
    for b in &batches {
        assert_eq!(b.len(), 1);
    }
}

/// `batch_size` larger than input → one batch with everything.
#[test]
fn test_batch_elements_size_larger_than_input() {
    let p = Pipeline::default();
    let batches = from_vec(&p, vec![1u32, 2, 3])
        .batch_elements(100)
        .collect_seq()
        .unwrap();
    assert_eq!(batches, vec![vec![1u32, 2, 3]]);
}

/// `batch_size` exactly equal to input size → exactly one batch.
#[test]
fn test_batch_elements_size_equal_to_input() {
    let p = Pipeline::default();
    let batches = from_vec(&p, vec![1u32, 2, 3, 4])
        .batch_elements(4)
        .collect_seq()
        .unwrap();
    assert_eq!(batches, vec![vec![1u32, 2, 3, 4]]);
}

/// Parallel execution: every batch is ≤ `batch_size`, all elements are
/// preserved, and the total batch count may exceed the sequential count
/// (because batches can't span partition boundaries).
#[test]
fn test_batch_elements_parallel_invariants() {
    let p = Pipeline::default();
    let n = 1000u32;
    let data = from_vec(&p, (0..n).collect::<Vec<_>>());
    let batches = data
        .batch_elements(7)
        .collect_par(Some(4), Some(4))
        .unwrap();

    // Every batch ≤ 7 and never empty.
    for b in &batches {
        assert!(!b.is_empty());
        assert!(b.len() <= 7);
    }

    // All elements present exactly once.
    let mut flat: Vec<u32> = batches.into_iter().flatten().collect();
    flat.sort_unstable();
    assert_eq!(flat, (0..n).collect::<Vec<_>>());
}

/// Sequential ordering within a single partition is preserved.
#[test]
fn test_batch_elements_preserves_order_sequential() {
    let p = Pipeline::default();
    let n = 20u32;
    let data: Vec<u32> = (0..n).collect();
    let batches = from_vec(&p, data.clone())
        .batch_elements(4)
        .collect_seq()
        .unwrap();
    let flat: Vec<u32> = batches.into_iter().flatten().collect();
    assert_eq!(flat, data);
}

/// Large input: with `batch_size = 33` over 1000 elements (sequential), we
/// expect 31 batches (30 of size 33, 1 of size 10).
#[test]
fn test_batch_elements_large_sequential() {
    let p = Pipeline::default();
    let data: Vec<u32> = (0..1_000u32).collect();
    let batches = from_vec(&p, data).batch_elements(33).collect_seq().unwrap();

    assert_eq!(batches.len(), 31);
    let mut sizes: Vec<usize> = batches.iter().map(Vec::len).collect();
    sizes.sort_unstable();
    assert_eq!(sizes[0], 10);
    assert!(sizes.iter().skip(1).all(|&s| s == 33));
}

/// Struct elements pass through faithfully.
#[test]
fn test_batch_elements_struct_elements() {
    #[derive(Clone, Debug, PartialEq)]
    struct Item {
        id: u32,
        tag: &'static str,
    }

    let p = Pipeline::default();
    let items: Vec<Item> = (0..5u32).map(|i| Item { id: i, tag: "x" }).collect();
    let batches = from_vec(&p, items).batch_elements(2).collect_seq().unwrap();

    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0].len(), 2);
    assert_eq!(batches[1].len(), 2);
    assert_eq!(batches[2].len(), 1);
    let total: usize = batches.iter().map(Vec::len).sum();
    assert_eq!(total, 5);
}

// ── batch_by_size (size-based) ───────────────────────────────────────────────

/// Basic: byte-size example from the module docs. Elements [2, 2, 4, 1] bytes
/// with `max_bytes = 4` → [[2, 2], [4], [1]].
#[test]
fn test_batch_by_size_basic_sequential() {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec!["aa".to_string(), "bb".into(), "cccc".into(), "d".into()],
    );
    let batches = data
        .batch_by_size(4, |s: &String| s.len())
        .collect_seq()
        .unwrap();

    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0], vec!["aa".to_string(), "bb".into()]);
    assert_eq!(batches[1], vec!["cccc".to_string()]);
    assert_eq!(batches[2], vec!["d".to_string()]);
}

/// Empty input produces no batches.
#[test]
fn test_batch_by_size_empty() {
    let p = Pipeline::default();
    let data: Vec<String> = vec![];
    let batches = from_vec(&p, data)
        .batch_by_size(100, |s: &String| s.len())
        .collect_seq()
        .unwrap();
    assert!(batches.is_empty());
}

/// Single element produces a single batch of that element (even if oversized).
#[test]
fn test_batch_by_size_single_element() {
    let p = Pipeline::default();
    let batches = from_vec(&p, vec!["only".to_string()])
        .batch_by_size(100, |s: &String| s.len())
        .collect_seq()
        .unwrap();
    assert_eq!(batches, vec![vec!["only".to_string()]]);
}

/// Oversized single element: still emitted alone in its own batch (never
/// silently dropped or partitioned).
#[test]
fn test_batch_by_size_oversized_single_element() {
    let p = Pipeline::default();
    let huge = "x".repeat(50);
    let data = from_vec(
        &p,
        vec![
            "a".to_string(),
            huge.clone(),
            "b".to_string(),
            "c".to_string(),
        ],
    );

    let batches = data
        .batch_by_size(5, |s: &String| s.len())
        .collect_seq()
        .unwrap();

    // Expected: ["a"] (size 1) — then encountering huge (50) forces flush of
    // "a" first; but "a" alone is size 1 ≤ 5, so it would *continue* to grow
    // if the next element fits. Concretely:
    //   - current = ["a"], current_bytes = 1
    //   - next "x"*50 (size 50): current non-empty AND 1+50 > 5 → flush ["a"];
    //     current = ["x"*50], current_bytes = 50
    //   - next "b" (size 1): current non-empty AND 50+1 > 5 → flush ["x"*50];
    //     current = ["b"], current_bytes = 1
    //   - next "c" (size 1): 1+1 ≤ 5 → stays in current; current = ["b","c"]
    //   - flush final ["b","c"].
    // → 3 batches: ["a"], ["x"*50], ["b", "c"]
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0], vec!["a".to_string()]);
    assert_eq!(batches[1], vec![huge]);
    assert_eq!(batches[2], vec!["b".to_string(), "c".to_string()]);
}

/// `max_bytes == 0` is silently clamped to 1 → each element ends up alone.
#[test]
fn test_batch_by_size_max_bytes_zero_clamped_to_one() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec!["a".to_string(), "b".into(), "c".into()]);
    let batches = data
        .batch_by_size(0, |s: &String| s.len())
        .collect_seq()
        .unwrap();
    // Each element has size 1. With max_bytes clamped to 1, the second element
    // would push total to 2 > 1, so each batch contains exactly one element.
    assert_eq!(
        batches,
        vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["c".to_string()],
        ]
    );
}

/// All-zero-sized elements: everything fits in a single batch.
#[test]
fn test_batch_by_size_zero_sized_elements() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![(); 10]); // zero-sized
    let batches = data.batch_by_size(1, |(): &()| 0).collect_seq().unwrap();
    // Every element contributes 0 — they all fit in one batch.
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 10);
}

/// Element size exactly hits the limit on every element.
#[test]
fn test_batch_by_size_exact_fit() {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec!["ab".to_string(), "cd".into(), "ef".into(), "gh".into()],
    );
    let batches = data
        .batch_by_size(4, |s: &String| s.len()) // 2 elements per batch fit exactly
        .collect_seq()
        .unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0], vec!["ab".to_string(), "cd".into()]);
    assert_eq!(batches[1], vec!["ef".to_string(), "gh".into()]);
}

/// Parallel execution: every batch (excluding lone oversized elements) is
/// ≤ `max_bytes`, and all elements are preserved.
#[test]
fn test_batch_by_size_parallel_invariants() {
    let p = Pipeline::default();
    // 100 strings of length 7 → each contributes 7 bytes.
    let data: Vec<String> = (0..100u32).map(|i| format!("{i:07}")).collect();
    let pc = from_vec(&p, data.clone());
    let batches = pc
        .batch_by_size(20, |s: &String| s.len())
        .collect_par(Some(4), Some(4))
        .unwrap();

    for b in &batches {
        assert!(!b.is_empty());
        let bytes: usize = b.iter().map(String::len).sum();
        // Either ≤ max_bytes, OR a single oversized element (no element here
        // exceeds 20 alone, so this branch shouldn't trigger).
        assert!(
            bytes <= 20 || b.len() == 1,
            "batch with {} elements and {bytes} bytes violates invariant",
            b.len()
        );
    }

    let mut flat: Vec<String> = batches.into_iter().flatten().collect();
    flat.sort();
    let mut expected = data;
    expected.sort();
    assert_eq!(flat, expected);
}

/// Custom-size function: count bytes via `serde_json` serialization.
#[test]
fn test_batch_by_size_custom_size_fn() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 22, 333, 4444]);
    // size_fn: `1` becomes "1" (1 byte), `22` → "22" (2 bytes), etc.
    let batches = data
        .batch_by_size(3, |n: &u32| n.to_string().len())
        .collect_seq()
        .unwrap();
    // 1 (1) -> current = [1], bytes=1
    // 22 (2) -> 1+2=3 ≤ 3 → current=[1,22], bytes=3
    // 333 (3) -> 3+3=6 > 3 → flush [1,22]; current=[333], bytes=3
    // 4444 (4) -> 3+4=7 > 3 → flush [333]; current=[4444], bytes=4
    // → 3 batches.
    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0], vec![1u32, 22]);
    assert_eq!(batches[1], vec![333u32]);
    assert_eq!(batches[2], vec![4444u32]);
}

// ── Composition ──────────────────────────────────────────────────────────────

/// `batch_elements` chained with `map` to compute per-batch statistics.
#[test]
fn test_batch_elements_then_map_per_batch_sum() {
    let p = Pipeline::default();
    let data = from_vec(&p, (1u32..=10).collect::<Vec<_>>());
    let sums = data
        .batch_elements(3)
        .map(|batch: &Vec<u32>| batch.iter().sum::<u32>())
        .collect_seq()
        .unwrap();

    // Batches: [1,2,3], [4,5,6], [7,8,9], [10] → sums 6, 15, 24, 10
    assert_eq!(sums, vec![6u32, 15, 24, 10]);
}

/// `batch_by_size` chained with `flat_map` to flatten back to elements
/// is equivalent to identity (count preserved).
#[test]
fn test_batch_by_size_then_flat_map_preserves_count() {
    let p = Pipeline::default();
    let data: Vec<String> = (0..50u32).map(|i| format!("v{i:02}")).collect();
    let pc = from_vec(&p, data.clone());

    let flat = pc
        .batch_by_size(7, |s: &String| s.len())
        .flat_map(|batch: &Vec<String>| batch.clone())
        .collect_seq()
        .unwrap();

    let mut got = flat;
    got.sort();
    let mut expected = data;
    expected.sort();
    assert_eq!(got, expected);
}
