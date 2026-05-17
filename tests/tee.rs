//! Comprehensive tests for feature 4.9: `tee` / `tee_n`.
//!
//! Validates fan-out semantics: each branch independently observes the same
//! upstream contents, can apply distinct transforms, and triggers the
//! pipeline's existing cache-placement behavior for shared upstream nodes.

use ironbeam::*;

// ── tee() — two-way split ────────────────────────────────────────────────────

/// Basic: two branches each observe all source elements.
#[test]
fn test_tee_basic() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let (a, b) = source.tee();

    let mut a_out = a.collect_seq().unwrap();
    let mut b_out = b.collect_seq().unwrap();
    a_out.sort_unstable();
    b_out.sort_unstable();

    assert_eq!(a_out, vec![1u32, 2, 3, 4, 5]);
    assert_eq!(b_out, vec![1u32, 2, 3, 4, 5]);
}

/// Branches can apply different transforms independently.
#[test]
fn test_tee_different_transforms() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let (a, b) = source.tee();

    let doubled = a.map(|x| x * 2).collect_seq_sorted().unwrap();
    let squared = b.map(|x| x * x).collect_seq_sorted().unwrap();

    assert_eq!(doubled, vec![2u32, 4, 6, 8, 10]);
    assert_eq!(squared, vec![1u32, 4, 9, 16, 25]);
}

/// Filters on each branch are independent.
#[test]
fn test_tee_independent_filters() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3, 4, 5, 6]);
    let (a, b) = source.tee();

    let evens = a.filter(|x| x % 2 == 0).collect_seq_sorted().unwrap();
    let odds = b.filter(|x| x % 2 == 1).collect_seq_sorted().unwrap();

    assert_eq!(evens, vec![2u32, 4, 6]);
    assert_eq!(odds, vec![1u32, 3, 5]);
}

/// Edge case: empty input produces empty results on both branches.
#[test]
fn test_tee_empty() {
    let p = Pipeline::default();
    let source = from_vec(&p, Vec::<u32>::new());
    let (a, b) = source.tee();

    assert!(a.collect_seq().unwrap().is_empty());
    assert!(b.collect_seq().unwrap().is_empty());
}

/// Edge case: single-element source.
#[test]
fn test_tee_single_element() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![42u32]);
    let (a, b) = source.tee();

    assert_eq!(a.collect_seq().unwrap(), vec![42u32]);
    assert_eq!(b.collect_seq().unwrap(), vec![42u32]);
}

/// Both branches see upstream transforms applied before the `tee`.
#[test]
fn test_tee_after_upstream_transform() {
    let p = Pipeline::default();
    let pre = from_vec(&p, vec![1u32, 2, 3, 4, 5]).map(|x| x + 10);
    let (a, b) = pre.tee();

    let mut a_out = a.collect_seq().unwrap();
    let mut b_out = b.collect_seq().unwrap();
    a_out.sort_unstable();
    b_out.sort_unstable();
    assert_eq!(a_out, vec![11u32, 12, 13, 14, 15]);
    assert_eq!(b_out, vec![11u32, 12, 13, 14, 15]);
}

/// Branches share the upstream `node_id` — they reference the same graph node.
#[test]
fn test_tee_same_upstream_node_id() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3]);
    let upstream_id = source.node_id();
    let (a, b) = source.tee();

    assert_eq!(a.node_id(), upstream_id);
    assert_eq!(b.node_id(), upstream_id);
    assert_eq!(a.node_id(), b.node_id());
}

/// Parallel execution of one branch while the other runs sequentially.
#[test]
fn test_tee_mixed_execution_modes() {
    let p = Pipeline::default();
    let source = from_vec(&p, (0u32..50).collect::<Vec<_>>());
    let (a, b) = source.tee();

    let par_out = a.collect_par(Some(4), Some(4)).unwrap();
    let seq_out = b.collect_seq().unwrap();
    assert_eq!(par_out.len(), 50);
    assert_eq!(seq_out.len(), 50);

    let mut par_sorted = par_out;
    par_sorted.sort_unstable();
    assert_eq!(par_sorted, (0..50).collect::<Vec<_>>());
}

/// Keyed `PCollection<(K, V)>` can be tee'd just like any other.
#[test]
fn test_tee_keyed_collection() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![("a", 1u32), ("a", 2), ("b", 3)]);
    let (a, b) = source.tee();

    let sums = a.sum_per_key().collect_seq_sorted().unwrap();
    let counts = b.count_per_key().collect_seq_sorted().unwrap();
    assert_eq!(sums, vec![("a", 3u32), ("b", 3u32)]);
    assert_eq!(counts, vec![("a", 2u64), ("b", 1u64)]);
}

// ── tee_n(n) — N-way split ───────────────────────────────────────────────────

/// `tee_n(3)` produces three branches each observing the full source.
#[test]
fn test_tee_n_three_branches() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let branches = source.tee_n(3);
    assert_eq!(branches.len(), 3);

    let counts: Vec<usize> = branches
        .into_iter()
        .map(|b| b.collect_seq().unwrap().len())
        .collect();
    assert_eq!(counts, vec![5usize, 5, 5]);
}

/// `tee_n(0)` returns an empty vector.
#[test]
fn test_tee_n_zero() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3]);
    let branches = source.tee_n(0);
    assert!(branches.is_empty());
}

/// `tee_n(1)` returns a single-element vector with the original collection.
#[test]
fn test_tee_n_one() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3]);
    let upstream_id = source.node_id();
    let branches = source.tee_n(1);
    assert_eq!(branches.len(), 1);
    assert_eq!(branches[0].node_id(), upstream_id);
    let mut out = branches.into_iter().next().unwrap().collect_seq().unwrap();
    out.sort_unstable();
    assert_eq!(out, vec![1u32, 2, 3]);
}

/// `tee_n(large)` works for many branches.
#[test]
fn test_tee_n_many_branches() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3]);
    let branches = source.tee_n(10);
    assert_eq!(branches.len(), 10);
    // Each branch yields the same set.
    for b in branches {
        let mut out = b.collect_seq().unwrap();
        out.sort_unstable();
        assert_eq!(out, vec![1u32, 2, 3]);
    }
}

/// `tee_n` branches share the upstream `node_id`.
#[test]
fn test_tee_n_same_node_id() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3]);
    let upstream_id = source.node_id();
    let branches = source.tee_n(5);
    for b in &branches {
        assert_eq!(b.node_id(), upstream_id);
    }
}

/// Each `tee_n` branch can apply a different transform.
#[test]
fn test_tee_n_different_transforms() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3, 4]);
    let branches = source.tee_n(4);

    // Apply +1, *2, square, identity.
    let mut iter = branches.into_iter();
    let plus_one = iter
        .next()
        .unwrap()
        .map(|x| x + 1)
        .collect_seq_sorted()
        .unwrap();
    let times_two = iter
        .next()
        .unwrap()
        .map(|x| x * 2)
        .collect_seq_sorted()
        .unwrap();
    let squared = iter
        .next()
        .unwrap()
        .map(|x| x * x)
        .collect_seq_sorted()
        .unwrap();
    let identity = iter.next().unwrap().collect_seq_sorted().unwrap();

    assert_eq!(plus_one, vec![2u32, 3, 4, 5]);
    assert_eq!(times_two, vec![2u32, 4, 6, 8]);
    assert_eq!(squared, vec![1u32, 4, 9, 16]);
    assert_eq!(identity, vec![1u32, 2, 3, 4]);
}

// ── Composition / advanced scenarios ─────────────────────────────────────────

/// `tee` after a chain of transforms: each branch sees the same materialized
/// output of the upstream chain.
#[test]
fn test_tee_after_long_chain() {
    let p = Pipeline::default();
    let processed = from_vec(&p, vec![1u32, 2, 3, 4, 5, 6, 7, 8])
        .filter(|x| x % 2 == 0)
        .map(|x| x * 10);
    let (a, b) = processed.tee();

    let mut a_out = a.collect_seq().unwrap();
    let mut b_out = b.collect_seq().unwrap();
    a_out.sort_unstable();
    b_out.sort_unstable();
    assert_eq!(a_out, vec![20u32, 40, 60, 80]);
    assert_eq!(b_out, vec![20u32, 40, 60, 80]);
}

/// `tee()` followed by `flatten()` on both branches reconstructs the source
/// with each element appearing twice.
#[test]
fn test_tee_then_flatten_doubles_elements() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3]);
    let (a, b) = source.tee();

    let combined = flatten(&[&a, &b]).collect_seq_sorted().unwrap();
    assert_eq!(combined, vec![1u32, 1, 2, 2, 3, 3]);
}

/// Nested tee: tee one of the tee branches further.
#[test]
fn test_tee_nested() {
    let p = Pipeline::default();
    let source = from_vec(&p, vec![1u32, 2, 3]);
    let (a, b) = source.tee();
    let (b1, b2) = b.tee();

    let a_out = a.map(|x| x + 100).collect_seq_sorted().unwrap();
    let b1_out = b1.map(|x| x + 200).collect_seq_sorted().unwrap();
    let b2_out = b2.map(|x| x + 300).collect_seq_sorted().unwrap();

    assert_eq!(a_out, vec![101u32, 102, 103]);
    assert_eq!(b1_out, vec![201u32, 202, 203]);
    assert_eq!(b2_out, vec![301u32, 302, 303]);
}

/// Large source with many tee branches preserves element count on each.
#[test]
fn test_tee_n_large_source() {
    const N: u32 = 500;
    let p = Pipeline::default();
    let source = from_vec(&p, (0..N).collect::<Vec<_>>());
    let branches = source.tee_n(5);

    let lens: Vec<usize> = branches
        .into_iter()
        .map(|b| b.collect_seq().unwrap().len())
        .collect();
    assert_eq!(lens, vec![N as usize; 5]);
}
