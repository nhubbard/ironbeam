//! Comprehensive tests for feature 4.13: HyperLogLog-based approximate
//! distinct count.
//!
//! Covers:
//! - `HllApproxDistinctCount` constructors (`new`, `with_error`,
//!   `with_precision`) including range clamping and error→precision math.
//! - Global helpers `approx_count_distinct` / `approx_count_distinct_with_error`.
//! - Per-key helpers `approx_count_distinct_per_key` /
//!   `approx_count_distinct_per_key_with_error`.
//! - Empty / single-element / all-duplicate / all-distinct inputs.
//! - Bounded-error accuracy on a large cardinality.
//! - Determinism: sequential and parallel produce the *same* estimate
//!   (the combiner uses a fixed-seed `BuildHasherDefault<DefaultHasher>`).
//! - Element types beyond primitive ints (strings).

use ironbeam::combiners::HllApproxDistinctCount;
use ironbeam::*;

// ── HllApproxDistinctCount constructors ──────────────────────────────────────

/// `new()` and `default()` both produce precision 12 (~1.6% rel. std. err.).
#[test]
fn test_hll_combiner_defaults() {
    let a: HllApproxDistinctCount<u32> = HllApproxDistinctCount::new();
    let b: HllApproxDistinctCount<u32> = HllApproxDistinctCount::default();
    assert_eq!(a.precision(), 12);
    assert_eq!(b.precision(), 12);
}

/// `with_error` maps target relative standard error onto the smallest
/// precision whose theoretical error bound satisfies it.
#[test]
fn test_hll_combiner_with_error_picks_smallest_precision() {
    // 1.6% nominal default
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_error(0.02).precision(),
        12
    );
    // 0.81% — precision 14
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_error(0.01).precision(),
        14
    );
    // ~0.4% — precision 16
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_error(0.005).precision(),
        16
    );
    // Very loose: would map below MIN_PRECISION; clamps to 4.
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_error(0.5).precision(),
        4
    );
    // Very tight: would exceed MAX_PRECISION; clamps to 18.
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_error(0.0001).precision(),
        18
    );
}

/// `with_precision` clamps out-of-range values into `[4, 18]`.
#[test]
fn test_hll_combiner_with_precision_clamps() {
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_precision(0).precision(),
        4
    );
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_precision(3).precision(),
        4
    );
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_precision(10).precision(),
        10
    );
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_precision(18).precision(),
        18
    );
    assert_eq!(
        HllApproxDistinctCount::<u32>::with_precision(255).precision(),
        18
    );
}

/// `with_error(NaN)` / `with_error(<=0)` / `with_error(>=1)` panic.
#[test]
#[should_panic(expected = "approx_count_distinct error bound must be in (0, 1)")]
fn test_hll_combiner_with_error_rejects_nan() {
    let _ = HllApproxDistinctCount::<u32>::with_error(f64::NAN);
}

#[test]
#[should_panic(expected = "approx_count_distinct error bound must be in (0, 1)")]
fn test_hll_combiner_with_error_rejects_zero() {
    let _ = HllApproxDistinctCount::<u32>::with_error(0.0);
}

#[test]
#[should_panic(expected = "approx_count_distinct error bound must be in (0, 1)")]
fn test_hll_combiner_with_error_rejects_one() {
    let _ = HllApproxDistinctCount::<u32>::with_error(1.0);
}

#[test]
#[should_panic(expected = "approx_count_distinct error bound must be in (0, 1)")]
fn test_hll_combiner_with_error_rejects_negative() {
    let _ = HllApproxDistinctCount::<u32>::with_error(-0.1);
}

#[test]
#[should_panic(expected = "approx_count_distinct error bound must be in (0, 1)")]
fn test_hll_combiner_with_error_rejects_infinity() {
    let _ = HllApproxDistinctCount::<u32>::with_error(f64::INFINITY);
}

// ── Global helper: approx_count_distinct ─────────────────────────────────────

/// Empty input ⇒ zero estimate.
#[test]
fn test_approx_count_distinct_empty() {
    let p = Pipeline::default();
    let out = from_vec(&p, Vec::<u32>::new())
        .approx_count_distinct()
        .collect_seq()
        .unwrap();
    assert_eq!(out, vec![0u64]);
}

/// Single element ⇒ exactly 1.
#[test]
fn test_approx_count_distinct_single_element() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![42u32])
        .approx_count_distinct()
        .collect_seq()
        .unwrap();
    assert_eq!(out, vec![1u64]);
}

/// All elements identical ⇒ exactly 1.
#[test]
fn test_approx_count_distinct_all_duplicate() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![7u32; 1_000])
        .approx_count_distinct()
        .collect_seq()
        .unwrap();
    assert_eq!(out, vec![1u64]);
}

/// Small all-distinct input — HLL++ sparse mode makes this exact in practice.
#[test]
fn test_approx_count_distinct_small_distinct_is_exact() {
    let p = Pipeline::default();
    let out = from_vec(&p, (0u32..500).collect::<Vec<_>>())
        .approx_count_distinct()
        .collect_seq()
        .unwrap();
    assert_eq!(out, vec![500u64]);
}

/// Large all-distinct input — estimate must be within the precision-12
/// bound (~1.6 % rel. std. err.). We use a generous ±5 % bracket to keep
/// the test deterministic against the fixed-seed hasher.
#[test]
fn test_approx_count_distinct_large_within_error() {
    const N: u32 = 50_000;
    let p = Pipeline::default();
    let est = from_vec(&p, (0..N).collect::<Vec<_>>())
        .approx_count_distinct()
        .collect_seq()
        .unwrap()[0];

    let lo = u64::from(N) * 95 / 100;
    let hi = u64::from(N) * 105 / 100;
    assert!(
        (lo..=hi).contains(&est),
        "estimate {est} not in [{lo}, {hi}] for true cardinality {N}"
    );
}

/// `approx_count_distinct_with_error(0.01)` produces a precision-14 sketch
/// whose estimate hugs the true count even tighter than the default.
#[test]
fn test_approx_count_distinct_with_error_tighter() {
    const N: u32 = 50_000;
    let p = Pipeline::default();
    let est = from_vec(&p, (0..N).collect::<Vec<_>>())
        .approx_count_distinct_with_error(0.01)
        .collect_seq()
        .unwrap()[0];

    // Precision 14: ~0.81 % rel. std. err. Allow 3 % margin for safety.
    let lo = u64::from(N) * 97 / 100;
    let hi = u64::from(N) * 103 / 100;
    assert!(
        (lo..=hi).contains(&est),
        "estimate {est} not in [{lo}, {hi}] for true cardinality {N} with 0.01 error"
    );
}

/// Works for `String` elements just as well as integers.
#[test]
fn test_approx_count_distinct_strings() {
    let p = Pipeline::default();
    let words: Vec<String> = (0..200).map(|i| format!("word-{i}")).collect();
    let est = from_vec(&p, words)
        .approx_count_distinct()
        .collect_seq()
        .unwrap()[0];
    assert_eq!(est, 200u64); // sparse-mode exact
}

/// Sequential and parallel execution produce **identical** estimates because
/// the combiner uses a deterministic hasher.
#[test]
fn test_approx_count_distinct_seq_par_identical() {
    const N: u32 = 50_000;
    let data: Vec<u32> = (0..N).collect();

    let p1 = Pipeline::default();
    let seq_est = from_vec(&p1, data.clone())
        .approx_count_distinct()
        .collect_seq()
        .unwrap()[0];

    let p2 = Pipeline::default();
    let par_est = from_vec(&p2, data)
        .approx_count_distinct()
        .collect_par(Some(4), Some(8))
        .unwrap()[0];

    assert_eq!(
        seq_est, par_est,
        "deterministic hasher should make seq == par"
    );
}

// ── Per-key helper: approx_count_distinct_per_key ────────────────────────────

/// Each key reports its own (sparse-mode exact) distinct-value count.
#[test]
fn test_approx_count_distinct_per_key_small() {
    let p = Pipeline::default();
    let counts = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("a".to_string(), 3),
            ("b".to_string(), 7u32),
            ("b".to_string(), 7),
            ("b".to_string(), 8),
        ],
    )
    .approx_count_distinct_per_key()
    .collect_seq_sorted()
    .unwrap();
    assert_eq!(
        counts,
        vec![("a".to_string(), 3u64), ("b".to_string(), 2u64)]
    );
}

/// Per-key version with a custom error bound still produces correct counts
/// on small inputs (sparse-mode exact).
#[test]
fn test_approx_count_distinct_per_key_with_error() {
    let p = Pipeline::default();
    let counts = from_vec(
        &p,
        vec![
            ("x".to_string(), 1u64),
            ("x".to_string(), 2),
            ("x".to_string(), 2),
            ("y".to_string(), 100u64),
            ("y".to_string(), 200),
            ("y".to_string(), 300),
            ("y".to_string(), 300),
        ],
    )
    .approx_count_distinct_per_key_with_error(0.005)
    .collect_seq_sorted()
    .unwrap();
    assert_eq!(
        counts,
        vec![("x".to_string(), 2u64), ("y".to_string(), 3u64)]
    );
}

/// Empty input ⇒ no output (no keys to aggregate).
#[test]
fn test_approx_count_distinct_per_key_empty() {
    let p = Pipeline::default();
    let out = from_vec(&p, Vec::<(String, u32)>::new())
        .approx_count_distinct_per_key()
        .collect_seq()
        .unwrap();
    assert!(out.is_empty());
}

/// Large per-key cardinality: estimate stays within the configured error.
#[test]
fn test_approx_count_distinct_per_key_large_within_error() {
    const N: u32 = 20_000;
    let p = Pipeline::default();
    // 2 keys, each with N distinct values.
    let mut data: Vec<(String, u32)> = Vec::with_capacity((N as usize) * 2);
    for v in 0..N {
        data.push(("L".to_string(), v));
        data.push(("R".to_string(), N + v));
    }

    let counts = from_vec(&p, data)
        .approx_count_distinct_per_key()
        .collect_seq_sorted()
        .unwrap();

    assert_eq!(counts.len(), 2);
    for (k, est) in counts {
        let lo = u64::from(N) * 95 / 100;
        let hi = u64::from(N) * 105 / 100;
        assert!(
            (lo..=hi).contains(&est),
            "key {k}: estimate {est} not in [{lo}, {hi}]"
        );
    }
}

/// Per-key sequential vs parallel: identical results, deterministic merging.
#[test]
fn test_approx_count_distinct_per_key_seq_par_identical() {
    const N: u32 = 5_000;
    let mut data: Vec<(String, u32)> = Vec::with_capacity(N as usize);
    for v in 0..N {
        data.push((if v % 2 == 0 { "even" } else { "odd" }.to_string(), v));
    }

    let p1 = Pipeline::default();
    let seq = from_vec(&p1, data.clone())
        .approx_count_distinct_per_key()
        .collect_seq_sorted()
        .unwrap();
    let p2 = Pipeline::default();
    let par = from_vec(&p2, data)
        .approx_count_distinct_per_key()
        .collect_par_sorted(Some(4), Some(8))
        .unwrap();
    assert_eq!(seq, par);
}

/// Compose `combine_values_lifted` end-to-end.
#[test]
fn test_hll_combine_values_lifted_end_to_end() {
    let p = Pipeline::default();
    let counts = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 1),
            ("a".to_string(), 2),
            ("a".to_string(), 3),
            ("b".to_string(), 7u32),
            ("b".to_string(), 7),
            ("b".to_string(), 8),
            ("c".to_string(), 1u32),
        ],
    )
    .group_by_key()
    .combine_values_lifted(HllApproxDistinctCount::<u32>::new())
    .collect_seq_sorted()
    .unwrap();

    assert_eq!(
        counts,
        vec![
            ("a".to_string(), 3u64),
            ("b".to_string(), 2u64),
            ("c".to_string(), 1u64)
        ]
    );
}
