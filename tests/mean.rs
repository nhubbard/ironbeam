//! Comprehensive tests for feature 4.3: `Mean<O>` combiner + helpers.
//!
//! This test suite validates:
//! - `Mean<O>` as a `CombineFn` used directly with `combine_globally` /
//!   `combine_values` (both `f64` and `f32` output flavors).
//! - The `combine_globally_lifted` /
//!   `combine_values_lifted`.
//! - The `mean_globally::<O>()` and `mean_per_key::<O>()` helpers.
//! - Edge cases: empty, single element, parallel, large collections.

use ironbeam::combiners::Mean;
use ironbeam::*;

const F64_EPS: f64 = 1e-12;
const F32_EPS: f32 = 1e-6;

// ── Mean<f64> via combine_globally ───────────────────────────────────────────

/// Basic: integer inputs, `f64` output via the combiner.
#[test]
fn test_mean_globally_combiner_f64_basic() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .combine_globally(Mean::<f64>::new(), None)
        .collect_seq()
        .unwrap();
    assert_eq!(out.len(), 1);
    assert!((out[0] - 3.0).abs() < F64_EPS);
}

/// `f64` mean of `f64` inputs (identity-like path).
#[test]
fn test_mean_globally_combiner_f64_from_f64() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1.5_f64, 2.5, 3.5, 4.5])
        .combine_globally(Mean::<f64>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 3.0).abs() < F64_EPS);
}

/// Empty collection: `f64` mean returns `0.0`.
#[test]
fn test_mean_globally_combiner_f64_empty() {
    let p = Pipeline::default();
    let input: Vec<u32> = vec![];
    let out = from_vec(&p, input)
        .combine_globally(Mean::<f64>::new(), None)
        .collect_seq()
        .unwrap();
    assert_eq!(out.len(), 1);
    assert!((out[0] - 0.0).abs() < F64_EPS);
}

/// Single element: `f64` mean equals the element value.
#[test]
fn test_mean_globally_combiner_f64_single() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![42u32])
        .combine_globally(Mean::<f64>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 42.0).abs() < F64_EPS);
}

/// All-equal inputs return the same value.
#[test]
fn test_mean_globally_combiner_f64_all_equal() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![7u32; 50])
        .combine_globally(Mean::<f64>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 7.0).abs() < F64_EPS);
}

/// Parallel execution gives the same result.
#[test]
fn test_mean_globally_combiner_f64_parallel() {
    let p = Pipeline::default();
    let out = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .combine_globally(Mean::<f64>::new(), Some(4))
        .collect_par(Some(4), Some(4))
        .unwrap();
    // mean(1..=100) = 50.5
    assert!((out[0] - 50.5).abs() < F64_EPS);
}

// ── Mean<f32> via combine_globally ───────────────────────────────────────────

/// Basic: integer inputs, `f32` output via the combiner.
#[test]
fn test_mean_globally_combiner_f32_basic() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1u16, 2, 3, 4, 5])
        .combine_globally(Mean::<f32>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 3.0_f32).abs() < F32_EPS);
}

/// `f32` mean of `f32` inputs.
#[test]
fn test_mean_globally_combiner_f32_from_f32() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1.0_f32, 2.0, 3.0, 4.0, 5.0])
        .combine_globally(Mean::<f32>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 3.0_f32).abs() < F32_EPS);
}

/// Empty: `f32` mean returns `0.0`.
#[test]
fn test_mean_globally_combiner_f32_empty() {
    let p = Pipeline::default();
    let input: Vec<u8> = vec![];
    let out = from_vec(&p, input)
        .combine_globally(Mean::<f32>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 0.0_f32).abs() < F32_EPS);
}

/// Single element: `f32` mean equals that element.
#[test]
fn test_mean_globally_combiner_f32_single() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![21u8])
        .combine_globally(Mean::<f32>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 21.0_f32).abs() < F32_EPS);
}

// ── Mean via combine_globally_lifted ─────────────────────────────────────────

/// Lifted-combiner path for `Mean<f64>` produces the same result.
#[test]
fn test_mean_globally_lifted_f64() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![10u32, 20, 30, 40])
        .combine_globally_lifted(Mean::<f64>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 25.0).abs() < F64_EPS);
}

/// Lifted-combiner path for `Mean<f32>` produces the same result.
#[test]
fn test_mean_globally_lifted_f32() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![10u16, 20, 30, 40])
        .combine_globally_lifted(Mean::<f32>::new(), None)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 25.0_f32).abs() < F32_EPS);
}

/// Lifted path with fanout.
#[test]
fn test_mean_globally_lifted_with_fanout() {
    let p = Pipeline::default();
    let out = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .combine_globally_lifted(Mean::<f64>::new(), Some(8))
        .collect_seq()
        .unwrap();
    assert!((out[0] - 50.5).abs() < F64_EPS);
}

// ── Mean via combine_values (per-key) ────────────────────────────────────────

/// Per-key mean via the combiner (f64).
#[test]
fn test_mean_combine_values_f64() {
    let p = Pipeline::default();
    let mut out = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 3),
            ("b".to_string(), 10),
            ("b".to_string(), 30),
        ],
    )
    .combine_values(Mean::<f64>::new())
    .collect_seq()
    .unwrap();
    out.sort_by_key(|(k, _)| k.clone());
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].0, "a");
    assert!((out[0].1 - 2.0).abs() < F64_EPS);
    assert_eq!(out[1].0, "b");
    assert!((out[1].1 - 20.0).abs() < F64_EPS);
}

/// Per-key mean via the combiner (f32).
#[test]
fn test_mean_combine_values_f32() {
    let p = Pipeline::default();
    let mut out = from_vec(
        &p,
        vec![
            ("a".to_string(), 2u8),
            ("a".to_string(), 4),
            ("b".to_string(), 6),
            ("b".to_string(), 8),
        ],
    )
    .combine_values(Mean::<f32>::new())
    .collect_seq()
    .unwrap();
    out.sort_by_key(|(k, _)| k.clone());
    assert!((out[0].1 - 3.0_f32).abs() < F32_EPS);
    assert!((out[1].1 - 7.0_f32).abs() < F32_EPS);
}

// ── Mean via group_by_key + combine_values_lifted (lifted per-key path) ──────

/// Lifted per-key path for `Mean<f64>` via `group_by_key().combine_values_lifted(...)`.
#[test]
fn test_mean_combine_values_lifted_f64() {
    let p = Pipeline::default();
    let mut out = from_vec(
        &p,
        vec![
            ("x".to_string(), 10i32),
            ("x".to_string(), 20),
            ("y".to_string(), 30),
            ("y".to_string(), 40),
        ],
    )
    .group_by_key()
    .combine_values_lifted(Mean::<f64>::new())
    .collect_seq()
    .unwrap();
    out.sort_by_key(|(k, _)| k.clone());
    assert!((out[0].1 - 15.0).abs() < F64_EPS);
    assert!((out[1].1 - 35.0).abs() < F64_EPS);
}

/// Lifted per-key path for `Mean<f32>`.
#[test]
fn test_mean_combine_values_lifted_f32() {
    let p = Pipeline::default();
    let mut out = from_vec(
        &p,
        vec![
            ("x".to_string(), 10u16),
            ("x".to_string(), 20),
            ("y".to_string(), 30),
            ("y".to_string(), 40),
        ],
    )
    .group_by_key()
    .combine_values_lifted(Mean::<f32>::new())
    .collect_seq()
    .unwrap();
    out.sort_by_key(|(k, _)| k.clone());
    assert!((out[0].1 - 15.0_f32).abs() < F32_EPS);
    assert!((out[1].1 - 35.0_f32).abs() < F32_EPS);
}

// ── mean_globally helper ─────────────────────────────────────────────────────

/// Helper: `mean_globally::<f64>()` matches the combiner result.
#[test]
fn test_mean_globally_helper_f64() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .mean_globally::<f64>()
        .collect_seq()
        .unwrap();
    assert!((out[0] - 3.0).abs() < F64_EPS);
}

/// Helper: `mean_globally::<f32>()` produces an `f32` output.
#[test]
fn test_mean_globally_helper_f32() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1u16, 2, 3, 4, 5])
        .mean_globally::<f32>()
        .collect_seq()
        .unwrap();
    assert!((out[0] - 3.0_f32).abs() < F32_EPS);
}

/// Helper: empty collection produces `0.0`.
#[test]
fn test_mean_globally_helper_empty() {
    let p = Pipeline::default();
    let input: Vec<u32> = vec![];
    let out = from_vec(&p, input)
        .mean_globally::<f64>()
        .collect_seq()
        .unwrap();
    assert!((out[0] - 0.0).abs() < F64_EPS);
}

/// Helper: parallel execution.
#[test]
fn test_mean_globally_helper_parallel() {
    let p = Pipeline::default();
    let out = from_vec(&p, (1u32..=200).collect::<Vec<_>>())
        .mean_globally::<f64>()
        .collect_par(Some(4), Some(4))
        .unwrap();
    // mean(1..=200) = 100.5
    assert!((out[0] - 100.5).abs() < F64_EPS);
}

/// Helper: large collection (sanity check that count + sum compose correctly).
#[test]
fn test_mean_globally_helper_large() {
    let p = Pipeline::default();
    let input: Vec<u32> = (1..=1_000).collect();
    let out = from_vec(&p, input)
        .mean_globally::<f64>()
        .collect_seq()
        .unwrap();
    // mean(1..=1000) = 500.5
    assert!((out[0] - 500.5).abs() < F64_EPS);
}

// ── mean_per_key helper ──────────────────────────────────────────────────────

/// Helper: `mean_per_key::<f64>()` matches the combiner result.
#[test]
fn test_mean_per_key_helper_f64() {
    let p = Pipeline::default();
    let mut out = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 3),
            ("b".to_string(), 10),
        ],
    )
    .mean_per_key::<f64>()
    .collect_seq()
    .unwrap();
    out.sort_by_key(|(k, _)| k.clone());
    assert!((out[0].1 - 2.0).abs() < F64_EPS);
    assert!((out[1].1 - 10.0).abs() < F64_EPS);
}

/// Helper: `mean_per_key::<f32>()` produces `(K, f32)`.
#[test]
fn test_mean_per_key_helper_f32() {
    let p = Pipeline::default();
    let mut out = from_vec(
        &p,
        vec![
            ("a".to_string(), 2u16),
            ("a".to_string(), 4),
            ("b".to_string(), 6),
            ("b".to_string(), 8),
        ],
    )
    .mean_per_key::<f32>()
    .collect_seq()
    .unwrap();
    out.sort_by_key(|(k, _)| k.clone());
    assert!((out[0].1 - 3.0_f32).abs() < F32_EPS);
    assert!((out[1].1 - 7.0_f32).abs() < F32_EPS);
}

/// Helper: single value per key — mean equals the value.
#[test]
fn test_mean_per_key_helper_single_value_per_key() {
    let p = Pipeline::default();
    let mut out = from_vec(
        &p,
        vec![
            ("a".to_string(), 42u32),
            ("b".to_string(), 7),
            ("c".to_string(), 100),
        ],
    )
    .mean_per_key::<f64>()
    .collect_seq()
    .unwrap();
    out.sort_by_key(|(k, _)| k.clone());
    assert!((out[0].1 - 42.0).abs() < F64_EPS);
    assert!((out[1].1 - 7.0).abs() < F64_EPS);
    assert!((out[2].1 - 100.0).abs() < F64_EPS);
}

/// Helper: parallel per-key execution.
#[test]
fn test_mean_per_key_helper_parallel() {
    let p = Pipeline::default();
    // Two keys, 100 values each: "a" averages 50.5, "b" averages 150.5
    let mut data: Vec<(String, u32)> = (1..=100).map(|i| ("a".to_string(), i)).collect();
    data.extend((101..=200).map(|i| ("b".to_string(), i)));
    let mut out = from_vec(&p, data)
        .mean_per_key::<f64>()
        .collect_par(Some(4), Some(4))
        .unwrap();
    out.sort_by_key(|(k, _)| k.clone());
    assert!((out[0].1 - 50.5).abs() < F64_EPS);
    assert!((out[1].1 - 150.5).abs() < F64_EPS);
}

/// Helper: large per-key collection.
#[test]
fn test_mean_per_key_helper_large() {
    let p = Pipeline::default();
    // 1000 keys, each with 10 values: key i has values i*1..=i*10, mean = i * 5.5
    let mut data = Vec::with_capacity(10_000);
    for i in 1u32..=1_000 {
        for j in 1u32..=10 {
            data.push((i, i * j));
        }
    }
    let mut out = from_vec(&p, data)
        .mean_per_key::<f64>()
        .collect_seq()
        .unwrap();
    out.sort_by_key(|(k, _)| *k);
    assert_eq!(out.len(), 1_000);
    for (k, v) in &out {
        let expected = f64::from(*k) * 5.5;
        assert!(
            (v - expected).abs() < 1e-9,
            "key={k}: expected ~{expected}, got {v}"
        );
    }
}

// ── Chaining and composition ─────────────────────────────────────────────────

/// `mean_globally` chained with another transform produces a usable result.
#[test]
fn test_mean_globally_helper_chained_with_map() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![10u32, 20, 30])
        .mean_globally::<f64>()
        .map(|v: &f64| v * 2.0)
        .collect_seq()
        .unwrap();
    assert!((out[0] - 40.0).abs() < F64_EPS);
}

/// `mean_per_key` chained with `values()` extracts just the means.
#[test]
fn test_mean_per_key_helper_then_values() {
    let p = Pipeline::default();
    let mut out = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 3),
            ("b".to_string(), 4),
            ("b".to_string(), 8),
        ],
    )
    .mean_per_key::<f64>()
    .values()
    .collect_seq()
    .unwrap();
    out.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert!((out[0] - 2.0).abs() < F64_EPS);
    assert!((out[1] - 6.0).abs() < F64_EPS);
}

// ── Mean<O> construction ─────────────────────────────────────────────────────

/// `Mean::<O>::default()` and `Mean::<O>::new()` produce equivalent combiners.
#[test]
fn test_mean_default_equals_new() {
    let p1 = Pipeline::default();
    let p2 = Pipeline::default();

    let from_new = from_vec(&p1, vec![1u32, 2, 3])
        .combine_globally(Mean::<f64>::new(), None)
        .collect_seq()
        .unwrap();
    let from_default = from_vec(&p2, vec![1u32, 2, 3])
        .combine_globally(Mean::<f64>::default(), None)
        .collect_seq()
        .unwrap();

    assert!((from_new[0] - from_default[0]).abs() < F64_EPS);
}

/// `Mean<O>` is `Copy + Clone + Debug` — verified at compile time by the
/// usages below, plus a runtime `Debug` formatting check.
#[test]
fn test_mean_trait_obligations() {
    fn assert_copy<T: Copy>() {}
    fn assert_clone<T: Clone>() {}
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_copy::<Mean<f64>>();
    assert_clone::<Mean<f64>>();
    assert_debug::<Mean<f64>>();

    let m: Mean<f64> = Mean::new();
    let m_clone = m;
    let dbg = format!("{m_clone:?}");
    assert!(dbg.contains("Mean"));
}
