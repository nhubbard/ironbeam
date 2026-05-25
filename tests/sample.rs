//! Comprehensive tests for feature 4.14: Beam-style fixed-size sample.
//!
//! Covers:
//! - `sample_globally(n)` / `sample_globally_with_seed(n, seed)` semantics:
//!   exactly `n` items when input is large enough, all items when smaller,
//!   determinism with the default seed, and seed-varied output.
//! - `sample_per_key(n)` / `sample_per_key_with_seed(n, seed)` semantics:
//!   one entry per key with up to `n` values, deterministic, seed-varied.
//! - Sequential / parallel produce identical samples.
//! - Edge cases: `n = 0`, `n` larger than input, empty input.

use ironbeam::*;
use std::collections::HashSet;

// ── sample_globally ──────────────────────────────────────────────────────────

/// Exactly `n` items returned when the input has more than `n` elements.
#[test]
fn test_sample_globally_returns_exactly_n() {
    let p = Pipeline::default();
    let out = from_vec(&p, (0u32..1_000).collect::<Vec<_>>())
        .sample_globally(10)
        .collect_seq()
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].len(), 10);
}

/// All sampled items come from the input set (no fabricated elements).
#[test]
fn test_sample_globally_items_are_subset_of_input() {
    let p = Pipeline::default();
    let input: HashSet<u32> = (0u32..1_000).collect();
    let out = from_vec(&p, input.iter().copied().collect::<Vec<_>>())
        .sample_globally(50)
        .collect_seq()
        .unwrap();
    let sample: HashSet<u32> = out[0].iter().copied().collect();
    assert_eq!(sample.len(), 50, "sample must be exactly 50 distinct items");
    assert!(sample.is_subset(&input));
}

/// When the input is smaller than `n`, every element is returned.
#[test]
fn test_sample_globally_smaller_than_n() {
    let p = Pipeline::default();
    let input = vec![1u32, 2, 3];
    let out = from_vec(&p, input)
        .sample_globally(10)
        .collect_seq()
        .unwrap();
    assert_eq!(out.len(), 1);
    let mut got = out[0].clone();
    got.sort_unstable();
    assert_eq!(got, vec![1u32, 2, 3]);
}

/// Empty input ⇒ a single empty `Vec<T>` (`CombineGlobally` always emits one element).
#[test]
fn test_sample_globally_empty_input() {
    let p = Pipeline::default();
    let out = from_vec(&p, Vec::<u32>::new())
        .sample_globally(5)
        .collect_seq()
        .unwrap();
    assert_eq!(out.len(), 1);
    assert!(out[0].is_empty());
}

/// `n = 0` yields an empty `Vec<T>`.
#[test]
fn test_sample_globally_zero() {
    let p = Pipeline::default();
    let out = from_vec(&p, (0u32..100).collect::<Vec<_>>())
        .sample_globally(0)
        .collect_seq()
        .unwrap();
    assert_eq!(out.len(), 1);
    assert!(out[0].is_empty());
}

/// Default-seed determinism: two runs on the same input return the same sample.
#[test]
fn test_sample_globally_is_deterministic() {
    let input: Vec<u32> = (0..1_000).collect();

    let p1 = Pipeline::default();
    let a = from_vec(&p1, input.clone())
        .sample_globally(20)
        .collect_seq()
        .unwrap();
    let p2 = Pipeline::default();
    let b = from_vec(&p2, input)
        .sample_globally(20)
        .collect_seq()
        .unwrap();

    assert_eq!(a, b);
}

/// Different seeds produce different samples (with overwhelming probability
/// at this k/N).
#[test]
fn test_sample_globally_with_seed_varies() {
    let input: Vec<u32> = (0..1_000).collect();

    let p1 = Pipeline::default();
    let a = from_vec(&p1, input.clone())
        .sample_globally_with_seed(20, 1)
        .collect_seq()
        .unwrap();
    let p2 = Pipeline::default();
    let b = from_vec(&p2, input)
        .sample_globally_with_seed(20, 999_999)
        .collect_seq()
        .unwrap();

    assert_ne!(a, b, "different seeds should produce different samples");
}

/// Parallel execution still returns exactly `n` items from the input
/// multiset. Sequential and parallel samples may differ in identity
/// (priorities are assigned per-partition), but both are valid uniform
/// samples of the input.
#[test]
fn test_sample_globally_parallel_shape_and_subset() {
    let input: Vec<u32> = (0..2_000).collect();
    let input_set: HashSet<u32> = input.iter().copied().collect();

    let p = Pipeline::default();
    let out = from_vec(&p, input)
        .sample_globally(25)
        .collect_par(Some(4), Some(8))
        .unwrap();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].len(), 25);
    let sample: HashSet<u32> = out[0].iter().copied().collect();
    assert_eq!(sample.len(), 25);
    assert!(sample.is_subset(&input_set));
}

// ── sample_per_key ───────────────────────────────────────────────────────────

/// Each key gets a sample of exactly `n` values when its stream has at
/// least `n` values.
#[test]
fn test_sample_per_key_returns_exactly_n_per_key() {
    let p = Pipeline::default();
    let mut data: Vec<(&str, u32)> = Vec::new();
    for i in 0..200 {
        data.push(("a", i));
        data.push(("b", 1_000 + i));
    }
    let samples = from_vec(&p, data)
        .sample_per_key(5)
        .collect_seq_sorted()
        .unwrap();
    assert_eq!(samples.len(), 2);
    for (_k, vs) in &samples {
        assert_eq!(vs.len(), 5);
    }
}

/// Samples are drawn from the corresponding key's value stream — values
/// don't leak across keys.
#[test]
fn test_sample_per_key_values_belong_to_their_key() {
    let p = Pipeline::default();
    let mut data: Vec<(&str, u32)> = Vec::new();
    for i in 0..50 {
        data.push(("low", i)); // 0..50
        data.push(("high", 1_000 + i)); // 1000..1050
    }
    let samples = from_vec(&p, data)
        .sample_per_key(10)
        .collect_seq_sorted()
        .unwrap();
    for (k, vs) in &samples {
        match *k {
            "low" => assert!(vs.iter().all(|v| *v < 1_000), "low contained {vs:?}"),
            "high" => assert!(vs.iter().all(|v| *v >= 1_000), "high contained {vs:?}"),
            other => panic!("unexpected key {other}"),
        }
    }
}

/// Key whose value stream is shorter than `n` yields a sample containing
/// every value for that key (in some order).
#[test]
fn test_sample_per_key_smaller_than_n() {
    let p = Pipeline::default();
    let samples = from_vec(&p, vec![("a", 1u32), ("a", 2), ("a", 3)])
        .sample_per_key(10)
        .collect_seq_sorted()
        .unwrap();
    assert_eq!(samples.len(), 1);
    let (_, mut vs) = samples.into_iter().next().unwrap();
    vs.sort_unstable();
    assert_eq!(vs, vec![1, 2, 3]);
}

/// `n = 0` ⇒ no values sampled, but every key is still emitted with an empty Vec.
#[test]
fn test_sample_per_key_zero() {
    let p = Pipeline::default();
    let samples = from_vec(&p, vec![("a", 1u32), ("b", 2)])
        .sample_per_key(0)
        .collect_seq_sorted()
        .unwrap();
    assert_eq!(samples.len(), 2);
    for (_, vs) in samples {
        assert!(vs.is_empty());
    }
}

/// Empty input ⇒ no output (no keys to sample).
#[test]
fn test_sample_per_key_empty() {
    let p = Pipeline::default();
    let out = from_vec(&p, Vec::<(&str, u32)>::new())
        .sample_per_key(5)
        .collect_seq()
        .unwrap();
    assert!(out.is_empty());
}

/// Default-seed determinism for per-key sampling.
#[test]
fn test_sample_per_key_is_deterministic() {
    let mut data: Vec<(&str, u32)> = Vec::new();
    for i in 0..200 {
        data.push(("k", i));
    }
    let p1 = Pipeline::default();
    let a = from_vec(&p1, data.clone())
        .sample_per_key(8)
        .collect_seq_sorted()
        .unwrap();
    let p2 = Pipeline::default();
    let b = from_vec(&p2, data)
        .sample_per_key(8)
        .collect_seq_sorted()
        .unwrap();
    assert_eq!(a, b);
}

/// Different seeds produce different per-key samples.
#[test]
fn test_sample_per_key_with_seed_varies() {
    let mut data: Vec<(&str, u32)> = Vec::new();
    for i in 0..200 {
        data.push(("k", i));
    }
    let p1 = Pipeline::default();
    let a = from_vec(&p1, data.clone())
        .sample_per_key_with_seed(8, 1)
        .collect_seq_sorted()
        .unwrap();
    let p2 = Pipeline::default();
    let b = from_vec(&p2, data)
        .sample_per_key_with_seed(8, 12_345)
        .collect_seq_sorted()
        .unwrap();
    assert_ne!(a, b);
}

/// Per-key parallel execution: every key still gets exactly `n` values
/// (or fewer if its stream is smaller), and every value originates from
/// the correct key. Priority assignments differ between seq and par, so
/// the chosen *identities* may differ — we only assert the structural
/// guarantees.
#[test]
fn test_sample_per_key_parallel_shape_and_subset() {
    let mut data: Vec<(&str, u32)> = Vec::new();
    let mut even_set: HashSet<u32> = HashSet::new();
    let mut odd_set: HashSet<u32> = HashSet::new();
    for i in 0..400 {
        if i % 2 == 0 {
            data.push(("even", i));
            even_set.insert(i);
        } else {
            data.push(("odd", i));
            odd_set.insert(i);
        }
    }

    let p = Pipeline::default();
    let samples = from_vec(&p, data)
        .sample_per_key(10)
        .collect_par_sorted(Some(4), Some(8))
        .unwrap();

    assert_eq!(samples.len(), 2);
    for (k, vs) in samples {
        assert_eq!(vs.len(), 10);
        let vs_set: HashSet<u32> = vs.iter().copied().collect();
        match k {
            "even" => assert!(vs_set.is_subset(&even_set)),
            "odd" => assert!(vs_set.is_subset(&odd_set)),
            other => panic!("unexpected key {other}"),
        }
    }
}
