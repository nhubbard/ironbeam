//! Comprehensive tests for feature 4.15: dead-letter / error-routing
//! pattern.
//!
//! Covers:
//! - `DeadLetter::new` construction (with `String` and `&str` errors).
//! - `map_catching` with mixed-success input, all-success, all-fail,
//!   empty input, and `&str`-typed errors.
//! - `flat_map_catching` with the same scenarios plus zero-output success.
//! - Parallel execution preserves the (good, errors) partition.
//! - Output collections are independent (transforms compose downstream).
//! - `DeadLetter` survives further pipeline transforms (`map`, `key_by`, …).
//! - Custom error types via `Display`.

use ironbeam::*;
use std::fmt;

// ── DeadLetter struct ────────────────────────────────────────────────────────

#[test]
fn test_dead_letter_new_from_string() {
    let d = DeadLetter::new(42u32, String::from("boom"));
    assert_eq!(d.element, 42);
    assert_eq!(d.error, "boom");
}

#[test]
fn test_dead_letter_new_from_str() {
    let d = DeadLetter::new("alpha", "kaboom");
    assert_eq!(d.element, "alpha");
    assert_eq!(d.error, "kaboom");
}

#[test]
fn test_dead_letter_clone_and_debug() {
    let a = DeadLetter::new(7u32, "x");
    let b = a.clone();
    assert_eq!(a.element, b.element);
    assert_eq!(a.error, b.error);
    // Debug formatting includes both fields.
    let s = format!("{a:?}");
    assert!(s.contains("element"));
    assert!(s.contains("error"));
}

// ── map_catching ─────────────────────────────────────────────────────────────

/// Mixed success/failure input: each item lands in exactly one of the
/// output collections.
#[test]
fn test_map_catching_mixed_input() {
    let p = Pipeline::default();
    let raw = from_vec(
        &p,
        vec!["1".to_string(), "x".into(), "42".into(), "noop".into()],
    );
    let (parsed, errors) =
        raw.map_catching(|s: &String| s.parse::<u32>().map_err(|e| e.to_string()));

    let mut good = parsed.collect_seq().unwrap();
    good.sort_unstable();
    assert_eq!(good, vec![1u32, 42]);

    let mut bad = errors.collect_seq().unwrap();
    bad.sort_by(|a, b| a.element.cmp(&b.element));
    assert_eq!(bad.len(), 2);
    assert_eq!(bad[0].element, "noop");
    assert_eq!(bad[1].element, "x");
    // Error messages come from `<ParseIntError as Display>::fmt`.
    assert!(bad[0].error.contains("invalid digit"));
    assert!(bad[1].error.contains("invalid digit"));
}

/// All-success input ⇒ empty errors collection, full good collection.
#[test]
fn test_map_catching_all_success() {
    let p = Pipeline::default();
    let raw = from_vec(&p, vec!["1".to_string(), "2".into(), "3".into()]);
    let (parsed, errors) =
        raw.map_catching(|s: &String| s.parse::<u32>().map_err(|e| e.to_string()));

    let mut good = parsed.collect_seq().unwrap();
    good.sort_unstable();
    assert_eq!(good, vec![1u32, 2, 3]);
    assert!(errors.collect_seq().unwrap().is_empty());
}

/// All-failure input ⇒ empty good collection, every input dead-lettered.
#[test]
fn test_map_catching_all_failure() {
    let p = Pipeline::default();
    let raw = from_vec(&p, vec!["a".to_string(), "b".into(), "c".into()]);
    let (parsed, errors) =
        raw.map_catching(|s: &String| s.parse::<u32>().map_err(|e| e.to_string()));

    assert!(parsed.collect_seq().unwrap().is_empty());
    let bad = errors.collect_seq().unwrap();
    assert_eq!(bad.len(), 3);
    for d in &bad {
        assert!(matches!(d.element.as_str(), "a" | "b" | "c"));
    }
}

/// Empty input ⇒ both outputs empty.
#[test]
fn test_map_catching_empty_input() {
    let p = Pipeline::default();
    let raw = from_vec(&p, Vec::<String>::new());
    let (parsed, errors) =
        raw.map_catching(|s: &String| s.parse::<u32>().map_err(|e| e.to_string()));
    assert!(parsed.collect_seq().unwrap().is_empty());
    assert!(errors.collect_seq().unwrap().is_empty());
}

/// The `good` and `errors` outputs can each carry further transforms
/// independently — the partition really is a fan-out.
#[test]
fn test_map_catching_outputs_are_independent() {
    let p = Pipeline::default();
    let raw = from_vec(&p, vec!["1".to_string(), "x".into(), "2".into()]);
    let (parsed, errors) =
        raw.map_catching(|s: &String| s.parse::<u32>().map_err(|e| e.to_string()));

    let doubled: Vec<u32> = parsed.map(|n| n * 2).collect_seq().unwrap();
    let mut doubled = doubled;
    doubled.sort_unstable();
    assert_eq!(doubled, vec![2u32, 4]);

    let elements: Vec<String> = errors.map(|d| d.element.clone()).collect_seq().unwrap();
    assert_eq!(elements, vec!["x".to_string()]);
}

/// Parallel execution still partitions every element correctly.
#[test]
fn test_map_catching_parallel() {
    let p = Pipeline::default();
    let mut raw: Vec<String> = Vec::with_capacity(2_000);
    for i in 0..1_000 {
        raw.push(i.to_string()); // parsable
        raw.push(format!("nope-{i}")); // not parsable
    }
    let (parsed, errors) =
        from_vec(&p, raw).map_catching(|s: &String| s.parse::<u32>().map_err(|e| e.to_string()));

    let good = parsed.collect_par(Some(4), Some(8)).unwrap();
    let bad = errors.collect_par(Some(4), Some(8)).unwrap();
    assert_eq!(good.len(), 1_000);
    assert_eq!(bad.len(), 1_000);
    // Spot-check that error messages match the format.
    assert!(bad[0].error.contains("invalid digit"));
}

/// Custom error type with `Display` works as long as the message is
/// renderable.
#[test]
fn test_map_catching_custom_error_type() {
    #[derive(Debug)]
    struct MyError {
        kind: &'static str,
    }
    impl fmt::Display for MyError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "MyError({})", self.kind)
        }
    }

    let p = Pipeline::default();
    let raw = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let (good, errors) = raw.map_catching(|n: &u32| {
        if n.is_multiple_of(2) {
            Ok(*n)
        } else {
            Err(MyError { kind: "odd" })
        }
    });

    let mut g = good.collect_seq().unwrap();
    g.sort_unstable();
    assert_eq!(g, vec![2u32, 4]);

    let mut b = errors.collect_seq().unwrap();
    b.sort_by_key(|d| d.element);
    assert_eq!(b.len(), 3);
    for d in &b {
        assert_eq!(d.error, "MyError(odd)");
    }
}

/// `DeadLetter` flows through further transforms like any other element.
#[test]
fn test_dead_letter_downstream_compose() {
    let p = Pipeline::default();
    let raw = from_vec(&p, vec!["1".to_string(), "boom".into(), "2".into()]);
    let (_parsed, errors) =
        raw.map_catching(|s: &String| s.parse::<u32>().map_err(|e| e.to_string()));

    // Use DeadLetter as a key-value source.
    let by_element_len = errors
        .key_by(|d: &DeadLetter<String>| u64::try_from(d.element.len()).unwrap())
        .map_values(|_: &DeadLetter<String>| 1u64)
        .sum_per_key()
        .collect_seq_sorted()
        .unwrap();
    assert_eq!(by_element_len, vec![(4u64, 1u64)]); // "boom" has length 4
}

// ── flat_map_catching ────────────────────────────────────────────────────────

/// Successful `Ok(vec)` expands to multiple elements in good; `Err`
/// produces one `DeadLetter` per input.
#[test]
fn test_flat_map_catching_mixed() {
    let p = Pipeline::default();
    let raw = from_vec(
        &p,
        vec![
            "1,2,3".to_string(),
            "bad".into(),
            "4,5".into(),
            "x,y".into(),
        ],
    );
    let (numbers, errors) = raw.flat_map_catching(|s: &String| {
        s.split(',')
            .map(|t| t.parse::<u32>().map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()
    });

    let mut good = numbers.collect_seq().unwrap();
    good.sort_unstable();
    assert_eq!(good, vec![1u32, 2, 3, 4, 5]);

    let mut bad = errors.collect_seq().unwrap();
    bad.sort_by(|a, b| a.element.cmp(&b.element));
    assert_eq!(bad.len(), 2);
    assert_eq!(bad[0].element, "bad");
    assert_eq!(bad[1].element, "x,y");
}

/// `Ok(vec![])` is a valid success that produces *zero* output elements;
/// the input is **not** dead-lettered.
#[test]
fn test_flat_map_catching_empty_ok_is_success() {
    let p = Pipeline::default();
    let raw = from_vec(&p, vec![1u32, 2, 3]);
    let (good, errors) = raw.flat_map_catching(|n: &u32| -> Result<Vec<u32>, String> {
        if *n == 2 {
            Ok(Vec::new())
        } else {
            Ok(vec![*n * 10])
        }
    });
    let mut g = good.collect_seq().unwrap();
    g.sort_unstable();
    assert_eq!(g, vec![10u32, 30]);
    assert!(errors.collect_seq().unwrap().is_empty());
}

/// All-failure `flat_map`: every input becomes a `DeadLetter`.
#[test]
fn test_flat_map_catching_all_failure() {
    let p = Pipeline::default();
    let raw = from_vec(&p, vec!["a".to_string(), "b".into()]);
    let (good, errors) = raw.flat_map_catching(|s: &String| -> Result<Vec<u32>, String> {
        s.parse::<u32>().map(|v| vec![v]).map_err(|e| e.to_string())
    });
    assert!(good.collect_seq().unwrap().is_empty());
    let bad = errors.collect_seq().unwrap();
    assert_eq!(bad.len(), 2);
}

/// Empty input ⇒ both outputs empty.
#[test]
fn test_flat_map_catching_empty_input() {
    let p = Pipeline::default();
    let raw = from_vec(&p, Vec::<String>::new());
    let (good, errors) = raw.flat_map_catching(|s: &String| -> Result<Vec<u32>, String> {
        s.parse::<u32>().map(|v| vec![v]).map_err(|e| e.to_string())
    });
    assert!(good.collect_seq().unwrap().is_empty());
    assert!(errors.collect_seq().unwrap().is_empty());
}

/// Parallel `flat_map_catching` also partitions correctly and preserves
/// the total count of good outputs.
#[test]
fn test_flat_map_catching_parallel() {
    let p = Pipeline::default();
    let mut raw: Vec<String> = Vec::with_capacity(2_000);
    for i in 0..1_000 {
        raw.push(format!("{i},{}", i + 1)); // expands to 2 numbers
        raw.push("oops".into()); // dead letter
    }
    let (numbers, errors) = from_vec(&p, raw).flat_map_catching(|s: &String| {
        s.split(',')
            .map(|t| t.parse::<u32>().map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()
    });

    assert_eq!(numbers.collect_par(Some(4), Some(8)).unwrap().len(), 2_000);
    assert_eq!(errors.collect_par(Some(4), Some(8)).unwrap().len(), 1_000);
}
