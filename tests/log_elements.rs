//! Comprehensive tests for feature 4.10: `log_elements` / `log_elements_with`.
//!
//! Validates that the debug-tap helpers behave as a strict passthrough — the
//! downstream collection contents and ordering match the input — while the
//! configured formatter (default `Debug` or user-supplied) is invoked exactly
//! once per element.
//!
//! Side effects on `stdout` are not asserted directly; instead, the
//! `log_elements_with` tests use an atomic counter inside the closure to
//! verify invocation count, which exercises the same code path as the
//! `Debug`-based default.

use ironbeam::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ── log_elements() — default Debug-based passthrough ─────────────────────────

/// Basic: `log_elements` re-emits every element unchanged and in order.
#[test]
fn test_log_elements_basic_passthrough() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .log_elements()
        .collect_seq()
        .unwrap();
    assert_eq!(out, vec![1u32, 2, 3, 4, 5]);
}

/// Empty input produces an empty output (no formatter calls, no panics).
#[test]
fn test_log_elements_empty() {
    let p = Pipeline::default();
    let out = from_vec(&p, Vec::<u32>::new())
        .log_elements()
        .collect_seq()
        .unwrap();
    assert!(out.is_empty());
}

/// Single-element source — degenerate but covered.
#[test]
fn test_log_elements_single_element() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![42u32])
        .log_elements()
        .collect_seq()
        .unwrap();
    assert_eq!(out, vec![42u32]);
}

/// Works with `Debug` types other than primitives.
#[test]
fn test_log_elements_with_string_elements() {
    let p = Pipeline::default();
    let out = from_vec(
        &p,
        vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
    )
    .log_elements()
    .collect_seq()
    .unwrap();
    assert_eq!(
        out,
        vec!["alpha".to_string(), "beta".into(), "gamma".into()]
    );
}

/// `log_elements` is composable: chained between two transforms it must not
/// disturb downstream values.
#[test]
fn test_log_elements_between_transforms() {
    let p = Pipeline::default();
    let out = from_vec(&p, vec![1u32, 2, 3, 4, 5])
        .filter(|x| x % 2 == 1)
        .log_elements()
        .map(|x| x * 10)
        .collect_seq_sorted()
        .unwrap();
    assert_eq!(out, vec![10u32, 30, 50]);
}

/// Passthrough holds under parallel execution as well as sequential.
#[test]
fn test_log_elements_parallel_passthrough() {
    let p = Pipeline::default();
    let mut out = from_vec(&p, (0u32..50).collect::<Vec<_>>())
        .log_elements()
        .collect_par(Some(4), Some(4))
        .unwrap();
    out.sort_unstable();
    assert_eq!(out, (0u32..50).collect::<Vec<_>>());
}

/// Keyed `PCollection<(K, V)>` is supported because tuples are `Debug` when
/// their components are.
#[test]
fn test_log_elements_on_keyed_collection() {
    let p = Pipeline::default();
    let out = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ],
    )
    .log_elements()
    .collect_seq_sorted()
    .unwrap();
    assert_eq!(
        out,
        vec![
            ("a".to_string(), 1u32),
            ("b".to_string(), 2),
            ("c".to_string(), 3)
        ]
    );
}

// ── log_elements_with(formatter) — user-supplied formatter ───────────────────

/// `log_elements_with` invokes the formatter once per element and preserves
/// the downstream values.
#[test]
fn test_log_elements_with_invokes_formatter_per_element() {
    let p = Pipeline::default();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_in_closure = Arc::clone(&counter);

    let out = from_vec(&p, vec![10u32, 20, 30, 40])
        .log_elements_with(move |x| {
            counter_in_closure.fetch_add(1, Ordering::SeqCst);
            format!("seen={x}")
        })
        .collect_seq()
        .unwrap();

    assert_eq!(out, vec![10u32, 20, 30, 40]);
    assert_eq!(counter.load(Ordering::SeqCst), 4);
}

/// Formatter is never invoked on an empty input.
#[test]
fn test_log_elements_with_empty_does_not_invoke_formatter() {
    let p = Pipeline::default();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_in_closure = Arc::clone(&counter);

    let out = from_vec(&p, Vec::<u32>::new())
        .log_elements_with(move |x| {
            counter_in_closure.fetch_add(1, Ordering::SeqCst);
            format!("{x}")
        })
        .collect_seq()
        .unwrap();

    assert!(out.is_empty());
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

/// Works for element types that do not implement `Debug`.
#[test]
fn test_log_elements_with_works_for_non_debug_type() {
    // A deliberately non-`Debug` payload.
    #[derive(Clone, Serialize, Deserialize)]
    struct NotDebug {
        n: u32,
    }

    let p = Pipeline::default();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_in_closure = Arc::clone(&counter);

    let out = from_vec(
        &p,
        vec![NotDebug { n: 1 }, NotDebug { n: 2 }, NotDebug { n: 3 }],
    )
    .log_elements_with(move |x: &NotDebug| {
        counter_in_closure.fetch_add(1, Ordering::SeqCst);
        format!("n={}", x.n)
    })
    .collect_seq()
    .unwrap();

    assert_eq!(out.len(), 3);
    assert_eq!(out[0].n, 1);
    assert_eq!(out[1].n, 2);
    assert_eq!(out[2].n, 3);
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

/// Parallel execution still results in exactly one formatter call per
/// element across all partitions.
#[test]
fn test_log_elements_with_parallel_count() {
    const N: u32 = 64;
    let p = Pipeline::default();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_in_closure = Arc::clone(&counter);

    let mut out = from_vec(&p, (0u32..N).collect::<Vec<_>>())
        .log_elements_with(move |x| {
            counter_in_closure.fetch_add(1, Ordering::SeqCst);
            format!("v={x}")
        })
        .collect_par(Some(4), Some(4))
        .unwrap();
    out.sort_unstable();

    assert_eq!(out, (0u32..N).collect::<Vec<_>>());
    assert_eq!(counter.load(Ordering::SeqCst), N as usize);
}

/// `log_elements_with` may live between transforms and downstream values
/// must remain unchanged.
#[test]
fn test_log_elements_with_between_transforms() {
    let p = Pipeline::default();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_in_closure = Arc::clone(&counter);

    let out = from_vec(&p, vec![1u32, 2, 3, 4])
        .map(|x| x + 1)
        .log_elements_with(move |x| {
            counter_in_closure.fetch_add(1, Ordering::SeqCst);
            format!("{x:08}")
        })
        .map(|x| x * 2)
        .collect_seq_sorted()
        .unwrap();

    assert_eq!(out, vec![4u32, 6, 8, 10]);
    assert_eq!(counter.load(Ordering::SeqCst), 4);
}

/// Two `log_elements_with` taps stacked one after the other each see every
/// element exactly once and still pass the data through unchanged.
#[test]
fn test_log_elements_with_stacked_taps() {
    let p = Pipeline::default();
    let counter_a = Arc::new(AtomicUsize::new(0));
    let counter_b = Arc::new(AtomicUsize::new(0));
    let ca = Arc::clone(&counter_a);
    let cb = Arc::clone(&counter_b);

    let out = from_vec(&p, vec![1u32, 2, 3])
        .log_elements_with(move |x| {
            ca.fetch_add(1, Ordering::SeqCst);
            format!("a:{x}")
        })
        .log_elements_with(move |x| {
            cb.fetch_add(1, Ordering::SeqCst);
            format!("b:{x}")
        })
        .collect_seq()
        .unwrap();

    assert_eq!(out, vec![1u32, 2, 3]);
    assert_eq!(counter_a.load(Ordering::SeqCst), 3);
    assert_eq!(counter_b.load(Ordering::SeqCst), 3);
}

/// `log_elements_with` is honest about ordering: in sequential mode, the
/// formatter sees elements in input order.
#[test]
fn test_log_elements_with_sequential_ordering() {
    use std::sync::Mutex;

    let p = Pipeline::default();
    let seen: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_in_closure = Arc::clone(&seen);

    let out = from_vec(&p, vec![3u32, 1, 4, 1, 5, 9, 2, 6])
        .log_elements_with(move |x| {
            seen_in_closure.lock().unwrap().push(*x);
            String::new()
        })
        .collect_seq()
        .unwrap();

    assert_eq!(out, vec![3u32, 1, 4, 1, 5, 9, 2, 6]);
    let recorded = seen.lock().unwrap().clone();
    assert_eq!(recorded, vec![3u32, 1, 4, 1, 5, 9, 2, 6]);
}
