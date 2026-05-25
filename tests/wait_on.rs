//! Comprehensive tests for feature 4.12: `PCollection::wait_on`.
//!
//! Validates that `wait_on(&signal)` is a strict passthrough on its element
//! type, value, and order; that the signal subchain is **executed to
//! completion** before downstream consumers run (verified via a shared
//! side-effect counter); and that the dependency works regardless of the
//! signal's element type, signal cardinality, or execution mode.

use ironbeam::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ── Passthrough semantics ────────────────────────────────────────────────────

/// `wait_on` returns the data unchanged and in input order.
#[test]
fn test_wait_on_passes_data_through_unchanged() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);
    let signal = from_vec(&p, vec!["ack".to_string()]);
    let out = data.wait_on(&signal).collect_seq().unwrap();
    assert_eq!(out, vec![1u32, 2, 3, 4, 5]);
}

/// Signal data is **never** observable on the primary path — even when
/// signal happens to share the data's element type and contains values
/// that would be "obvious" if they leaked through.
#[test]
fn test_wait_on_does_not_leak_signal_data_into_primary_path() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 2, 3]);
    let signal = from_vec(&p, vec![99u32, 100, 101]); // would clearly leak
    let mut out = data.wait_on(&signal).collect_seq().unwrap();
    out.sort_unstable();
    assert_eq!(out, vec![1u32, 2, 3]); // no 99 / 100 / 101
}

/// The downstream element type matches the data side, not the signal side.
#[test]
fn test_wait_on_data_type_is_preserved_when_signal_type_differs() {
    let p = Pipeline::default();
    let data: PCollection<String> = from_vec(&p, vec!["x".to_string(), "y".to_string()]);
    let signal: PCollection<u64> = from_vec(&p, vec![1u64, 2, 3]);

    let out: Vec<String> = data.wait_on(&signal).collect_seq().unwrap();
    assert_eq!(
        out,
        vec![String::from("x"), String::from("y")] // <- typed String, not u64
    );
}

/// Empty data yields empty output, regardless of signal content.
#[test]
fn test_wait_on_empty_data_yields_empty_output() {
    let p = Pipeline::default();
    let data: PCollection<u32> = from_vec(&p, Vec::<u32>::new());
    let signal = from_vec(&p, vec!["work".to_string()]);
    let out = data.wait_on(&signal).collect_seq().unwrap();
    assert!(out.is_empty());
}

// ── Signal-side execution ────────────────────────────────────────────────────

/// The signal's transform **runs** during execution of the gated result,
/// even though its data is dropped. We prove this by counting side effects
/// on the signal branch.
#[test]
fn test_wait_on_executes_signal_branch() {
    let p = Pipeline::default();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_in_signal = Arc::clone(&counter);

    let data = from_vec(&p, vec![1u32, 2, 3]);
    let signal = from_vec(&p, vec![10u32, 20, 30, 40]).map(move |x| {
        counter_in_signal.fetch_add(1, Ordering::SeqCst);
        *x
    });

    let gated = data.wait_on(&signal);
    // Counter is incremented only when the pipeline runs.
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    let out = gated.collect_seq().unwrap();
    assert_eq!(out, vec![1u32, 2, 3]);
    assert_eq!(counter.load(Ordering::SeqCst), 4);
}

/// When the signal has zero elements, its chain still completes (so any
/// fixed-cost setup runs) but no per-element side effects fire.
#[test]
fn test_wait_on_with_empty_signal_still_runs_chain() {
    let p = Pipeline::default();
    let elem_counter = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&elem_counter);

    let data = from_vec(&p, vec![7u32, 8, 9]);
    let signal = from_vec(&p, Vec::<u32>::new()).map(move |x| {
        c.fetch_add(1, Ordering::SeqCst);
        *x
    });

    let out = data.wait_on(&signal).collect_seq().unwrap();
    assert_eq!(out, vec![7u32, 8, 9]);
    assert_eq!(elem_counter.load(Ordering::SeqCst), 0);
}

/// The signal chain may itself end at a barrier (e.g. a per-key combine)
/// without disturbing the passthrough.
#[test]
fn test_wait_on_signal_ending_at_barrier() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 2, 3]);
    // Signal terminates in a per-key sum — a `CombineValues` barrier.
    let signal = from_vec(&p, vec![("a", 1u32), ("a", 2), ("b", 3)]).sum_per_key();
    let out = data.wait_on(&signal).collect_seq().unwrap();
    assert_eq!(out, vec![1u32, 2, 3]);
}

// ── Composition ──────────────────────────────────────────────────────────────

/// `wait_on` chains: `data.wait_on(&a).wait_on(&b)` runs both signals
/// before the data is observable.
#[test]
fn test_wait_on_chained_runs_all_signals() {
    let p = Pipeline::default();
    let a_count = Arc::new(AtomicUsize::new(0));
    let b_count = Arc::new(AtomicUsize::new(0));
    let ac = Arc::clone(&a_count);
    let bc = Arc::clone(&b_count);

    let data = from_vec(&p, vec![1u32, 2, 3]);
    let a = from_vec(&p, vec![10u32, 20]).map(move |x| {
        ac.fetch_add(1, Ordering::SeqCst);
        *x
    });
    let b = from_vec(&p, vec![100u32, 200, 300]).map(move |x| {
        bc.fetch_add(1, Ordering::SeqCst);
        *x
    });

    let out = data.wait_on(&a).wait_on(&b).collect_seq().unwrap();
    assert_eq!(out, vec![1u32, 2, 3]);
    assert_eq!(a_count.load(Ordering::SeqCst), 2);
    assert_eq!(b_count.load(Ordering::SeqCst), 3);
}

/// Downstream transforms after `wait_on` operate on the data only and
/// never see signal elements.
#[test]
fn test_wait_on_downstream_sees_only_data() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 2, 3]);
    let signal = from_vec(&p, vec![1000u32, 2000, 3000]);

    let mut out = data
        .wait_on(&signal)
        .map(|x| x * 100)
        .collect_seq()
        .unwrap();
    out.sort_unstable();
    assert_eq!(out, vec![100u32, 200, 300]); // not 100_000…
}

// ── Execution modes ──────────────────────────────────────────────────────────

/// Parallel execution preserves the passthrough contract and still
/// executes the signal exactly once per element.
#[test]
fn test_wait_on_under_parallel_execution() {
    let p = Pipeline::default();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&counter);

    let data = from_vec(&p, (0u32..64).collect::<Vec<_>>());
    let signal = from_vec(&p, (0u32..32).collect::<Vec<_>>()).map(move |x| {
        c.fetch_add(1, Ordering::SeqCst);
        *x
    });

    let mut out = data.wait_on(&signal).collect_par(Some(4), Some(4)).unwrap();
    out.sort_unstable();
    assert_eq!(out, (0u32..64).collect::<Vec<_>>());
    assert_eq!(counter.load(Ordering::SeqCst), 32);
}

// ── Cross-feature integration ────────────────────────────────────────────────

/// `wait_on` is compatible with `with_name` — the Flatten barrier node
/// the call inserts can be labelled, and the name surfaces in the explain
/// output's per-step annotation.
#[test]
fn test_wait_on_can_be_named() {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 2, 3]);
    let signal = from_vec(&p, vec!["ok".to_string()]);
    let gated = data.wait_on(&signal).with_name("AfterSignal");

    assert_eq!(p.node_name(gated.node_id()).as_deref(), Some("AfterSignal"));

    let plan = build_plan(&p, gated.node_id()).unwrap();
    let s = format!("{}", plan.explain());
    assert!(s.contains("[AfterSignal]"), "{s}");
}
