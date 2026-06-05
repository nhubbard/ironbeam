//! Tests for `PCollection::map_values_batches` (backed by `BatchMapValuesOp`).
//!
//! Covers:
//! - Basic functionality and batch-boundary semantics.
//! - Parallel execution.
//! - Planner reorder optimisation (triggers `cost_hint` on `BatchMapValuesOp`).
//! - Contract enforcement: wrong output length panics at runtime.

use ironbeam::*;

/// Basic: square every value; single-key, batch_size covers all elements.
#[test]
fn test_map_values_batches_basic() {
    let p = Pipeline::default();
    let mut result = from_vec(&p, vec![("a".to_string(), 2u32), ("a".to_string(), 3u32)])
        .map_values_batches(10, |vals: &[u32]| vals.iter().map(|v| v * v).collect::<Vec<_>>())
        .collect_seq()
        .unwrap();
    result.sort_unstable();
    assert_eq!(result, vec![("a".to_string(), 4u32), ("a".to_string(), 9u32)]);
}

/// batch_size=1 forces one call per element; verifies chunking correctness.
#[test]
fn test_map_values_batches_one_per_batch() {
    let p = Pipeline::default();
    let mut result = from_vec(
        &p,
        vec![
            ("k".to_string(), 1u32),
            ("k".to_string(), 2u32),
            ("k".to_string(), 3u32),
        ],
    )
    .map_values_batches(1, |vals: &[u32]| vals.iter().map(|v| v + 100).collect::<Vec<_>>())
    .collect_seq()
    .unwrap();
    result.sort_unstable();
    assert_eq!(
        result,
        vec![
            ("k".to_string(), 101u32),
            ("k".to_string(), 102u32),
            ("k".to_string(), 103u32),
        ]
    );
}

/// Multiple keys; verifies that keys are preserved end-to-end.
#[test]
fn test_map_values_batches_multiple_keys() {
    let p = Pipeline::default();
    let mut result = from_vec(
        &p,
        vec![
            ("x".to_string(), 10u32),
            ("y".to_string(), 20u32),
            ("x".to_string(), 30u32),
        ],
    )
    .map_values_batches(5, |vals: &[u32]| vals.iter().map(|v| v * 2).collect::<Vec<_>>())
    .collect_seq()
    .unwrap();
    result.sort_unstable();
    assert_eq!(
        result,
        vec![
            ("x".to_string(), 20u32),
            ("x".to_string(), 60u32),
            ("y".to_string(), 40u32),
        ]
    );
}

/// Parallel execution produces the same element set as sequential.
#[test]
fn test_map_values_batches_parallel_matches_sequential() {
    let items: Vec<(String, u32)> = (0..200)
        .map(|i| (format!("k{}", i % 5), i as u32))
        .collect();

    let p = Pipeline::default();
    let mut seq = from_vec(&p, items.clone())
        .map_values_batches(8, |vals: &[u32]| vals.iter().map(|v| v + 1).collect::<Vec<_>>())
        .collect_seq()
        .unwrap();
    seq.sort_unstable();

    let runner = Runner {
        mode: ExecMode::Parallel {
            threads: Some(4),
            partitions: Some(4),
        },
        ..Default::default()
    };
    let p2 = Pipeline::default();
    let node = from_vec(&p2, items)
        .map_values_batches(8, |vals: &[u32]| vals.iter().map(|v| v + 1).collect::<Vec<_>>())
        .node_id();
    let mut par: Vec<(String, u32)> = runner.run_collect(&p2, node).unwrap();
    par.sort_unstable();

    assert_eq!(seq, par);
}

/// `map_values_batches` (cost_hint=2) followed by `filter_values` (cost_hint=1):
/// the planner fuses them into one stateless block and reorders by cost, calling
/// `cost_hint()` on `BatchMapValuesOp`. The predicate is designed so that both
/// orderings (filter-first and map-first) yield the same result — this is the
/// contract for `reorder_safe_with_value_only` ops.
#[test]
fn test_map_values_batches_reordered_before_filter_values() {
    let p = Pipeline::default();
    // map adds 0 (identity on the value); filter drops negatives.
    // Since adding 0 never changes the sign, filter-first == map-first.
    let mut result = from_vec(
        &p,
        vec![
            ("a".to_string(), 5u32),
            ("b".to_string(), 0u32),
            ("b".to_string(), 3u32),
        ],
    )
    .map_values_batches(10, |vals: &[u32]| vals.iter().map(|v| v + 0).collect::<Vec<_>>())
    .filter_values(|v: &u32| *v > 0)
    .collect_seq()
    .unwrap();
    result.sort_unstable();
    assert_eq!(
        result,
        vec![("a".to_string(), 5u32), ("b".to_string(), 3u32)]
    );
}

/// Contract: the provided function must return the same number of elements as
/// the input slice; violating this triggers a runtime panic.
#[test]
#[should_panic(expected = "BatchMapValuesOp: f(chunk) must return same length")]
fn test_map_values_batches_panics_on_wrong_length() {
    let p = Pipeline::default();
    let _ = from_vec(&p, vec![("a".to_string(), 1u32), ("a".to_string(), 2u32)])
        .map_values_batches(10, |vals: &[u32]| {
            // Returns fewer elements than the input — contract violation.
            vals.iter().take(1).map(|v| *v).collect::<Vec<_>>()
        })
        .collect_seq()
        .unwrap();
}
