use anyhow::Result;
use ironbeam::from_vec;
use ironbeam::testing::*;
use ironbeam::{OptimizationDecision, Pipeline, Runner, SharedCSECache, build_plan};

#[test]
fn planner_fuses_stateless_equivalence() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<u32> = (0..10_000).collect();

    // Build with many tiny stateless ops
    let many = from_vec(&p, input)
        .map(|x: &u32| x + 1)
        .filter(|x: &u32| x.is_multiple_of(2))
        .flat_map(|x: &u32| vec![*x, *x]) // duplicate
        .map(|x: &u32| x / 2)
        .filter(|x: &u32| !x.is_multiple_of(3));

    // Collect seq and par to ensure planner changes don't affect results
    let seq = many.clone().collect_seq_sorted()?;
    let par = many.collect_par_sorted(Some(8), None)?;
    assert_collections_equal(&seq, &par);
    Ok(())
}

#[test]
fn planner_drops_mid_materialized_equivalence() -> Result<()> {
    let p = TestPipeline::new();
    let v: Vec<String> = (0..1000).map(|i| format!("w{i}")).collect();

    // Force a mid-chain materialized by collecting and re-inserting (simulating a checkpoint)
    let col = from_vec(&p, v)
        .map(|s: &String| s.to_uppercase())
        .filter(|s: &String| s.len() > 1);

    // Not directly accessible to force Materialized in public API,
    // but if you have tests that insert Node::Materialized, the planner will drop it mid-chain.
    // We just ensure the runner still yields stable results:
    let seq = col.clone().collect_seq_sorted()?;
    let par = col.collect_par_sorted(Some(6), None)?;
    assert_collections_equal(&seq, &par);

    Ok(())
}

/// Functional correctness: `filter_values` before GBK produces correct grouped results.
/// Also exercises the predicate pushdown detection path in the planner.
#[test]
fn predicate_pushed_before_group_by_key_reduces_input() -> Result<()> {
    let p = TestPipeline::new();
    let input = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ],
    );
    // filter_values is key_preserving + value_only + cardinality_reducing — ideal pre-GBK op.
    let result = input
        .filter_values(|v: &u32| *v > 1) // only ("a",2) and ("b",3) survive
        .group_by_key()
        .flat_map(|(k, vs): &(String, Vec<u32>)| {
            vs.iter().map(|&v| format!("{k}:{v}")).collect::<Vec<_>>()
        })
        .collect_seq()?;
    let mut result = result;
    result.sort_unstable();
    assert_eq!(result, vec!["a:2".to_string(), "b:3".to_string()]);
    Ok(())
}

/// CSE: shared prefix executes only once when `run_collect_cached` is used with the
/// same cache for two terminals that branch from the same `PCollection`.
#[test]
fn cse_shared_prefix_executes_once() -> Result<()> {
    use std::sync::{Arc, Mutex};

    let counter = Arc::new(Mutex::new(0usize));
    let c = counter.clone();

    let p = Pipeline::default();
    let src = from_vec(&p, vec![1u32, 2, 3]);
    // The `+10` map increments the counter each time it runs.
    let mapped = src.map(move |x: &u32| {
        *c.lock().unwrap() += 1;
        x + 10
    });
    // Two independent branches from the same intermediate collection.
    let a = mapped.clone().map(|x: &u32| x * 2);
    let b = mapped.map(|x: &u32| x + 1);

    let cache = SharedCSECache::default();
    let runner = Runner {
        mode: ironbeam::ExecMode::Sequential,
        ..Runner::default()
    };

    let mut out_a = runner.run_collect_cached::<u32>(&p, a.node_id(), &cache)?;
    let mut out_b = runner.run_collect_cached::<u32>(&p, b.node_id(), &cache)?;

    out_a.sort_unstable();
    out_b.sort_unstable();

    // Correctness: each branch produces the right values.
    assert_eq!(out_a, vec![22u32, 24, 26]);
    assert_eq!(out_b, vec![12u32, 13, 14]);

    // The shared `+10` map ran exactly 3 times (once per source element),
    // not 6 (which would happen without CSE).
    assert_eq!(
        *counter.lock().unwrap(),
        3,
        "shared prefix should execute only once across both terminals"
    );
    Ok(())
}

/// The explain output must reflect that the predicate-pushdown pass fired.
#[test]
fn predicate_pushdown_is_reflected_in_explain() -> Result<()> {
    let p = TestPipeline::new();
    let input = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let pc = input.filter_values(|v: &u32| *v > 0).group_by_key();
    let plan = build_plan(&p, pc.node_id())?;
    let has_pushdown = plan
        .optimizations
        .iter()
        .any(|o| matches!(o, OptimizationDecision::PushedDownPredicates { .. }));
    assert!(
        has_pushdown,
        "expected PushedDownPredicates optimization to fire"
    );
    Ok(())
}
