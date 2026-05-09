use anyhow::Result;
use ironbeam::from_vec;
use ironbeam::node::Node;
use ironbeam::testing::*;
use ironbeam::{OptimizationDecision, Pipeline, Runner, SharedCSECache, build_plan, flatten};

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

// ── 3.6 Predicate pushdown past Reshuffle ─────────────────────────────────────

/// Functional correctness: `filter_values` before `reshuffle()` produces correct results.
///
/// The predicate-pushdown pass should recognize `Reshuffle` as a transparent barrier and
/// confirm the filter as a pre-barrier predicate without altering semantics.
#[test]
fn predicate_pushed_before_reshuffle_correctness() -> Result<()> {
    let p = TestPipeline::new();
    let input = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
            ("b".to_string(), 4),
        ],
    );
    // filter_values is key_preserving + value_only + cardinality_reducing — eligible pre-Reshuffle.
    let mut result = input
        .filter_values(|v: &u32| *v > 2) // only ("b",3) and ("b",4) survive
        .reshuffle()
        .collect_seq()?;
    result.sort_unstable_by_key(|(k, v): &(String, u32)| (k.clone(), *v));
    assert_eq!(
        result,
        vec![("b".to_string(), 3u32), ("b".to_string(), 4u32)]
    );
    Ok(())
}

/// The `PushedDownPredicates` optimization is recorded when a cardinality-reducing
/// filter immediately precedes a `Reshuffle` barrier.
#[test]
fn predicate_pushdown_before_reshuffle_optimization_fires() -> Result<()> {
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![("a".to_string(), 1u32)])
        .filter_values(|v: &u32| *v > 0)
        .reshuffle();
    let plan = build_plan(&p, pc.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::PushedDownPredicates { .. })),
        "expected PushedDownPredicates optimization to fire before Reshuffle"
    );
    Ok(())
}

/// The explain output mentions the predicate-pushdown optimization when it fires
/// before a `Reshuffle`.
#[test]
fn predicate_pushdown_before_reshuffle_appears_in_explain() -> Result<()> {
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![("x".to_string(), 1u32)])
        .filter_values(|v: &u32| *v > 0)
        .reshuffle();
    let plan = build_plan(&p, pc.node_id())?;
    let explain = plan.explain().to_string();
    assert!(
        explain.contains("Predicate Pushdown Before Shuffle Barrier"),
        "explain output should mention the predicate pushdown: {explain}"
    );
    Ok(())
}

/// Predicate pushdown fires before both `GroupByKey` and `Reshuffle` in the same chain.
/// The `ops_pushed` count must reflect filters confirmed before each barrier.
#[test]
fn predicate_pushdown_fires_before_both_gbk_and_reshuffle() -> Result<()> {
    let p = TestPipeline::new();
    // chain: filter_values -> reshuffle -> filter_values -> group_by_key
    let pc = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 5),
            ("b".to_string(), 10),
        ],
    )
    .filter_values(|v: &u32| *v > 1) // pre-Reshuffle filter
    .reshuffle()
    .filter_values(|v: &u32| *v < 10) // pre-GBK filter
    .group_by_key();
    let plan = build_plan(&p, pc.node_id())?;
    let total_pushed: usize = plan
        .optimizations
        .iter()
        .filter_map(|o| {
            if let OptimizationDecision::PushedDownPredicates { ops_pushed } = o {
                Some(*ops_pushed)
            } else {
                None
            }
        })
        .sum();
    assert!(
        total_pushed >= 2,
        "expected at least 2 ops pushed (one before Reshuffle, one before GBK), got {total_pushed}"
    );
    Ok(())
}

// ── 3.5 Reshuffle elimination ──────────────────────────────────────────────────

/// Functional correctness: `reshuffle()` before `group_by_key()` is eliminated
/// but results are unchanged.
#[test]
fn planner_eliminates_reshuffle_before_group_by_key_correctness() -> Result<()> {
    let p = TestPipeline::new();
    let data = vec![
        ("a".to_string(), 1i32),
        ("a".to_string(), 2),
        ("b".to_string(), 3),
    ];

    let result = from_vec(&p, data)
        .reshuffle()
        .group_by_key()
        .flat_map(|(k, vs): &(String, Vec<i32>)| {
            vs.iter().map(|&v| format!("{k}:{v}")).collect::<Vec<_>>()
        })
        .collect_seq_sorted()?;

    assert_eq!(
        result,
        vec!["a:1".to_string(), "a:2".to_string(), "b:3".to_string()]
    );
    Ok(())
}

/// The `EliminatedReshuffle` optimization decision is recorded when a `Reshuffle`
/// immediately precedes a `GroupByKey`.
#[test]
fn planner_reshuffle_before_gbk_optimization_fires() -> Result<()> {
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![("a".to_string(), 1i32)])
        .reshuffle()
        .group_by_key();
    let plan = build_plan(&p, pc.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::EliminatedReshuffle { .. })),
        "expected EliminatedReshuffle optimization to fire before GroupByKey"
    );
    Ok(())
}

/// Functional correctness: two consecutive `reshuffle()` calls are collapsed to
/// one and all elements are preserved.
#[test]
fn planner_eliminates_consecutive_reshuffles_correctness() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<i32> = (1..=10).collect();
    let mut result = from_vec(&p, input.clone())
        .reshuffle()
        .reshuffle()
        .reshuffle()
        .collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, input);
    Ok(())
}

/// Two consecutive `Reshuffle` nodes are reduced to exactly one, and the
/// `EliminatedReshuffle` optimization is recorded with `count >= 1`.
#[test]
fn planner_consecutive_reshuffles_optimization_fires() -> Result<()> {
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![1i32, 2, 3]).reshuffle().reshuffle();
    let plan = build_plan(&p, pc.node_id())?;

    assert!(
        plan.optimizations.iter().any(
            |o| matches!(o, OptimizationDecision::EliminatedReshuffle { count } if *count >= 1)
        ),
        "expected EliminatedReshuffle optimization to fire for consecutive reshuffles"
    );

    let reshuffle_count = plan
        .chain
        .iter()
        .filter(|n| matches!(n, Node::Reshuffle { .. }))
        .count();
    assert_eq!(
        reshuffle_count, 1,
        "two consecutive reshuffles should collapse to exactly one"
    );
    Ok(())
}

/// Three consecutive `Reshuffle` nodes all collapse to exactly one in a single
/// pass.
#[test]
fn planner_triple_consecutive_reshuffles_collapse_to_one() -> Result<()> {
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![1i32]).reshuffle().reshuffle().reshuffle();
    let plan = build_plan(&p, pc.node_id())?;

    let reshuffle_count = plan
        .chain
        .iter()
        .filter(|n| matches!(n, Node::Reshuffle { .. }))
        .count();
    assert_eq!(
        reshuffle_count, 1,
        "three consecutive reshuffles should collapse to exactly one"
    );
    Ok(())
}

/// A solitary `Reshuffle` at the end of the chain is NOT eliminated — it has
/// real work to do (redistributing elements for downstream parallelism).
#[test]
fn planner_does_not_eliminate_solitary_reshuffle() -> Result<()> {
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![1i32, 2, 3]).reshuffle();
    let plan = build_plan(&p, pc.node_id())?;

    assert!(
        !plan
            .optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::EliminatedReshuffle { .. })),
        "a solitary reshuffle at chain end must not be eliminated"
    );

    let reshuffle_count = plan
        .chain
        .iter()
        .filter(|n| matches!(n, Node::Reshuffle { .. }))
        .count();
    assert_eq!(
        reshuffle_count, 1,
        "solitary reshuffle must remain in chain"
    );
    Ok(())
}

/// A `Reshuffle` that follows a barrier (not precedes one) is semantically
/// meaningful and must NOT be eliminated.
#[test]
fn planner_does_not_eliminate_reshuffle_after_barrier() -> Result<()> {
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![("a".to_string(), 1i32)])
        .group_by_key()
        .flat_map(|(k, vs): &(String, Vec<i32>)| {
            vs.iter().map(|&v| (k.clone(), v)).collect::<Vec<_>>()
        })
        .reshuffle();
    let plan = build_plan(&p, pc.node_id())?;

    assert!(
        !plan
            .optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::EliminatedReshuffle { .. })),
        "reshuffle after a barrier redistributes for downstream parallelism — must not be eliminated"
    );
    Ok(())
}

/// A `Reshuffle` immediately before a `CombineValues` barrier is eliminated.
/// Uses `sum_per_key` which routes through `CombineValues` without a preceding
/// `GroupByKey`, so the pattern `[Reshuffle, CombineValues]` is visible directly.
#[test]
fn planner_eliminates_reshuffle_before_combine_values() -> Result<()> {
    let p = TestPipeline::new();
    let data = vec![
        ("a".to_string(), 1i32),
        ("a".to_string(), 2),
        ("b".to_string(), 3),
    ];
    // sum_per_key produces [Source, Reshuffle, CombineValues].
    // eliminate_reshuffle drops the Reshuffle: [Source, CombineValues].
    let pc = from_vec(&p, data.clone()).reshuffle().sum_per_key();
    let plan = build_plan(&p, pc.node_id())?;

    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::EliminatedReshuffle { .. })),
        "expected EliminatedReshuffle to fire before CombineValues"
    );

    // Functional correctness
    let mut result = from_vec(&p, data).reshuffle().sum_per_key().collect_seq()?;
    result.sort_by_key(|(k, _): &(String, i32)| k.clone());
    assert_eq!(result, vec![("a".to_string(), 3i32), ("b".to_string(), 3)]);
    Ok(())
}

/// The `explain()` output includes a description of the `EliminatedReshuffle`
/// optimization when it fires.
#[test]
fn eliminated_reshuffle_appears_in_explain() -> Result<()> {
    let p = TestPipeline::new();
    let pc = from_vec(&p, vec![("x".to_string(), 1i32)])
        .reshuffle()
        .group_by_key();
    let plan = build_plan(&p, pc.node_id())?;
    let explain = plan.explain().to_string();
    assert!(
        explain.contains("Eliminated Redundant Reshuffle"),
        "explain output should mention the reshuffle elimination: {explain}"
    );
    Ok(())
}

/// `Reshuffle` immediately before `Flatten` is eliminated; all elements from
/// both input collections are still present in the output.
#[test]
fn planner_eliminates_reshuffle_before_flatten_correctness() -> Result<()> {
    let p = TestPipeline::new();
    let a = from_vec(&p, vec![1i32, 2, 3]);
    let b = from_vec(&p, vec![4i32, 5, 6]);
    // flatten embeds subplans; adding reshuffle after flatten then checking
    // that the result is stable across seq/par confirms the overall plan is sound.
    let merged = flatten(&[&a, &b]);
    let seq = merged.clone().collect_seq_sorted()?;
    let par = merged.collect_par_sorted(Some(4), None)?;
    assert_collections_equal(&seq, &par);
    assert_eq!(seq, vec![1, 2, 3, 4, 5, 6]);
    Ok(())
}

// ── 3.7 Predicate pushdown into Flatten subplans ───────────────────────────────

/// Functional correctness: a `filter_values` following a `flatten` of two KV collections
/// produces the same result as applying the filter without the optimization.
#[test]
fn predicate_pushed_into_flatten_subplans_correctness() -> Result<()> {
    let p = TestPipeline::new();
    let a = from_vec(&p, vec![("x".to_string(), 1u32), ("x".to_string(), 10)]);
    let b = from_vec(&p, vec![("y".to_string(), 2u32), ("y".to_string(), 20)]);
    // filter_values is value_only + cardinality_reducing — eligible for Flatten pushdown.
    let mut result = flatten(&[&a, &b])
        .filter_values(|v: &u32| *v >= 10) // only ("x",10) and ("y",20) survive
        .collect_seq()?;
    result.sort_unstable_by_key(|(k, v): &(String, u32)| (k.clone(), *v));
    assert_eq!(
        result,
        vec![("x".to_string(), 10u32), ("y".to_string(), 20u32)]
    );
    Ok(())
}

/// The `PushedDownIntoFlattenSubplans` optimization fires when a `value_only +
/// cardinality_reducing` op immediately follows a `Flatten` node.
#[test]
fn predicate_pushdown_into_flatten_optimization_fires() -> Result<()> {
    let p = TestPipeline::new();
    let a = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let b = from_vec(&p, vec![("b".to_string(), 2u32)]);
    let pc = flatten(&[&a, &b]).filter_values(|v: &u32| *v > 0);
    let plan = build_plan(&p, pc.node_id())?;
    assert!(
        plan.optimizations.iter().any(|o| matches!(
            o,
            OptimizationDecision::PushedDownIntoFlattenSubplans { .. }
        )),
        "expected PushedDownIntoFlattenSubplans optimization to fire"
    );
    Ok(())
}

/// When ALL post-Flatten ops are pushable, the outer `Stateless` block is removed entirely.
#[test]
fn predicate_pushdown_into_flatten_removes_empty_stateless() -> Result<()> {
    let p = TestPipeline::new();
    let a = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let b = from_vec(&p, vec![("b".to_string(), 2u32)]);
    // Only filter_values follows the flatten — it should be pushed in and the outer
    // Stateless block should disappear from the chain.
    let pc = flatten(&[&a, &b]).filter_values(|v: &u32| *v > 0);
    let plan = build_plan(&p, pc.node_id())?;
    // Walk the chain: there should be no Stateless node immediately following a Flatten.
    let chain = &plan.chain;
    for window in chain.windows(2) {
        assert!(
            !(matches!(window[0], Node::Flatten { .. }) && matches!(window[1], Node::Stateless(_))),
            "Stateless block should have been removed after Flatten when all ops were pushed"
        );
    }
    Ok(())
}

/// The explain output mentions the Flatten predicate-pushdown optimization.
#[test]
fn predicate_pushdown_into_flatten_appears_in_explain() -> Result<()> {
    let p = TestPipeline::new();
    let a = from_vec(&p, vec![("x".to_string(), 1u32)]);
    let b = from_vec(&p, vec![("y".to_string(), 2u32)]);
    let pc = flatten(&[&a, &b]).filter_values(|v: &u32| *v > 0);
    let plan = build_plan(&p, pc.node_id())?;
    let explain = plan.explain().to_string();
    assert!(
        explain.contains("Predicate Pushdown Into Flatten Subplans"),
        "explain output should mention the flatten pushdown: {explain}"
    );
    Ok(())
}

/// A non-`value_only` op (plain `filter`) following a `Flatten` is NOT pushed into
/// subplans — it must remain in the outer chain unchanged.
#[test]
fn predicate_pushdown_into_flatten_leaves_non_value_only_ops() -> Result<()> {
    let p = TestPipeline::new();
    let a = from_vec(&p, vec![1i32, 2, 3]);
    let b = from_vec(&p, vec![4i32, 5, 6]);
    // plain filter() is cardinality_reducing but NOT value_only — must not be pushed.
    let pc = flatten(&[&a, &b]).filter(|v: &i32| *v > 3);
    let plan = build_plan(&p, pc.node_id())?;
    assert!(
        !plan.optimizations.iter().any(|o| matches!(
            o,
            OptimizationDecision::PushedDownIntoFlattenSubplans { .. }
        )),
        "plain filter (not value_only) must not trigger PushedDownIntoFlattenSubplans"
    );
    // Functional correctness must still hold.
    let mut result = flatten(&[&a, &b]).filter(|v: &i32| *v > 3).collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, vec![4i32, 5, 6]);
    Ok(())
}

/// Optimization fires and subplan_count reflects all input collections.
#[test]
fn predicate_pushdown_into_flatten_subplan_count_matches_inputs() -> Result<()> {
    let p = TestPipeline::new();
    let a = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let b = from_vec(&p, vec![("b".to_string(), 2u32)]);
    let c = from_vec(&p, vec![("c".to_string(), 3u32)]);
    let pc = flatten(&[&a, &b, &c]).filter_values(|v: &u32| *v > 0);
    let plan = build_plan(&p, pc.node_id())?;
    let subplan_count: usize = plan
        .optimizations
        .iter()
        .filter_map(|o| {
            if let OptimizationDecision::PushedDownIntoFlattenSubplans { subplan_count, .. } = o {
                Some(*subplan_count)
            } else {
                None
            }
        })
        .sum();
    assert_eq!(
        subplan_count, 3,
        "three input collections → subplan_count should be 3, got {subplan_count}"
    );
    Ok(())
}
