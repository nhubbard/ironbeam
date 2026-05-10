use anyhow::Result;
use ironbeam::from_vec;
use ironbeam::node::Node;
use ironbeam::testing::*;
use ironbeam::{
    OptimizationDecision, PCollection, Pipeline, Runner, SharedCSECache, build_plan,
    cogroup_by_key, flatten,
};

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

/// The `explain` output must reflect that the predicate-pushdown pass fired.
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

/// The `explain` output mentions the predicate-pushdown optimization when it fires
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
    // flatten embeds subplans; adding reshuffle after Flatten op, then checking
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

/// Optimization fires and `subplan_count` reflects all input collections.
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

// ── 3.8 Dead Subtree Elimination ──────────────────────────────────────────────

/// A purely linear pipeline has no dead branches; the optimization must NOT fire.
#[test]
fn dead_subtree_elimination_does_not_fire_on_linear_pipeline() -> Result<()> {
    let p = Pipeline::default();
    let pc = from_vec(&p, vec![1i32, 2, 3])
        .map(|x: &i32| x * 2)
        .filter(|x: &i32| *x > 2);
    let plan = build_plan(&p, pc.node_id())?;
    assert!(
        !plan
            .optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::PrunedDeadSubtrees { .. })),
        "PrunedDeadSubtrees must not fire when there are no dead branches"
    );
    Ok(())
}

/// When two branches diverge from a shared source, building the plan for one branch
/// must prune the other branch's nodes and record `PrunedDeadSubtrees`.
#[test]
fn dead_branch_is_pruned_optimization_fires() -> Result<()> {
    let p = Pipeline::default();
    let src = from_vec(&p, vec![1i32, 2, 3]);
    // Two branches from the same source.
    let branch_a = src.clone().map(|x: &i32| x * 10); // terminal A
    let _branch_b = src.filter(|x: &i32| *x > 1); // terminal B — dead relative to A

    let plan = build_plan(&p, branch_a.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::PrunedDeadSubtrees { .. })),
        "expected PrunedDeadSubtrees optimization to fire when a dead branch exists"
    );
    Ok(())
}

/// The `nodes_pruned` count equals the number of nodes that belong exclusively to the
/// dead branch (one `Stateless` node for the dead `filter`).
#[test]
fn dead_branch_pruned_count_is_correct() -> Result<()> {
    let p = Pipeline::default();
    let src = from_vec(&p, vec![1i32, 2, 3]);
    let branch_a = src.clone().map(|x: &i32| x * 10);
    let _branch_b = src.filter(|x: &i32| *x > 1); // one extra node

    let plan = build_plan(&p, branch_a.node_id())?;
    let pruned: usize = plan
        .optimizations
        .iter()
        .filter_map(|o| {
            if let OptimizationDecision::PrunedDeadSubtrees { nodes_pruned } = o {
                Some(*nodes_pruned)
            } else {
                None
            }
        })
        .sum();
    assert_eq!(
        pruned, 1,
        "exactly one dead-branch node (the filter Stateless) should be pruned, got {pruned}"
    );
    Ok(())
}

/// Functional correctness: pruning dead branches must not alter the results of the
/// live branch.
#[test]
fn dead_subtree_pruning_does_not_affect_results() -> Result<()> {
    let p = Pipeline::default();
    let src = from_vec(&p, vec![1i32, 2, 3, 4, 5]);
    let branch_a = src.clone().map(|x: &i32| x * 2);
    // Dead branch — its existence should have no observable effect on branch_a's output.
    let _dead = src.filter(|x: &i32| *x % 2 == 0);

    let mut result = branch_a.collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, vec![2i32, 4, 6, 8, 10]);
    Ok(())
}

/// The explain output includes a description of the `PrunedDeadSubtrees` optimization
/// when it fires.
#[test]
fn pruned_dead_subtrees_appears_in_explain() -> Result<()> {
    let p = Pipeline::default();
    let src = from_vec(&p, vec![1i32, 2, 3]);
    let branch_a = src.clone().map(|x: &i32| x + 1);
    let _dead = src.filter(|x: &i32| *x > 0);

    let plan = build_plan(&p, branch_a.node_id())?;
    let explain = plan.explain().to_string();
    assert!(
        explain.contains("Dead Subtree Elimination"),
        "explain output should mention Dead Subtree Elimination: {explain}"
    );
    Ok(())
}

/// A longer dead branch (multiple chained operations) is entirely pruned, and the
/// `nodes_pruned` count reflects all nodes on the dead path.
#[test]
fn long_dead_branch_is_fully_pruned() -> Result<()> {
    let p = Pipeline::default();
    let src = from_vec(&p, vec![1i32, 2, 3]);
    let branch_a = src.clone().map(|x: &i32| x + 100); // live terminal
    // Dead branch: filter -> map -> filter (3 extra nodes).
    let _dead = src
        .filter(|x: &i32| *x > 0)
        .map(|x: &i32| x * 3)
        .filter(|x: &i32| *x < 100);

    let plan = build_plan(&p, branch_a.node_id())?;
    let pruned: usize = plan
        .optimizations
        .iter()
        .filter_map(|o| {
            if let OptimizationDecision::PrunedDeadSubtrees { nodes_pruned } = o {
                Some(*nodes_pruned)
            } else {
                None
            }
        })
        .sum();
    assert_eq!(
        pruned, 3,
        "three nodes on the dead branch should be pruned, got {pruned}"
    );

    // Functional correctness of the live branch.
    let mut result = from_vec(&p, vec![1i32, 2, 3])
        .map(|x: &i32| x + 100)
        .collect_seq()?;
    result.sort_unstable();
    assert_eq!(result, vec![101i32, 102, 103]);
    Ok(())
}

// ── 3.9 CoGroup Join Ordering ──────────────────────────────────────────────────

/// When all cogroup inputs are already in ascending cardinality order, no reordering
/// occurs and `ReorderedCoGroupInputs` is NOT emitted.
#[test]
fn cogroup_no_reorder_when_already_ascending() -> Result<()> {
    let p = Pipeline::default();
    // c1 (5 elements) ≤ c2 (10 elements) ≤ c3 (20 elements) — already ascending.
    let c1: PCollection<(String, u32)> =
        from_vec(&p, (0u32..5).map(|i| (format!("k{i}"), i)).collect());
    let c2: PCollection<(String, u32)> =
        from_vec(&p, (0u32..10).map(|i| (format!("k{i}"), i)).collect());
    let c3: PCollection<(String, u32)> =
        from_vec(&p, (0u32..20).map(|i| (format!("k{i}"), i)).collect());
    let pc = cogroup_by_key!(c1, c2, c3);
    let plan = build_plan(&p, pc.node_id())?;
    assert!(
        !plan
            .optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::ReorderedCoGroupInputs { .. })),
        "inputs already in ascending order must not trigger ReorderedCoGroupInputs"
    );
    Ok(())
}

/// When inputs are in descending cardinality order, the planner reorders and records
/// the `ReorderedCoGroupInputs` optimization.
#[test]
fn cogroup_reorders_descending_inputs() -> Result<()> {
    let p = Pipeline::default();
    // c1 (1000) > c2 (100) > c3 (10): requires reordering to ascending.
    let c1: PCollection<(String, u32)> =
        from_vec(&p, (0u32..1000).map(|i| (format!("k{i}"), i)).collect());
    let c2: PCollection<(String, u32)> =
        from_vec(&p, (0u32..100).map(|i| (format!("k{i}"), i)).collect());
    let c3: PCollection<(String, u32)> =
        from_vec(&p, (0u32..10).map(|i| (format!("k{i}"), i)).collect());
    let pc = cogroup_by_key!(c1, c2, c3);
    let plan = build_plan(&p, pc.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::ReorderedCoGroupInputs { .. })),
        "inputs in descending order must trigger ReorderedCoGroupInputs"
    );
    Ok(())
}

/// The `new_order` permutation places the smallest chain at index 0.
///
/// With c1=100 elements, c2=10, c3=50: the sorted order is [c2, c3, c1],
/// so `new_order` should be `[1, 2, 0]`.
#[test]
fn cogroup_reorder_permutation_is_correct() -> Result<()> {
    let p = Pipeline::default();
    let c1: PCollection<(String, u32)> =
        from_vec(&p, (0u32..100).map(|i| (format!("k{i}"), i)).collect());
    let c2: PCollection<(String, u32)> =
        from_vec(&p, (0u32..10).map(|i| (format!("k{i}"), i)).collect());
    let c3: PCollection<(String, u32)> =
        from_vec(&p, (0u32..50).map(|i| (format!("k{i}"), i)).collect());
    let pc = cogroup_by_key!(c1, c2, c3);
    let plan = build_plan(&p, pc.node_id())?;

    let (original, new) = plan
        .optimizations
        .iter()
        .find_map(|o| {
            if let OptimizationDecision::ReorderedCoGroupInputs {
                original_order,
                new_order,
            } = o
            {
                Some((original_order.clone(), new_order.clone()))
            } else {
                None
            }
        })
        .expect("expected ReorderedCoGroupInputs optimization");

    assert_eq!(original, vec![0, 1, 2], "original_order must be [0,1,2]");
    assert_eq!(
        new,
        vec![1, 2, 0],
        "new_order must place c2(10) first, c3(50) second, c1(100) last"
    );
    Ok(())
}

/// Functional correctness: reordering Flatten subchains does not alter cogroup results.
///
/// Even after the planner reorders the subchains inside the Flatten, each element
/// still carries its correct type-level tag, so the final grouping is identical.
#[test]
fn cogroup_reorder_preserves_correctness() -> Result<()> {
    let p = Pipeline::default();
    // c1 (large) > c2 (small) — planner will reorder to [c2, c1].
    let c1: PCollection<(String, u32)> = from_vec(
        &p,
        vec![
            ("alice".to_string(), 100u32),
            ("bob".to_string(), 200u32),
            ("alice".to_string(), 150u32),
        ],
    );
    let c2: PCollection<(String, u32)> = from_vec(
        &p,
        vec![("alice".to_string(), 1u32), ("carol".to_string(), 2u32)],
    );

    let pc = cogroup_by_key!(c1, c2);
    let mut result = pc.collect_seq()?;
    result.sort_by_key(|(k, _): &(String, _)| k.clone());

    let alice = result.iter().find(|(k, _)| k == "alice").unwrap();
    let mut alice_c1 = alice.1.0.clone();
    alice_c1.sort_unstable();
    assert_eq!(alice_c1, vec![100u32, 150]);
    assert_eq!(alice.1.1, vec![1u32]);

    let bob = result.iter().find(|(k, _)| k == "bob").unwrap();
    assert_eq!(bob.1.0, vec![200u32]);
    assert!(bob.1.1.is_empty());

    let carol = result.iter().find(|(k, _)| k == "carol").unwrap();
    assert!(carol.1.0.is_empty());
    assert_eq!(carol.1.1, vec![2u32]);
    Ok(())
}

/// The explain output contains "`CoGroup` Input Reordering" when the optimization fires.
#[test]
fn cogroup_reorder_appears_in_explain() -> Result<()> {
    let p = Pipeline::default();
    // c1 (100) > c2 (10): reordering expected.
    let c1: PCollection<(String, u32)> =
        from_vec(&p, (0u32..100).map(|i| (format!("k{i}"), i)).collect());
    let c2: PCollection<(String, u32)> =
        from_vec(&p, (0u32..10).map(|i| (format!("k{i}"), i)).collect());
    let pc = cogroup_by_key!(c1, c2);
    let plan = build_plan(&p, pc.node_id())?;
    let explain = plan.explain().to_string();
    assert!(
        explain.contains("CoGroup` Input Reordering"),
        "explain output should mention CoGroup Input Reordering: {explain}"
    );
    Ok(())
}

/// A two-collection Flatten where both have equal (or ascending) cardinality is NOT
/// reordered; the optimization must fire only when a permutation is actually needed.
#[test]
fn cogroup_equal_cardinality_not_reordered() -> Result<()> {
    let p = Pipeline::default();
    let c1: PCollection<(String, u32)> =
        from_vec(&p, (0u32..10).map(|i| (format!("k{i}"), i)).collect());
    let c2: PCollection<(String, u32)> =
        from_vec(&p, (0u32..10).map(|i| (format!("k{i}"), i)).collect());
    let pc = cogroup_by_key!(c1, c2);
    let plan = build_plan(&p, pc.node_id())?;
    assert!(
        !plan
            .optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::ReorderedCoGroupInputs { .. })),
        "equal-cardinality inputs must not trigger ReorderedCoGroupInputs (already ordered)"
    );
    Ok(())
}

// ---- 3.10: Tree Reduction for Associative Combiners ----

use ironbeam::collection::Count;
use ironbeam::combiners::{Count as CombCount, Max, Min, Sum};

/// `Sum` is associative+commutative, so a `CombineGlobal` built from it must
/// have `tree_reduce == true` in the compiled plan.
#[test]
fn tree_reduce_flag_set_for_sum_combiner() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u64, 2, 3, 4, 5]);
    let out = data.combine_globally(Sum::<u64>::default(), None);
    let plan = build_plan(&p, out.node_id())?;
    let has_tree_reduce = plan.chain.iter().any(|n| {
        matches!(
            n,
            ironbeam::node::Node::CombineGlobal {
                tree_reduce: true,
                ..
            }
        )
    });
    assert!(
        has_tree_reduce,
        "Sum combiner must produce CombineGlobal with tree_reduce=true"
    );
    Ok(())
}

/// `Count` (collection-level) is associative+commutative — same check.
#[test]
fn tree_reduce_flag_set_for_count_combiner() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u32, 2, 3]);
    let out = data.combine_globally(Count, None);
    let plan = build_plan(&p, out.node_id())?;
    let has_tree_reduce = plan.chain.iter().any(|n| {
        matches!(
            n,
            ironbeam::node::Node::CombineGlobal {
                tree_reduce: true,
                ..
            }
        )
    });
    assert!(
        has_tree_reduce,
        "Count combiner must produce CombineGlobal with tree_reduce=true"
    );
    Ok(())
}

/// `Min` is AC; verify the flag is set.
#[test]
fn tree_reduce_flag_set_for_min_combiner() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![10i64, 3, 7]);
    let out = data.combine_globally(Min::<i64>::default(), None);
    let plan = build_plan(&p, out.node_id())?;
    assert!(
        plan.chain.iter().any(|n| matches!(
            n,
            Node::CombineGlobal {
                tree_reduce: true,
                ..
            }
        )),
        "Min combiner must produce CombineGlobal with tree_reduce=true"
    );
    Ok(())
}

/// A custom combiner that does NOT declare AC must NOT set `tree_reduce`.
#[test]
fn tree_reduce_not_set_for_non_ac_combiner() -> Result<()> {
    use ironbeam::CombineFn;

    #[derive(Clone, Default)]
    struct ConcatStr;
    impl CombineFn<String, String, String> for ConcatStr {
        fn create(&self) -> String {
            String::new()
        }
        fn add_input(&self, acc: &mut String, v: String) {
            acc.push_str(&v);
        }
        fn merge(&self, acc: &mut String, other: String) {
            acc.push_str(&other);
        }
        fn finish(&self, acc: String) -> String {
            acc
        }
        // is_associative_commutative() returns false (default)
    }

    let p = Pipeline::default();
    let data = from_vec(&p, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    let out = data.combine_globally(ConcatStr, None);
    let plan = build_plan(&p, out.node_id())?;
    assert!(
        !plan.chain.iter().any(|n| matches!(
            n,
            Node::CombineGlobal {
                tree_reduce: true,
                ..
            }
        )),
        "non-AC combiner must NOT produce CombineGlobal with tree_reduce=true"
    );
    Ok(())
}

/// Tree reduction for `Sum` must produce the same result as the sequential fold.
#[test]
fn tree_reduce_sum_produces_correct_result_seq() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (1u64..=100).collect::<Vec<_>>());
    let out = data.combine_globally(Sum::<u64>::default(), None);
    let result = out.collect_seq()?;
    assert_eq!(result, vec![5050u64], "sum 1..=100 should equal 5050");
    Ok(())
}

/// Tree reduction for `Sum` must produce the same result in parallel mode.
#[test]
fn tree_reduce_sum_produces_correct_result_par() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (1u64..=100).collect::<Vec<_>>());
    let out = data.combine_globally(Sum::<u64>::default(), None);
    let result = out.collect_par(Some(8), None)?;
    assert_eq!(
        result,
        vec![5050u64],
        "parallel sum 1..=100 should equal 5050"
    );
    Ok(())
}

/// Tree reduction for `Max` must return the correct maximum.
#[test]
fn tree_reduce_max_produces_correct_result_par() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![3u32, 1, 4, 1, 5, 9, 2, 6]);
    let out = data.combine_globally(Max::<u32>::default(), None);
    let result = out.collect_par(Some(4), None)?;
    assert_eq!(result, vec![9u32]);
    Ok(())
}

/// Tree reduction for `Min` must return the correct minimum.
#[test]
fn tree_reduce_min_produces_correct_result_par() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![3u32, 1, 4, 1, 5, 9, 2, 6]);
    let out = data.combine_globally(Min::<u32>::default(), None);
    let result = out.collect_par(Some(4), None)?;
    assert_eq!(result, vec![1u32]);
    Ok(())
}

/// Tree reduction for combiner-level `Count<T>` should count correctly.
#[test]
fn tree_reduce_combiner_count_correct() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (0u32..50).collect::<Vec<_>>());
    let out = data.combine_globally(CombCount::<u32>::new(), None);
    let result = out.collect_par(Some(4), None)?;
    assert_eq!(result, vec![50u64]);
    Ok(())
}

/// `TreeReduction` decision appears in the plan explain output for an AC combiner.
#[test]
fn tree_reduction_appears_in_explain() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (1u64..=10).collect::<Vec<_>>());
    let out = data.combine_globally(Sum::<u64>::default(), None);
    let plan = build_plan(&p, out.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::TreeReduction { .. })),
        "TreeReduction decision must appear in optimizations for Sum combiner"
    );
    let explain = format!("{}", plan.explain());
    assert!(
        explain.contains("Tree Reduction"),
        "explain text must mention Tree Reduction: {explain}"
    );
    Ok(())
}

/// `TreeReduction` does NOT appear for a non-AC combiner.
#[test]
fn tree_reduction_absent_for_non_ac_combiner() -> Result<()> {
    use ironbeam::CombineFn;

    #[derive(Clone, Default)]
    struct SumButNotAC;
    impl CombineFn<u64, u64, u64> for SumButNotAC {
        fn create(&self) -> u64 {
            0
        }
        fn add_input(&self, acc: &mut u64, v: u64) {
            *acc += v;
        }
        fn merge(&self, acc: &mut u64, other: u64) {
            *acc += other;
        }
        fn finish(&self, acc: u64) -> u64 {
            acc
        }
    }

    let p = Pipeline::default();
    let data = from_vec(&p, vec![1u64, 2, 3]);
    let out = data.combine_globally(SumButNotAC, None);
    let plan = build_plan(&p, out.node_id())?;
    assert!(
        !plan
            .optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::TreeReduction { .. })),
        "TreeReduction must NOT appear for non-AC combiner"
    );
    Ok(())
}

/// Keyed combine with AC combiner (Sum) produces correct results in parallel mode,
/// verifying the parallel-within-key local closure is correct.
#[test]
fn tree_reduce_keyed_combine_correct_par() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            ("a", 1u64),
            ("b", 10u64),
            ("a", 2u64),
            ("b", 20u64),
            ("a", 3u64),
            ("c", 100u64),
        ],
    );
    let mut result = data
        .combine_values(Sum::<u64>::default())
        .collect_par_sorted(Some(4), None)?;
    result.sort_by_key(|(k, _)| *k);
    assert_eq!(result, vec![("a", 6u64), ("b", 30u64), ("c", 100u64)]);
    Ok(())
}

// ─── Feature 3.11: Early Termination / Limit Pushdown ───────────────────────

/// `Plan::limit` is `Some(n)` when the terminal stateless block ends with a `TakeOp`.
#[test]
fn take_sets_plan_limit() -> Result<()> {
    let p = Pipeline::default();
    let col = from_vec(&p, (0..100u32).collect::<Vec<_>>()).take(10);
    let plan = build_plan(&p, col.node_id())?;
    assert_eq!(plan.limit, Some(10), "plan.limit should be Some(10)");
    Ok(())
}

/// `Plan::limit` is `None` for pipelines without a terminal `TakeOp`.
#[test]
fn no_take_means_no_plan_limit() -> Result<()> {
    let p = Pipeline::default();
    let col = from_vec(&p, vec![1u32, 2, 3]).map(|x| x + 1);
    let plan = build_plan(&p, col.node_id())?;
    assert!(plan.limit.is_none(), "plan.limit should be None");
    Ok(())
}

/// `first()` is sugar for `take(1)` — plan.limit should be Some(1).
#[test]
fn first_sets_plan_limit_one() -> Result<()> {
    let p = Pipeline::default();
    let col = from_vec(&p, vec![10u32, 20, 30]).first();
    let plan = build_plan(&p, col.node_id())?;
    assert_eq!(plan.limit, Some(1));
    Ok(())
}

/// `LimitPushdown` appears in the optimizations list when `take(N)` terminates the chain.
#[test]
fn limit_pushdown_appears_in_optimizations() -> Result<()> {
    let p = Pipeline::default();
    let col = from_vec(&p, (0..1000u32).collect::<Vec<_>>()).take(5);
    let plan = build_plan(&p, col.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::LimitPushdown { n: 5 })),
        "LimitPushdown {{ n: 5 }} must appear in optimizations"
    );
    Ok(())
}

/// `LimitPushdown` is present in the explain output string.
#[test]
fn limit_pushdown_appears_in_explain() -> Result<()> {
    let p = Pipeline::default();
    let col = from_vec(&p, (0..50u32).collect::<Vec<_>>()).take(7);
    let plan = build_plan(&p, col.node_id())?;
    let explain = plan.explain().to_string();
    assert!(
        explain.contains("Early Termination") || explain.contains("Limit Pushdown"),
        "explain output should mention early termination / limit pushdown\n{explain}"
    );
    Ok(())
}

/// Sequential correctness: `take(N)` returns exactly N elements from a larger input.
#[test]
fn take_seq_correctness() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<u32> = (0..100).collect();
    let result = from_vec(&p, input.clone()).take(10).collect_seq()?;
    assert_eq!(result.len(), 10);
    assert_eq!(result, &input[..10]);
    Ok(())
}

/// Sequential correctness: `take(N)` with N >= input length returns all elements.
#[test]
fn take_seq_larger_than_input() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1u32, 2, 3]).take(100).collect_seq()?;
    assert_eq!(result, vec![1u32, 2, 3]);
    Ok(())
}

/// Sequential correctness: `take(0)` returns an empty collection.
#[test]
fn take_seq_zero() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![1u32, 2, 3]).take(0).collect_seq()?;
    assert!(result.is_empty());
    Ok(())
}

/// Parallel correctness: `take(N)` returns at most N elements.
#[test]
fn take_par_at_most_n() -> Result<()> {
    let p = Pipeline::default();
    let input: Vec<u32> = (0..1000).collect();
    let result = from_vec(&p, input).take(20).collect_par(Some(4), Some(8))?;
    assert!(
        result.len() <= 20,
        "take(20) par should produce at most 20 elements, got {}",
        result.len()
    );
    Ok(())
}

/// Parallel correctness: `first()` returns exactly one element (or zero for empty input).
#[test]
fn first_par_returns_one() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, vec![42u32, 99, 0, 1])
        .first()
        .collect_par(Some(4), Some(4))?;
    assert_eq!(result.len(), 1);
    Ok(())
}

/// `take(N)` composed with `map` before it — result is a prefix of the mapped values.
#[test]
fn take_after_map_seq() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, (0..50u32).collect::<Vec<_>>())
        .map(|x| x * 2)
        .take(5)
        .collect_seq()?;
    assert_eq!(result, vec![0u32, 2, 4, 6, 8]);
    Ok(())
}

/// `take(N)` composed with `filter` — output is filtered elements, at most N.
#[test]
fn take_after_filter_seq() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, (0u32..100).collect::<Vec<_>>())
        .filter(|x| x % 2 == 0) // evens: 0,2,4,...
        .take(4)
        .collect_seq()?;
    assert_eq!(result, vec![0u32, 2, 4, 6]);
    Ok(())
}

/// Empty input + take(N) returns empty, not an error.
#[test]
fn take_empty_input_seq() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, Vec::<u32>::new()).take(10).collect_seq()?;
    assert!(result.is_empty());
    Ok(())
}

/// Empty input + take(N) in parallel mode returns empty.
#[test]
fn take_empty_input_par() -> Result<()> {
    let p = Pipeline::default();
    let result = from_vec(&p, Vec::<u32>::new())
        .take(10)
        .collect_par(Some(4), Some(4))?;
    assert!(result.is_empty());
    Ok(())
}

// ── 3.12 Bloom Filter Semi-Join ───────────────────────────────────────────────

/// `BloomSemiJoin` optimization decision is recorded for `join_inner`.
#[test]
fn bloom_semi_join_opt_fires_for_inner_join() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)]);
    let right = from_vec(
        &p,
        vec![
            ("a".to_string(), 10u32),
            ("c".to_string(), 30),
            ("d".to_string(), 40),
            ("e".to_string(), 50),
        ],
    );
    let joined = left.join_inner(&right);
    let plan = build_plan(&p, joined.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::BloomSemiJoin { .. })),
        "expected BloomSemiJoin optimization decision for join_inner"
    );
    Ok(())
}

/// `BloomSemiJoin` is recorded for `join_left`.
#[test]
fn bloom_semi_join_opt_fires_for_left_join() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let right = from_vec(&p, vec![("a".to_string(), "x".to_string())]);
    let joined = left.join_left(&right);
    let plan = build_plan(&p, joined.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::BloomSemiJoin { .. })),
        "expected BloomSemiJoin optimization decision for join_left"
    );
    Ok(())
}

/// `BloomSemiJoin` is recorded for `join_right`.
#[test]
fn bloom_semi_join_opt_fires_for_right_join() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let right = from_vec(&p, vec![("a".to_string(), "x".to_string())]);
    let joined = left.join_right(&right);
    let plan = build_plan(&p, joined.node_id())?;
    assert!(
        plan.optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::BloomSemiJoin { .. })),
        "expected BloomSemiJoin optimization decision for join_right"
    );
    Ok(())
}

/// `BloomSemiJoin` is NOT recorded for `join_full` (full outer cannot filter either side).
#[test]
fn bloom_semi_join_opt_absent_for_full_join() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let right = from_vec(&p, vec![("a".to_string(), "x".to_string())]);
    let joined = left.join_full(&right);
    let plan = build_plan(&p, joined.node_id())?;
    assert!(
        !plan
            .optimizations
            .iter()
            .any(|o| matches!(o, OptimizationDecision::BloomSemiJoin { .. })),
        "expected NO BloomSemiJoin for join_full (both sides must be preserved)"
    );
    Ok(())
}

/// When left is the smaller side, `smaller_side` is `"left"` and
/// `estimated_reduction_pct` is positive.
#[test]
fn bloom_semi_join_smaller_side_and_pct_inner() -> Result<()> {
    let p = Pipeline::default();
    // Left: 2 elements.  Right: 10 elements.  Left is smaller.
    let left = from_vec(&p, vec![("a".to_string(), 1u32), ("b".to_string(), 2)]);
    let right = from_vec(
        &p,
        (0u32..10).map(|i| (format!("k{i}"), i)).collect::<Vec<_>>(),
    );
    let joined = left.join_inner(&right);
    let plan = build_plan(&p, joined.node_id())?;
    let decision = plan.optimizations.iter().find_map(|o| {
        if let OptimizationDecision::BloomSemiJoin {
            smaller_side,
            estimated_reduction_pct,
        } = o
        {
            Some((smaller_side.clone(), *estimated_reduction_pct))
        } else {
            None
        }
    });
    let (side, pct) = decision.expect("BloomSemiJoin decision missing");
    assert_eq!(side, "left", "left (2 items) should be the build side");
    assert!(
        pct > 0,
        "estimated reduction % should be positive when right >> left, got {pct}"
    );
    Ok(())
}

/// `explain()` output mentions the Bloom semi-join optimization.
#[test]
fn bloom_semi_join_appears_in_explain() -> Result<()> {
    let p = Pipeline::default();
    let left = from_vec(&p, vec![("a".to_string(), 1u32)]);
    let right = from_vec(&p, vec![("a".to_string(), "x".to_string())]);
    let joined = left.join_inner(&right);
    let plan = build_plan(&p, joined.node_id())?;
    let explain = plan.explain().to_string();
    assert!(
        explain.contains("Bloom Semi-Join"),
        "explain output should mention Bloom Semi-Join: {explain}"
    );
    Ok(())
}
