//! Tests for the execution plan explanation feature.

use anyhow::Result;
use ironbeam::testing::*;
use ironbeam::*;

#[test]
fn test_explain_simple_pipeline() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1, 2, 3, 4, 5]);
    let doubled = data.map(|x| x * 2);
    let filtered = doubled.filter(|x| *x > 5);

    // Build the plan and explain it
    let plan = build_plan(&p, filtered.node_id())?;
    let explanation = plan.explain();

    // Verify we have the expected steps
    assert!(!explanation.steps.is_empty());
    assert!(explanation.cost_estimate.total_ops > 0);

    // Print the explanation for manual inspection
    println!("{explanation}");

    Ok(())
}

#[test]
fn test_explain_with_grouping() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![("a", 1u64), ("b", 2u64), ("a", 3u64)]);
    let grouped = data.group_by_key();

    // Build the plan and explain it
    let plan = build_plan(&p, grouped.node_id())?;
    let explanation = plan.explain();

    // Verify we have barrier operations
    assert!(explanation.cost_estimate.barriers > 0);
    assert!(explanation.steps.iter().any(|s| s.is_barrier));

    // Print the explanation for manual inspection
    println!("{explanation}");

    Ok(())
}

#[test]
fn test_explain_with_optimizations() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Create multiple stateless operations that should be fused
    let result = data
        .map(|x| x * 2)
        .filter(|x| *x > 5)
        .map(|x| x + 1)
        .filter(|x| x % 2 == 0);

    // Build the plan and explain it
    let plan = build_plan(&p, result.node_id())?;
    let explanation = plan.explain();

    // Verify optimizations were applied
    assert!(
        !explanation.optimizations.is_empty(),
        "Expected optimizations to be applied"
    );

    // Check for stateless fusion optimization
    let has_fusion = explanation
        .optimizations
        .iter()
        .any(|opt| matches!(opt, OptimizationDecision::FusedStateless { .. }));
    assert!(has_fusion, "Expected stateless fusion optimization");

    // Print the explanation for manual inspection
    println!("{explanation}");

    Ok(())
}

#[test]
fn test_explain_cost_estimates() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, (1..=1000).collect::<Vec<_>>());
    let result = data.map(|x| (*x % 10, *x)).group_by_key();

    // Build the plan and explain it
    let plan = build_plan(&p, result.node_id())?;
    let explanation = plan.explain();

    // Verify cost estimates
    assert_eq!(explanation.cost_estimate.source_size, Some(1000));
    assert!(explanation.cost_estimate.barriers >= 1);
    assert!(explanation.cost_estimate.total_ops > 0);
    assert!(explanation.suggested_partitions.is_some());

    // Print the explanation for manual inspection
    println!("{explanation}");

    Ok(())
}

#[test]
fn test_explain_display_format() -> Result<()> {
    let p = TestPipeline::new();
    let data = from_vec(&p, vec![1, 2, 3]);
    let result = data.map(|x| x * 2);

    // Build the plan and explain it
    let plan = build_plan(&p, result.node_id())?;
    let explanation = plan.explain();

    // Convert to string and verify it contains expected sections
    let output = format!("{explanation}");

    assert!(output.contains("EXECUTION PLAN EXPLANATION"));
    assert!(output.contains("COST ESTIMATES"));
    assert!(output.contains("EXECUTION STEPS"));
    assert!(output.contains("Source Size"));
    assert!(output.contains("Total Operations"));

    println!("{output}");

    Ok(())
}
