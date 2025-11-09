//! Example demonstrating the pipeline explain/visualization feature.
//!
//! This example shows how to use the `explain()` method to understand
//! what the query planner is doing with your pipeline.

use anyhow::Result;
use rustflow::*;

fn main() -> Result<()> {
    println!("=== Example 1: Simple Pipeline ===\n");
    example_simple_pipeline()?;

    println!("\n=== Example 2: Pipeline with Grouping ===\n");
    example_with_grouping()?;

    println!("\n=== Example 3: Complex Pipeline with Optimizations ===\n");
    example_with_optimizations()?;

    Ok(())
}

/// Demonstrates explain on a simple map/filter pipeline.
fn example_simple_pipeline() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (1..=100).collect::<Vec<_>>());
    let result = data
        .map(|x| x * 2)
        .filter(|x| x % 3 == 0);

    // Build the plan and explain it
    let plan = build_plan(&p, result.node_id())?;
    let explanation = plan.explain();

    println!("{}", explanation);
    Ok(())
}

/// Demonstrates explain with a grouping operation.
fn example_with_grouping() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![
        ("apple", 5),
        ("banana", 3),
        ("apple", 2),
        ("banana", 7),
        ("cherry", 1),
    ]);
    let result = data.group_by_key();

    // Build the plan and explain it
    let plan = build_plan(&p, result.node_id())?;
    let explanation = plan.explain();

    println!("{}", explanation);
    Ok(())
}

/// Demonstrates explain with multiple operations that trigger optimizations.
fn example_with_optimizations() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, (1..=10000).collect::<Vec<_>>());

    // Create a pipeline with multiple stateless operations that will be fused
    let result = data
        .map(|x| x * 2)
        .filter(|x| *x > 100)
        .map(|x| x + 10)
        .filter(|x| x % 5 == 0)
        .map(|x| (*x / 100, *x));

    // Build the plan and explain it
    let plan = build_plan(&p, result.node_id())?;
    let explanation = plan.explain();

    println!("{}", explanation);

    // You can also access individual components
    println!("\n=== Detailed Breakdown ===");
    println!("Total operations: {}", explanation.cost_estimate.total_ops);
    println!("Stateless ops: {}", explanation.cost_estimate.stateless_ops);
    println!("Barrier ops: {}", explanation.cost_estimate.barriers);
    println!("Source size: {:?}", explanation.cost_estimate.source_size);
    println!("Suggested partitions: {:?}", explanation.suggested_partitions);

    println!("\n=== Optimizations Applied ===");
    for opt in &explanation.optimizations {
        match opt {
            OptimizationDecision::FusedStateless { blocks_before, blocks_after, ops_count } => {
                println!("✓ Fused {} stateless blocks into {} ({} ops total)",
                    blocks_before, blocks_after, ops_count);
            }
            OptimizationDecision::ReorderedValueOps { ops_count, by_cost } => {
                println!("✓ Reordered {} value-only operations (by_cost={})",
                    ops_count, by_cost);
            }
            OptimizationDecision::LiftedGBKCombine { removed_barrier } => {
                println!("✓ Lifted GroupByKey→CombineValues (removed_barrier={})",
                    removed_barrier);
            }
            OptimizationDecision::DroppedMidMaterialized { count } => {
                println!("✓ Dropped {} mid-pipeline materialized nodes", count);
            }
            OptimizationDecision::PartitionSuggestion { source_len, partitions } => {
                if let Some(len) = source_len {
                    println!("✓ Suggested {} partitions for {} elements", partitions, len);
                } else {
                    println!("✓ Suggested {} partitions", partitions);
                }
            }
        }
    }

    Ok(())
}
