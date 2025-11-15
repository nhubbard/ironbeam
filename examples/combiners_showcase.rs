//! Comprehensive combiners showcase.
//!
//! Demonstrates all built-in combiners:
//! - Basic: `Count`, `Sum`, `Min`, `Max`, `Average`
//! - Statistical: Quantiles (`TDigest`), `DistinctCount`
//! - `TopK`: Finding top N items
//! - Sampling: Reservoir sampling
//!
//! Run with: `cargo run --example combiners_showcase`

use anyhow::Result;
use ironbeam::combiners::{ApproxQuantiles, AverageF64, DistinctCount, Max, Min, Sum};
use ironbeam::{Count, OrdF64, Pipeline, from_vec};

#[allow(clippy::too_many_lines)]
fn main() -> Result<()> {
    println!("🎯 Combiners Showcase Example\n");

    let pipeline = Pipeline::default();

    // Sample e-commerce transaction data
    let transactions = vec![
        ("Electronics", 599.99),
        ("Books", 19.99),
        ("Electronics", 1299.99),
        ("Clothing", 49.99),
        ("Books", 24.99),
        ("Electronics", 799.99),
        ("Clothing", 79.99),
        ("Books", 14.99),
        ("Clothing", 39.99),
        ("Electronics", 449.99),
    ];

    let data = from_vec(
        &pipeline,
        transactions
            .iter()
            .map(|(cat, price)| ((*cat).to_string(), *price))
            .collect(),
    );

    // =============================================================================
    // Basic Combiners
    // =============================================================================
    println!("📊 BASIC COMBINERS\n");

    // Count per category
    let counts = data.clone().combine_values(Count);
    println!("Transaction Counts by Category:");
    for (category, count) in counts.collect_seq_sorted()? {
        println!("  {category}: {count} transactions");
    }

    // Sum (total revenue) per category
    let totals = data.clone().combine_values(Sum::<f64>::default());
    println!("\nTotal Revenue by Category:");
    let mut total_results = totals.collect_seq()?;
    total_results.sort_by_key(|(cat, _)| cat.clone());
    for (category, total) in total_results {
        println!("  {category}: ${total:.2}");
    }

    // Min price per category
    let mins = data
        .clone()
        .map_values(|&price| OrdF64(price))
        .combine_values(Min::<OrdF64>::new());
    println!("\nMinimum Price by Category:");
    let mut min_results = mins.collect_seq()?;
    min_results.sort_by_key(|(cat, _)| cat.clone());
    for (category, min) in min_results {
        println!("  {category}: ${:.2}", min.0);
    }

    // Max price per category
    let maxs = data
        .clone()
        .map_values(|&price| OrdF64(price))
        .combine_values(Max::<OrdF64>::new());
    println!("\nMaximum Price by Category:");
    let mut max_results = maxs.collect_seq()?;
    max_results.sort_by_key(|(cat, _)| cat.clone());
    for (category, max) in max_results {
        println!("  {}: ${:.2}", category, max.0);
    }

    // Average price per category
    let avgs = data.clone().combine_values(AverageF64);
    println!("\nAverage Price by Category:");
    let mut avg_results = avgs.collect_seq()?;
    avg_results.sort_by_key(|(cat, _)| cat.clone());
    for (category, avg) in avg_results {
        println!("  {category}: ${avg:.2}");
    }

    // =============================================================================
    // TopK Combiner
    // =============================================================================
    println!("\n📊 TOP-K COMBINER\n");

    // Get top 2 highest-priced items per category
    let top2 = data
        .clone()
        .map_values(|&price| OrdF64(price))
        .top_k_per_key(2);
    println!("Top 2 Prices by Category:");
    let mut top2_results = top2.collect_seq()?;
    top2_results.sort_by_key(|(cat, _)| cat.clone());
    for (category, prices) in top2_results {
        println!(
            "  {}: {:?}",
            category,
            prices
                .iter()
                .map(|p| format!("${:.2}", p.0))
                .collect::<Vec<_>>()
        );
    }

    // =============================================================================
    // Distinct Count
    // =============================================================================
    println!("\n📊 DISTINCT COUNT\n");

    // Count distinct price points per category
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let distinct_prices = data
        .clone()
        .map_values(|price| (price * 100.0) as u64) // Convert to cents for Ord
        .combine_values(DistinctCount::<u64>::default());

    println!("Distinct Price Points by Category:");
    for (category, count) in distinct_prices.collect_seq_sorted()? {
        println!("  {category}: {count} unique prices");
    }

    // =============================================================================
    // Approximate Quantiles (TDigest)
    // =============================================================================
    println!("\n📊 APPROXIMATE QUANTILES (TDigest)\n");

    let quantiles = data
        .clone()
        .combine_values(ApproxQuantiles::new(vec![0.25, 0.5, 0.75, 0.95], 100.0));

    println!("Price Quantiles by Category:");
    let mut quantile_results = quantiles.collect_seq()?;
    quantile_results.sort_by_key(|(cat, _)| cat.clone());
    for (category, qs) in quantile_results {
        println!("  {category}:");
        println!("    25th percentile: ${:.2}", qs[0]);
        println!("    50th percentile (median): ${:.2}", qs[1]);
        println!("    75th percentile: ${:.2}", qs[2]);
        println!("    95th percentile: ${:.2}", qs[3]);
    }

    // =============================================================================
    // Reservoir Sampling
    // =============================================================================
    println!("\n📊 RESERVOIR SAMPLING\n");

    // Sample 2 transactions per category
    let samples = data.clone().sample_values_reservoir_vec(2, 42);

    println!("Random Sample (2 per category):");
    let mut sample_results = samples.collect_seq()?;
    sample_results.sort_by_key(|(cat, _)| cat.clone());
    for (category, sampled_prices) in sample_results {
        println!(
            "  {}: {:?}",
            category,
            sampled_prices
                .iter()
                .map(|p: &f64| format!("${p:.2}"))
                .collect::<Vec<_>>()
        );
    }

    // =============================================================================
    // Global Combiners (across all categories)
    // =============================================================================
    println!("\n📊 GLOBAL COMBINERS\n");

    let all_prices = data.map(|(_, price)| *price);

    let global_count = all_prices.clone().combine_globally(Count, None);
    let global_sum = all_prices
        .clone()
        .combine_globally(Sum::<f64>::default(), None);
    let global_avg = all_prices.clone().combine_globally(AverageF64, None);
    let global_min = all_prices
        .clone()
        .map(|&p| OrdF64(p))
        .combine_globally(Min::<OrdF64>::new(), None);
    let global_max = all_prices
        .map(|&p| OrdF64(p))
        .combine_globally(Max::<OrdF64>::new(), None);

    println!("Overall Statistics:");
    println!("  Total transactions: {}", global_count.collect_seq()?[0]);
    println!("  Total revenue: ${:.2}", global_sum.collect_seq()?[0]);
    println!("  Average price: ${:.2}", global_avg.collect_seq()?[0]);
    println!("  Min price: ${:.2}", global_min.collect_seq()?[0].0);
    println!("  Max price: ${:.2}", global_max.collect_seq()?[0].0);

    println!("\n✅ Combiners Showcase Complete!");
    println!("\nKey Concepts:");
    println!("  • Combiners: Efficient aggregation with associative operations");
    println!("  • Per-key: Use combine_values() on keyed collections");
    println!("  • Global: Use combine_globally() for dataset-wide aggregations");
    println!("  • Lifted: Pre-grouped data can use combine_values_lifted()");
    println!("  • All combiners work in both sequential and parallel modes");

    Ok(())
}
