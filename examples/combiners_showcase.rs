//! Basic, statistical, top-K, sampling, and global combiners on the v5 graph.
//!
//! Run with `cargo run --example combiners_showcase`.

use anyhow::Result;
use ironbeam::v5::{OrdF64, Pipeline, prelude::*};
use std::collections::BTreeMap;

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn main() -> Result<()> {
    println!("🎯 Combiners Showcase Example\n");
    let pipeline = Pipeline::default();
    let transactions = [
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
    let data = pipeline.from_vec(
        "transactions",
        transactions
            .into_iter()
            .map(|(category, price)| (category.to_string(), price))
            .collect(),
    )?;

    println!("📊 BASIC COMBINERS\n");
    let counts: BTreeMap<_, _> = data
        .count_per_key_named("count-group", "count-result")?
        .collect_seq()?
        .into_iter()
        .collect();
    assert_eq!(
        counts,
        BTreeMap::from([
            ("Books".to_string(), 3),
            ("Clothing".to_string(), 3),
            ("Electronics".to_string(), 4),
        ])
    );
    println!("Transaction Counts by Category:");
    for (category, count) in &counts {
        println!("  {category}: {count} transactions");
    }

    let totals: BTreeMap<_, _> = data
        .sum_per_key_named("revenue-group")?
        .collect_seq()?
        .into_iter()
        .collect();
    println!("\nTotal Revenue by Category:");
    for (category, total) in &totals {
        println!("  {category}: ${total:.2}");
    }

    let ordered = data.map_values_named("ordered-prices", OrdF64)?;
    let mins: BTreeMap<_, _> = ordered
        .min_per_key_named("min-group", "min-result")?
        .collect_seq()?
        .into_iter()
        .collect();
    let maxs: BTreeMap<_, _> = ordered
        .max_per_key_named("max-group", "max-result")?
        .collect_seq()?
        .into_iter()
        .collect();
    let avgs: BTreeMap<_, _> = data
        .average_per_key_named("average-group", "average-result")?
        .collect_seq()?
        .into_iter()
        .collect();
    assert_eq!(mins["Books"].0, 14.99);
    assert_eq!(maxs["Electronics"].0, 1299.99);
    println!("\nMin, Max, and Average Price by Category:");
    for category in counts.keys() {
        println!(
            "  {category}: ${:.2} / ${:.2} / ${:.2}",
            mins[category].0, maxs[category].0, avgs[category]
        );
    }

    println!("\n📊 TOP-K COMBINER\n");
    let top_two: BTreeMap<_, _> = ordered
        .top_k_per_key_named("top-two-group", "top-two-result", 2)?
        .collect_seq()?
        .into_iter()
        .collect();
    assert_eq!(
        top_two["Electronics"],
        vec![OrdF64(1299.99), OrdF64(799.99)]
    );
    for (category, prices) in &top_two {
        println!(
            "  {category}: {:?}",
            prices
                .iter()
                .map(|price| format!("${:.2}", price.0))
                .collect::<Vec<_>>()
        );
    }

    println!("\n📊 DISTINCT COUNT\n");
    let cents = data.map_values_named("prices-in-cents", |price| (price * 100.0) as u64)?;
    let distinct_prices: BTreeMap<_, _> = cents
        .distinct_count_per_key_named("distinct-group", "distinct-result")?
        .collect_seq()?
        .into_iter()
        .collect();
    assert_eq!(distinct_prices, counts);
    for (category, count) in &distinct_prices {
        println!("  {category}: {count} unique prices");
    }

    println!("\n📊 APPROXIMATE QUANTILES (TDigest)\n");
    let quantiles: BTreeMap<_, _> = data
        .approx_quantiles_per_key_named(
            "quantile-group",
            "quantile-result",
            vec![0.25, 0.5, 0.75, 0.95],
            100.0,
        )?
        .collect_seq()?
        .into_iter()
        .collect();
    for (category, values) in &quantiles {
        assert_eq!(values.len(), 4);
        assert!(values.windows(2).all(|pair| pair[0] <= pair[1]));
        println!(
            "  {category}: 25th ${:.2}, median ${:.2}, 75th ${:.2}, 95th ${:.2}",
            values[0], values[1], values[2], values[3]
        );
    }

    println!("\n📊 RESERVOIR SAMPLING\n");
    let samples: BTreeMap<_, _> = data
        .sample_values_reservoir_vec_named("sample-group", "sample-result", 2, 42)?
        .collect_seq()?
        .into_iter()
        .collect();
    for (category, prices) in &samples {
        assert_eq!(prices.len(), 2);
        println!(
            "  {category}: {:?}",
            prices
                .iter()
                .map(|price| format!("${price:.2}"))
                .collect::<Vec<_>>()
        );
    }

    println!("\n📊 GLOBAL COMBINERS\n");
    let all_prices = data.values_named("all-prices")?;
    let total_count = all_prices
        .count_globally_named("global-count")?
        .collect_seq()?;
    let total_sum = all_prices.sum_globally_named("global-sum")?.collect_seq()?;
    let overall_average = all_prices
        .average_globally_named("global-average")?
        .collect_seq()?;
    let ordered_global = all_prices.map_named("ordered-global-prices", OrdF64)?;
    let overall_min = ordered_global
        .min_globally_named("global-min")?
        .collect_seq()?;
    let overall_max = ordered_global
        .max_globally_named("global-max")?
        .collect_seq()?;
    assert_eq!(total_count, vec![10]);
    assert_eq!(overall_min, vec![OrdF64(14.99)]);
    assert_eq!(overall_max, vec![OrdF64(1299.99)]);
    println!("Overall Statistics:");
    println!("  Total transactions: {}", total_count[0]);
    println!("  Total revenue: ${:.2}", total_sum[0]);
    println!("  Average price: ${:.2}", overall_average[0]);
    println!("  Min price: ${:.2}", overall_min[0].0);
    println!("  Max price: ${:.2}", overall_max[0].0);
    println!("\n✅ Combiners Showcase Complete!");
    Ok(())
}
