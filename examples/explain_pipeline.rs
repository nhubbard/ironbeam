//! Inspect the v5 Direct plan before any source is opened.
//!
//! Run with `cargo run --example explain_pipeline`.

use ironbeam::v5::{DirectRunner, JobConfig, Pipeline, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Elementwise pipeline ===");
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("numbers", (1..=100_i32).collect())?
        .map_named("double", |value| value * 2)?
        .filter_named("divisible-by-three", |value| value % 3 == 0)?
        .output("elementwise-result")?;
    let plan = DirectRunner.explain(&output, &JobConfig::default())?;
    assert!(!plan.materialized);
    println!("{}\n", plan.explain());

    println!("=== Grouping pipeline ===");
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec(
            "fruit",
            vec![
                ("apple".to_owned(), 5_i32),
                ("banana".to_owned(), 3),
                ("apple".to_owned(), 2),
                ("banana".to_owned(), 7),
                ("cherry".to_owned(), 1),
            ],
        )?
        .group_by_key_named("fruit-by-name")?
        .output("grouped-result")?;
    let plan = DirectRunner.explain(&output, &JobConfig::default())?;
    assert!(plan.materialized);
    println!("{}\n", plan.explain());

    println!("=== Fused pipeline with a smaller batch budget ===");
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("large-input", (1..=10_000_i32).collect())?
        .map_named("double", |value| value * 2)?
        .filter_named("over-hundred", |value| *value > 100)?
        .map_named("add-ten", |value| value + 10)?
        .filter_named("divisible-by-five", |value| value % 5 == 0)?
        .map_named("pair", |value| (value / 100, value))?
        .output("fused-result")?;
    let config = JobConfig {
        buffer_budget_elements: 1_024,
        ..JobConfig::default()
    };
    println!("{}", DirectRunner.explain(&output, &config)?.explain());
    Ok(())
}
