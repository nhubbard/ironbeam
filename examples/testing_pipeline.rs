//! Pipeline testing with the separate Ironbeam 5 testing crate.
//!
//! Run with `cargo run --example testing_pipeline --features v5-testing`.

#![allow(clippy::too_many_lines)]

use anyhow::Result;
use ironbeam::v5::{
    JobConfig, Pipeline,
    prelude::*,
    testing::{
        KVTestDataBuilder, PCollectionDebugExt, TestDataBuilder, TestPipeline, assert_all,
        assert_any, assert_collection_size, assert_collections_equal, assert_contains,
        assert_grouped_kv_equal, assert_kv_collections_equal, product_metadata, sample_log_entries,
        sequential_data, skewed_key_value_data, time_series_data, user_product_interactions,
        word_count_data,
    },
};

fn main() -> Result<()> {
    println!("🧪 Ironbeam Pipeline Testing Examples\n");
    basic_assertions()?;
    data_builders()?;
    key_value_assertions()?;
    debug_utilities()?;
    fixtures()?;
    aggregations()?;
    joins()?;
    println!("\n✅ All testing examples completed successfully!");
    Ok(())
}

fn basic_assertions() -> Result<()> {
    println!("📝 Example 1: Basic Assertions");
    let p = TestPipeline::new();
    let doubled = p
        .from_vec("numbers", vec![1_i32, 2, 3, 4, 5])?
        .map_named("double", |value| value * 2)?
        .collect_seq()?;
    assert_collections_equal(&doubled, &[2, 4, 6, 8, 10]);

    let evens = p
        .from_vec("filter-input", vec![1_i32, 2, 3, 4, 5])?
        .filter_named("even", |value| value % 2 == 0)?
        .collect_seq()?;
    assert_collections_equal(&evens, &[2, 4]);

    let shifted = p
        .from_vec("predicate-input", vec![10_i32, 20, 30])?
        .map_named("add-five", |value| value + 5)?
        .collect_seq()?;
    assert_all(&shifted, |value| *value > 10);
    println!("  ✓ Map, filter, and predicate assertions passed\n");
    Ok(())
}

fn data_builders() -> Result<()> {
    println!("📝 Example 2: Test Data Builders");
    let p = TestPipeline::new();
    let data = TestDataBuilder::<i32>::new()
        .add_range(1..=5)
        .add_value(100)
        .add_repeated(42, 3)
        .build();
    let values = p.from_vec("builder-values", data)?.collect_seq()?;
    assert_collection_size(&values, 9);
    assert_contains(&values, &100);
    assert_contains(&values, &42);

    let pairs = KVTestDataBuilder::new()
        .add_kv("a".to_string(), 1_i32)
        .add_kv("b".to_string(), 2)
        .add_key_with_values("a".to_string(), vec![3, 4])
        .build();
    assert_collection_size(&p.from_vec("builder-pairs", pairs)?.collect_seq()?, 4);
    assert_collections_equal(&sequential_data(1, 10), &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    println!("  ✓ Value and key-value builders passed\n");
    Ok(())
}

fn key_value_assertions() -> Result<()> {
    println!("📝 Example 3: Key-Value Testing");
    let p = TestPipeline::new();
    let pairs = p
        .from_vec(
            "pairs",
            vec![("b".to_string(), 2_i32), ("a".into(), 1), ("c".into(), 3)],
        )?
        .collect_seq()?;
    assert_kv_collections_equal(
        pairs,
        vec![("a".to_string(), 1), ("b".into(), 2), ("c".into(), 3)],
    );

    let grouped = p
        .from_vec(
            "group-input",
            vec![
                ("a".to_string(), 1_i32),
                ("b".into(), 2),
                ("a".into(), 3),
                ("b".into(), 4),
            ],
        )?
        .group_by_key_named("group")?
        .collect_seq()?;
    assert_grouped_kv_equal(
        grouped,
        vec![("a".to_string(), vec![1, 3]), ("b".into(), vec![2, 4])],
    );
    println!("  ✓ Key-value and grouped assertions passed\n");
    Ok(())
}

fn debug_utilities() -> Result<()> {
    println!("📝 Example 4: Debug Utilities");
    let p = TestPipeline::new();
    let values = p
        .from_vec("debug-input", vec![1_i32, 2, 3, 4, 5])?
        .debug_inspect_named("inspect-source", "after source")?
        .map_named("debug-double", |value| value * 2)?
        .debug_inspect_named("inspect-map", "after map")?
        .filter_named("debug-filter", |value| *value > 5)?
        .collect_debug_count_named("debug-result", "after filter", &JobConfig::default())?;
    assert_collections_equal(&values, &[6, 8, 10]);

    p.from_vec("sample-input", (1..=100).collect::<Vec<i32>>())?
        .debug_sample_named("sample", "first 3 of 100", 3)?
        .collect_seq()?;
    println!(
        "  Graph nodes: {}, edges: {}\n",
        p.node_count()?,
        p.edge_count()?
    );
    Ok(())
}

fn fixtures() -> Result<()> {
    println!("📝 Example 5: Using Fixtures");
    let p = TestPipeline::new();
    let counts = p
        .from_vec("sentences", word_count_data())?
        .flat_map_named("words", |line| {
            line.split_whitespace().map(str::to_string).collect()
        })?
        .map_named("word-pairs", |word| (word, 1_u8))?
        .count_per_key_named("word-groups", "word-counts")?
        .collect_seq()?;
    assert_any(&counts, |(word, _)| word == "hello");
    assert_any(&counts, |(word, _)| word == "world");

    let logs = sample_log_entries()
        .into_iter()
        .map(|log| (log.status, 1_u8))
        .collect();
    let status_counts = p
        .from_vec("logs", logs)?
        .count_per_key_named("status-groups", "status-counts")?
        .collect_seq()?;
    assert_any(&status_counts, |(status, _)| *status == 200);

    let average = p
        .from_vec("timeseries", time_series_data())?
        .map_named("values", |(_, value)| value)?
        .collect_seq()?
        .iter()
        .sum::<f64>()
        / 11.0;
    assert!((10.0..20.0).contains(&average));
    println!("  ✓ Word, log, and time-series fixtures passed\n");
    Ok(())
}

fn aggregations() -> Result<()> {
    println!("📝 Example 6: Testing Aggregations");
    let p = TestPipeline::new();
    let counts = p
        .from_vec("skewed", skewed_key_value_data())?
        .count_per_key_named("skew-groups", "skew-counts")?
        .collect_seq()?;
    assert!(
        counts
            .iter()
            .any(|(key, count)| key == "hot_key" && *count >= 50)
    );

    let sums = p
        .from_vec(
            "sums-input",
            vec![
                ("a".to_string(), 10_i32),
                ("b".into(), 20),
                ("a".into(), 30),
            ],
        )?
        .sum_per_key_named("sum-groups")?
        .collect_seq()?;
    assert_kv_collections_equal(sums, vec![("a".to_string(), 40), ("b".into(), 20)]);
    println!("  ✓ Count and sum aggregations passed\n");
    Ok(())
}

fn joins() -> Result<()> {
    println!("📝 Example 7: Testing Joins");
    let p = Pipeline::default();
    let left = p.from_vec(
        "left",
        vec![("a".to_string(), 1_i32), ("b".into(), 2), ("c".into(), 3)],
    )?;
    let right = p.from_vec(
        "right",
        vec![
            ("a".to_string(), 10_i32),
            ("b".into(), 20),
            ("d".into(), 40),
        ],
    )?;
    let inner = left.join_inner_named("inner", &right)?.collect_seq()?;
    assert_kv_collections_equal(
        inner,
        vec![("a".to_string(), (1, 10)), ("b".into(), (2, 20))],
    );
    let left_join = left.join_left_named("left-join", &right)?.collect_seq()?;
    assert_collection_size(&left_join, 3);
    assert_any(&left_join, |(key, (_, right))| {
        key == "c" && right.is_none()
    });

    let p = Pipeline::default();
    let interactions = p
        .from_vec("interactions", user_product_interactions())?
        .map_named("interaction-products", |(user, product, rating)| {
            (product, (user, rating))
        })?;
    let products = p
        .from_vec("products", product_metadata())?
        .map_named("product-names", |(id, name, _, _)| (id, name))?;
    let joined = interactions
        .join_inner_named("fixture-join", &products)?
        .collect_seq()?;
    assert_ne!(joined.len(), 0);
    println!("  ✓ Inner, left, and fixture joins passed\n");
    Ok(())
}
