//! Comprehensive example demonstrating pipeline testing utilities.
//!
//! This example shows how to use Rustflow's testing facilities to write
//! idiomatic Rust tests for data pipelines.
//!
//! Run with: cargo run --example testing_pipeline

use anyhow::Result;
use rustflow::testing::*;
use rustflow::*;

fn main() -> Result<()> {
    println!("🧪 Rustflow Pipeline Testing Examples\n");

    // Example 1: Basic assertions
    example_basic_assertions()?;

    // Example 2: Test data builders
    example_test_data_builders()?;

    // Example 3: Key-value testing
    example_key_value_testing()?;

    // Example 4: Debug utilities
    example_debug_utilities()?;

    // Example 5: Using fixtures
    example_fixtures()?;

    // Example 6: Testing aggregations
    example_aggregations()?;

    // Example 7: Testing joins
    example_joins()?;

    println!("\n✅ All testing examples completed successfully!");

    Ok(())
}

/// Example 1: Basic assertions with simple transformations
fn example_basic_assertions() -> Result<()> {
    println!("📝 Example 1: Basic Assertions");

    let p = TestPipeline::new();

    let result = from_vec(&p, vec![1, 2, 3, 4, 5])
        .map(|x: &i32| x * 2)
        .collect_seq()?;

    // Use assertion utilities
    assert_collections_equal(&result, &vec![2, 4, 6, 8, 10]);
    println!("  ✓ Map transformation test passed");

    // Test filtering
    let result = from_vec(&p, vec![1, 2, 3, 4, 5])
        .filter(|x: &i32| *x % 2 == 0)
        .collect_seq()?;

    assert_collections_equal(&result, &vec![2, 4]);
    println!("  ✓ Filter transformation test passed");

    // Test with predicates
    let result = from_vec(&p, vec![10, 20, 30])
        .map(|x: &i32| x + 5)
        .collect_seq()?;

    assert_all(&result, |x| *x > 10);
    println!("  ✓ Predicate assertion test passed\n");

    Ok(())
}

/// Example 2: Using test data builders
fn example_test_data_builders() -> Result<()> {
    println!("📝 Example 2: Test Data Builders");

    let p = TestPipeline::new();

    // Build test data fluently
    let data = TestDataBuilder::<i32>::new()
        .add_range(1..=5)
        .add_value(100)
        .add_repeated(42, 3)
        .build();

    let result = from_vec(&p, data).collect_seq()?;

    assert_collection_size(&result, 9); // 5 + 1 + 3
    assert_contains(&result, &100);
    assert_contains(&result, &42);
    println!("  ✓ Test data builder test passed");

    // Build key-value test data
    let kv_data = KVTestDataBuilder::new()
        .add_kv("a", 1)
        .add_kv("b", 2)
        .add_key_with_values("a", vec![3, 4])
        .build();

    let result = from_vec(&p, kv_data).collect_seq()?;
    assert_collection_size(&result, 4);
    println!("  ✓ KV test data builder test passed");

    // Use helper functions
    let seq_data = sequential_data(1, 10);
    assert_collections_equal(&seq_data, &vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    println!("  ✓ Sequential data helper test passed\n");

    Ok(())
}

/// Example 3: Testing key-value operations
fn example_key_value_testing() -> Result<()> {
    println!("📝 Example 3: Key-Value Testing");

    let p = TestPipeline::new();

    let kvs = vec![("b", 2), ("a", 1), ("c", 3)];
    let result = from_vec(&p, kvs).collect_seq()?;

    // Use KV-specific assertion (automatically sorts)
    assert_kv_collections_equal(result, vec![("a", 1), ("b", 2), ("c", 3)]);
    println!("  ✓ KV collections assertion test passed");

    // Test grouped data
    let kvs = vec![("a", 1), ("b", 2), ("a", 3), ("b", 4)];
    let grouped = from_vec(&p, kvs).group_by_key().collect_seq()?;

    // Values within each group can be in any order
    assert_grouped_kv_equal(grouped, vec![("a", vec![1, 3]), ("b", vec![2, 4])]);
    println!("  ✓ Grouped KV assertion test passed\n");

    Ok(())
}

/// Example 4: Using debug utilities
fn example_debug_utilities() -> Result<()> {
    println!("📝 Example 4: Debug Utilities");

    let p = TestPipeline::new();

    // Pipeline with debug points
    let _result = from_vec(&p, vec![1, 2, 3, 4, 5])
        .debug_inspect("after source")
        .map(|x: &i32| x * 2)
        .debug_inspect("after map")
        .filter(|x: &i32| *x > 5)
        .debug_count("after filter")
        .collect_seq()?;

    println!("  ✓ Debug utilities work correctly");

    // Sample first few elements
    let _result = from_vec(&p, (1..=100).collect::<Vec<_>>())
        .debug_sample(3, "first 3 of 100")
        .collect_seq()?;

    println!("  ✓ Debug sample works correctly");

    // Inspect pipeline graph
    println!("\n  Pipeline graph stats:");
    println!("    Nodes: {}", p.node_count());
    println!("    Edges: {}", p.edge_count());

    Ok(())
}

/// Example 5: Using pre-built fixtures
fn example_fixtures() -> Result<()> {
    println!("\n📝 Example 5: Using Fixtures");

    let p = TestPipeline::new();

    // Word count example with fixtures
    let words = word_count_data();
    let result = from_vec(&p, words)
        .flat_map(|line: &String| {
            line.split_whitespace()
                .map(|w| w.to_string())
                .collect::<Vec<_>>()
        })
        .key_by(|word: &String| word.clone())
        .combine_values(Count)
        .collect_seq()?;

    assert_any(&result.clone(), |(word, _count)| word == "hello");
    assert_any(&result, |(word, _count)| word == "world");
    println!("  ✓ Word count fixture test passed");

    // Log entry processing
    let logs = sample_log_entries();
    let status_counts = from_vec(&p, logs)
        .key_by(|log| log.status)
        .combine_values(Count)
        .collect_seq()?;

    assert_any(&status_counts, |(status, _)| *status == 200);
    println!("  ✓ Log entry fixture test passed");

    // Time series data
    let ts = time_series_data();
    let avg_value = from_vec(&p, ts)
        .map(|(_timestamp, value): &(u64, f64)| *value)
        .collect_seq()?
        .iter()
        .sum::<f64>()
        / 11.0;

    assert!(avg_value > 10.0 && avg_value < 20.0);
    println!("  ✓ Time series fixture test passed\n");

    Ok(())
}

/// Example 6: Testing aggregations
fn example_aggregations() -> Result<()> {
    println!("📝 Example 6: Testing Aggregations");

    let p = TestPipeline::new();

    // Test combine_values with Count
    let kvs = skewed_key_value_data();
    let counts = from_vec(&p, kvs)
        .key_by(|(k, _v)| k.clone())
        .combine_values(Count)
        .collect_seq_sorted()?;

    // Hot key should have the most counts
    let hot_key_count = counts
        .iter()
        .find(|(k, _)| k == "hot_key")
        .map(|(_, count)| *count)
        .unwrap_or(0);

    assert!(hot_key_count >= 50);
    println!("  ✓ Count combiner test passed");

    // Test with Sum combiner
    let kvs = vec![("a", 10), ("b", 20), ("a", 30)];
    let sums = from_vec(&p, kvs)
        .combine_values(Sum::<i32>::default())
        .collect_seq_sorted()?;

    assert_kv_collections_equal(sums, vec![("a", 40), ("b", 20)]);
    println!("  ✓ Sum combiner test passed\n");

    Ok(())
}

/// Example 7: Testing joins
fn example_joins() -> Result<()> {
    println!("📝 Example 7: Testing Joins");

    let p = TestPipeline::new();

    let left = vec![("a", 1), ("b", 2), ("c", 3)];
    let right = vec![("a", 10), ("b", 20), ("d", 40)];

    let left_pc = from_vec(&p, left);
    let right_pc = from_vec(&p, right);

    // Inner join
    let inner = left_pc.join_inner(&right_pc).collect_seq_sorted()?;
    assert_kv_collections_equal(inner, vec![("a", (1, 10)), ("b", (2, 20))]);
    println!("  ✓ Inner join test passed");

    // Left join
    let left_pc = from_vec(&p, vec![("a", 1), ("b", 2), ("c", 3)]);
    let right_pc = from_vec(&p, vec![("a", 10), ("b", 20), ("d", 40)]);

    let left_join = left_pc.join_left(&right_pc).collect_seq_sorted()?;
    assert_collection_size(&left_join, 3);
    assert_any(&left_join.clone(), |(k, _)| *k == "c"); // c should be present with None
    println!("  ✓ Left join test passed");

    // Test with realistic fixture data
    let interactions = user_product_interactions();
    let products = product_metadata();

    let user_products = from_vec(&p, interactions)
        .map(|(user, product, rating): &(String, String, u8)| (product.clone(), (user.clone(), *rating)))
        .collect_seq()?;

    let product_info = from_vec(&p, products)
        .map(|(pid, name, _category, _price): &(String, String, String, f64)| (pid.clone(), name.clone()))
        .collect_seq()?;

    let user_products_pc = from_vec(&p, user_products);
    let product_info_pc = from_vec(&p, product_info);

    let joined = user_products_pc
        .join_inner(&product_info_pc)
        .collect_seq()?;

    assert!(!joined.is_empty());
    println!("  ✓ Realistic join test passed\n");

    Ok(())
}
