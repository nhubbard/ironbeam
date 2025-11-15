//! Integration tests demonstrating the testing utilities.

use anyhow::Result;
use ironbeam::testing::*;
use ironbeam::*;

#[test]
fn test_basic_pipeline_with_assertions() -> Result<()> {
    let p = TestPipeline::new();

    let result = from_vec(&p, vec![1, 2, 3])
        .map(|x: &i32| x * 2)
        .collect_seq()?;

    assert_collections_equal(&result, &[2, 4, 6]);
    Ok(())
}

#[test]
fn test_unordered_comparison() -> Result<()> {
    let p = TestPipeline::new();

    let result = from_vec(&p, vec![3, 1, 2])
        .map(|x: &i32| x * 10)
        .collect_seq()?;

    assert_collections_unordered_equal(&result, &[10, 20, 30]);
    Ok(())
}

#[test]
fn test_kv_operations() -> Result<()> {
    let p = TestPipeline::new();

    let kvs = KVTestDataBuilder::new()
        .add_kv("a", 1)
        .add_kv("b", 2)
        .add_kv("a", 3)
        .build();

    let grouped = from_vec(&p, kvs).group_by_key().collect_seq()?;

    assert_grouped_kv_equal(grouped, vec![("a", vec![1, 3]), ("b", vec![2])]);
    Ok(())
}

#[test]
fn test_with_fixtures() -> Result<()> {
    let p = TestPipeline::new();

    let logs = sample_log_entries();
    let status_200_count = from_vec(&p, logs)
        .filter(|log| log.status == 200)
        .collect_seq()?
        .len();

    assert!(status_200_count >= 2);
    Ok(())
}

#[test]
fn test_predicate_assertions() -> Result<()> {
    let p = TestPipeline::new();

    let result = from_vec(&p, vec![2, 4, 6, 8])
        .map(|x: &i32| x * 2)
        .collect_seq()?;

    assert_all(&result, |x| *x % 2 == 0);
    assert_any(&result, |x| *x > 10);
    assert_none(&result, |x| *x < 0);
    Ok(())
}

#[test]
fn test_pipeline_graph_inspection() -> Result<()> {
    let p = TestPipeline::new();

    let _result = from_vec(&p, vec![1, 2, 3])
        .map(|x: &i32| x * 2)
        .filter(|x: &i32| *x > 2)
        .collect_seq()?;

    assert!(p.node_count() >= 3); // At least source, map, and filter
    assert!(p.edge_count() >= 2); // At least 2 connections
    Ok(())
}

#[test]
fn test_sequential_data_builder() {
    let data = sequential_data(1, 10);
    assert_collection_size(&data, 10);
    assert_eq!(data[0], 1);
    assert_eq!(data[9], 10);
}

#[test]
fn test_skewed_data_generation() {
    let kvs = skewed_key_value_data();
    assert_collection_size(&kvs, 100);

    let hot_key_count = kvs.iter().filter(|(k, _)| k == "hot_key").count();
    assert!(hot_key_count >= 40); // Should be around 50
}

#[test]
fn test_pseudo_random_determinism() {
    let data1 = pseudo_random_data(100, 0, 1000);
    let data2 = pseudo_random_data(100, 0, 1000);

    assert_collections_equal(&data1, &data2);
}

#[test]
fn test_word_count_with_fixtures() -> Result<()> {
    let p = TestPipeline::new();

    let words = word_count_data();
    let counts = from_vec(&p, words)
        .flat_map(|line: &String| {
            line.split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .key_by(|word: &String| word.clone())
        .combine_values(Count)
        .collect_seq()?;

    assert_any(&counts, |(word, _)| word == "hello");
    assert_any(&counts, |(word, _)| word == "world");
    Ok(())
}

#[test]
fn test_aggregation_with_sum() -> Result<()> {
    let p = TestPipeline::new();

    let kvs = vec![("a", 10), ("b", 20), ("a", 30), ("b", 40)];
    let sums = from_vec(&p, kvs)
        .combine_values(Sum::<i32>::default())
        .collect_seq_sorted()?;

    assert_kv_collections_equal(sums, vec![("a", 40), ("b", 60)]);
    Ok(())
}

#[test]
fn test_contains_assertion() {
    let data = vec![1, 2, 3, 4, 5];
    assert_contains(&data, &3);
    assert_contains(&data, &1);
    assert_contains(&data, &5);
}

#[test]
fn test_collection_size_assertion() -> Result<()> {
    let p = TestPipeline::new();

    let result = from_vec(&p, vec![1, 2, 3, 4, 5])
        .filter(|x: &i32| x % 2 == 0)
        .collect_seq()?;

    assert_collection_size(&result, 2);
    Ok(())
}
