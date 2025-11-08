use anyhow::Result;
use rustflow::extensions::CompositeTransform;
use rustflow::node::DynOp;
use rustflow::type_token::{Partition, VecOps};
use rustflow::*;
use std::any::Any;
use std::sync::Arc;

// Test custom DynOp
struct ReverseStringOp;

impl DynOp for ReverseStringOp {
    fn apply(&self, input: Partition) -> Partition {
        let v = input
            .downcast::<Vec<String>>()
            .expect("ReverseStringOp expects Vec<String>");
        let out: Vec<String> = v.iter().map(|s| s.chars().rev().collect()).collect();
        Box::new(out)
    }
}

#[test]
fn apply_transform_custom_op() -> Result<()> {
    let p = Pipeline::default();
    let words = from_vec(&p, vec!["hello".to_string(), "world".to_string()]);

    let reversed: PCollection<String> = words.apply_transform(Arc::new(ReverseStringOp));
    let result = reversed.collect_seq()?;

    assert_eq!(result, vec!["olleh", "dlrow"]);
    Ok(())
}

// Test custom DynOp with numbers
struct DoubleOp;

impl DynOp for DoubleOp {
    fn apply(&self, input: Partition) -> Partition {
        let v = input
            .downcast::<Vec<i32>>()
            .expect("DoubleOp expects Vec<i32>");
        let out: Vec<i32> = v.iter().map(|n| n * 2).collect();
        Box::new(out)
    }
}

#[test]
fn apply_transform_with_numbers() -> Result<()> {
    let p = Pipeline::default();
    let nums = from_vec(&p, vec![1, 2, 3, 4, 5]);

    let doubled: PCollection<i32> = nums.apply_transform(Arc::new(DoubleOp));
    let result = doubled.collect_seq()?;

    assert_eq!(result, vec![2, 4, 6, 8, 10]);
    Ok(())
}

// Test custom DynOp with key-preserving flag
struct UppercaseValueOp;

impl DynOp for UppercaseValueOp {
    fn apply(&self, input: Partition) -> Partition {
        let v = input
            .downcast::<Vec<(String, String)>>()
            .expect("UppercaseValueOp expects Vec<(String, String)>");
        let out: Vec<(String, String)> =
            v.into_iter().map(|(k, v)| (k, v.to_uppercase())).collect();
        Box::new(out)
    }

    fn key_preserving(&self) -> bool {
        true
    }

    fn value_only(&self) -> bool {
        true
    }
}

#[test]
fn apply_transform_key_preserving() -> Result<()> {
    let p = Pipeline::default();
    let pairs = from_vec(
        &p,
        vec![
            ("a".to_string(), "hello".to_string()),
            ("b".to_string(), "world".to_string()),
        ],
    );

    let upper: PCollection<(String, String)> = pairs.apply_transform(Arc::new(UppercaseValueOp));
    let result = upper.collect_seq()?;

    assert_eq!(
        result,
        vec![
            ("a".to_string(), "HELLO".to_string()),
            ("b".to_string(), "WORLD".to_string())
        ]
    );
    Ok(())
}

// Test CompositeTransform
struct TrimAndFilter;

impl CompositeTransform<String, String> for TrimAndFilter {
    fn expand(&self, input: PCollection<String>) -> PCollection<String> {
        input
            .map(|s: &String| s.trim().to_string())
            .filter(|s: &String| !s.is_empty())
    }
}

#[test]
fn composite_transform_basic() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            "  hello  ".to_string(),
            "".to_string(),
            "world".to_string(),
            "   ".to_string(),
        ],
    );

    let cleaned = data.apply_composite(TrimAndFilter);
    let result = cleaned.collect_seq()?;

    assert_eq!(result, vec!["hello", "world"]);
    Ok(())
}

// Test CompositeTransform with type changes
struct ParseInts;

impl CompositeTransform<String, i32> for ParseInts {
    fn expand(&self, input: PCollection<String>) -> PCollection<i32> {
        input
            .filter(|s: &String| s.parse::<i32>().is_ok())
            .map(|s: &String| s.parse::<i32>().unwrap())
    }
}

#[test]
fn composite_transform_type_change() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(
        &p,
        vec![
            "123".to_string(),
            "not_a_number".to_string(),
            "456".to_string(),
        ],
    );

    let parsed = data.apply_composite(ParseInts);
    let result = parsed.collect_seq()?;

    assert_eq!(result, vec![123, 456]);
    Ok(())
}

// Test custom VecOps
#[derive(Clone)]
struct SimpleShards {
    chunks: Vec<Vec<i32>>,
}

struct SimpleShardsVecOps;

impl VecOps for SimpleShardsVecOps {
    fn len(&self, data: &dyn Any) -> Option<usize> {
        data.downcast_ref::<SimpleShards>()
            .map(|s| s.chunks.iter().map(|c| c.len()).sum())
    }

    fn split(&self, data: &dyn Any, _n: usize) -> Option<Vec<Partition>> {
        let shards = data.downcast_ref::<SimpleShards>()?;
        let parts: Vec<Partition> = shards
            .chunks
            .iter()
            .map(|chunk| Box::new(chunk.clone()) as Partition)
            .collect();
        Some(parts)
    }

    fn clone_any(&self, data: &dyn Any) -> Option<Partition> {
        let shards = data.downcast_ref::<SimpleShards>()?;
        let all: Vec<i32> = shards.chunks.iter().flatten().cloned().collect();
        Some(Box::new(all))
    }
}

#[test]
fn custom_source_with_vec_ops() -> Result<()> {
    let p = Pipeline::default();
    let shards = SimpleShards {
        chunks: vec![vec![1, 2, 3], vec![4, 5], vec![6, 7, 8, 9]],
    };

    let data: PCollection<i32> = from_custom_source(&p, shards, Arc::new(SimpleShardsVecOps));

    let result = data.collect_seq()?;
    assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    Ok(())
}

#[test]
fn custom_source_with_parallel_execution() -> Result<()> {
    let p = Pipeline::default();
    let shards = SimpleShards {
        chunks: vec![vec![1, 2], vec![3, 4], vec![5, 6]],
    };

    let data: PCollection<i32> = from_custom_source(&p, shards, Arc::new(SimpleShardsVecOps));

    // Transform and collect in parallel
    let doubled = data.map(|n: &i32| n * 2);
    let mut result = doubled.collect_par(None, None)?;
    result.sort();

    assert_eq!(result, vec![2, 4, 6, 8, 10, 12]);
    Ok(())
}

// Test chaining custom ops
#[test]
fn chain_multiple_custom_ops() -> Result<()> {
    let p = Pipeline::default();
    let data = from_vec(&p, vec![1, 2, 3]);

    let doubled: PCollection<i32> = data.apply_transform(Arc::new(DoubleOp));
    let quadrupled: PCollection<i32> = doubled.apply_transform(Arc::new(DoubleOp));

    let result = quadrupled.collect_seq()?;
    assert_eq!(result, vec![4, 8, 12]);
    Ok(())
}

// Test CompositeTransform with aggregation
struct WordCount;

impl CompositeTransform<String, (String, u64)> for WordCount {
    fn expand(&self, input: PCollection<String>) -> PCollection<(String, u64)> {
        input
            .flat_map(|line: &String| {
                line.split_whitespace()
                    .map(|w| w.to_lowercase())
                    .collect::<Vec<_>>()
            })
            .key_by(|word: &String| word.clone())
            .map_values(|_: &String| 1u64)
            .combine_values(Count)
    }
}

#[test]
fn composite_transform_with_aggregation() -> Result<()> {
    let p = Pipeline::default();
    let lines = from_vec(
        &p,
        vec![
            "the quick brown fox".to_string(),
            "the lazy dog".to_string(),
        ],
    );

    let counts = lines.apply_composite(WordCount);
    let result = counts.collect_seq()?;

    let mut counts_map = std::collections::HashMap::new();
    for (word, count) in result {
        counts_map.insert(word, count);
    }

    assert_eq!(counts_map.get("the"), Some(&2));
    assert_eq!(counts_map.get("quick"), Some(&1));
    assert_eq!(counts_map.get("lazy"), Some(&1));
    Ok(())
}
