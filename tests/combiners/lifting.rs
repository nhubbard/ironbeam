use anyhow::Result;
use rustflow::combiners::{Max, Min, Sum};
use rustflow::testing::*;
use rustflow::{from_vec, Count, Pipeline};
use rustflow::testing::*;

#[test]
fn gbk_then_combine_lifted_equals_classic_combine() -> Result<()> {
    let p = TestPipeline::new();
    let words: Vec<String> = (0..20_000).map(|i| format!("w{}", i % 137)).collect();

    // Baseline: key-by then direct combine
    let direct = from_vec(&p, words.clone())
        .key_by(|w: &String| w.clone())
        .combine_values(Count)
        .collect_par_sorted_by_key(Some(8), None)?;

    // Pipeline that groups first, then uses lifted combine
    let with_lift = from_vec(&p, words)
        .key_by(|w: &String| w.clone())
        .group_by_key()
        .combine_values_lifted(Count)
        .collect_par_sorted_by_key(Some(8), None)?;

    assert_eq!(direct, with_lift);
    Ok(())
}

#[test]
fn test_sum_liftable_combiner() -> Result<()> {
    use rustflow::collection::LiftableCombiner;

    let sum_combiner = Sum::<i32>::new();
    let values = vec![1, 2, 3, 4, 5];
    let result = sum_combiner.build_from_group(&values);
    assert_eq!(result, 15);

    // Empty group
    let empty: Vec<i32> = vec![];
    let result = sum_combiner.build_from_group(&empty);
    assert_eq!(result, 0);

    Ok(())
}

#[test]
fn test_min_liftable_combiner() -> Result<()> {
    use rustflow::collection::LiftableCombiner;

    let min_combiner = Min::<i32>::new();
    let values = vec![5, 2, 8, 1, 9];
    let result = min_combiner.build_from_group(&values);
    assert_eq!(result, Some(1));

    // Empty group
    let empty: Vec<i32> = vec![];
    let result = min_combiner.build_from_group(&empty);
    assert_eq!(result, None);

    Ok(())
}

#[test]
fn test_max_liftable_combiner() -> Result<()> {
    use rustflow::collection::LiftableCombiner;

    let max_combiner = Max::<i32>::new();
    let values = vec![5, 2, 8, 1, 9];
    let result = max_combiner.build_from_group(&values);
    assert_eq!(result, Some(9));

    // Empty group
    let empty: Vec<i32> = vec![];
    let result = max_combiner.build_from_group(&empty);
    assert_eq!(result, None);

    Ok(())
}
