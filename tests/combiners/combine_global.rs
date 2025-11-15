use anyhow::Result;
use ironbeam::combiners::{AverageF64, DistinctCount, Sum};
use ironbeam::testing::*;
use ironbeam::*;

#[test]
fn combine_globally_sum_basic() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<u64> = (0..100u64).collect(); // sum = 4950

    let pc = from_vec(&p, input);
    // no explicit fanout
    let summed = pc.combine_globally(Sum::<u64>::default(), None);
    let out: Vec<u64> = summed.collect_seq()?;

    assert_eq!(out.len(), 1);
    assert_eq!(out[0], 4950);
    Ok(())
}

#[test]
fn combine_globally_sum_with_fanout() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<u64> = (0..10_000u64).collect(); // sum = 49_995_000

    let pc = from_vec(&p, input);
    // force a small fanout to exercise multi-round merge
    let summed = pc.combine_globally(Sum::<u64>::default(), Some(3));
    let out: Vec<u64> = summed.collect_par(None, Some(32))?;

    assert_eq!(out.len(), 1);
    assert_eq!(out[0], 49_995_000);
    Ok(())
}

#[test]
fn combine_globally_average_lifted() -> Result<()> {
    let p = TestPipeline::new();
    let input = vec![1u32, 2, 3, 4]; // avg = 2.5

    let pc = from_vec(&p, input);
    // AverageF64 implements LiftableCombiner, so the lifted local path is exercised
    let avg = pc.combine_globally(AverageF64, None);
    let out: Vec<f64> = avg.collect_seq()?;

    assert_eq!(out.len(), 1);
    assert!((out[0] - 2.5).abs() < 1e-12);
    Ok(())
}

#[test]
fn combine_globally_distinct_count() -> Result<()> {
    let p = TestPipeline::new();
    // 0..100 modulo 7 => 7 distinct values
    let input: Vec<u32> = (0..100u32).map(|n| n % 7).collect();

    let pc = from_vec(&p, input);
    let dc = pc.combine_globally(DistinctCount::<u32>::default(), Some(4));
    let out: Vec<u64> = dc.collect_par(None, Some(8))?;

    assert_eq!(out.len(), 1);
    assert_eq!(out[0], 7);
    Ok(())
}

#[test]
fn combine_globally_empty_input() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<u64> = vec![]; // empty

    let pc = from_vec(&p, input);
    let summed = pc.combine_globally(Sum::<u64>::default(), None);
    let out: Vec<u64> = summed.collect_seq()?;

    // Should produce one element (the default accumulator finished)
    assert_eq!(out.len(), 1);
    assert_eq!(out[0], 0); // Sum default is 0
    Ok(())
}

#[test]
fn combine_globally_single_element() -> Result<()> {
    let p = TestPipeline::new();
    let input = vec![42u64];

    let pc = from_vec(&p, input);
    let summed = pc.combine_globally(Sum::<u64>::default(), None);
    let out: Vec<u64> = summed.collect_seq()?;

    assert_eq!(out.len(), 1);
    assert_eq!(out[0], 42);
    Ok(())
}

#[test]
fn combine_globally_lifted_path() -> Result<()> {
    let p = TestPipeline::new();
    let input = vec![10u32, 20, 30, 40, 50];

    let pc = from_vec(&p, input);
    // Use combine_globally_lifted to test the lifted path
    let avg = pc.combine_globally_lifted(AverageF64, None);
    let out: Vec<f64> = avg.collect_seq()?;

    assert_eq!(out.len(), 1);
    assert!((out[0] - 30.0).abs() < 1e-12); // (10+20+30+40+50)/5 = 30
    Ok(())
}

#[test]
fn combine_globally_lifted_with_fanout() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<u32> = (1..=1000).collect();

    let pc = from_vec(&p, input);
    let avg = pc.combine_globally_lifted(AverageF64, Some(8));
    let out: Vec<f64> = avg.collect_par(None, Some(16))?;

    assert_eq!(out.len(), 1);
    // Average of 1..=1000 is 500.5
    assert!((out[0] - 500.5).abs() < 1e-10);
    Ok(())
}

#[test]
fn combine_globally_lifted_empty() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<u32> = vec![];

    let pc = from_vec(&p, input);
    let avg = pc.combine_globally_lifted(AverageF64, None);
    let out: Vec<f64> = avg.collect_seq()?;

    assert_eq!(out.len(), 1);
    assert_approx_eq!(out[0], 0.0); // Empty average returns 0.0
    Ok(())
}

#[test]
fn combine_globally_min_max() -> Result<()> {
    let p = TestPipeline::new();
    let input = vec![5, 2, 9, 1, 7, 3];

    let pc_min = from_vec(&p, input.clone());
    let min = pc_min.combine_globally(Min::<i32>::default(), None);
    let out_min: Vec<i32> = min.collect_seq()?;

    let pc_max = from_vec(&p, input);
    let max = pc_max.combine_globally(Max::<i32>::default(), None);
    let out_max: Vec<i32> = max.collect_seq()?;

    assert_eq!(out_min.len(), 1);
    assert_eq!(out_min[0], 1);

    assert_eq!(out_max.len(), 1);
    assert_eq!(out_max[0], 9);
    Ok(())
}

#[test]
fn combine_globally_parallel_with_many_partitions() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<u64> = (1..=10_000).collect();

    let pc = from_vec(&p, input);
    let summed = pc.combine_globally(Sum::<u64>::default(), Some(4));
    // Force many partitions
    let out: Vec<u64> = summed.collect_par(None, Some(64))?;

    assert_eq!(out.len(), 1);
    // Sum of 1..=10000 = 10000 * 10001 / 2 = 50005000
    assert_eq!(out[0], 50_005_000);
    Ok(())
}

#[test]
fn combine_globally_various_fanout_sizes() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<u64> = (1..=100).collect();

    // Test with different fanout sizes
    for fanout in [2, 4, 8, 16, 32] {
        let pc = from_vec(&p, input.clone());
        let summed = pc.combine_globally(Sum::<u64>::default(), Some(fanout));
        let out: Vec<u64> = summed.collect_par(None, Some(16))?;

        assert_eq!(out.len(), 1);
        assert_eq!(out[0], 5050); // Sum of 1..=100
    }
    Ok(())
}

#[test]
fn combine_globally_lifted_large_dataset() -> Result<()> {
    let p = TestPipeline::new();
    let input: Vec<i32> = (1..=10_000).collect();

    let pc = from_vec(&p, input);
    let avg = pc.combine_globally_lifted(AverageF64, Some(16));
    let out: Vec<f64> = avg.collect_par(None, Some(32))?;

    assert_eq!(out.len(), 1);
    // Average of 1..=10000 is 5000.5
    assert!((out[0] - 5000.5).abs() < 1e-8);
    Ok(())
}
