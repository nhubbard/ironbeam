use rustflow::combiners::{AverageF64, DistinctCount, Sum};
use rustflow::*;

#[test]
fn combine_globally_sum_basic() -> anyhow::Result<()> {
    let p = Pipeline::default();
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
fn combine_globally_sum_with_fanout() -> anyhow::Result<()> {
    let p = Pipeline::default();
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
fn combine_globally_average_lifted() -> anyhow::Result<()> {
    let p = Pipeline::default();
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
fn combine_globally_distinct_count() -> anyhow::Result<()> {
    let p = Pipeline::default();
    // 0..100 modulo 7 => 7 distinct values
    let input: Vec<u32> = (0..100u32).map(|n| n % 7).collect();

    let pc = from_vec(&p, input);
    let dc = pc.combine_globally(DistinctCount::<u32>::default(), Some(4));
    let out: Vec<u64> = dc.collect_par(None, Some(8))?;

    assert_eq!(out.len(), 1);
    assert_eq!(out[0], 7);
    Ok(())
}
