use anyhow::Result;
use rustflow::{AverageF64, Max, Min, Pipeline, Sum, from_vec};

#[test]
fn sum_min_max_basic() -> Result<()> {
    let p = Pipeline::default();
    let xs: Vec<i32> = (0..100).collect();

    let sum = from_vec(&p, xs.clone())
        .key_by(|x| x % 3)
        .combine_values(Sum::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    let min = from_vec(&p, xs.clone())
        .key_by(|x| x % 3)
        .combine_values(Min::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    let max = from_vec(&p, xs.clone())
        .key_by(|x| x % 3)
        .combine_values(Max::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;

    // quick sanity
    assert_eq!(min[0].1, 0); // key 0 min in 0..99
    assert_eq!(max[0].1, 99); // key 0 max
    assert!(sum.iter().map(|(_, v)| v).sum::<i32>() > 0);
    Ok(())
}

#[test]
fn average_f64_basic() -> Result<()> {
    let p = Pipeline::default();
    let xs: Vec<u32> = (1..=10).collect(); // avg per key 0/1 mod 2

    let avgs = from_vec(&p, xs)
        .key_by(|x| x % 2)
        .combine_values(AverageF64)
        .collect_par_sorted_by_key(Some(2), None)?;

    // roughly: evens avg ~ 6, odds avg ~ 5
    assert!((avgs[0].1 - 5.0).abs() < 1.0 || (avgs[1].1 - 5.0).abs() < 1.0);
    Ok(())
}
