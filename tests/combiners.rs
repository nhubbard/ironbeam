use anyhow::Result;
use rustflow::{from_vec, AverageF64, DistinctCount, Max, Min, Pipeline, Sum, TopK};

#[test]
fn sum_min_max_average_basic_and_lifted() -> Result<()> {
    let p = Pipeline::default();
    let vals: Vec<i32> = (0..100).collect();

    // Sum
    let sum_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 5)
        .combine_values(Sum::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;

    let sum_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 5)
        .group_by_key()
        .combine_values_lifted(Sum::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;

    assert_eq!(sum_direct, sum_lifted);

    // Min
    let min_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 7)
        .combine_values(Min::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    let min_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 7)
        .group_by_key()
        .combine_values_lifted(Min::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    assert_eq!(min_direct, min_lifted);

    // Max
    let max_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 9)
        .combine_values(Max::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    let max_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 9)
        .group_by_key()
        .combine_values_lifted(Max::<i32>::new())
        .collect_par_sorted_by_key(Some(4), None)?;
    assert_eq!(max_direct, max_lifted);

    // AverageF64
    let avg_direct = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .key_by(|x| x % 4)
        .combine_values(AverageF64)
        .collect_par_sorted_by_key(Some(4), None)?;
    let avg_lifted = from_vec(&p, (1u32..=100).collect::<Vec<_>>())
        .key_by(|x| x % 4)
        .group_by_key()
        .combine_values_lifted(AverageF64)
        .collect_par_sorted_by_key(Some(4), None)?;
    assert_eq!(avg_direct, avg_lifted);

    Ok(())
}

#[test]
fn distinct_count_basic_and_lifted() -> Result<()> {
    let p = Pipeline::default();
    // keys: 0..5, values: repeated pattern to ensure duplicates
    let vals: Vec<u32> = (0..500).map(|i| (i % 25) as u32).collect();

    let dc_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 5) // 5 buckets
        .combine_values(DistinctCount::<u32>::new())
        .collect_par_sorted_by_key(Some(6), None)?;

    let dc_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 5)
        .group_by_key()
        .combine_values_lifted(DistinctCount::<u32>::new())
        .collect_par_sorted_by_key(Some(6), None)?;

    assert_eq!(dc_direct, dc_lifted);
    Ok(())
}

#[test]
fn topk_basic_and_lifted() -> Result<()> {
    let p = Pipeline::default();
    let vals: Vec<i32> = (0..200).collect();

    // Top 3 per key (keyed by mod 7)
    let k = 3usize;

    let top_direct = from_vec(&p, vals.clone())
        .key_by(|x| x % 7)
        .combine_values(TopK::<i32>::new(k))
        .collect_par_sorted_by_key(Some(6), None)?;

    let top_lifted = from_vec(&p, vals.clone())
        .key_by(|x| x % 7)
        .group_by_key()
        .combine_values_lifted(TopK::<i32>::new(k))
        .collect_par_sorted_by_key(Some(6), None)?;

    assert_eq!(top_direct, top_lifted);

    // sanity: each bucket should have descending order and len ≤ k
    for (_k, v) in top_direct {
        assert!(v.windows(2).all(|w| w[0] >= w[1]));
        assert!(v.len() <= 3);
    }
    Ok(())
}
