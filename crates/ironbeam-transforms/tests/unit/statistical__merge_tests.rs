// Unit tests for src/statistical.rs.
use super::{Median, Quantiles, TDigest};
use ironbeam_core::CombineFn;

#[test]
fn quantile_and_median_combiners_merge_partial_digests() {
    let quantiles = Quantiles {
        quantiles: vec![0.0, 0.5, 1.0],
        compression: 100.0,
    };
    let mut left: TDigest = <Quantiles as CombineFn<i32, TDigest, Vec<f64>>>::create(&quantiles);
    let mut right: TDigest = <Quantiles as CombineFn<i32, TDigest, Vec<f64>>>::create(&quantiles);
    for value in [1, 2] {
        <Quantiles as CombineFn<i32, TDigest, Vec<f64>>>::add_input(&quantiles, &mut left, value);
    }
    for value in [3, 4] {
        <Quantiles as CombineFn<i32, TDigest, Vec<f64>>>::add_input(&quantiles, &mut right, value);
    }
    <Quantiles as CombineFn<i32, TDigest, Vec<f64>>>::merge(&quantiles, &mut left, right);
    let result = <Quantiles as CombineFn<i32, TDigest, Vec<f64>>>::finish(&quantiles, left);
    assert_eq!(result[0], 1.0);
    assert_eq!(result[2], 4.0);

    let median = Median { compression: 100.0 };
    let mut left: TDigest = <Median as CombineFn<i32, TDigest, f64>>::create(&median);
    let mut right: TDigest = <Median as CombineFn<i32, TDigest, f64>>::create(&median);
    for value in [1, 2] {
        <Median as CombineFn<i32, TDigest, f64>>::add_input(&median, &mut left, value);
    }
    for value in [3, 4] {
        <Median as CombineFn<i32, TDigest, f64>>::add_input(&median, &mut right, value);
    }
    <Median as CombineFn<i32, TDigest, f64>>::merge(&median, &mut left, right);
    let result = <Median as CombineFn<i32, TDigest, f64>>::finish(&median, left);
    assert!((1.0..=4.0).contains(&result));
}
