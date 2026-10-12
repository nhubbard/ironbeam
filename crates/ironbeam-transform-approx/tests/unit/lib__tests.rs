// Unit tests for src/lib.rs.
use super::{Hll, HllSketch, Kmv, precision_for_error};
use ironbeam_core::CombineFn;
use std::collections::BTreeSet;

#[test]
fn kmv_merges_partial_sketches_and_keeps_exact_small_counts() {
    let combiner = Kmv { k: 8 };
    let mut left = <Kmv as CombineFn<u32, BTreeSet<u64>, f64>>::create(&combiner);
    let mut right = <Kmv as CombineFn<u32, BTreeSet<u64>, f64>>::create(&combiner);
    for value in [1_u32, 2, 2] {
        combiner.add_input(&mut left, value);
    }
    for value in [2_u32, 3, 4] {
        combiner.add_input(&mut right, value);
    }
    <Kmv as CombineFn<u32, BTreeSet<u64>, f64>>::merge(&combiner, &mut left, right);
    assert_eq!(
        <Kmv as CombineFn<u32, BTreeSet<u64>, f64>>::finish(&combiner, left),
        4.0
    );

    let bounded = Kmv { k: 4 };
    let mut first = <Kmv as CombineFn<u32, BTreeSet<u64>, f64>>::create(&bounded);
    let mut second = <Kmv as CombineFn<u32, BTreeSet<u64>, f64>>::create(&bounded);
    for value in 0_u32..4 {
        bounded.add_input(&mut first, value);
        bounded.add_input(&mut second, value + 4);
    }
    <Kmv as CombineFn<u32, BTreeSet<u64>, f64>>::merge(&bounded, &mut first, second);
    assert_eq!(first.len(), 4);
}

#[test]
fn hll_merges_partial_sketches_and_rejects_invalid_error_bounds() {
    let combiner = Hll { precision: 12 };
    let mut left = <Hll as CombineFn<u32, HllSketch<u32>, u64>>::create(&combiner);
    let mut right = <Hll as CombineFn<u32, HllSketch<u32>, u64>>::create(&combiner);
    for value in [1_u32, 2, 2] {
        combiner.add_input(&mut left, value);
    }
    for value in [2_u32, 3, 4] {
        combiner.add_input(&mut right, value);
    }
    <Hll as CombineFn<u32, HllSketch<u32>, u64>>::merge(&combiner, &mut left, right);
    assert_eq!(
        <Hll as CombineFn<u32, HllSketch<u32>, u64>>::finish(&combiner, left),
        4
    );
    for error in [f64::NAN, f64::INFINITY, 0.0, 1.0, -0.1] {
        assert!(precision_for_error("bad", error).is_err());
    }
    assert_eq!(precision_for_error("high", 0.00001).unwrap(), 18);
    assert_eq!(precision_for_error("low", 0.9).unwrap(), 4);
}
