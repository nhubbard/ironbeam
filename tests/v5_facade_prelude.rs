#![cfg(feature = "v5-preview")]

use ironbeam::v5::{Pipeline, prelude::*};

#[test]
fn facade_prelude_runs_v4_shaped_helper_chain_without_compat() {
    let pipeline = Pipeline::default();
    let result = pipeline
        .from_vec("numbers", vec![1_i32, 2, 3, 4])
        .unwrap()
        .map_named("double", |value| value * 2)
        .unwrap()
        .filter_eq_named("fours", &4)
        .unwrap()
        .output("result")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(result, vec![4]);
}
