// Unit tests for src/selection.rs.
use super::{BottomK, TopK};
use ironbeam_core::CombineFn;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[test]
fn top_and_bottom_selection_merge_partitioned_inputs_and_handle_zero_limit() {
    let top = TopK { k: 2 };
    let mut top_left = <TopK as CombineFn<i32, BinaryHeap<Reverse<i32>>, Vec<i32>>>::create(&top);
    let mut top_right = <TopK as CombineFn<i32, BinaryHeap<Reverse<i32>>, Vec<i32>>>::create(&top);
    for value in [1, 5] {
        top.add_input(&mut top_left, value);
    }
    for value in [3, 4] {
        top.add_input(&mut top_right, value);
    }
    top.merge(&mut top_left, top_right);
    assert_eq!(top.finish(top_left), [5, 4]);

    let bottom = BottomK { k: 2 };
    let mut bottom_left = <BottomK as CombineFn<i32, BinaryHeap<i32>, Vec<i32>>>::create(&bottom);
    let mut bottom_right = <BottomK as CombineFn<i32, BinaryHeap<i32>, Vec<i32>>>::create(&bottom);
    for value in [1, 5] {
        bottom.add_input(&mut bottom_left, value);
    }
    for value in [3, 4] {
        bottom.add_input(&mut bottom_right, value);
    }
    bottom.merge(&mut bottom_left, bottom_right);
    assert_eq!(bottom.finish(bottom_left), [1, 3]);

    let zero = BottomK { k: 0 };
    let mut empty = <BottomK as CombineFn<i32, BinaryHeap<i32>, Vec<i32>>>::create(&zero);
    zero.add_input(&mut empty, 9);
    assert_eq!(zero.finish(empty), Vec::<i32>::new());
}
