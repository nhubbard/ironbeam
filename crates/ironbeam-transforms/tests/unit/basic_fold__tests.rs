// Unit tests for src/basic_fold.rs.
use super::{Average, Count, DistinctCount, Maximum, Mean, Minimum, Sum};
use ironbeam_core::CombineFn;
use std::collections::HashSet;
use std::marker::PhantomData;

fn merge_two<V, A, O, C: CombineFn<V, A, O>>(
    combiner: &C,
    left: impl IntoIterator<Item = V>,
    right: impl IntoIterator<Item = V>,
) -> O {
    let mut first = combiner.create();
    let mut second = combiner.create();
    for value in left {
        combiner.add_input(&mut first, value);
    }
    for value in right {
        combiner.add_input(&mut second, value);
    }
    combiner.merge(&mut first, second);
    combiner.finish(first)
}

#[test]
fn exact_accumulators_merge_partial_results_and_keep_empty_identity() {
    assert_eq!(merge_two(&Count, [1_i32, 2], [3]), 3);
    assert_eq!(merge_two(&Sum, [1_i32, 2], [3]), 6);
    assert_eq!(merge_two(&Minimum, [3_i32, 1], [2]), [1]);
    assert_eq!(merge_two(&Maximum, [3_i32, 1], [2]), [3]);
    assert_eq!(merge_two(&Average, [2_i32, 4], [6]), 4.0);
    assert_eq!(
        merge_two(&Mean::<f64>(PhantomData), [2_f64, 4.0], [6.0]),
        4.0
    );
    assert_eq!(
        merge_two::<i32, _, _, _>(&Minimum, [], []),
        Vec::<i32>::new()
    );
    assert_eq!(
        merge_two::<i32, _, _, _>(&Maximum, [], []),
        Vec::<i32>::new()
    );
    assert_eq!(
        merge_two::<f64, _, _, _>(&Mean::<f64>(PhantomData), [], []),
        0.0
    );
}

#[test]
fn count_saturates_and_distinct_count_merges_without_duplicates() {
    let mut saturated = u64::MAX;
    <Count as CombineFn<i32, u64, u64>>::merge(&Count, &mut saturated, 1);
    assert_eq!(saturated, u64::MAX);
    let distinct: u64 = merge_two(&DistinctCount, [1_i32, 2], [2, 3]);
    assert_eq!(distinct, 3);
    let empty: u64 = merge_two::<i32, HashSet<i32>, u64, _>(&DistinctCount, [], []);
    assert_eq!(empty, 0);
}
