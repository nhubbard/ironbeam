// Unit tests for src/collections.rs.
use super::CollectSet;
use ironbeam_core::CombineFn;
use std::collections::HashSet;

#[test]
fn collection_set_merge_deduplicates_across_partitions() {
    let combiner = CollectSet;
    let mut first = <CollectSet as CombineFn<i32, HashSet<i32>, HashSet<i32>>>::create(&combiner);
    let mut second = <CollectSet as CombineFn<i32, HashSet<i32>, HashSet<i32>>>::create(&combiner);
    for value in [1, 2] {
        combiner.add_input(&mut first, value);
    }
    for value in [2, 3] {
        combiner.add_input(&mut second, value);
    }
    combiner.merge(&mut first, second);
    assert_eq!(combiner.finish(first), HashSet::from([1, 2, 3]));
}
