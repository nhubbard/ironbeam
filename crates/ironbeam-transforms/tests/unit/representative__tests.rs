use super::*;

#[test]
fn partial_representatives_merge_independently_of_partition_order() {
    let combiner = MinimumEncoded;
    let mut first = <MinimumEncoded as CombineFn<_, _, Vec<String>>>::create(&combiner);
    combiner.add_input(&mut first, (b"a3".to_vec(), "a3".to_owned()));
    combiner.add_input(&mut first, (b"a1".to_vec(), "a1".to_owned()));
    let mut second = <MinimumEncoded as CombineFn<_, _, Vec<String>>>::create(&combiner);
    combiner.add_input(&mut second, (b"a2".to_vec(), "a2".to_owned()));
    let mut forward = first.clone();
    combiner.merge(&mut forward, second.clone());
    let mut reverse = second;
    combiner.merge(&mut reverse, first);
    assert_eq!(combiner.finish(forward), ["a1"]);
    assert_eq!(combiner.finish(reverse), ["a1"]);
}
