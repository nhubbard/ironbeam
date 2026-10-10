use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectKeySortedExt, DirectKeyedCollectExt, DirectSortedExt, JobConfig};

#[test]
fn sorted_terminals_and_hashmap_materialization_match_v4() {
    let p = Pipeline::default();
    let source = p.from_iter("numbers", [3_i32, 1, 2]).unwrap();
    assert_eq!(
        source.collect_seq_sorted_named("sequential").unwrap(),
        [1, 2, 3]
    );
    assert_eq!(
        source.collect_par_sorted_named("parallel", 3).unwrap(),
        [1, 2, 3]
    );

    let pairs = p
        .from_vec(
            "pairs",
            vec![("a".to_string(), 1_i32), ("b".into(), 2), ("a".into(), 3)],
        )
        .unwrap();
    let map = pairs
        .to_hashmap_named("map", &JobConfig::default())
        .unwrap();
    assert_eq!(map.get("a"), Some(&3));
    assert_eq!(map.get("b"), Some(&2));
    let sorted = pairs
        .collect_sorted_by_key_named(
            "sorted-keys",
            &JobConfig {
                parallelism: 3,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(
        sorted,
        [
            ("a".to_string(), 1),
            ("a".to_string(), 3),
            ("b".to_string(), 2)
        ]
    );
}
