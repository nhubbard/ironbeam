use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{CollectionExt, KeyedCollectionExt};
use std::collections::{HashMap, HashSet};

#[test]
fn set_list_and_dictionary_helpers_preserve_v4_values() {
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("values", vec![3_i32, 1, 3, 2]).unwrap();
    let set = values
        .to_set_globally_named("set")
        .unwrap()
        .output("set-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&set, &JobConfig::default())
            .unwrap()
            .0,
        [HashSet::from([1, 2, 3])]
    );

    let pairs = pipeline
        .from_vec(
            "pairs",
            vec![
                ("a".to_string(), 1_i32),
                ("a".into(), 2),
                ("b".into(), 3),
                ("a".into(), 2),
            ],
        )
        .unwrap();
    let lists = pairs
        .to_list_per_key_named("lists")
        .unwrap()
        .output("lists-out")
        .unwrap();
    let sets = pairs
        .to_set_per_key_named("groups", "sets")
        .unwrap()
        .output("sets-out")
        .unwrap();
    let dict = pairs
        .to_dict_named("dict")
        .unwrap()
        .output("dict-out")
        .unwrap();
    let cfg = JobConfig::default();
    assert_eq!(
        DirectRunner.run_collect(&lists, &cfg).unwrap().0,
        [("a".to_string(), vec![1, 2, 2]), ("b".into(), vec![3])]
    );
    assert_eq!(
        DirectRunner.run_collect(&sets, &cfg).unwrap().0,
        [
            ("a".to_string(), HashSet::from([1, 2])),
            ("b".into(), HashSet::from([3]))
        ]
    );
    assert_eq!(
        DirectRunner.run_collect(&dict, &cfg).unwrap().0,
        [HashMap::from([("a".to_string(), 2), ("b".into(), 3)])]
    );
}
