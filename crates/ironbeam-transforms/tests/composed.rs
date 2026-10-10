use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{ElementwiseExt, KeyedExt};

#[test]
fn composed_keyed_and_count_transforms_execute() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec!["a".to_string(), "bb".into(), "c".into()])
        .unwrap();
    let keyed = input
        .filter_map_named("nonempty", |value| (!value.is_empty()).then_some(value))
        .unwrap()
        .key_by_named("length", |value| i32::try_from(value.len()).unwrap())
        .unwrap();
    let counts = keyed
        .count_per_key_named("group", "count")
        .unwrap()
        .output("counts")
        .unwrap();
    let (values, _) = DirectRunner
        .run_collect(&counts, &JobConfig::default())
        .unwrap();
    assert_eq!(values, vec![(1, 2), (2, 1)]);
}

#[test]
fn key_and_value_projection_and_global_count_execute() {
    let pipeline = Pipeline::default();
    let pairs = pipeline
        .from_vec("pairs", vec![(1_i32, 5_i32), (2, 8)])
        .unwrap();
    let keys = pairs
        .keys_named("keys")
        .unwrap()
        .output("keys-out")
        .unwrap();
    let values = pairs
        .map_values_named("double", |value| value * 2)
        .unwrap()
        .filter_values_named("large", |value| *value > 10)
        .unwrap()
        .values_named("values")
        .unwrap()
        .output("values-out")
        .unwrap();
    let count = pairs
        .count_globally_named("count")
        .unwrap()
        .output("count-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&keys, &JobConfig::default())
            .unwrap()
            .0,
        vec![1, 2]
    );
    assert_eq!(
        DirectRunner
            .run_collect(&values, &JobConfig::default())
            .unwrap()
            .0,
        vec![16]
    );
    assert_eq!(
        DirectRunner
            .run_collect(&count, &JobConfig::default())
            .unwrap()
            .0,
        vec![2]
    );
}
