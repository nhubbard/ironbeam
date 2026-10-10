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

#[test]
fn finite_convenience_transforms_keep_order_and_empty_semantics() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![3_i32, 1, 3, 2]).unwrap();
    let distinct = input
        .distinct_named("distinct")
        .unwrap()
        .output("distinct-out")
        .unwrap();
    let first = input
        .take_named("first-two", 2)
        .unwrap()
        .output("first-out")
        .unwrap();
    let counts = input
        .count_per_element_named("counts")
        .unwrap()
        .output("counts-out")
        .unwrap();
    let list = input
        .to_list_globally_named("list")
        .unwrap()
        .output("list-out")
        .unwrap();
    let sum = input
        .sum_globally_named("sum")
        .unwrap()
        .output("sum-out")
        .unwrap();
    let min = input
        .min_globally_named("min")
        .unwrap()
        .output("min-out")
        .unwrap();
    let max = input
        .max_globally_named("max")
        .unwrap()
        .output("max-out")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&distinct, &config).unwrap().0,
            [3, 1, 2]
        );
        assert_eq!(DirectRunner.run_collect(&first, &config).unwrap().0, [3, 1]);
        assert_eq!(
            DirectRunner.run_collect(&counts, &config).unwrap().0,
            [(3, 2), (1, 1), (2, 1)]
        );
        assert_eq!(
            DirectRunner.run_collect(&list, &config).unwrap().0,
            [vec![3, 1, 3, 2]]
        );
        assert_eq!(DirectRunner.run_collect(&sum, &config).unwrap().0, [9]);
        assert_eq!(DirectRunner.run_collect(&min, &config).unwrap().0, [1]);
        assert_eq!(DirectRunner.run_collect(&max, &config).unwrap().0, [3]);
    }
    let pipeline = Pipeline::default();
    let empty = pipeline.from_vec("empty", Vec::<i32>::new()).unwrap();
    let minimum = empty
        .min_globally_named("min")
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&minimum, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<i32>::new()
    );
}

#[test]
fn keyed_sum_constant_key_swap_and_display_compose() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![2_i32, 3, 4]).unwrap();
    let pairs = input
        .with_constant_key_named("constant", "all".to_string())
        .unwrap();
    let total = pairs
        .sum_per_key_named("sum-per-key")
        .unwrap()
        .output("total")
        .unwrap();
    let swapped = pairs.swap_named("swap").unwrap().output("swapped").unwrap();
    let labels = input
        .to_display_string_named("labels")
        .unwrap()
        .output("labels-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&total, &JobConfig::default())
            .unwrap()
            .0,
        [("all".to_string(), 9)]
    );
    assert_eq!(
        DirectRunner
            .run_collect(&swapped, &JobConfig::default())
            .unwrap()
            .0,
        [(2, "all".to_string()), (3, "all".into()), (4, "all".into())]
    );
    assert_eq!(
        DirectRunner
            .run_collect(&labels, &JobConfig::default())
            .unwrap()
            .0,
        ["2", "3", "4"]
    );
}
