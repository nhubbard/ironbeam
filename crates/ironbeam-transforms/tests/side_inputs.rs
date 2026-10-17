use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{
    ConstantSideInputExt, DynamicSideInputExt, side_hashmap, side_multimap, side_singleton,
    side_vec,
};

#[test]
#[allow(clippy::too_many_lines)]
fn constant_side_inputs_match_v4_lookups_and_filters() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec!["a".to_string(), "b".into(), "c".into()])
        .unwrap();
    let allowed = side_vec(vec!["a".to_string(), "c".into()]);
    let filtered = input
        .filter_with_side_vec_named("filter", &allowed, |value, allow| allow.contains(value))
        .unwrap()
        .output("filtered")
        .unwrap();
    let vec_sizes = input
        .map_with_side_vec_named("map-vec", &allowed, |value, allow| {
            (value.clone(), u64::try_from(allow.len()).unwrap())
        })
        .unwrap()
        .output("vec-sizes")
        .unwrap();
    let extras = side_hashmap(vec![("a".to_string(), 10_u32), ("b".to_string(), 20)]);
    let mapped = input
        .map_with_side_map_named("map", &extras, |value, map| {
            map.get(value).copied().unwrap_or(0)
        })
        .unwrap()
        .output("mapped")
        .unwrap();
    let map_filtered = input
        .filter_with_side_map_named("filter-map", &extras, |value, map| {
            map.get(value).is_some_and(|score| *score > 10)
        })
        .unwrap()
        .output("map-filtered")
        .unwrap();
    let threshold = side_singleton("b".to_string());
    let above = input
        .filter_with_singleton_named("above", &threshold, |value, t| value > t)
        .unwrap()
        .output("above-out")
        .unwrap();
    let with_threshold = input
        .map_with_singleton_named("map-singleton", &threshold, |value, side| {
            format!("{value}:{side}")
        })
        .unwrap()
        .output("with-threshold")
        .unwrap();
    let tags = side_multimap(vec![
        ("a".to_string(), 1_u32),
        ("a".into(), 2),
        ("b".into(), 3),
    ]);
    let tagged = input
        .map_with_side_multimap_named("tags", &tags, |value, map| {
            map.get(value)
                .map_or(0_u32, |items| u32::try_from(items.len()).unwrap())
        })
        .unwrap()
        .output("tags-out")
        .unwrap();
    let multi_filtered = input
        .filter_with_side_multimap_named("filter-multimap", &tags, |value, map| {
            map.get(value).is_some_and(|values| values.len() > 1)
        })
        .unwrap()
        .output("multi-filtered")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&filtered, &config).unwrap().0,
            ["a".to_string(), "c".to_string()]
        );
        assert_eq!(
            DirectRunner.run_collect(&mapped, &config).unwrap().0,
            [10, 20, 0]
        );
        assert_eq!(
            DirectRunner.run_collect(&vec_sizes, &config).unwrap().0,
            [
                ("a".to_string(), 2),
                ("b".to_string(), 2),
                ("c".to_string(), 2)
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&map_filtered, &config).unwrap().0,
            ["b"]
        );
        assert_eq!(
            DirectRunner.run_collect(&above, &config).unwrap().0,
            ["c".to_string()]
        );
        assert_eq!(
            DirectRunner
                .run_collect(&with_threshold, &config)
                .unwrap()
                .0,
            ["a:b", "b:b", "c:b"]
        );
        assert_eq!(
            DirectRunner.run_collect(&tagged, &config).unwrap().0,
            [2, 1, 0]
        );
        assert_eq!(
            DirectRunner
                .run_collect(&multi_filtered, &config)
                .unwrap()
                .0,
            ["a"]
        );
    }
}

#[test]
fn dynamic_side_views_materialize_once_and_keep_v4_map_semantics() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("input", vec!["a".to_string(), "b".into()])
        .unwrap();
    let pairs = pipeline
        .from_vec(
            "pairs",
            vec![("a".to_string(), 1_i32), ("a".into(), 2), ("b".into(), 3)],
        )
        .unwrap();
    let singleton = pipeline.from_vec("single", vec![10_i32]).unwrap();
    let mapped = input
        .map_with_dynamic_map_named("map", &pairs, |key, map| {
            map.get(&key).copied().unwrap_or_default()
        })
        .unwrap()
        .output("map-out")
        .unwrap();
    let grouped = input
        .map_with_dynamic_multimap_named("multimap", &pairs, |key, map| {
            map.get(&key).cloned().unwrap_or_default()
        })
        .unwrap()
        .output("multimap-out")
        .unwrap();
    let filtered = input
        .filter_with_dynamic_map_named("filter-map", &pairs, |key, map| {
            map.get(key).is_some_and(|value| *value > 2)
        })
        .unwrap()
        .output("filter-map-out")
        .unwrap();
    let filtered_multi = input
        .filter_with_dynamic_multimap_named("filter-multi", &pairs, |key, map| {
            map.get(key).is_some_and(|values| values.len() > 1)
        })
        .unwrap()
        .output("filter-multi-out")
        .unwrap();
    let raised = pipeline
        .from_vec("numbers", vec![1_i32, 2])
        .unwrap()
        .map_with_dynamic_singleton_named("singleton", &singleton, |value, side| value + side)
        .unwrap()
        .output("single-out")
        .unwrap();
    let below = pipeline
        .from_vec("filter-numbers", vec![5_i32, 15])
        .unwrap()
        .filter_with_dynamic_singleton_named("filter-single", &singleton, |value, side| {
            value < side
        })
        .unwrap()
        .output("filter-single-out")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&mapped, &config).unwrap().0,
            [2, 3]
        );
        assert_eq!(
            DirectRunner.run_collect(&grouped, &config).unwrap().0,
            [vec![1, 2], vec![3]]
        );
        assert_eq!(
            DirectRunner.run_collect(&raised, &config).unwrap().0,
            [11, 12]
        );
        assert_eq!(
            DirectRunner.run_collect(&filtered, &config).unwrap().0,
            ["b".to_string()]
        );
        assert_eq!(
            DirectRunner
                .run_collect(&filtered_multi, &config)
                .unwrap()
                .0,
            ["a".to_string()]
        );
        assert_eq!(DirectRunner.run_collect(&below, &config).unwrap().0, [5]);
    }
}

#[test]
fn dynamic_singleton_rejects_missing_and_duplicate_values() {
    for values in [vec![], vec![1_i32, 2]] {
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", vec![1_i32]).unwrap();
        let side = pipeline.from_vec("side", values).unwrap();
        let output = input
            .map_with_dynamic_singleton_named("singleton", &side, |value, side| value + side)
            .unwrap()
            .output("out")
            .unwrap();
        let error = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("singleton side input requires one value")
        );
    }
}
