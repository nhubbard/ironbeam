use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{
    ConstantSideInputExt, side_hashmap, side_multimap, side_singleton, side_vec,
};

#[test]
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
    let extras = side_hashmap(vec![("a".to_string(), 10_u32), ("b".to_string(), 20)]);
    let mapped = input
        .map_with_side_map_named("map", &extras, |value, map| {
            map.get(value).copied().unwrap_or(0)
        })
        .unwrap()
        .output("mapped")
        .unwrap();
    let threshold = side_singleton("b".to_string());
    let above = input
        .filter_with_singleton_named("above", &threshold, |value, t| value > t)
        .unwrap()
        .output("above-out")
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
            DirectRunner.run_collect(&above, &config).unwrap().0,
            ["c".to_string()]
        );
        assert_eq!(
            DirectRunner.run_collect(&tagged, &config).unwrap().0,
            [2, 1, 0]
        );
    }
}
