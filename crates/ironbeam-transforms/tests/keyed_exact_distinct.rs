use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{ElementwiseExt, KeyedExt};

#[test]
fn keyed_exact_distinct_matches_v4_for_both_execution_modes() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("values", vec!["aa".to_string(), "aa".into(), "bbb".into()])
        .unwrap();
    let keyed = values
        .with_keys_named("key", |value| u32::try_from(value.len()).unwrap())
        .unwrap();
    let distinct = keyed
        .distinct_per_key_named("group-a", "distinct")
        .unwrap()
        .output("distinct-out")
        .unwrap();
    let count = keyed
        .distinct_count_per_key_named("group-b", "count")
        .unwrap()
        .output("count-out")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&distinct, &config).unwrap().0,
            [(2, "aa".to_string()), (3, "bbb".to_string())]
        );
        assert_eq!(
            DirectRunner.run_collect(&count, &config).unwrap().0,
            [(2, 1), (3, 1)]
        );
    }
}
