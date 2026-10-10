use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{ApproxDistinctExt, KeyedApproxDistinctExt};

#[test]
fn kmv_is_exact_below_k_and_bounded_for_larger_sets() {
    let pipeline = Pipeline::default();
    let small = pipeline.from_vec("small", vec![1_u64, 1, 2, 3]).unwrap();
    let small_output = small
        .approx_distinct_count_named("small-kmv", 8)
        .unwrap()
        .output("small-out")
        .unwrap();
    let large = pipeline
        .from_vec("large", (0_u64..10_000).map(|n| n % 1234).collect())
        .unwrap();
    let large_output = large
        .approx_distinct_count_named("large-kmv", 256)
        .unwrap()
        .output("large-out")
        .unwrap();
    let hll_output = large
        .approx_count_distinct_named("large-hll")
        .unwrap()
        .output("hll-out")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            memory_budget_bytes: 512,
            buffer_budget_elements: 17,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&small_output, &config).unwrap().0,
            [3.0]
        );
        let estimate = DirectRunner.run_collect(&large_output, &config).unwrap().0[0];
        assert!((1000.0..1500.0).contains(&estimate), "estimate: {estimate}");
        let hll_estimate = DirectRunner.run_collect(&hll_output, &config).unwrap().0[0];
        assert!(
            (1200..=1270).contains(&hll_estimate),
            "HLL estimate: {hll_estimate}"
        );
    }
}

#[test]
fn kmv_counts_each_key_and_empty_input() {
    let pipeline = Pipeline::default();
    let pairs = pipeline
        .from_vec(
            "pairs",
            vec![
                ("a".to_string(), 1_u32),
                ("a".into(), 1),
                ("a".into(), 2),
                ("b".into(), 9),
            ],
        )
        .unwrap();
    let output = pairs
        .approx_distinct_count_per_key_named("group", "kmv", 8)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [("a".to_string(), 2.0), ("b".to_string(), 1.0)]
    );
    let hll_output = pairs
        .approx_count_distinct_per_key_with_error_named("hll-group", "hll", 0.01)
        .unwrap()
        .output("hll-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&hll_output, &JobConfig::default())
            .unwrap()
            .0,
        [("a".to_string(), 2), ("b".to_string(), 1)]
    );
    assert_eq!(
        pairs
            .approx_count_distinct_per_key_with_error_named("bad-group", "bad", 1.0)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    let empty = pipeline.from_vec::<u32>("empty", vec![]).unwrap();
    let empty_output = empty
        .approx_distinct_count_named("empty-kmv", 8)
        .unwrap()
        .output("empty-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&empty_output, &JobConfig::default())
            .unwrap()
            .0,
        [0.0]
    );
}
