use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::{KeyedStatisticalExt, StatisticalExt, TDigest};

#[test]
fn digest_handles_empty_non_finite_and_merge() {
    let mut digest = TDigest::new(100.0);
    assert!(digest.quantile(0.5).is_nan());
    digest.add(f64::NAN);
    digest.add(f64::INFINITY);
    assert!(digest.is_empty());
    digest.add(1.0);
    digest.add(2.0);
    let mut right = TDigest::new(100.0);
    right.add(3.0);
    right.add(4.0);
    digest.merge(&right);
    assert_eq!(digest.count(), 4.0);
    assert_eq!(digest.quantile(0.0), 1.0);
    assert_eq!(digest.quantile(1.0), 4.0);
    assert!(digest.quantile(0.5).is_finite());
    assert!((0.0..=1.0).contains(&digest.cdf(2.5)));
    assert_eq!(digest.cdf(0.0), 0.0);
    assert_eq!(digest.cdf(4.0), 1.0);
    assert_eq!(digest.quantiles(&[0.0, 1.0]), [1.0, 4.0]);
    let mut singleton = TDigest::new(100.0);
    singleton.add(7.0);
    assert_eq!(singleton.quantile(0.5), 7.0);
    assert_eq!(singleton.cdf(7.0), 1.0);
    digest.merge(&TDigest::new(100.0));
    assert_eq!(digest.count(), 4.0);
}

#[test]
fn quantiles_are_monotone_for_unsorted_small_groups() {
    for values in [
        vec![49.99, 79.99, 39.99],
        vec![599.99, 1299.99, 799.99, 449.99],
    ] {
        let mut digest = TDigest::new(100.0);
        for value in &values {
            digest.add(*value);
        }
        let result = digest.quantiles(&[0.0, 0.25, 0.5, 0.75, 0.95, 1.0]);
        assert_eq!(
            result.first().copied(),
            Some(values.iter().copied().fold(f64::INFINITY, f64::min))
        );
        assert_eq!(
            result.last().copied(),
            Some(values.iter().copied().fold(f64::NEG_INFINITY, f64::max))
        );
        assert!(
            result.windows(2).all(|pair| pair[0] <= pair[1]),
            "{result:?}"
        );
        digest.compress();
        let compressed = digest.quantiles(&[0.0, 0.25, 0.5, 0.75, 0.95, 1.0]);
        assert!(
            compressed.windows(2).all(|pair| pair[0] <= pair[1]),
            "{compressed:?}"
        );
    }
}

#[test]
fn merged_single_centroid_keeps_both_range_endpoints() {
    let mut digest = TDigest::new(100.0);
    digest.add_weighted(1.0, 0.1);
    digest.add_weighted(3.0, 0.1);
    digest.compress();
    assert_eq!(digest.quantiles(&[0.0, 0.5, 1.0]), [1.0, 2.0, 3.0]);
}

#[test]
fn graph_quantiles_match_v4_boundaries_in_sequential_and_parallel_runs() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("data", vec![1.0_f64, 2.0, 3.0, 4.0, 5.0, f64::NAN])
        .unwrap();
    let quantiles = input
        .approx_quantiles_named("quartiles", vec![0.0, 0.25, 0.5, 0.75, 1.0], 100.0)
        .unwrap()
        .output("quartiles-out")
        .unwrap();
    let median = input
        .approx_median_named("median", 100.0)
        .unwrap()
        .output("median-out")
        .unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        let values = DirectRunner.run_collect(&quantiles, &config).unwrap().0;
        assert_eq!(values.len(), 1);
        assert_eq!(values[0][0], 1.0);
        assert_eq!(values[0][4], 5.0);
        assert_eq!(
            values[0][2],
            DirectRunner.run_collect(&median, &config).unwrap().0[0]
        );
    }
    assert_eq!(
        input
            .approx_median_named("invalid", 0.0)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        input
            .approx_quantiles_named("invalid-q", vec![f64::NAN], 100.0)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
}

#[test]
fn keyed_quantiles_and_empty_global_input() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec(
            "pairs",
            vec![
                ("a".to_string(), 1.0_f64),
                ("a".into(), 3.0),
                ("b".into(), 10.0),
            ],
        )
        .unwrap();
    let per_key = input
        .approx_quantiles_per_key_named("group", "q", vec![0.0, 1.0], 100.0)
        .unwrap()
        .output("out")
        .unwrap();
    let values = DirectRunner
        .run_collect(&per_key, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(
        values,
        vec![
            ("a".to_string(), vec![1.0, 3.0]),
            ("b".to_string(), vec![10.0, 10.0])
        ]
    );
    let empty = pipeline.from_vec::<f64>("empty", vec![]).unwrap();
    let output = empty
        .approx_median_named("empty-median", 100.0)
        .unwrap()
        .output("empty-out")
        .unwrap();
    assert!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0[0]
            .is_nan()
    );
}

#[test]
fn keyed_median_keeps_each_key_separate() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec(
            "values",
            vec![
                ("a".to_owned(), 1.0_f64),
                ("a".to_owned(), 3.0),
                ("b".to_owned(), 10.0),
            ],
        )
        .unwrap();
    let output = input
        .approx_median_per_key_named("median-group", "median", 100.0)
        .unwrap()
        .output("median-out")
        .unwrap();
    let values = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(values, [("a".to_owned(), 2.0), ("b".to_owned(), 10.0)]);
}

#[test]
fn quantiles_stream_large_linear_source() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .from_vec("large-digest-source", (0_i32..10_000).collect())
        .unwrap();
    let output = source
        .approx_quantiles_named("large-digest", vec![0.0, 0.5, 1.0], 100.0)
        .unwrap()
        .output("large-digest-out")
        .unwrap();
    let config = JobConfig {
        memory_budget_bytes: 512,
        buffer_budget_elements: 17,
        ..JobConfig::default()
    };
    let values = DirectRunner.run_collect(&output, &config).unwrap().0;
    assert_eq!(values[0][0], 0.0);
    assert_eq!(values[0][2], 9999.0);
    assert!((4500.0..5500.0).contains(&values[0][1]));
}
