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
