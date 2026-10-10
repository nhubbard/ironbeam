use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_transforms::PredicateExt;

#[test]
fn v4_predicate_boundaries_and_order_match() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .from_vec("source", vec![1_i32, 5, 10, 15, 20, 10])
        .unwrap();
    let variants = [
        (source.filter_eq_named("eq", &10).unwrap(), vec![10, 10]),
        (
            source.filter_ne_named("ne", &10).unwrap(),
            vec![1, 5, 15, 20],
        ),
        (source.filter_lt_named("lt", &10).unwrap(), vec![1, 5]),
        (
            source.filter_le_named("le", &10).unwrap(),
            vec![1, 5, 10, 10],
        ),
        (source.filter_gt_named("gt", &10).unwrap(), vec![15, 20]),
        (
            source.filter_ge_named("ge", &10).unwrap(),
            vec![10, 15, 20, 10],
        ),
        (
            source.filter_range_named("range", &5, &15).unwrap(),
            vec![5, 10, 10],
        ),
        (
            source
                .filter_range_inclusive_named("inclusive", &5, &15)
                .unwrap(),
            vec![5, 10, 15, 10],
        ),
        (
            source
                .filter_by_named("by", |n| n % 2, |rem| *rem == 1)
                .unwrap(),
            vec![1, 5, 15],
        ),
    ];
    for (index, (collection, expected)) in variants.into_iter().enumerate() {
        let output = collection.output(&format!("output-{index}")).unwrap();
        for parallelism in [1, 3] {
            let config = JobConfig {
                parallelism,
                ..JobConfig::default()
            };
            assert_eq!(
                DirectRunner.run_collect(&output, &config).unwrap().0,
                expected
            );
        }
    }
}

#[test]
fn predicate_helpers_keep_empty_input_empty() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", Vec::<i32>::new()).unwrap();
    let output = source
        .filter_range_named("range", &0, &10)
        .unwrap()
        .output("out")
        .unwrap();
    assert!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0
            .is_empty()
    );
}
