use ironbeam_core::{Element, Pipeline};
use ironbeam_direct::{DirectFlatPartitionExt, DirectPartitionExt, DirectRunner, JobConfig};
use ironbeam_transforms::{DeadLetter, DeadLetterExt};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn map_and_flat_map_route_errors_with_original_values() {
    let pipeline = Pipeline::default();
    let lines = pipeline
        .from_vec("lines", vec!["1".to_string(), "bad".into(), "2".into()])
        .unwrap();
    let (good, errors) = lines
        .map_catching_named("classify", "good", "errors", |value| value.parse::<u32>())
        .unwrap();
    let good = good.output("good-out").unwrap();
    let errors = errors.output("errors-out").unwrap();
    let batches = pipeline
        .from_vec("batches", vec!["1,2".to_string(), "bad".into()])
        .unwrap();
    let (flat_good, flat_errors) = batches
        .flat_map_catching_named("flat-classify", "flat-good", "flat-errors", |value| {
            value
                .split(',')
                .map(str::parse::<u32>)
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap();
    let flat_good = flat_good.output("flat-good-out").unwrap();
    let flat_errors = flat_errors.output("flat-errors-out").unwrap();
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(DirectRunner.run_collect(&good, &config).unwrap().0, [1, 2]);
        let bad = DirectRunner.run_collect(&errors, &config).unwrap().0;
        assert_eq!(bad.len(), 1);
        assert_eq!(bad[0].element, "bad");
        assert_ne!(bad[0].error, "");
        assert_eq!(
            DirectRunner.run_collect(&flat_good, &config).unwrap().0,
            [1, 2]
        );
        assert_eq!(
            DirectRunner.run_collect(&flat_errors, &config).unwrap().0[0].element,
            "bad"
        );
    }
}

#[test]
fn dead_letter_coder_round_trips_and_rejects_truncation() {
    let item = DeadLetter::new("bad".to_string(), "parse error");
    let bytes = item.encode().unwrap();
    assert_eq!(DeadLetter::<String>::decode(&bytes).unwrap(), item);
    assert!(DeadLetter::<String>::decode(&bytes[..bytes.len() - 1]).is_err());
}

#[test]
fn direct_collects_both_classification_arms_in_one_run() {
    let pipeline = Pipeline::default();
    let lines = pipeline
        .from_vec("lines", vec!["1".to_string(), "bad".into(), "2".into()])
        .unwrap();
    let called = Arc::new(AtomicUsize::new(0));
    let tally = Arc::clone(&called);
    let classified = lines
        .classify_catching_named("classify", move |value| {
            tally.fetch_add(1, Ordering::Relaxed);
            value.parse::<u32>()
        })
        .unwrap();
    let (good, errors) = classified
        .collect_partitioned_named("both", &JobConfig::default())
        .unwrap();
    assert_eq!(good, [1, 2]);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].element, "bad");
    assert_eq!(called.load(Ordering::Relaxed), 3);

    let classified_flat = lines
        .classify_flat_map_catching_named("flat-classify", |value| {
            value
                .split(',')
                .map(str::parse::<u32>)
                .collect::<Result<Vec<_>, _>>()
        })
        .unwrap();
    let (good_flat, bad_flat) = classified_flat
        .collect_flat_partitioned_named("flat-both", &JobConfig::default())
        .unwrap();
    assert_eq!(good_flat, [1, 2]);
    assert_eq!(bad_flat.len(), 1);
}
