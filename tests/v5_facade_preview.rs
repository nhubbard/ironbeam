#![cfg(feature = "v5-preview")]

use ironbeam::v5::{DirectRunner, JobConfig, Pipeline};

#[test]
fn facade_exposes_core_and_direct_slice() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("numbers", vec![1_i32, 2, 3])
        .unwrap()
        .map_named("double/v1", |value| value * 2)
        .unwrap()
        .filter_named("large/v1", |value| *value > 2)
        .unwrap()
        .output("result")
        .unwrap();
    let (result, job) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(result, [4, 6]);
    assert_eq!(job.status(), ironbeam::v5::JobStatus::Succeeded);
}
