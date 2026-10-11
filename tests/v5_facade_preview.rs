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

#[cfg(feature = "coders")]
#[test]
fn coders_feature_exposes_user_record_postcard_coding() {
    use ironbeam::v5::impl_postcard_element;

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Row {
        id: i32,
    }
    impl_postcard_element!(Row, "tests/facade-row/v1");

    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("rows", vec![Row { id: 3 }, Row { id: 5 }])
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(rows, [Row { id: 3 }, Row { id: 5 }]);
}
