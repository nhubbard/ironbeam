use ironbeam_core::Pipeline;
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn bounded_group_accepts_allowed_lateness() {
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (rows, _) = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap();
    assert_eq!(rows, vec![("a".to_owned(), vec![2])]);
}

#[test]
fn direct_rejects_lateness_with_unsupported_stateful_fan_in() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![("a".to_owned(), 2_i32)])
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), 3_i32)])
        .unwrap();
    let left = left
        .fixed_windows("left-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("left-lateness", 5_000_000)
        .unwrap();
    let right = right
        .fixed_windows("right-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("right-lateness", 5_000_000)
        .unwrap();
    left.join_inner_named("join", &right)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let error = DirectRunner.validate(&graph).unwrap_err();
    assert!(error.to_string().contains("allowed-lateness"));
}
