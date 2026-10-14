use ironbeam_core::Pipeline;
use ironbeam_direct::DirectRunner;

#[test]
fn direct_rejects_allowed_lateness_before_opening_a_source() {
    let pipeline = Pipeline::default();
    pipeline
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
    let (graph, _) = pipeline.snapshot().unwrap();
    let error = DirectRunner.validate(&graph).unwrap_err();
    assert!(error.to_string().contains("allowed-lateness"));
}
