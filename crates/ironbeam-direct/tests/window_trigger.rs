use ironbeam_core::Pipeline;
use ironbeam_direct::DirectRunner;

#[test]
fn direct_rejects_unimplemented_early_count_before_execution() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .early_count_trigger_named("count-trigger", 1)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let error = DirectRunner.validate(&graph).unwrap_err();
    assert_eq!(error.node.as_deref(), Some("count-trigger"));
}
