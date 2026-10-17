use ironbeam_core::{ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn generic_output_route_enforces_memory_budget_without_spill() {
    let output = Pipeline::default()
        .from_vec("source", vec![1_i32])
        .unwrap()
        .map_named("format", |value| format!("a value too large: {value}"))
        .unwrap()
        .output("result")
        .unwrap();
    let error = DirectRunner
        .run_collect(
            &output,
            &JobConfig {
                memory_budget_bytes: 1,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Resource);
}
