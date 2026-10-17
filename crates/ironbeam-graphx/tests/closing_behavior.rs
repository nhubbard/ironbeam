use beam_proto::pipeline_v1::closing_behavior;
use ironbeam_core::{Pipeline, WindowClosingBehavior};
use ironbeam_graphx::v5::{translate_v5, validate_beam_contract};

#[test]
fn closing_policy_lowers_to_distinct_beam_strategy() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![(1_i32, 7_i32)])
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .closing_behavior_named("closing", WindowClosingBehavior::EmitAlways)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, "example.test/ironbeam:v5", "closing-test").unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "group")
        .unwrap();
    assert_eq!(
        components.windowing_strategies[&pc.windowing_strategy_id].closing_behavior,
        closing_behavior::Enum::EmitAlways as i32
    );
}
