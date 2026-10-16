use beam_proto::pipeline_v1::on_time_behavior;
use ironbeam_core::{Pipeline, WindowOnTimeBehavior};
use ironbeam_graphx::v5::{translate_v5, validate_beam_contract};

#[test]
fn explicit_on_time_behavior_lowers_to_beam_strategy() {
    for (behavior, expected) in [
        (
            WindowOnTimeBehavior::FireIfNonempty,
            on_time_behavior::Enum::FireIfNonempty,
        ),
        (
            WindowOnTimeBehavior::FireAlways,
            on_time_behavior::Enum::FireAlways,
        ),
    ] {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![(1_i32, 7_i32)])
            .unwrap()
            .fixed_windows("window", 10_000_000)
            .unwrap()
            .early_count_trigger_named("early", 1)
            .unwrap()
            .on_time_behavior_named("on-time", behavior)
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .output("result")
            .unwrap();
        let (graph, _) = pipeline.snapshot().unwrap();
        let translated =
            translate_v5(&pipeline, "example.test/ironbeam:v5", "on-time-test").unwrap();
        validate_beam_contract(&graph, &translated).unwrap();
        let components = translated.pipeline.components.as_ref().unwrap();
        let pc = components
            .pcollections
            .values()
            .find(|pc| pc.unique_name == "group")
            .unwrap();
        assert_eq!(
            components.windowing_strategies[&pc.windowing_strategy_id].on_time_behavior,
            expected as i32
        );
    }
}
