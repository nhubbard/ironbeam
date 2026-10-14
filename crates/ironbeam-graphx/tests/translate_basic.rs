//! Package-local v5 graph-to-Beam protobuf contract tests.

use beam_proto::pipeline_v1::test_stream_payload::event::Event as StreamEvent;
use beam_proto::pipeline_v1::{
    accumulation_mode, output_time, trigger, ParDoPayload, StateSpec, TimerFamilySpec,
};
use ironbeam_core::{
    cogroup_by_key_2_named, Error, OrderedFoldFn, Pipeline, Replay, SinkMode, SinkSpec,
    TestStreamEvent, TypedSink, Version,
};
use ironbeam_graphx::v5::{identity_from_beam, translate_v5, validate_beam_contract, V5Error};
use ironbeam_graphx::{ids, urns};
use prost::Message;

const IMAGE: &str = "example.test/ironbeam:v5";
const BUILD: &str = "graphx-test-build";

#[test]
fn unbounded_group_into_batches_adds_portable_count_trigger() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![(
                ("a".to_owned(), 2_i32),
                2_000_000,
            )])],
        )
        .unwrap()
        .group_into_batches_named("batches", 2)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let trigger_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "batches/count-trigger")
        .unwrap();
    let strategy = &components.windowing_strategies[&trigger_pc.windowing_strategy_id];
    let Some(trigger::Trigger::AfterEndOfWindow(end)) = strategy
        .trigger
        .as_ref()
        .and_then(|trigger| trigger.trigger.as_ref())
    else {
        panic!("expected end-of-window trigger");
    };
    let Some(trigger::Trigger::Repeat(repeat)) = end
        .early_firings
        .as_ref()
        .and_then(|early| early.trigger.as_ref())
    else {
        panic!("expected repeated early trigger");
    };
    assert!(
        matches!(repeat.subtrigger.as_ref().and_then(|sub| sub.trigger.as_ref()), Some(trigger::Trigger::ElementCount(count)) if count.element_count == 2)
    );
}

#[test]
fn v5_contract_rejects_unimplemented_state_and_timer_payloads() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .map_named("mapped", |value| value + 1)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    for fault in ["state", "timer", "window-expiration"] {
        let mut translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
        let transform = translated
            .pipeline
            .components
            .as_mut()
            .unwrap()
            .transforms
            .values_mut()
            .find(|transform| transform.unique_name == "mapped")
            .unwrap();
        let spec = transform.spec.as_mut().unwrap();
        let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
        match fault {
            "state" => {
                payload
                    .state_specs
                    .insert("cell".into(), StateSpec::default());
            }
            "timer" => {
                payload
                    .timer_family_specs
                    .insert("fire".into(), TimerFamilySpec::default());
            }
            "window-expiration" => payload.on_window_expiration_timer_family_spec = "fire".into(),
            _ => unreachable!(),
        }
        spec.payload = payload.encode_to_vec();
        assert!(
            matches!(
                validate_beam_contract(&graph, &translated),
                Err(V5Error::Wire(_))
            ),
            "{fault}"
        );
    }
}

#[test]
fn unbounded_global_count_group_lowers_to_portable_global_trigger() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "source",
            vec![TestStreamEvent::Elements(vec![(
                ("a".to_owned(), 2_i32),
                2_000_000,
            )])],
        )
        .unwrap()
        .early_count_trigger_named("trigger", 2)
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let trigger_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "trigger")
        .unwrap();
    let strategy = &components.windowing_strategies[&trigger_pc.windowing_strategy_id];
    assert_eq!(
        strategy.window_fn.as_ref().unwrap().urn,
        urns::WINDOW_FN_GLOBAL
    );
    let Some(trigger::Trigger::AfterEndOfWindow(end)) = strategy
        .trigger
        .as_ref()
        .and_then(|trigger| trigger.trigger.as_ref())
    else {
        panic!("expected end-of-window trigger");
    };
    let Some(trigger::Trigger::Repeat(repeat)) = end
        .early_firings
        .as_ref()
        .and_then(|early| early.trigger.as_ref())
    else {
        panic!("expected repeated early trigger");
    };
    assert!(matches!(
        repeat.subtrigger.as_ref().and_then(|sub| sub.trigger.as_ref()),
        Some(trigger::Trigger::ElementCount(count)) if count.element_count == 2
    ));
}

#[test]
fn early_count_trigger_is_a_distinct_portable_windowing_strategy() {
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
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let window_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "windows")
        .unwrap();
    let trigger_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "count-trigger")
        .unwrap();
    assert_ne!(
        window_pc.windowing_strategy_id,
        trigger_pc.windowing_strategy_id
    );
    let strategy = &components.windowing_strategies[&trigger_pc.windowing_strategy_id];
    let Some(trigger::Trigger::AfterEndOfWindow(end)) = strategy
        .trigger
        .as_ref()
        .and_then(|trigger| trigger.trigger.as_ref())
    else {
        panic!("expected end-of-window trigger");
    };
    let Some(trigger::Trigger::Repeat(repeat)) = end
        .early_firings
        .as_ref()
        .and_then(|early| early.trigger.as_ref())
    else {
        panic!("expected repeated early trigger");
    };
    assert!(matches!(
        repeat.subtrigger.as_ref().and_then(|sub| sub.trigger.as_ref()),
        Some(trigger::Trigger::ElementCount(count)) if count.element_count == 1
    ));
}

#[test]
fn accumulating_count_trigger_sets_beam_accumulation_mode() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .accumulating_count_trigger_named("count-trigger", 1)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let window_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "windows")
        .unwrap();
    let trigger_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "count-trigger")
        .unwrap();
    assert_ne!(
        window_pc.windowing_strategy_id,
        trigger_pc.windowing_strategy_id
    );
    let strategy = &components.windowing_strategies[&trigger_pc.windowing_strategy_id];
    assert_eq!(
        strategy.accumulation_mode,
        accumulation_mode::Enum::Accumulating as i32
    );
    assert_eq!(strategy.allowed_lateness, 0);
}

#[test]
fn sliding_count_trigger_does_not_reassign_existing_windows() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .sliding_windows("windows", 10_000_000, 5_000_000, 0)
        .unwrap()
        .early_count_trigger_named("count-trigger", 1)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let windows = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "windows")
        .unwrap();
    let trigger = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "count-trigger")
        .unwrap();
    assert_eq!(
        windows.spec.as_ref().unwrap().urn,
        urns::TRANSFORM_WINDOW_INTO
    );
    assert_eq!(trigger.spec.as_ref().unwrap().urn, urns::TRANSFORM_PARDO);
    let window_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "windows")
        .unwrap();
    let trigger_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "count-trigger")
        .unwrap();
    assert_ne!(
        window_pc.windowing_strategy_id,
        trigger_pc.windowing_strategy_id
    );
}

#[test]
fn end_of_window_timestamp_policy_has_a_distinct_beam_strategy() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .end_of_window_output_time_named("output-policy")
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let default_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "windows")
        .unwrap();
    let policy_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "output-policy")
        .unwrap();
    assert_ne!(
        default_pc.windowing_strategy_id,
        policy_pc.windowing_strategy_id
    );
    let strategy = &components.windowing_strategies[&policy_pc.windowing_strategy_id];
    assert_eq!(strategy.output_time, output_time::Enum::EndOfWindow as i32);
    assert_eq!(strategy.allowed_lateness, 0);
}

#[test]
fn latest_in_pane_timestamp_policy_lowers_to_beam_strategy() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .latest_in_pane_output_time_named("output-policy")
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let policy_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "output-policy")
        .unwrap();
    let strategy = &components.windowing_strategies[&policy_pc.windowing_strategy_id];
    assert_eq!(strategy.output_time, output_time::Enum::LatestInPane as i32);
}

#[test]
fn allowed_lateness_lowers_to_beam_windowing_strategy() {
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
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let window_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "windows")
        .unwrap();
    let late_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "lateness")
        .unwrap();
    assert_ne!(
        window_pc.windowing_strategy_id,
        late_pc.windowing_strategy_id
    );
    assert_eq!(
        components.windowing_strategies[&late_pc.windowing_strategy_id].allowed_lateness,
        5
    );
}

#[test]
fn late_count_lowers_to_repeated_late_firings() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .late_count_trigger_named("late-count", 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let trigger_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "late-count")
        .unwrap();
    let strategy = &components.windowing_strategies[&trigger_pc.windowing_strategy_id];
    assert_eq!(strategy.allowed_lateness, 5);
    assert_eq!(
        strategy.accumulation_mode,
        accumulation_mode::Enum::Discarding as i32
    );
    let Some(trigger::Trigger::AfterEndOfWindow(end)) =
        strategy.trigger.as_ref().and_then(|t| t.trigger.as_ref())
    else {
        panic!("expected end-of-window trigger");
    };
    assert!(end.early_firings.is_none());
    let Some(trigger::Trigger::Repeat(repeat)) =
        end.late_firings.as_ref().and_then(|t| t.trigger.as_ref())
    else {
        panic!("expected repeated late trigger");
    };
    assert!(
        matches!(repeat.subtrigger.as_ref().and_then(|t| t.trigger.as_ref()),
        Some(trigger::Trigger::ElementCount(count)) if count.element_count == 2)
    );
}

#[test]
fn accumulating_late_count_sets_beam_accumulation_mode() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .accumulating_late_count_trigger_named("late-count", 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let trigger_pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "late-count")
        .unwrap();
    let strategy = &components.windowing_strategies[&trigger_pc.windowing_strategy_id];
    assert_eq!(
        strategy.accumulation_mode,
        accumulation_mode::Enum::Accumulating as i32
    );
    assert_eq!(strategy.allowed_lateness, 5);
}

#[test]
fn combined_count_trigger_lowers_early_and_late_firings() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("values", vec![("a".to_owned(), 2_i32)])
        .unwrap()
        .fixed_windows("windows", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .early_and_late_count_trigger_named("combined", 1, 2)
        .unwrap()
        .group_by_key_named("groups")
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let pc = components
        .pcollections
        .values()
        .find(|pc| pc.unique_name == "combined")
        .unwrap();
    let strategy = &components.windowing_strategies[&pc.windowing_strategy_id];
    let Some(trigger::Trigger::AfterEndOfWindow(end)) =
        strategy.trigger.as_ref().and_then(|t| t.trigger.as_ref())
    else {
        panic!("expected end-of-window trigger");
    };
    for (firing, expected_count) in [(&end.early_firings, 1), (&end.late_firings, 2)] {
        let Some(trigger::Trigger::Repeat(repeat)) =
            firing.as_ref().and_then(|t| t.trigger.as_ref())
        else {
            panic!("expected repeated count trigger");
        };
        assert!(
            matches!(repeat.subtrigger.as_ref().and_then(|t| t.trigger.as_ref()),
            Some(trigger::Trigger::ElementCount(count)) if count.element_count == expected_count)
        );
    }
}

#[test]
fn bounded_multi_signal_wait_lowers_two_portable_side_dependencies() {
    let pipeline = Pipeline::default();
    let first = pipeline.from_vec("first", vec![1_i32]).unwrap();
    let second = pipeline.from_vec("second", vec![2_i32]).unwrap();
    pipeline
        .from_vec("main", vec![10_i32])
        .unwrap()
        .wait_on_all_named("wait-all", &[first, second])
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    for key in ["wait-all/signal-0", "wait-all"] {
        let transform = components
            .transforms
            .values()
            .find(|transform| transform.unique_name == key)
            .unwrap();
        let payload =
            ParDoPayload::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
        assert_eq!(payload.side_inputs.len(), 1);
        assert!(payload.side_inputs.contains_key("side"));
    }
}

struct EncounterSum;

impl OrderedFoldFn<i32, i32, i32> for EncounterSum {
    fn create(&self) -> i32 {
        0
    }
    fn add_input(&self, total: &mut i32, value: i32) {
        *total += value;
    }
    fn finish(&self, total: i32) -> i32 {
        total
    }
}

#[test]
fn direct_only_operations_fail_portable_preflight() {
    let marker = Pipeline::default();
    marker
        .from_vec("marker-source", vec![1_i32])
        .unwrap()
        .custom_named("marker", "tests/marker", Version { major: 1, minor: 0 })
        .unwrap()
        .output("marker-output")
        .unwrap();
    assert!(matches!(
        translate_v5(&marker, IMAGE, BUILD),
        Err(V5Error::Core(error)) if error.node.as_deref() == Some("marker")
    ));

    let batch = Pipeline::default();
    batch
        .from_vec("batch-source", vec![(1_i32, 2_i32), (1, 3)])
        .unwrap()
        .map_values_batches_named("unordered-batch", 2, |values| values.to_vec())
        .unwrap()
        .output("batch-output")
        .unwrap();
    assert!(matches!(
        translate_v5(&batch, IMAGE, BUILD),
        Err(V5Error::Core(error)) if error.node.as_deref() == Some("unordered-batch")
    ));

    let fold = Pipeline::default();
    fold.from_vec("fold-source", vec![1_i32, 2])
        .unwrap()
        .fold_globally_ordered_named("encounter-fold", EncounterSum)
        .unwrap()
        .output("fold-output")
        .unwrap();
    assert!(matches!(
        translate_v5(&fold, IMAGE, BUILD),
        Err(V5Error::Core(error)) if error.node.as_deref() == Some("encounter-fold")
    ));
}

#[test]
fn unbounded_test_stream_lowers_with_time_and_payload_validation() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "events",
            vec![
                TestStreamEvent::Elements(vec![(7_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceProcessingTime(1_000_000),
            ],
        )
        .unwrap()
        .map_named("double", |value| value * 2)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let source = &components.pcollections[&ironbeam_graphx::v5::collection_id("events")];
    assert_eq!(
        source.is_bounded,
        beam_proto::pipeline_v1::is_bounded::Enum::Unbounded as i32
    );
    let primitive = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "events")
        .unwrap();
    assert_eq!(
        primitive.spec.as_ref().unwrap().urn,
        urns::TRANSFORM_TEST_STREAM
    );
    assert!(primitive.inputs.is_empty());
    let mut tampered = translated;
    let transform = tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "events")
        .unwrap();
    transform.spec.as_mut().unwrap().payload.clear();
    assert!(validate_beam_contract(&graph, &tampered).is_err());

    let invalid = Pipeline::default();
    invalid
        .test_stream_source("events", vec![TestStreamEvent::Elements(vec![(7_i32, 1)])])
        .unwrap()
        .output("result")
        .unwrap();
    assert!(translate_v5(&invalid, IMAGE, BUILD).is_err());

    let multiple = Pipeline::default();
    for key in ["first", "second", "third"] {
        multiple
            .test_stream_source(
                key,
                vec![TestStreamEvent::Elements(vec![(1_i32, 1_000_000)])],
            )
            .unwrap()
            .output(&format!("{key}-out"))
            .unwrap();
    }
    let (graph, _) = multiple.snapshot().unwrap();
    let translated = translate_v5(&multiple, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let transforms = &translated.pipeline.components.as_ref().unwrap().transforms;
    let test_streams = transforms
        .values()
        .filter(|transform| {
            transform
                .spec
                .as_ref()
                .is_some_and(|spec| spec.urn == urns::TRANSFORM_TEST_STREAM)
        })
        .collect::<Vec<_>>();
    assert_eq!(test_streams.len(), 1);
    assert_eq!(test_streams[0].outputs.len(), 3);
    assert_eq!(translated.test_stream_payloads.len(), 1);
    let tags = translated.test_stream_payloads["first"]
        .events
        .iter()
        .filter_map(|event| match event.event.as_ref() {
            Some(StreamEvent::ElementEvent(elements)) => Some(elements.tag.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tags, ["first", "second", "third"]);
    let mut broken = translated;
    broken
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "first")
        .unwrap()
        .outputs
        .remove("second");
    assert!(validate_beam_contract(&graph, &broken).is_err());

    let mixed = Pipeline::default();
    mixed
        .test_stream_source(
            "first",
            vec![TestStreamEvent::Elements(vec![(1_i32, 1_000_000)])],
        )
        .unwrap()
        .output("first-out")
        .unwrap();
    mixed
        .test_stream_source(
            "second",
            vec![TestStreamEvent::Elements(vec![("x".to_owned(), 1_000_000)])],
        )
        .unwrap()
        .output("second-out")
        .unwrap();
    assert!(translate_v5(&mixed, IMAGE, BUILD).is_err());

    let timed = Pipeline::default();
    timed
        .test_stream_source::<i32>(
            "first",
            vec![TestStreamEvent::AdvanceProcessingTime(1_000_000)],
        )
        .unwrap()
        .output("first-out")
        .unwrap();
    timed
        .test_stream_source(
            "second",
            vec![TestStreamEvent::Elements(vec![(1_i32, 1_000_000)])],
        )
        .unwrap()
        .output("second-out")
        .unwrap();
    assert!(translate_v5(&timed, IMAGE, BUILD).is_err());
}

#[test]
fn tagged_test_stream_preserves_each_source_event_before_its_watermark() {
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "main",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 0)]),
                TestStreamEvent::AdvanceWatermark(6_000_000),
                TestStreamEvent::Elements(vec![(2_i32, 8_000_000)]),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .output("main-output")
        .unwrap();
    pipeline
        .test_stream_source(
            "side",
            vec![
                TestStreamEvent::Elements(vec![(3_i32, 4_000_000)]),
                TestStreamEvent::AdvanceWatermark(6_000_000),
                TestStreamEvent::AdvanceWatermark(20_000_000),
            ],
        )
        .unwrap()
        .output("side-output")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let events = &translated.test_stream_payloads["main"].events;
    let sequence = events
        .iter()
        .map(|event| match event.event.as_ref().unwrap() {
            StreamEvent::ElementEvent(row) => {
                format!("{}:element:{}", row.tag, row.elements[0].timestamp)
            }
            StreamEvent::WatermarkEvent(watermark) => {
                format!("{}:watermark:{}", watermark.tag, watermark.new_watermark)
            }
            StreamEvent::ProcessingTimeEvent(_) => unreachable!(),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sequence,
        [
            "main:element:0",
            "side:element:4",
            "main:watermark:6",
            "side:watermark:6",
            "main:element:8",
            "side:watermark:20",
            "main:watermark:20",
        ]
    );
}

#[test]
fn elementwise_graph_lowers_with_stable_identity_and_declared_coders() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .map_named("double", |value| value * 2)
        .unwrap()
        .filter_named("keep", |value| value > &2)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let first = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let second = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &first).unwrap();
    assert_eq!(first.identity, second.identity);
    assert_eq!(first.chain, second.chain);
    assert_eq!(identity_from_beam(&first.pipeline).unwrap(), first.identity);
    let components = first.pipeline.components.as_ref().unwrap();
    let source = &components.pcollections[&ironbeam_graphx::v5::collection_id("source")];
    assert_eq!(
        components.coders[&source.coder_id]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        urns::CODER_LENGTH_PREFIX
    );
    assert_eq!(
        components.windowing_strategies[ids::WINDOWING_ID].window_coder_id,
        ids::CODER_GLOBAL_WINDOW
    );

    let changed = Pipeline::default();
    changed
        .from_vec("source", vec![1_i32, 2, 4])
        .unwrap()
        .map_named("double", |value| value * 2)
        .unwrap()
        .filter_named("keep", |value| value > &2)
        .unwrap()
        .output("result")
        .unwrap();
    assert_ne!(
        first.identity.source_config_digest,
        translate_v5(&changed, IMAGE, BUILD)
            .unwrap()
            .identity
            .source_config_digest
    );
}

#[test]
fn grouping_and_outputless_sink_lower_as_v5_portable_graph() {
    struct NullSink;
    impl TypedSink<(String, u64)> for NullSink {
        fn write(&mut self, _: (String, u64)) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![("a".to_owned(), 1_u64), ("a".to_owned(), 2)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .map_named("sum", |(key, values)| (key, values.into_iter().sum()))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            "test:logging-sink:v1".into(),
            || Ok(Box::new(NullSink)),
        )
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let group = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "group")
        .unwrap();
    assert_eq!(
        group.spec.as_ref().unwrap().urn,
        urns::TRANSFORM_GROUP_BY_KEY
    );
    let sink = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "sink")
        .unwrap();
    assert!(sink.outputs.is_empty());

    let mut broken = translated;
    broken.pipeline.display_data.clear();
    assert!(validate_beam_contract(&graph, &broken).is_err());
}

#[test]
fn mixed_boundedness_join_and_cogroup_preserve_each_tagged_input() {
    use beam_proto::pipeline_v1::is_bounded::Enum as BeamBoundedness;
    use ironbeam_graphx::v5::{cogroup_flatten_collection_id, cogroup_tag_collection_id};

    let pipeline = Pipeline::default();
    let unbounded = pipeline
        .test_stream_source(
            "events",
            vec![TestStreamEvent::Elements(vec![(
                ("a".to_owned(), 1_i32),
                1_000_000,
            )])],
        )
        .unwrap()
        .fixed_windows("events-window", 4_000_000)
        .unwrap();
    let bounded = pipeline
        .from_vec("records", vec![(2_000_000_i64, ("a".to_owned(), 2_i32))])
        .unwrap()
        .timestamps_from_pairs_named("records-time")
        .unwrap()
        .fixed_windows("records-window", 4_000_000)
        .unwrap();
    cogroup_by_key_2_named("mixed-group", &unbounded, &bounded)
        .unwrap()
        .output("group-out")
        .unwrap();
    unbounded
        .join_full_named("mixed-join", &bounded)
        .unwrap()
        .output("join-out")
        .unwrap();

    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let collections = &translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .pcollections;
    for key in ["mixed-group", "mixed-join"] {
        assert_eq!(
            collections[&cogroup_tag_collection_id(key, 0)].is_bounded,
            BeamBoundedness::Unbounded as i32
        );
        assert_eq!(
            collections[&cogroup_tag_collection_id(key, 1)].is_bounded,
            BeamBoundedness::Bounded as i32
        );
        assert_eq!(
            collections[&cogroup_flatten_collection_id(key)].is_bounded,
            BeamBoundedness::Unbounded as i32
        );
    }
}
