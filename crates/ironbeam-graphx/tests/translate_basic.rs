//! Package-local v5 graph-to-Beam protobuf contract tests.

use beam_proto::pipeline_v1::test_stream_payload::event::Event as StreamEvent;
use beam_proto::pipeline_v1::ParDoPayload;
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
