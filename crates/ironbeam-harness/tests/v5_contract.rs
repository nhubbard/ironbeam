use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

// Prost derives use ::prost internally; bind this integration test to the
// connector's version while the Fn API protobuf types still use Prost 0.13.
extern crate prost as beam_prost;
extern crate prost_v5 as prost;

use ironbeam_core::{
    Element, Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress, TypedSink,
};
use ironbeam_graphx::v5::{identity_from_beam, translate_v5, validate_beam_contract, V5Error};
use ironbeam_harness::v5::run_mock_bundle;
use ironbeam_io_csv::{csv_glob_source, csv_source};
use ironbeam_io_protobuf::{protobuf_glob_source, protobuf_source};
use ironbeam_io_xml::{xml_glob_source, xml_source};

const IMAGE: &str = "example.test/ironbeam:v5";
const BUILD: &str = "fixture-build-1";

#[test]
fn translator_expands_cogroup_into_tag_flatten_and_native_grouping() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), true)])
        .unwrap();
    ironbeam_core::cogroup_by_key_2_named("cogroup", &left, &right)
        .unwrap()
        .map_named("count", |(key, (left, right))| {
            (key, (left.len() + right.len()) as i32)
        })
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    for index in 0..2 {
        let tag = components
            .transforms
            .values()
            .find(|transform| transform.unique_name == format!("cogroup:tag{index}"))
            .unwrap();
        assert_eq!(
            tag.spec.as_ref().unwrap().urn,
            ironbeam_graphx::urns::TRANSFORM_PARDO
        );
        assert_eq!(
            components.pcollections
                [&ironbeam_graphx::v5::cogroup_tag_collection_id("cogroup", index)]
                .coder_id,
            "v5_kv_bytes"
        );
    }
    let flatten = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "cogroup:flatten")
        .unwrap();
    assert_eq!(
        flatten.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_FLATTEN
    );
    let group = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "cogroup")
        .unwrap();
    assert_eq!(
        group.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_GROUP_BY_KEY
    );
    let mut tampered = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "cogroup:tag1")
        .unwrap()
        .inputs
        .insert("in".into(), ironbeam_graphx::v5::collection_id("left"));
    assert!(validate_beam_contract(&graph, &tampered).is_err());
}

#[test]
fn translator_lowers_reshuffle_as_runner_primitive() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .reshuffle_named("redistribute")
        .unwrap()
        .map_named("increment", |value| value + 1)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let transform = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "redistribute")
        .unwrap();
    assert_eq!(
        transform.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_RESHUFFLE
    );
    assert!(transform.spec.as_ref().unwrap().payload.is_empty());
    assert!(transform.environment_id.is_empty());
    let mut tampered = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "redistribute")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .urn = ironbeam_graphx::urns::TRANSFORM_FLATTEN.into();
    assert!(validate_beam_contract(&graph, &tampered).is_err());
}

#[test]
fn translator_lowers_fixed_windows_to_native_beam_window_into() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .assign_timestamps_named("time", |_| 12_000_000)
        .unwrap()
        .fixed_windows_with_offset("window", 10_000_000, 2_000_000)
        .unwrap()
        .with_window_named("inspect-window")
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let transform = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "window")
        .unwrap();
    assert_eq!(
        transform.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_WINDOW_INTO
    );
    let collection = &components.pcollections[&ironbeam_graphx::v5::collection_id("window")];
    let strategy = &components.windowing_strategies[&collection.windowing_strategy_id];
    assert_eq!(
        strategy.window_coder_id,
        ironbeam_graphx::ids::CODER_INTERVAL_WINDOW
    );
    assert_eq!(
        components.coders[ironbeam_graphx::ids::CODER_INTERVAL_WINDOW]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        ironbeam_graphx::urns::CODER_INTERVAL_WINDOW
    );
    let mut tampered = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .pcollections
        .get_mut(&ironbeam_graphx::v5::collection_id("window"))
        .unwrap()
        .windowing_strategy_id = ironbeam_graphx::ids::WINDOWING_ID.into();
    assert!(validate_beam_contract(&graph, &tampered).is_err());
}

#[test]
fn native_grouping_uses_beam_kv_and_iterable_coders() {
    let pipeline = Pipeline::default();
    let pairs = pipeline
        .from_vec("pairs", vec![("a".to_owned(), 1_i32), ("a".to_owned(), 2)])
        .unwrap();
    let grouped = pairs.group_by_key_named("group").unwrap();
    grouped
        .map_named("count", |(key, values)| (key, values.len() as i32))
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let group = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "group")
        .unwrap();
    assert_eq!(
        group.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_GROUP_BY_KEY
    );
    let pair_pc = components
        .pcollections
        .get(&ironbeam_graphx::v5::collection_id("pairs"))
        .unwrap();
    let grouped_pc = components
        .pcollections
        .get(&ironbeam_graphx::v5::collection_id("group"))
        .unwrap();
    assert_eq!(
        components.coders[&pair_pc.coder_id]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        ironbeam_graphx::urns::CODER_KV
    );
    assert_eq!(
        components.coders[&grouped_pc.coder_id]
            .spec
            .as_ref()
            .unwrap()
            .urn,
        ironbeam_graphx::urns::CODER_KV
    );
    let mut tampered = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .pcollections
        .get_mut(&ironbeam_graphx::v5::collection_id("group"))
        .unwrap()
        .coder_id = "v5_lp_bytes".into();
    assert!(validate_beam_contract(&graph, &tampered).is_err());
    let latest = Pipeline::default();
    let pairs = latest
        .from_vec("pairs", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    pairs
        .latest_per_key_named("latest")
        .unwrap()
        .output("out")
        .unwrap();
    let latest_translation = translate_v5(&latest, IMAGE, BUILD).unwrap();
    let (latest_graph, _) = latest.snapshot().unwrap();
    validate_beam_contract(&latest_graph, &latest_translation).unwrap();
    let latest_group = latest_translation
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find(|transform| transform.unique_name == "latest/group")
        .unwrap();
    assert_eq!(
        latest_group.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_GROUP_BY_KEY
    );
    let fanned = Pipeline::default();
    let pairs = fanned
        .from_vec("pairs", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    pairs
        .map_named("other-consumer", |pair| pair)
        .unwrap()
        .output("other-output")
        .unwrap();
    pairs
        .group_by_key_named("group")
        .unwrap()
        .output("group-output")
        .unwrap();
    let fanned_translation = translate_v5(&fanned, IMAGE, BUILD).unwrap();
    let (fanned_graph, _) = fanned.snapshot().unwrap();
    validate_beam_contract(&fanned_graph, &fanned_translation).unwrap();
}

struct NullSink;
impl TypedSink<i32> for NullSink {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn portable_sink_requires_configuration_identity() {
    let build = |config: Option<&str>| {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
        let spec = SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        };
        match config {
            Some(config) => {
                source
                    .sink_named_with_config("sink", spec, config.into(), || Ok(Box::new(NullSink)))
                    .unwrap();
            }
            None => {
                source
                    .sink_named("sink", spec, || Ok(Box::new(NullSink)))
                    .unwrap();
            }
        }
        pipeline
    };
    let missing = build(None);
    assert!(matches!(translate_v5(&missing, IMAGE, BUILD),
        Err(V5Error::Unsupported { key, .. }) if key == "sink"));
    let driver = translate_v5(&build(Some("destination-a")), IMAGE, BUILD).unwrap();
    let worker = translate_v5(&build(Some("destination-b")), IMAGE, BUILD).unwrap();
    assert!(driver.identity.verify(&worker.identity).is_err());
    assert_ne!(
        driver.identity.sink_config_digest,
        worker.identity.sink_config_digest
    );
}

struct FinalizableNullSink;
impl TypedSink<i32> for FinalizableNullSink {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn requires_bundle_finalization(&self) -> bool {
        true
    }
    fn finalize_bundle(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn finalized_sink_sets_static_beam_flag_and_changes_identity() {
    use beam_prost::Message;
    use beam_proto::pipeline_v1::ParDoPayload;
    let build = |finalized| {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
        let source = source.reshuffle_named("boundary").unwrap();
        let spec = SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        };
        if finalized {
            source
                .sink_named_with_config_and_finalization(
                    "sink",
                    spec,
                    "same-destination".into(),
                    || Ok(Box::new(FinalizableNullSink)),
                )
                .unwrap();
        } else {
            source
                .sink_named_with_config("sink", spec, "same-destination".into(), || {
                    Ok(Box::new(NullSink))
                })
                .unwrap();
        }
        pipeline
    };
    let ordinary = translate_v5(&build(false), IMAGE, BUILD).unwrap();
    assert!(ordinary
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find(|transform| transform.unique_name == "sink")
        .unwrap()
        .outputs
        .is_empty());
    let pipeline = build(true);
    let finalized = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &finalized).unwrap();
    assert_ne!(
        ordinary.identity.sink_config_digest,
        finalized.identity.sink_config_digest
    );
    assert!(finalized.finalization_sinks.contains("sink"));
    let transform = finalized
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find(|transform| transform.unique_name == "sink")
        .unwrap();
    assert!(transform.outputs.is_empty());
    assert!(!finalized
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .pcollections
        .contains_key(&ironbeam_graphx::v5::collection_id("sink")));
    let payload =
        ParDoPayload::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    assert!(payload.requests_finalization);
}

#[test]
fn finalized_sink_without_stage_boundary_is_rejected() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    source
        .sink_named_with_config_and_finalization(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            "destination".into(),
            || Ok(Box::new(FinalizableNullSink)),
        )
        .unwrap();
    assert!(matches!(translate_v5(&pipeline, IMAGE, BUILD),
        Err(V5Error::Unsupported { reason, .. }) if reason.contains("Reshuffle stage boundary")));
}

#[test]
fn whole_job_final_result_sink_is_rejected_before_worker_execution() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    source
        .sink_named_with_config(
            "final-file",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            "destination-a".into(),
            || Ok(Box::new(NullSink)),
        )
        .unwrap();
    assert!(matches!(translate_v5(&pipeline, IMAGE, BUILD),
        Err(V5Error::Core(error))
        if matches!(error.kind, ErrorKind::UnsupportedCapability { .. })));
}

#[test]
fn beam_contract_rejects_unregistered_finalization_requests() {
    use beam_prost::Message;
    use beam_proto::pipeline_v1::ParDoPayload;

    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let mut translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let transform = translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "source")
        .unwrap();
    let spec = transform.spec.as_mut().unwrap();
    let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    payload.requests_finalization = true;
    spec.payload = payload.encode_to_vec();
    assert!(matches!(validate_beam_contract(&graph, &translated),
        Err(V5Error::Wire(message)) if message.contains("bundle finalization contract differs")));

    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), true)])
        .unwrap();
    ironbeam_core::cogroup_by_key_2_named("group", &left, &right)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let mut translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let transform = translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "group:tag0")
        .unwrap();
    let spec = transform.spec.as_mut().unwrap();
    let mut payload = ParDoPayload::decode(spec.payload.as_slice()).unwrap();
    payload.requests_finalization = true;
    spec.payload = payload.encode_to_vec();
    assert!(matches!(validate_beam_contract(&graph, &translated),
        Err(V5Error::Wire(message)) if message.contains("unexpected bundle finalization request")));
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserRow {
    id: i32,
    category: String,
}
impl Element for UserRow {
    const TYPE_ID: &'static str = "tests/user-row";
    const CODER_ID: &'static str = "tests/user-row-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, ironbeam_core::Error> {
        let mut bytes = self.id.to_le_bytes().to_vec();
        bytes.extend_from_slice(self.category.as_bytes());
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, ironbeam_core::Error> {
        let id = bytes
            .get(..4)
            .ok_or_else(|| ironbeam_core::Error::incompatible("truncated user row"))?;
        let id = i32::from_le_bytes(id.try_into().unwrap());
        let category = String::from_utf8(bytes[4..].to_vec())
            .map_err(|_| ironbeam_core::Error::incompatible("invalid user row text"))?;
        Ok(Self { id, category })
    }
}

#[test]
fn custom_element_coder_crosses_the_v5_worker_boundary() {
    let build = || {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec(
                "source",
                vec![
                    UserRow {
                        id: 1,
                        category: "a".into(),
                    },
                    UserRow {
                        id: 2,
                        category: "b".into(),
                    },
                ],
            )
            .unwrap()
            .map_named("increment", |mut row| {
                row.id += 10;
                row
            })
            .unwrap()
            .output("result")
            .unwrap();
        pipeline
    };
    let translated = translate_v5(&build(), IMAGE, BUILD).unwrap();
    let actual = run_mock_bundle(&build(), &translated, BUILD)
        .unwrap()
        .iter()
        .map(|bytes| UserRow::decode(bytes).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            UserRow {
                id: 11,
                category: "a".into()
            },
            UserRow {
                id: 12,
                category: "b".into()
            },
        ]
    );
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CsvRow {
    id: i32,
    label: String,
}
impl Element for CsvRow {
    const TYPE_ID: &'static str = "tests/portable-csv-row";
    const CODER_ID: &'static str = "tests/portable-csv-row-json-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|error| Error::incompatible(error.to_string()))
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|error| Error::incompatible(error.to_string()))
    }
}

#[test]
fn isolated_csv_source_crosses_v5_worker_coder_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.csv");
    std::fs::write(&path, b"id,label\n1,Ada\n2,Lin\n").unwrap();
    let build = || {
        let pipeline = Pipeline::default();
        csv_source::<CsvRow>(&pipeline, "source", &path)
            .unwrap()
            .map_named("double", |mut row| {
                row.id *= 2;
                row
            })
            .unwrap()
            .output("result")
            .unwrap();
        pipeline
    };
    let translated = translate_v5(&build(), IMAGE, BUILD).unwrap();
    let actual = run_mock_bundle(&build(), &translated, BUILD)
        .unwrap()
        .iter()
        .map(|bytes| CsvRow::decode(bytes).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        [
            CsvRow {
                id: 2,
                label: "Ada".into()
            },
            CsvRow {
                id: 4,
                label: "Lin".into()
            },
        ]
    );
}

#[test]
fn csv_glob_lowers_as_multiple_sources_and_native_flatten() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.csv"), b"id,label\n1,Ada\n").unwrap();
    std::fs::write(dir.path().join("b.csv"), b"id,label\n2,Lin\n").unwrap();
    let pattern = dir.path().join("*.csv").to_string_lossy().into_owned();
    let pipeline = Pipeline::default();
    csv_glob_source::<CsvRow>(&pipeline, "all", &pattern)
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    assert_eq!(
        graph
            .nodes
            .iter()
            .filter(|node| matches!(node.operation, ironbeam_core::Operation::Source { .. }))
            .count(),
        2
    );
    let flatten = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find(|transform| transform.unique_name == "all")
        .unwrap();
    assert_eq!(
        flatten.spec.as_ref().unwrap().urn,
        "beam:transform:flatten:v1"
    );
}

#[test]
fn isolated_xml_source_crosses_v5_worker_coder_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.xml");
    std::fs::write(&path, b"<records><item><id>1</id><label>Ada</label></item><item><id>2</id><label>Lin</label></item></records>").unwrap();
    let build = || {
        let pipeline = Pipeline::default();
        xml_source::<CsvRow>(&pipeline, "source", &path)
            .unwrap()
            .map_named("double", |mut row| {
                row.id *= 2;
                row
            })
            .unwrap()
            .output("result")
            .unwrap();
        pipeline
    };
    let translated = translate_v5(&build(), IMAGE, BUILD).unwrap();
    let actual = run_mock_bundle(&build(), &translated, BUILD)
        .unwrap()
        .iter()
        .map(|bytes| CsvRow::decode(bytes).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        [
            CsvRow {
                id: 2,
                label: "Ada".into()
            },
            CsvRow {
                id: 4,
                label: "Lin".into()
            },
        ]
    );
}

#[test]
fn xml_glob_lowers_as_multiple_sources_and_native_flatten() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("a.xml"),
        b"<records><item><id>1</id><label>Ada</label></item></records>",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("b.xml"),
        b"<records><item><id>2</id><label>Lin</label></item></records>",
    )
    .unwrap();
    let pattern = dir.path().join("*.xml").to_string_lossy().into_owned();
    let pipeline = Pipeline::default();
    xml_glob_source::<CsvRow>(&pipeline, "all", &pattern)
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    assert_eq!(
        graph
            .nodes
            .iter()
            .filter(|node| matches!(node.operation, ironbeam_core::Operation::Source { .. }))
            .count(),
        2
    );
    let flatten = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find(|transform| transform.unique_name == "all")
        .unwrap();
    assert_eq!(
        flatten.spec.as_ref().unwrap().urn,
        "beam:transform:flatten:v1"
    );
}

#[derive(Clone, PartialEq, prost_v5::Message)]
struct ProtoRow {
    #[prost(int32, tag = "1")]
    id: i32,
    #[prost(string, tag = "2")]
    label: String,
}

impl Element for ProtoRow {
    const TYPE_ID: &'static str = "tests/v5-protobuf-row";
    const CODER_ID: &'static str = "tests/v5-protobuf-row-prost14";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(prost_v5::Message::encode_to_vec(self))
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        <Self as prost_v5::Message>::decode(bytes)
            .map_err(|error| Error::incompatible(error.to_string()))
    }
}

#[test]
fn isolated_protobuf_source_crosses_v5_worker_coder_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.pb");
    let rows = [
        ProtoRow {
            id: 1,
            label: "Ada".into(),
        },
        ProtoRow {
            id: 2,
            label: "Lin".into(),
        },
    ];
    let bytes = rows
        .iter()
        .flat_map(prost_v5::Message::encode_length_delimited_to_vec)
        .collect::<Vec<_>>();
    std::fs::write(&path, bytes).unwrap();
    let build = || {
        let pipeline = Pipeline::default();
        protobuf_source::<ProtoRow>(&pipeline, "source", &path)
            .unwrap()
            .map_named("double", |mut row| {
                row.id *= 2;
                row
            })
            .unwrap()
            .output("result")
            .unwrap();
        pipeline
    };
    let translated = translate_v5(&build(), IMAGE, BUILD).unwrap();
    let actual = run_mock_bundle(&build(), &translated, BUILD)
        .unwrap()
        .iter()
        .map(|bytes| ProtoRow::decode(bytes).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        actual.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![2, 4]
    );
    assert_eq!(
        actual
            .iter()
            .map(|row| row.label.as_str())
            .collect::<Vec<_>>(),
        vec!["Ada", "Lin"]
    );
}

#[test]
fn protobuf_glob_lowers_as_multiple_sources_and_native_flatten() {
    let dir = tempfile::tempdir().unwrap();
    for id in [1, 2] {
        let row = ProtoRow {
            id,
            label: format!("row-{id}"),
        };
        std::fs::write(
            dir.path().join(format!("part-{id}.pb")),
            prost_v5::Message::encode_length_delimited_to_vec(&row),
        )
        .unwrap();
    }
    let pattern = dir.path().join("*.pb").to_string_lossy().into_owned();
    let pipeline = Pipeline::default();
    protobuf_glob_source::<ProtoRow>(&pipeline, "all", &pattern)
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    assert_eq!(
        graph
            .nodes
            .iter()
            .filter(|node| matches!(node.operation, ironbeam_core::Operation::Source { .. }))
            .count(),
        2
    );
    let flatten = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find(|transform| transform.unique_name == "all")
        .unwrap();
    assert_eq!(
        flatten.spec.as_ref().unwrap().urn,
        "beam:transform:flatten:v1"
    );
}

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2, 3]).unwrap();
    source
        .map_named("double", |n| n * 2)
        .unwrap()
        .filter_named("even-over-two", |n| *n > 2)
        .unwrap()
        .flat_map_named("expand", |n| vec![n, n + 1])
        .unwrap()
        .output("result")
        .unwrap();
    pipeline
}

#[test]
fn driver_and_mock_worker_agree_on_graph_coders_and_values() {
    let driver = pipeline();
    let worker = pipeline();
    let translated = translate_v5(&driver, IMAGE, BUILD).unwrap();
    let (graph, _) = worker.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    assert_eq!(
        translated.chain,
        ["source", "double", "even-over-two", "expand", "result"]
    );
    assert_eq!(
        identity_from_beam(&translated.pipeline).unwrap(),
        translated.identity
    );
    assert_eq!(
        translated
            .pipeline
            .components
            .as_ref()
            .unwrap()
            .transforms
            .len(),
        5
    );
    let actual: Vec<i32> = run_mock_bundle(&worker, &translated, BUILD)
        .unwrap()
        .iter()
        .map(|bytes| i32::decode(bytes).unwrap())
        .collect();
    assert_eq!(actual, [4, 5, 6, 7]);
}

#[test]
fn translator_preserves_branched_collection_wiring() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    source
        .map_named("left", |n| n * 2)
        .unwrap()
        .output("left-out")
        .unwrap();
    source
        .map_named("right", |n| n + 10)
        .unwrap()
        .output("right-out")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let transforms = &translated.pipeline.components.as_ref().unwrap().transforms;
    let source_pc = transforms
        .values()
        .find(|t| t.unique_name == "source")
        .unwrap()
        .outputs
        .get("out")
        .unwrap();
    for key in ["left", "right"] {
        let transform = transforms.values().find(|t| t.unique_name == key).unwrap();
        assert_eq!(transform.inputs.get("in"), Some(source_pc));
    }
    assert!(matches!(run_mock_bundle(&pipeline, &translated, BUILD),
        Err(V5Error::Unsupported { reason, .. }) if reason.contains("mock bundle")));
}

#[test]
fn translator_lowers_two_source_roots_into_one_flatten() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("source-left", vec![1_i32]).unwrap();
    let right = pipeline.from_vec("source-right", vec![2_i32]).unwrap();
    left.flatten_named("merged", &[right])
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    assert_eq!(
        translated.chain,
        ["source-left", "source-right", "merged", "result"]
    );
}

#[test]
fn translator_lowers_flatten_as_native_beam_transform() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    let left = source.map_named("left", |n| n * 2).unwrap();
    let right = source.map_named("right", |n| n + 10).unwrap();
    left.flatten_named("merged", &[right])
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let transforms = &translated.pipeline.components.as_ref().unwrap().transforms;
    let merged = transforms
        .values()
        .find(|transform| transform.unique_name == "merged")
        .unwrap();
    assert_eq!(
        merged.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_FLATTEN
    );
    assert_eq!(merged.inputs.len(), 2);
    assert!(merged.environment_id.is_empty());
}

#[test]
fn translator_lowers_all_join_variants_through_native_shuffle() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![("a".to_owned(), 1_i32)])
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![("a".to_owned(), true)])
        .unwrap();
    left.join_inner_named("inner", &right)
        .unwrap()
        .output("inner-out")
        .unwrap();
    left.join_left_named("left-join", &right)
        .unwrap()
        .output("left-out")
        .unwrap();
    left.join_right_named("right-join", &right)
        .unwrap()
        .output("right-out")
        .unwrap();
    left.join_full_named("full", &right)
        .unwrap()
        .output("full-out")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    for key in ["inner", "left-join", "right-join", "full"] {
        let group = components
            .transforms
            .values()
            .find(|transform| transform.unique_name == key)
            .unwrap();
        assert_eq!(
            group.spec.as_ref().unwrap().urn,
            ironbeam_graphx::urns::TRANSFORM_GROUP_BY_KEY
        );
        assert!(group.environment_id.is_empty());
        let flatten = components
            .transforms
            .values()
            .find(|transform| transform.unique_name == format!("{key}:flatten"))
            .unwrap();
        assert_eq!(
            flatten.spec.as_ref().unwrap().urn,
            ironbeam_graphx::urns::TRANSFORM_FLATTEN
        );
        assert_eq!(flatten.inputs.len(), 2);
    }
    let mut tampered = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let tag = tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "full:tag1")
        .unwrap();
    tag.inputs.insert("in".into(), "wrong-parent".into());
    assert!(validate_beam_contract(&graph, &tampered).is_err());
}

#[test]
fn translator_declares_bounded_side_input_state_contract() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32, 2]).unwrap();
    let side = pipeline.from_vec("side", vec![10_i32, 20]).unwrap();
    main.map_with_side_named("add-side", &side, |value, values| {
        value + values.iter().sum::<i32>()
    })
    .unwrap()
    .output("result")
    .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .values()
        .find(|transform| transform.unique_name == "add-side")
        .unwrap();
    assert_eq!(transform.inputs.len(), 2);
    let payload: beam_proto::pipeline_v1::ParDoPayload =
        beam_prost::Message::decode(transform.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    assert_eq!(
        payload.side_inputs["side"]
            .access_pattern
            .as_ref()
            .unwrap()
            .urn,
        "beam:side_input:iterable:v1"
    );

    let mut tampered = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let transform = tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "add-side")
        .unwrap();
    transform.inputs.insert("side".into(), "wrong-side".into());
    assert!(validate_beam_contract(&graph, &tampered).is_err());

    let windowed = Pipeline::default();
    let main = windowed
        .from_vec("main", vec![1_i32])
        .unwrap()
        .fixed_windows_with_offset("main-window", 2_000_000, 0)
        .unwrap();
    let side = windowed
        .from_vec("side", vec![10_i32])
        .unwrap()
        .fixed_windows_with_offset("side-window", 2_000_000, 0)
        .unwrap();
    main.map_with_side_named("windowed-side", &side, |value, values| value + values[0])
        .unwrap()
        .output("windowed-result")
        .unwrap();
    let translated = translate_v5(&windowed, IMAGE, BUILD).unwrap();
    let (graph, _) = windowed.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let components = translated.pipeline.components.as_ref().unwrap();
    let transform = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "windowed-side:side-global")
        .unwrap();
    assert_eq!(
        transform.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_WINDOW_INTO
    );
    let side = components
        .transforms
        .values()
        .find(|transform| transform.unique_name == "windowed-side")
        .unwrap();
    assert_eq!(side.inputs["side"], transform.outputs["out"]);
    let mut tampered = translate_v5(&windowed, IMAGE, BUILD).unwrap();
    let transform = tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "windowed-side:side-global")
        .unwrap();
    transform.inputs.insert("in".into(), "wrong-side".into());
    assert!(validate_beam_contract(&graph, &tampered).is_err());
}

#[test]
fn translator_lowers_wait_on_as_a_global_bounded_signal_view() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32, 2]).unwrap();
    let signal = pipeline
        .from_vec("signal", vec![10_i32])
        .unwrap()
        .assign_timestamps_named("signal-time", |_| 1_000_000)
        .unwrap()
        .fixed_windows_with_offset("signal-window", 2_000_000, 0)
        .unwrap();
    main.wait_on_named("wait", &signal)
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    validate_beam_contract(&graph, &translated).unwrap();
    let transforms = &translated.pipeline.components.as_ref().unwrap().transforms;
    let global = transforms
        .values()
        .find(|transform| transform.unique_name == "wait:side-global")
        .unwrap();
    assert_eq!(
        global.spec.as_ref().unwrap().urn,
        ironbeam_graphx::urns::TRANSFORM_WINDOW_INTO
    );
    let wait = transforms
        .values()
        .find(|transform| transform.unique_name == "wait")
        .unwrap();
    assert_eq!(wait.inputs["side"], global.outputs["out"]);
    let payload: beam_proto::pipeline_v1::ParDoPayload =
        beam_prost::Message::decode(wait.spec.as_ref().unwrap().payload.as_slice()).unwrap();
    assert_eq!(
        payload.side_inputs["side"]
            .access_pattern
            .as_ref()
            .unwrap()
            .urn,
        "beam:side_input:iterable:v1"
    );

    let mut tampered = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let wait = tampered
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .values_mut()
        .find(|transform| transform.unique_name == "wait")
        .unwrap();
    wait.inputs.insert("side".into(), "wrong-signal".into());
    assert!(validate_beam_contract(&graph, &tampered).is_err());
}

#[test]
fn event_time_is_reified_after_v5_worker_coder_boundaries() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .assign_timestamps_named("event-time", |value| i64::from(*value) * 1_000_000)
        .unwrap()
        .reify_timestamps_named("reify-time")
        .unwrap()
        .output("result")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    let actual = run_mock_bundle(&pipeline, &translated, BUILD)
        .unwrap()
        .iter()
        .map(|bytes| <(i64, i32) as Element>::decode(bytes).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actual, [(1_000_000, 1), (2_000_000, 2)]);
}

#[test]
fn identity_and_coder_mismatches_fail_before_opening_a_source() {
    let opened = Arc::new(AtomicUsize::new(0));
    let make_worker = |values: Vec<i32>| {
        let p = Pipeline::default();
        let opened = Arc::clone(&opened);
        let src = p
            .bounded_source(
                "source",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: false,
                },
                format!("values:{values:?}"),
                Some(values.len()),
                move || {
                    opened.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(values.clone().into_iter().map(Ok)))
                },
            )
            .unwrap();
        src.map_named("double", |n: i32| n * 2)
            .unwrap()
            .output("result")
            .unwrap();
        p
    };
    let driver = make_worker(vec![1, 2]);
    let translated = translate_v5(&driver, IMAGE, BUILD).unwrap();
    let worker = make_worker(vec![1, 2]);
    assert!(run_mock_bundle(&worker, &translated, "different-build").is_err());
    let changed = make_worker(vec![1, 3]);
    assert!(run_mock_bundle(&changed, &translated, BUILD).is_err());

    let mut corrupted = translate_v5(&driver, IMAGE, BUILD).unwrap();
    let components = corrupted.pipeline.components.as_mut().unwrap();
    let inner = components
        .coders
        .values_mut()
        .find(|coder| {
            coder
                .spec
                .as_ref()
                .is_some_and(|spec| spec.urn == ironbeam_graphx::urns::CODER_IRONBEAM_V5)
        })
        .unwrap();
    inner.spec.as_mut().unwrap().payload = b"{}".to_vec();
    assert!(matches!(
        run_mock_bundle(&worker, &corrupted, BUILD),
        Err(V5Error::Wire(_))
    ));
    let mut wrong_ids = translate_v5(&driver, IMAGE, BUILD).unwrap();
    wrong_ids.pipeline.root_transform_ids.pop();
    assert!(matches!(
        run_mock_bundle(&worker, &wrong_ids, BUILD),
        Err(V5Error::Wire(_))
    ));
    assert_eq!(opened.load(Ordering::SeqCst), 0);
}

#[test]
fn translator_rejects_unsupported_operations_and_unregistered_unbounded_sources() {
    let p = Pipeline::default();
    let source = p.from_vec("source", vec![1_i32]).unwrap();
    source
        .fixed_windows("window", 5)
        .unwrap()
        .output("result")
        .unwrap();
    assert!(matches!(translate_v5(&p, IMAGE, BUILD),
        Err(V5Error::Unsupported { key, .. }) if key == "window"));

    let p = Pipeline::default();
    let source = p
        .unbounded_source::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            0,
        )
        .unwrap();
    source.output("result").unwrap();
    assert!(matches!(translate_v5(&p, IMAGE, BUILD),
        Err(V5Error::Unsupported { key, .. }) if key == "events"));
}

#[test]
fn translator_accepts_registered_unbounded_reader_without_opening_it() {
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "stream-config-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |_| {
                    opens.fetch_add(1, Ordering::Relaxed);
                    Err(Error::incompatible("reader must not open"))
                }
            },
        )
        .unwrap()
        .output("out")
        .unwrap();
    let translated = translate_v5(&pipeline, IMAGE, BUILD).unwrap();
    validate_beam_contract(&pipeline.snapshot().unwrap().0, &translated).unwrap();
    assert_eq!(opens.load(Ordering::Relaxed), 0);
}
