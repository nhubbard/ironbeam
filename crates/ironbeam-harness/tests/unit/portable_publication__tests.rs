// Unit tests for src/portable_publication.rs.
use super::*;
use crate::finalization_journal::FinalizationJournal;
use beam_proto::pipeline_v1::{
    Components, FunctionSpec, MonitoringInfo, PTransform, Pipeline as BeamPipeline,
};

fn identity() -> ExecutionIdentity {
    ExecutionIdentity {
        graph_fingerprint: "graph".into(),
        binary_build_id: "build".into(),
        registry_manifest: "registry".into(),
        coder_manifest: "coder".into(),
        source_config_digest: "source".into(),
        sink_config_digest: "sink".into(),
    }
}

fn metrics() -> MetricResults {
    MetricResults {
        attempted: Vec::new(),
        committed: vec![MonitoringInfo {
            urn: "beam:metric:user:sum_int64:v1".into(),
            r#type: "beam:metrics:sum_int64:v1".into(),
            labels: [
                ("PTRANSFORM".into(), "beam-sink".into()),
                ("NAMESPACE".into(), "ironbeam.sink".into()),
                ("NAME".into(), "input_elements".into()),
            ]
            .into(),
            payload: vec![3],
            ..Default::default()
        }],
    }
}

#[test]
fn plan_requires_one_outputless_finalizing_transform_per_sink() {
    let mut translated = TranslatedV5 {
        pipeline: BeamPipeline {
            components: Some(Components::default()),
            ..Default::default()
        },
        identity: identity(),
        chain: Vec::new(),
        finalization_sinks: ["sink".into()].into(),
        test_stream_payloads: Default::default(),
    };
    assert!(PortablePublicationPlan::from_translated(&translated).is_err());
    translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .insert(
            "beam-sink".into(),
            PTransform {
                unique_name: "sink".into(),
                spec: Some(FunctionSpec {
                    urn: "beam:transform:pardo:v1".into(),
                    payload: ParDoPayload::default().encode_to_vec(),
                }),
                ..Default::default()
            },
        );
    assert!(PortablePublicationPlan::from_translated(&translated).is_err());
    translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .get_mut("beam-sink")
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .payload = ParDoPayload {
        requests_finalization: true,
        ..Default::default()
    }
    .encode_to_vec();
    assert!(PortablePublicationPlan::from_translated(&translated).is_ok());
    translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .get_mut("beam-sink")
        .unwrap()
        .outputs
        .insert("unexpected".into(), "pc".into());
    assert!(PortablePublicationPlan::from_translated(&translated).is_err());
    translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .get_mut("beam-sink")
        .unwrap()
        .outputs
        .clear();
    let duplicate =
        translated.pipeline.components.as_ref().unwrap().transforms["beam-sink"].clone();
    translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .insert("duplicate".into(), duplicate);
    assert!(PortablePublicationPlan::from_translated(&translated).is_err());
    translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .remove("duplicate");
    translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .transforms
        .insert(
            "omitted".into(),
            PTransform {
                unique_name: "unlisted-final-result".into(),
                spec: Some(FunctionSpec {
                    urn: "beam:transform:pardo:v1".into(),
                    payload: ParDoPayload {
                        requests_finalization: true,
                        ..Default::default()
                    }
                    .encode_to_vec(),
                }),
                ..Default::default()
            },
        );
    assert!(PortablePublicationPlan::from_translated(&translated).is_err());
}

#[test]
fn local_publication_requires_done_complete_journal_and_committed_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let plan = PortablePublicationPlan {
        identity: identity(),
        sink_transforms: [("sink".into(), "beam-sink".into())].into(),
    };
    let journal = FileFinalizationJournal::new(dir.path(), identity()).unwrap();
    journal.record("bundle", &["sink".into()]).unwrap();
    assert!(plan
        .complete_local(job_state::Enum::Failed, &metrics(), dir.path())
        .is_err());
    assert!(plan
        .complete_local(job_state::Enum::Done, &metrics(), dir.path())
        .is_err());
    journal.complete("bundle").unwrap();
    assert!(plan
        .complete_local(job_state::Enum::Done, &MetricResults::default(), dir.path(),)
        .is_err());
    let ready = plan
        .complete_local(job_state::Enum::Done, &metrics(), dir.path())
        .unwrap();
    assert_eq!(ready.sinks().collect::<Vec<_>>(), ["sink"]);
    assert_eq!(ready.committed_rows("sink").unwrap(), 3);
    assert_eq!(ready.bundle_attempts("sink").unwrap(), ["bundle"]);
    assert!(ready.committed_rows("other").is_err());
    assert!(ready.bundle_attempts("other").is_err());
}
