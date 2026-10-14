// Unit tests for src/dispatch.rs.
use super::*;
use beam_proto::fn_execution_v1::{
    FinalizeBundleRequest, MonitoringInfosMetadataRequest, ProcessBundleRequest,
};

fn req(id: &str, r: InstrReq) -> InstructionRequest {
    InstructionRequest {
        instruction_id: id.to_string(),
        request: Some(r),
    }
}

#[test]
fn process_bundle_uncached_asks_control_loop_to_fetch() {
    let d = Dispatcher::new(PbdCache::new(), 0xdead_beef);
    let resp = d.handle(req(
        "i1",
        InstrReq::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: "pbd-1".into(),
            cache_tokens: vec![],
            elements: None,
            has_no_state: false,
            only_bundle_for_keys: false,
        }),
    ));
    assert_eq!(resp.instruction_id, "i1");
    assert!(resp.response.is_none());
    assert!(
        resp.error.contains("unknown pbd_id=pbd-1"),
        "got: {}",
        resp.error
    );
    assert!(
        resp.error.contains("GetProcessBundleDescriptor"),
        "got: {}",
        resp.error
    );
}

#[test]
fn process_bundle_cached_plans_and_reports_steps() {
    use beam_proto::fn_execution_v1::RemoteGrpcPort;
    use beam_proto::pipeline_v1::{ApiServiceDescriptor, FunctionSpec, PTransform, ParDoPayload};
    use ironbeam_graphx::{urns, v5::V5NodeRef};
    use prost::Message;

    let cache = PbdCache::new();
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-ok".into(),
        ..Default::default()
    };
    // Source
    let src = {
        let port = RemoteGrpcPort {
            api_service_descriptor: Some(ApiServiceDescriptor {
                url: "runner:1".into(),
                ..Default::default()
            }),
            coder_id: "c".into(),
        };
        PTransform {
            spec: Some(FunctionSpec {
                urn: crate::bundle::URN_RUNNER_SOURCE.to_string(),
                payload: port.encode_to_vec(),
            }),
            outputs: std::iter::once(("out".to_string(), "pc0".to_string())).collect(),
            ..Default::default()
        }
    };
    pbd.transforms.insert("s".into(), src);
    // ParDo
    let pardo_payload = ParDoPayload {
        do_fn: Some(FunctionSpec {
            urn: urns::DOFN_IRONBEAM_V5.to_string(),
            payload: V5NodeRef {
                key: "p".into(),
                identity: ironbeam_core::ExecutionIdentity {
                    graph_fingerprint: "sha256-v1:abc".into(),
                    binary_build_id: "test-build".into(),
                    registry_manifest: "registry".into(),
                    coder_manifest: "coders".into(),
                    source_config_digest: "source".into(),
                    sink_config_digest: "sink".into(),
                },
                co_group_input: None,
            }
            .encode()
            .unwrap(),
        }),
        ..Default::default()
    };
    pbd.transforms.insert(
        "p".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: urns::TRANSFORM_PARDO.to_string(),
                payload: pardo_payload.encode_to_vec(),
            }),
            inputs: std::iter::once(("in".to_string(), "pc0".to_string())).collect(),
            outputs: std::iter::once(("out".to_string(), "pc1".to_string())).collect(),
            ..Default::default()
        },
    );
    cache.insert(pbd);

    let d = Dispatcher::new(cache, 0);
    let resp = d.handle(req(
        "i2",
        InstrReq::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: "pbd-ok".into(),
            cache_tokens: vec![],
            elements: None,
            has_no_state: false,
            only_bundle_for_keys: false,
        }),
    ));
    assert!(resp.response.is_none());
    assert!(
        resp.error.contains("not yet wired (M2c)"),
        "got: {}",
        resp.error
    );
    assert!(resp.error.contains("2 steps"), "got: {}", resp.error);
    assert!(resp.error.contains("key=p"), "got: {}", resp.error);
}

#[test]
fn process_bundle_cached_plan_error_surfaces() {
    let cache = PbdCache::new();
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-bad".into(),
        ..Default::default()
    };
    // One transform with nonsense URN triggers PlanError::UnsupportedUrn.
    pbd.transforms.insert(
        "t".into(),
        beam_proto::pipeline_v1::PTransform {
            spec: Some(beam_proto::pipeline_v1::FunctionSpec {
                urn: "beam:transform:nope:v1".into(),
                payload: vec![],
            }),
            ..Default::default()
        },
    );
    cache.insert(pbd);

    let d = Dispatcher::new(cache, 0);
    let resp = d.handle(req(
        "i3",
        InstrReq::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: "pbd-bad".into(),
            cache_tokens: vec![],
            elements: None,
            has_no_state: false,
            only_bundle_for_keys: false,
        }),
    ));
    assert!(resp.response.is_none());
    assert!(
        resp.error.contains("bundle plan failed"),
        "got: {}",
        resp.error
    );
}

#[test]
fn progress_is_ok_empty() {
    let d = Dispatcher::new(PbdCache::new(), 0);
    let resp = d.handle(req(
        "i2",
        InstrReq::ProcessBundleProgress(Default::default()),
    ));
    assert_eq!(resp.instruction_id, "i2");
    assert!(resp.error.is_empty());
    assert!(matches!(
        resp.response,
        Some(InstrResp::ProcessBundleProgress(_))
    ));
}

#[test]
fn finalization_without_executor_is_not_acknowledged() {
    let d = Dispatcher::new(PbdCache::new(), 0);
    let resp = d.handle(req(
        "finalize-request",
        InstrReq::FinalizeBundle(FinalizeBundleRequest {
            instruction_id: "bundle-1".into(),
        }),
    ));
    assert_eq!(resp.instruction_id, "finalize-request");
    assert!(resp.response.is_none());
    assert!(resp.error.contains("bundle-1"));
}

#[test]
fn committed_metric_metadata_is_registered_and_unknown_ids_fail_closed() {
    let d = Dispatcher::new(PbdCache::new(), 0);
    let mut bundle = ProcessBundleResponse {
        monitoring_infos: vec![crate::executor::element_count_info("pc-one", 128).unwrap()],
        ..Default::default()
    };
    d.register_bundle_metrics("bundle-1", &mut bundle).unwrap();
    assert_eq!(bundle.monitoring_data.len(), 1);
    let metric_id = "ironbeam-element-count:6:pc-one:bundle-1";
    assert_eq!(bundle.monitoring_data[metric_id], vec![0x80, 0x01]);
    let response = d.handle(req(
        "metric-request",
        InstrReq::MonitoringInfos(MonitoringInfosMetadataRequest {
            monitoring_info_id: vec![metric_id.into()],
        }),
    ));
    let Some(InstrResp::MonitoringInfos(metadata)) = response.response else {
        panic!("expected monitoring metadata response");
    };
    let info = &metadata.monitoring_info[metric_id];
    assert_eq!(info.labels["PCOLLECTION"], "pc-one");
    assert!(info.payload.is_empty());

    let unknown = d.handle(req(
        "unknown",
        InstrReq::MonitoringInfos(MonitoringInfosMetadataRequest {
            monitoring_info_id: vec!["missing".into()],
        }),
    ));
    assert!(unknown.response.is_none());
    assert!(unknown.error.contains("unknown monitoring info id"));

    bundle.monitoring_infos[0]
        .labels
        .insert("EXTRA".into(), "changed".into());
    assert!(d
        .register_bundle_metrics("bundle-1", &mut bundle)
        .unwrap_err()
        .contains("metadata changed"));

    let mut user = ProcessBundleResponse {
        monitoring_infos: vec![MonitoringInfo {
            urn: "beam:metric:user:sum_int64:v1".into(),
            r#type: "beam:metrics:sum_int64:v1".into(),
            labels: [
                ("PTRANSFORM".into(), "sink-transform".into()),
                ("NAMESPACE".into(), "ironbeam.sink".into()),
                ("NAME".into(), "input_elements".into()),
            ]
            .into(),
            payload: vec![3],
            ..Default::default()
        }],
        ..Default::default()
    };
    d.register_bundle_metrics("bundle-1", &mut user).unwrap();
    let user_id = "ironbeam-user-sum:14:sink-transform:bundle-1";
    assert_eq!(user.monitoring_data[user_id], vec![3]);

    let mut second_bundle = ProcessBundleResponse {
        monitoring_infos: vec![crate::executor::element_count_info("pc-one", 2).unwrap()],
        ..Default::default()
    };
    d.register_bundle_metrics("bundle-2", &mut second_bundle)
        .unwrap();
    assert_ne!(
        bundle.monitoring_data.keys().next(),
        second_bundle.monitoring_data.keys().next()
    );
    let response = d.handle(req(
        "metric-request-2",
        InstrReq::MonitoringInfos(MonitoringInfosMetadataRequest {
            monitoring_info_id: vec!["ironbeam-element-count:6:pc-one:bundle-2".into()],
        }),
    ));
    assert!(matches!(
        response.response,
        Some(InstrResp::MonitoringInfos(_))
    ));
}

#[test]
fn empty_request_errors_cleanly() {
    let d = Dispatcher::new(PbdCache::new(), 0);
    let resp = d.handle(InstructionRequest {
        instruction_id: "i3".into(),
        request: None,
    });
    assert!(resp.response.is_none());
    assert!(resp.error.contains("no request oneof"));
}

#[test]
fn pbd_cache_roundtrip() {
    let c = PbdCache::new();
    assert!(c.is_empty());
    c.insert(ProcessBundleDescriptor {
        id: "p1".into(),
        ..Default::default()
    });
    assert_eq!(c.len(), 1);
    assert_eq!(c.get("p1").unwrap().id, "p1");
    assert!(c.get("missing").is_none());
}
