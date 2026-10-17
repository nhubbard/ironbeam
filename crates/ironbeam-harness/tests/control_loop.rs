//! Integration test: spin up an in-process BeamFnControl server, have the
//! harness's control client connect, exchange a few instructions, and assert
//! that the dispatcher replies with the expected shapes.

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use beam_proto::fn_execution_v1::beam_fn_control_server::{BeamFnControl, BeamFnControlServer};
use beam_proto::fn_execution_v1::{
    instruction_request::Request as InstrReq, instruction_response::Response as InstrResp,
    GetProcessBundleDescriptorRequest, InstructionRequest, InstructionResponse,
    ProcessBundleDescriptor, ProcessBundleProgressRequest, ProcessBundleRequest,
    ProcessBundleResponse, ProcessBundleSplitRequest, ProcessBundleSplitResponse,
};
use ironbeam_harness::{bundle::BundlePlan, executor::BundleRunner};
use ironbeam_harness::{control, dispatch::Dispatcher, PbdCache};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

/// An in-process BeamFnControl server that:
/// - On `Control`, sends a pre-scripted list of `InstructionRequest`s to the
///   SDK and then closes the stream.
/// - Collects every `InstructionResponse` the SDK sends back into a Vec the
///   test can inspect.
/// - Serves `GetProcessBundleDescriptor` from a preset map so fetch-on-miss
///   can be exercised end-to-end.
struct FakeRunner {
    scripted: Vec<InstructionRequest>,
    collected: Arc<Mutex<Vec<InstructionResponse>>>,
    pbds: Arc<std::collections::HashMap<String, ProcessBundleDescriptor>>,
}

#[tonic::async_trait]
impl BeamFnControl for FakeRunner {
    type ControlStream = ReceiverStream<Result<InstructionRequest, Status>>;

    async fn control(
        &self,
        request: Request<Streaming<InstructionResponse>>,
    ) -> Result<Response<Self::ControlStream>, Status> {
        let (tx, rx) = mpsc::channel::<Result<InstructionRequest, Status>>(16);

        // Forward scripted instructions to the SDK.
        let scripted = self.scripted.clone();
        tokio::spawn(async move {
            for req in scripted {
                if tx.send(Ok(req)).await.is_err() {
                    return;
                }
            }
            // Let the SDK drain its responses before we drop the sender.
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        // Collect responses the SDK sends back.
        let collected = Arc::clone(&self.collected);
        let mut incoming = request.into_inner();
        tokio::spawn(async move {
            while let Ok(Some(resp)) = incoming.message().await {
                collected.lock().await.push(resp);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_process_bundle_descriptor(
        &self,
        request: Request<GetProcessBundleDescriptorRequest>,
    ) -> Result<Response<ProcessBundleDescriptor>, Status> {
        let id = request.into_inner().process_bundle_descriptor_id;
        match self.pbds.get(&id) {
            Some(pbd) => Ok(Response::new(pbd.clone())),
            None => Err(Status::not_found(format!("no PBD with id {id}"))),
        }
    }
}

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

async fn run_scenario(
    scripted: Vec<InstructionRequest>,
    pbds: Vec<ProcessBundleDescriptor>,
) -> Vec<InstructionResponse> {
    run_scenario_with_runner(scripted, pbds, None).await
}

async fn run_scenario_with_runner(
    scripted: Vec<InstructionRequest>,
    pbds: Vec<ProcessBundleDescriptor>,
    bundle_runner: Option<Arc<dyn BundleRunner>>,
) -> Vec<InstructionResponse> {
    let addr = free_addr();
    let collected = Arc::new(Mutex::new(Vec::new()));

    let pbd_map: std::collections::HashMap<String, ProcessBundleDescriptor> =
        pbds.into_iter().map(|p| (p.id.clone(), p)).collect();

    let runner = FakeRunner {
        scripted,
        collected: Arc::clone(&collected),
        pbds: Arc::new(pbd_map),
    };
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(BeamFnControlServer::new(runner))
            .serve(addr)
            .await
            .unwrap();
    });

    // Give the server a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let dispatcher = Arc::new(Dispatcher::new(PbdCache::new(), 42));
    let session = control::start(&addr.to_string(), dispatcher, bundle_runner)
        .await
        .expect("control start");

    // The scripted set is finite; once the runner drops its sender, our
    // control loop sees `Ok(None)` and exits cleanly.
    tokio::time::timeout(Duration::from_secs(3), session.join())
        .await
        .expect("session join timeout")
        .expect("session result");

    server_handle.abort();

    let out = collected.lock().await.clone();
    out
}

struct SplittingRunner {
    release: tokio::sync::Notify,
    split_seen: std::sync::atomic::AtomicBool,
}

#[tonic::async_trait]
impl BundleRunner for SplittingRunner {
    async fn run(&self, _: &str, _: &BundlePlan) -> anyhow::Result<ProcessBundleResponse> {
        self.release.notified().await;
        Ok(ProcessBundleResponse::default())
    }

    async fn split_bundle(
        &self,
        request: &ProcessBundleSplitRequest,
    ) -> anyhow::Result<ProcessBundleSplitResponse> {
        assert_eq!(request.instruction_id, "bundle-live");
        self.split_seen
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.release.notify_one();
        Ok(ProcessBundleSplitResponse::default())
    }
}

#[tokio::test]
async fn live_bundle_receives_split_before_it_finishes() {
    let runner = Arc::new(SplittingRunner {
        release: tokio::sync::Notify::new(),
        split_seen: std::sync::atomic::AtomicBool::new(false),
    });
    let scripted = vec![
        InstructionRequest {
            instruction_id: "bundle-live".into(),
            request: Some(InstrReq::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "pbd-live".into(),
                ..Default::default()
            })),
        },
        InstructionRequest {
            instruction_id: "split-live".into(),
            request: Some(InstrReq::ProcessBundleSplit(ProcessBundleSplitRequest {
                instruction_id: "bundle-live".into(),
                ..Default::default()
            })),
        },
    ];
    let responses = run_scenario_with_runner(
        scripted,
        vec![ProcessBundleDescriptor {
            id: "pbd-live".into(),
            ..Default::default()
        }],
        Some(runner.clone()),
    )
    .await;
    assert!(runner.split_seen.load(std::sync::atomic::Ordering::SeqCst));
    assert!(responses
        .iter()
        .any(|response| response.instruction_id == "split-live"
            && matches!(response.response, Some(InstrResp::ProcessBundleSplit(_)))));
    assert!(responses
        .iter()
        .any(|response| response.instruction_id == "bundle-live"
            && matches!(response.response, Some(InstrResp::ProcessBundle(_)))));
}

#[tokio::test]
async fn control_loop_handles_progress_and_process_bundle() {
    let scripted = vec![
        InstructionRequest {
            instruction_id: "p1".to_string(),
            request: Some(InstrReq::ProcessBundleProgress(
                ProcessBundleProgressRequest::default(),
            )),
        },
        InstructionRequest {
            instruction_id: "b1".to_string(),
            request: Some(InstrReq::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "pbd-42".into(),
                cache_tokens: vec![],
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
            })),
        },
    ];

    let responses = run_scenario(scripted, vec![]).await;
    assert_eq!(responses.len(), 2, "got {:?}", responses);

    // p1: progress -> OK empty
    let p1 = responses.iter().find(|r| r.instruction_id == "p1").unwrap();
    assert!(
        p1.error.is_empty(),
        "progress should not error: {}",
        p1.error
    );
    assert!(matches!(
        p1.response,
        Some(InstrResp::ProcessBundleProgress(_))
    ));

    // b1: process_bundle with no cached PBD AND the fake runner refuses to
    // serve one (empty pbds) -> error asking the control loop to fetch it.
    let b1 = responses.iter().find(|r| r.instruction_id == "b1").unwrap();
    assert!(b1.response.is_none());
    assert!(
        b1.error.contains("unknown pbd_id=pbd-42"),
        "got: {}",
        b1.error
    );
}

#[tokio::test]
async fn control_loop_fetches_pbd_on_miss_then_plans() {
    use beam_proto::fn_execution_v1::RemoteGrpcPort;
    use beam_proto::pipeline_v1::{ApiServiceDescriptor, FunctionSpec, PTransform, ParDoPayload};
    use ironbeam_graphx::{urns, v5::V5NodeRef};
    use prost::Message;

    // Build a PBD the fake runner will serve when the harness fetches it.
    let mut pbd = ProcessBundleDescriptor {
        id: "pbd-remote".into(),
        ..Default::default()
    };
    let src_port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: "runner:1".into(),
            ..Default::default()
        }),
        coder_id: "c".into(),
    };
    pbd.transforms.insert(
        "src".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: ironbeam_harness::bundle::URN_RUNNER_SOURCE.to_string(),
                payload: src_port.encode_to_vec(),
            }),
            outputs: std::iter::once(("out".to_string(), "pc0".to_string())).collect(),
            ..Default::default()
        },
    );
    pbd.transforms.insert(
        "p".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: urns::TRANSFORM_PARDO.to_string(),
                payload: ParDoPayload {
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
                }
                .encode_to_vec(),
            }),
            inputs: std::iter::once(("in".to_string(), "pc0".to_string())).collect(),
            outputs: std::iter::once(("out".to_string(), "pc1".to_string())).collect(),
            ..Default::default()
        },
    );

    let scripted = vec![InstructionRequest {
        instruction_id: "bf1".to_string(),
        request: Some(InstrReq::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: "pbd-remote".into(),
            cache_tokens: vec![],
            elements: None,
            has_no_state: false,
            only_bundle_for_keys: false,
        })),
    }];

    let responses = run_scenario(scripted, vec![pbd]).await;
    assert_eq!(responses.len(), 1);

    // Control loop fetched the PBD and the Dispatcher planned it — response
    // error text carries the plan summary (M2c will replace this with a real
    // ProcessBundleResponse).
    let r = &responses[0];
    assert!(r.response.is_none());
    assert!(r.error.contains("not yet wired (M2c)"), "got: {}", r.error);
    assert!(r.error.contains("key=p"), "got: {}", r.error);
    assert!(r.error.contains("Source src"), "got: {}", r.error);
}

#[tokio::test]
async fn endpoints_parse_from_argv() {
    use ironbeam_harness::Endpoints;
    let e = Endpoints::from_argv([
        "--id=w",
        "--control_endpoint=localhost:9",
        "--logging_endpoint=localhost:10",
    ])
    .unwrap();
    assert_eq!(e.id, "w");
}
