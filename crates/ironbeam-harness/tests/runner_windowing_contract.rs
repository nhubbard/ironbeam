//! Runner-supplied windowing descriptors traverse planning and Fn Data execution.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use beam_proto::fn_execution_v1::beam_fn_data_server::{BeamFnData, BeamFnDataServer};
use beam_proto::fn_execution_v1::{
    elements::Data, Elements, ProcessBundleDescriptor, RemoteGrpcPort,
};
use beam_proto::pipeline_v1::{ApiServiceDescriptor, Coder, FunctionSpec, PCollection, PTransform};
use ironbeam_core::Pipeline;
use ironbeam_graphx::{urns, windowing};
use ironbeam_harness::bundle::{plan, Step, WindowMapping, URN_RUNNER_SINK, URN_RUNNER_SOURCE};
use ironbeam_harness::coders::{
    encode_interval_window, encode_interval_windowed_value, encode_length_prefix,
    try_decode_windowed_value,
};
use ironbeam_harness::executor::BundleRunner;
use ironbeam_harness::v5_executor::V5BundleExecutor;
use prost::Message;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

struct WindowRunnerData {
    record: Vec<u8>,
    received: Arc<Mutex<Vec<Data>>>,
}

#[tonic::async_trait]
impl BeamFnData for WindowRunnerData {
    type DataStream = ReceiverStream<Result<Elements, Status>>;

    async fn data(
        &self,
        request: Request<Streaming<Elements>>,
    ) -> Result<Response<Self::DataStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(Elements {
            data: vec![
                Data {
                    instruction_id: "bundle".into(),
                    transform_id: "input".into(),
                    data: self.record.clone(),
                    is_last: false,
                },
                Data {
                    instruction_id: "bundle".into(),
                    transform_id: "input".into(),
                    data: Vec::new(),
                    is_last: true,
                },
            ],
            ..Default::default()
        }))
        .await
        .unwrap();
        let received = Arc::clone(&self.received);
        tokio::spawn(async move {
            let mut stream = request.into_inner();
            while let Ok(Some(elements)) = stream.message().await {
                received.lock().await.extend(elements.data);
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn coder(urn: &str, components: &[&str]) -> Coder {
    Coder {
        spec: Some(FunctionSpec {
            urn: urn.into(),
            payload: Vec::new(),
        }),
        component_coder_ids: components.iter().map(|id| (*id).into()).collect(),
    }
}

fn port_transform(urn: &str, addr: SocketAddr, coder_id: &str, pc: &str) -> PTransform {
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: coder_id.into(),
    };
    PTransform {
        spec: Some(FunctionSpec {
            urn: urn.into(),
            payload: port.encode_to_vec(),
        }),
        inputs: if urn == URN_RUNNER_SINK {
            HashMap::from([("in".into(), pc.into())])
        } else {
            HashMap::new()
        },
        outputs: if urn == URN_RUNNER_SOURCE {
            HashMap::from([("out".into(), pc.into())])
        } else {
            HashMap::new()
        },
        ..Default::default()
    }
}

fn descriptor(addr: SocketAddr, merge: bool) -> ProcessBundleDescriptor {
    let mut pbd = ProcessBundleDescriptor {
        id: if merge { "runner-merge" } else { "runner-map" }.into(),
        ..Default::default()
    };
    for (id, urn, components) in [
        ("key", urns::CODER_BYTES, vec![]),
        ("interval", urns::CODER_INTERVAL_WINDOW, vec![]),
        ("input-iter", urns::CODER_ITERABLE, vec!["interval"]),
        ("unmerged-iter", urns::CODER_ITERABLE, vec!["interval"]),
        ("consumed-iter", urns::CODER_ITERABLE, vec!["interval"]),
        (
            "merged-kv",
            urns::CODER_KV,
            vec!["interval", "consumed-iter"],
        ),
        ("merged-iter", urns::CODER_ITERABLE, vec!["merged-kv"]),
        (
            "output-value",
            urns::CODER_KV,
            vec!["unmerged-iter", "merged-iter"],
        ),
    ] {
        pbd.coders.insert(id.into(), coder(urn, &components));
    }
    pbd.coders.insert(
        "input-kv".into(),
        coder(
            urns::CODER_KV,
            &["key", if merge { "input-iter" } else { "interval" }],
        ),
    );
    pbd.coders.insert(
        "output-kv".into(),
        coder(
            urns::CODER_KV,
            &["key", if merge { "output-value" } else { "interval" }],
        ),
    );
    pbd.coders.insert(
        "input-wv".into(),
        coder(urns::CODER_WINDOWED_VALUE, &["input-kv", "interval"]),
    );
    pbd.coders.insert(
        "output-wv".into(),
        coder(urns::CODER_WINDOWED_VALUE, &["output-kv", "interval"]),
    );
    for (id, coder_id) in [("before", "input-wv"), ("after", "output-wv")] {
        pbd.pcollections.insert(
            id.into(),
            PCollection {
                coder_id: coder_id.into(),
                ..Default::default()
            },
        );
    }
    pbd.transforms.insert(
        "input".into(),
        port_transform(URN_RUNNER_SOURCE, addr, "input-wv", "before"),
    );
    let window_fn = if merge {
        windowing::session_window_fn(5_000_000).unwrap()
    } else {
        let mut spec = windowing::sliding_window_fn(50_000_000, 100_000_000, 0).unwrap();
        spec.urn = urns::WINDOW_MAPPING_SLIDING.into();
        spec
    };
    pbd.transforms.insert(
        "mapping".into(),
        PTransform {
            spec: Some(FunctionSpec {
                urn: if merge {
                    urns::TRANSFORM_MERGE_WINDOWS
                } else {
                    urns::TRANSFORM_MAP_WINDOWS
                }
                .into(),
                payload: window_fn.encode_to_vec(),
            }),
            inputs: HashMap::from([("in".into(), "before".into())]),
            outputs: HashMap::from([("out".into(), "after".into())]),
            ..Default::default()
        },
    );
    pbd.transforms.insert(
        "output".into(),
        port_transform(URN_RUNNER_SINK, addr, "output-wv", "after"),
    );
    pbd
}

async fn execute(merge: bool, body: &[u8]) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let incoming = async_stream::stream! {
        loop {
            yield listener.accept().await.map(|(stream, _)| stream);
        }
    };
    let mut record = Vec::new();
    encode_interval_windowed_value(150, 100, 200, body, &mut record).unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(WindowRunnerData {
                record,
                received: Arc::clone(&received),
            }))
            .serve_with_incoming(incoming),
    );
    let pbd = descriptor(addr, merge);
    let plan = plan(&pbd).unwrap();
    assert_eq!(plan.steps.len(), 3);
    if merge {
        assert!(matches!(plan.steps[1], Step::MergeWindows { .. }));
    } else {
        assert!(matches!(
            plan.steps[1],
            Step::MapWindows {
                mapping: WindowMapping::Sliding {
                    size_millis: 50,
                    period_millis: 100,
                    offset_millis: 0
                },
                ..
            }
        ));
    }
    let runner = V5BundleExecutor::new(&Pipeline::default(), "test-build").unwrap();
    tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
        .await
        .unwrap()
        .unwrap();
    let chunks = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let chunks = received.lock().await.clone();
            if chunks
                .iter()
                .any(|chunk| chunk.transform_id == "output" && chunk.is_last)
            {
                break chunks;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    server.abort();
    assert!(chunks
        .iter()
        .any(|chunk| chunk.transform_id == "output" && chunk.is_last));
    chunks
        .into_iter()
        .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .unwrap()
        .data
}

#[tokio::test]
async fn runner_descriptor_maps_sliding_side_window_through_fn_data() {
    let mut body = Vec::new();
    encode_length_prefix(b"scope-key", &mut body).unwrap();
    let key_len = body.len();
    encode_interval_window(100, 200, &mut body).unwrap();
    let output = execute(false, &body).await;
    let (mapped, header_len) = try_decode_windowed_value(&output, true).unwrap().unwrap();
    let mut expected = body[..key_len].to_vec();
    encode_interval_window(200, 250, &mut expected).unwrap();
    assert_eq!(mapped.element, expected);
    assert_eq!(output.len(), header_len + expected.len());
    assert_eq!(mapped.interval_millis, Some((100, 200)));
    assert_eq!(mapped.timestamp_millis, 150);
}

#[tokio::test]
async fn runner_descriptor_merges_sessions_through_fn_data() {
    let mut body = Vec::new();
    encode_length_prefix(b"merge-nonce", &mut body).unwrap();
    body.extend_from_slice(&3_i32.to_be_bytes());
    for (start, end) in [(9, 14), (0, 5), (4, 9)] {
        encode_interval_window(start, end, &mut body).unwrap();
    }
    let output = execute(true, &body).await;
    let (mapped, header_len) = try_decode_windowed_value(&output, true).unwrap().unwrap();
    let mut expected = Vec::new();
    encode_length_prefix(b"merge-nonce", &mut expected).unwrap();
    expected.extend_from_slice(&1_i32.to_be_bytes());
    encode_interval_window(9, 14, &mut expected).unwrap();
    expected.extend_from_slice(&1_i32.to_be_bytes());
    encode_interval_window(0, 9, &mut expected).unwrap();
    expected.extend_from_slice(&2_i32.to_be_bytes());
    encode_interval_window(0, 5, &mut expected).unwrap();
    encode_interval_window(4, 9, &mut expected).unwrap();
    assert_eq!(mapped.element, expected);
    assert_eq!(output.len(), header_len + expected.len());
    assert_eq!(mapped.interval_millis, Some((100, 200)));
    assert_eq!(mapped.timestamp_millis, 150);
}
