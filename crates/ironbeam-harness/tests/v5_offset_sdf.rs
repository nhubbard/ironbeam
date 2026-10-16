//! Fn Data proof for a registered bounded user offset transform.

use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use beam_proto::fn_execution_v1::beam_fn_data_server::{BeamFnData, BeamFnDataServer};
use beam_proto::fn_execution_v1::{elements::Data, Elements, RemoteGrpcPort};
use beam_proto::pipeline_v1::ApiServiceDescriptor;
use ironbeam_core::{Element, OffsetRange, Pipeline, Version};
use ironbeam_graphx::v5::{collection_id, execution_identity, V5NodeRef};
use ironbeam_harness::bundle::{BundlePlan, SdfStage, SinkWire, SourceWire, Step};
use ironbeam_harness::coders::{
    decode_length_prefix, decode_windowed_value, encode_length_prefix, encode_windowed_value,
};
use ironbeam_harness::executor::BundleRunner;
use ironbeam_harness::sdf_offset_wire::decode_sized;
use ironbeam_harness::v5_executor::V5BundleExecutor;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

struct RunnerData {
    record: Vec<u8>,
    received: Arc<Mutex<Vec<Data>>>,
}

#[tonic::async_trait]
impl BeamFnData for RunnerData {
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

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("input", vec![2_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/offset-sdf",
            Version { major: 1, minor: 0 },
            |_| OffsetRange::new(0, 3),
            |value, context| {
                for offset in 0..3 {
                    assert!(context.try_claim(offset)?);
                    context.emit(value + offset as i32, offset * 1_000_000)?;
                }
                Ok(())
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    pipeline
}

async fn run_bundle(
    pipeline: &Pipeline,
    record: Vec<u8>,
    steps: Vec<Step>,
) -> (
    beam_proto::fn_execution_v1::ProcessBundleResponse,
    Vec<Data>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                record,
                received: Arc::clone(&received),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let steps = steps
        .into_iter()
        .map(|step| match step {
            Step::Source {
                transform_id,
                output_pc,
                wire,
                ..
            } => Step::Source {
                transform_id,
                port: port.clone(),
                output_pc,
                wire,
            },
            Step::Sink {
                transform_id,
                input_pc,
                wire,
                ..
            } => Step::Sink {
                transform_id,
                port: port.clone(),
                input_pc,
                wire,
            },
            other => other,
        })
        .collect();
    let plan = BundlePlan {
        pbd_id: "offset-sdf".into(),
        state_endpoint: None,
        side_inputs: HashMap::new(),
        steps,
    };
    let runner = V5BundleExecutor::new(pipeline, "test-build").unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    server.abort();
    (response, chunks)
}

#[tokio::test]
async fn registered_offset_callbacks_cross_pair_split_and_process_fn_data_boundaries() {
    let pipeline = pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node_ref = V5NodeRef {
        key: "offsets".into(),
        identity: execution_identity(&graph, &registry, "test-build").unwrap(),
        co_group_input: None,
    };
    let mut input_body = Vec::new();
    encode_length_prefix(&2_i32.encode().unwrap(), &mut input_body).unwrap();
    let mut input_record = Vec::new();
    encode_windowed_value(0, &input_body, &mut input_record).unwrap();
    let (_, chunks) = run_bundle(
        &pipeline,
        input_record,
        vec![
            Step::Source {
                transform_id: "input".into(),
                port: RemoteGrpcPort::default(),
                output_pc: collection_id("input"),
                wire: SourceWire::Opaque,
            },
            Step::Sdf {
                stage: SdfStage::PairWithRestriction,
                transform_id: "pair".into(),
                node_ref: node_ref.clone(),
                input_pc: collection_id("input"),
                output_pc: "pair-pc".into(),
            },
            Step::Sdf {
                stage: SdfStage::SplitAndSizeRestrictions,
                transform_id: "split".into(),
                node_ref: node_ref.clone(),
                input_pc: "pair-pc".into(),
                output_pc: "sized-pc".into(),
            },
            Step::Sink {
                transform_id: "output".into(),
                port: RemoteGrpcPort::default(),
                input_pc: "sized-pc".into(),
                wire: SinkWire::Opaque,
            },
        ],
    )
    .await;
    let output = chunks
        .iter()
        .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .unwrap();
    let (windowed, _) = decode_windowed_value(&output.data).unwrap();
    let sized = decode_sized(windowed.element).unwrap();
    assert_eq!(sized.pair.restriction, OffsetRange::new(0, 3).unwrap());
    assert_eq!(sized.size, 3.0);
    assert_eq!(sized.pair.element, 2_i32.encode().unwrap());

    let (_, truncated) = run_bundle(
        &pipeline,
        output.data.clone(),
        vec![
            Step::Source {
                transform_id: "input".into(),
                port: RemoteGrpcPort::default(),
                output_pc: "sized-pc".into(),
                wire: SourceWire::Opaque,
            },
            Step::Sdf {
                stage: SdfStage::TruncateSizedRestrictions,
                transform_id: "truncate".into(),
                node_ref: node_ref.clone(),
                input_pc: "sized-pc".into(),
                output_pc: "truncated-pc".into(),
            },
            Step::Sink {
                transform_id: "output".into(),
                port: RemoteGrpcPort::default(),
                input_pc: "truncated-pc".into(),
                wire: SinkWire::Opaque,
            },
        ],
    )
    .await;
    let truncated_record = truncated
        .iter()
        .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .unwrap()
        .data
        .clone();
    assert_eq!(truncated_record, output.data);

    let (_, chunks) = run_bundle(
        &pipeline,
        truncated_record,
        vec![
            Step::Source {
                transform_id: "input".into(),
                port: RemoteGrpcPort::default(),
                output_pc: "sized-pc".into(),
                wire: SourceWire::Opaque,
            },
            Step::Sdf {
                stage: SdfStage::ProcessSizedElements,
                transform_id: "process".into(),
                node_ref,
                input_pc: "sized-pc".into(),
                output_pc: collection_id("offsets"),
            },
            Step::Sink {
                transform_id: "output".into(),
                port: RemoteGrpcPort::default(),
                input_pc: collection_id("offsets"),
                wire: SinkWire::Opaque,
            },
        ],
    )
    .await;
    let mut values = Vec::new();
    for chunk in chunks
        .iter()
        .filter(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
    {
        let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
        let (body, used) = decode_length_prefix(windowed.element).unwrap();
        assert_eq!(used, windowed.element.len());
        values.push((i32::decode(body).unwrap(), windowed.timestamp_millis));
    }
    assert_eq!(values, [(2, 0), (3, 1), (4, 2)]);
}

#[tokio::test]
async fn user_split_residual_runs_in_a_fresh_worker_bundle() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("input", vec![10_i32])
        .unwrap()
        .splittable_flat_map_named(
            "offsets",
            "tests/offset-residual",
            Version { major: 1, minor: 0 },
            |_| OffsetRange::new(0, 4),
            |value, context| {
                let tracker = context.tracker();
                let original = tracker.restriction()?;
                if original.start == 0 {
                    assert!(tracker.try_split(0.5)?.is_some());
                    context.update_watermark(1_000_000)?;
                }
                let primary = tracker.restriction()?;
                for offset in primary.start..primary.stop {
                    assert!(context.try_claim(offset)?);
                    context.emit(value + offset as i32, offset * 1_000_000)?;
                }
                Ok(())
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node_ref = V5NodeRef {
        key: "offsets".into(),
        identity: execution_identity(&graph, &registry, "test-build").unwrap(),
        co_group_input: None,
    };
    let steps = || {
        vec![
            Step::Source {
                transform_id: "input".into(),
                port: RemoteGrpcPort::default(),
                output_pc: "sized-pc".into(),
                wire: SourceWire::Opaque,
            },
            Step::Sdf {
                stage: SdfStage::ProcessSizedElements,
                transform_id: "process".into(),
                node_ref: node_ref.clone(),
                input_pc: "sized-pc".into(),
                output_pc: collection_id("offsets"),
            },
            Step::Sink {
                transform_id: "output".into(),
                port: RemoteGrpcPort::default(),
                input_pc: collection_id("offsets"),
                wire: SinkWire::Opaque,
            },
        ]
    };
    let body = ironbeam_harness::sdf_offset_wire::encode_sized(
        &ironbeam_harness::sdf_offset_wire::SizedOffsetPair {
            pair: ironbeam_harness::sdf_offset_wire::OffsetPair {
                element: 10_i32.encode().unwrap(),
                restriction: OffsetRange::new(0, 4).unwrap(),
            },
            size: 4.0,
        },
    )
    .unwrap();
    let mut initial = Vec::new();
    encode_windowed_value(0, &body, &mut initial).unwrap();
    let (response, first_chunks) = run_bundle(&pipeline, initial, steps()).await;
    assert_eq!(response.residual_roots.len(), 1);
    let residual = response.residual_roots[0].application.as_ref().unwrap();
    assert_eq!(
        residual.is_bounded,
        beam_proto::pipeline_v1::is_bounded::Enum::Bounded as i32
    );
    assert_eq!(residual.output_watermarks.len(), 1);
    let (windowed, _) = decode_windowed_value(&residual.element).unwrap();
    assert_eq!(
        decode_sized(windowed.element).unwrap().pair.restriction,
        OffsetRange::new(1, 4).unwrap()
    );
    let (_, second_chunks) = run_bundle(&pipeline, residual.element.clone(), steps()).await;
    let mut values = Vec::new();
    for chunk in first_chunks.into_iter().chain(second_chunks) {
        if chunk.transform_id != "output" || chunk.data.is_empty() {
            continue;
        }
        let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
        let (body, _) = decode_length_prefix(windowed.element).unwrap();
        values.push((i32::decode(body).unwrap(), windowed.timestamp_millis));
    }
    values.sort_unstable();
    assert_eq!(values, [(10, 0), (11, 1), (12, 2), (13, 3)]);
}
