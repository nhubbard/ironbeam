//! A runner Data stream drives a separately built v5 graph through the Fn API.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::Command;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use beam_proto::fn_execution_v1::beam_fn_data_server::{BeamFnData, BeamFnDataServer};
use beam_proto::fn_execution_v1::beam_fn_state_server::{BeamFnState, BeamFnStateServer};
use beam_proto::fn_execution_v1::{
    elements::Data, state_key, state_request, state_response, Elements, RemoteGrpcPort,
    StateAppendResponse, StateClearResponse, StateGetResponse, StateRequest, StateResponse,
};
use beam_proto::pipeline_v1::ApiServiceDescriptor;
use ironbeam_core::{
    Element, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress, Timestamped,
    TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, UnboundedSource, Version,
};
use ironbeam_graphx::sdf::SourceRestriction;
use ironbeam_graphx::v5::{collection_id, execution_identity, V5NodeRef};
use ironbeam_harness::bundle::{
    BundlePlan, SdfStage, SinkWire, SourceWire, Step, WindowCoder, WindowMapping,
};
use ironbeam_harness::coders::{
    decode_length_prefix, decode_windowed_value, encode_interval_window,
    encode_interval_windowed_value, encode_length_prefix, encode_varint, encode_windowed_value,
    try_decode_windowed_value,
};
use ironbeam_harness::executor::BundleRunner;
use ironbeam_harness::sdf_wire::decode_sized;
use ironbeam_harness::sdf_wire::{encode_pair, encode_sized, SizedSourcePair, SourcePair};
use ironbeam_harness::v5_executor::V5BundleExecutor;
use prost::Message;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};

#[derive(Clone, Debug, PartialEq, Eq)]
struct UserRow(i32);

impl Element for UserRow {
    const TYPE_ID: &'static str = "tests/v5-fn-user-row";
    const CODER_ID: &'static str = "tests/v5-fn-user-row-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, ironbeam_core::Error> {
        Ok(self.0.to_le_bytes().to_vec())
    }
    fn decode(bytes: &[u8]) -> Result<Self, ironbeam_core::Error> {
        let value: [u8; 4] = bytes
            .try_into()
            .map_err(|_| ironbeam_core::Error::incompatible("bad row"))?;
        Ok(Self(i32::from_le_bytes(value)))
    }
}

struct RunnerData {
    received: Arc<Mutex<Vec<Data>>>,
    impulse_count: usize,
    impulse_payload: Vec<u8>,
}

struct TwoBundleRunnerData {
    received: Arc<Mutex<Vec<Data>>>,
    connections: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl BeamFnData for TwoBundleRunnerData {
    type DataStream = ReceiverStream<Result<Elements, Status>>;

    async fn data(
        &self,
        request: Request<Streaming<Elements>>,
    ) -> Result<Response<Self::DataStream>, Status> {
        self.connections.fetch_add(1, Ordering::SeqCst);
        // Keep connection setup in flight so concurrent bundles exercise the
        // endpoint cache instead of serializing by scheduler luck.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (tx, rx) = mpsc::channel(4);
        let mut body = Vec::new();
        encode_length_prefix(&[], &mut body).unwrap();
        let mut impulse = Vec::new();
        encode_windowed_value(0, &body, &mut impulse).unwrap();
        let mut data = Vec::new();
        for instruction_id in ["bundle-a", "bundle-b"] {
            data.push(Data {
                instruction_id: instruction_id.into(),
                transform_id: "input".into(),
                data: impulse.clone(),
                is_last: false,
            });
            data.push(Data {
                instruction_id: instruction_id.into(),
                transform_id: "input".into(),
                data: Vec::new(),
                is_last: true,
            });
        }
        tx.send(Ok(Elements {
            data,
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

struct GroupedRunnerData {
    record: Vec<u8>,
    received: Arc<Mutex<Vec<Data>>>,
}

#[tonic::async_trait]
impl BeamFnData for GroupedRunnerData {
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

struct GroupedRunnerState {
    suffix: Vec<u8>,
    requests: Arc<Mutex<Vec<StateRequest>>>,
}

type ValueCells = HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>;

struct ValueRunnerState {
    cells: Arc<Mutex<ValueCells>>,
    requests: Arc<Mutex<Vec<StateRequest>>>,
    state_id: &'static str,
}

#[tonic::async_trait]
impl BeamFnState for ValueRunnerState {
    type StateStream = ReceiverStream<Result<StateResponse, Status>>;

    async fn state(
        &self,
        request: Request<Streaming<StateRequest>>,
    ) -> Result<Response<Self::StateStream>, Status> {
        let (tx, rx) = mpsc::channel(8);
        let cells = Arc::clone(&self.cells);
        let requests = Arc::clone(&self.requests);
        let state_id = self.state_id;
        tokio::spawn(async move {
            let mut stream = request.into_inner();
            while let Ok(Some(message)) = stream.message().await {
                requests.lock().await.push(message.clone());
                let Some(state_key::Type::BagUserState(scope)) = message
                    .state_key
                    .as_ref()
                    .and_then(|key| key.r#type.as_ref())
                else {
                    break;
                };
                if scope.user_state_id != state_id || scope.transform_id != "stateful" {
                    break;
                }
                let cell = (scope.window.clone(), scope.key.clone());
                let response = match message.request {
                    Some(state_request::Request::Get(_)) => {
                        state_response::Response::Get(StateGetResponse {
                            data: cells.lock().await.get(&cell).cloned().unwrap_or_default(),
                            continuation_token: Vec::new(),
                        })
                    }
                    Some(state_request::Request::Clear(_)) => {
                        cells.lock().await.remove(&cell);
                        state_response::Response::Clear(StateClearResponse {})
                    }
                    Some(state_request::Request::Append(append)) => {
                        cells
                            .lock()
                            .await
                            .entry(cell)
                            .or_default()
                            .extend(append.data);
                        state_response::Response::Append(StateAppendResponse {})
                    }
                    _ => break,
                };
                if tx
                    .send(Ok(StateResponse {
                        id: message.id,
                        response: Some(response),
                        ..Default::default()
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl BeamFnState for GroupedRunnerState {
    type StateStream = ReceiverStream<Result<StateResponse, Status>>;

    async fn state(
        &self,
        request: Request<Streaming<StateRequest>>,
    ) -> Result<Response<Self::StateStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let suffix = self.suffix.clone();
        let requests = Arc::clone(&self.requests);
        tokio::spawn(async move {
            let mut stream = request.into_inner();
            while let Ok(Some(message)) = stream.message().await {
                requests.lock().await.push(message.clone());
                if tx
                    .send(Ok(StateResponse {
                        id: message.id,
                        response: Some(state_response::Response::Get(StateGetResponse {
                            data: suffix.clone(),
                            continuation_token: Vec::new(),
                        })),
                        ..Default::default()
                    }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl BeamFnData for RunnerData {
    type DataStream = ReceiverStream<Result<Elements, Status>>;

    async fn data(
        &self,
        request: Request<Streaming<Elements>>,
    ) -> Result<Response<Self::DataStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let mut impulse = Vec::new();
        encode_length_prefix(&self.impulse_payload, &mut impulse).unwrap();
        let mut windowed = Vec::new();
        encode_windowed_value(0, &impulse, &mut windowed).unwrap();
        tx.send(Ok(Elements {
            data: vec![
                Data {
                    instruction_id: "bundle".into(),
                    transform_id: "input".into(),
                    data: windowed.repeat(self.impulse_count),
                    is_last: false,
                },
                Data {
                    instruction_id: "bundle".into(),
                    transform_id: "input".into(),
                    data: vec![],
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

fn pipeline(values: Vec<UserRow>) -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", values)
        .unwrap()
        .map_named("double", |row| UserRow(row.0 * 2))
        .unwrap()
        .filter_named("large", |row| row.0 > 2)
        .unwrap()
        .flat_map_named("expand", |row| vec![row.clone(), UserRow(row.0 + 1)])
        .unwrap()
        .output("result")
        .unwrap();
    pipeline
}

fn counted_pipeline(opened: Arc<AtomicUsize>) -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "fixture-source-v1".into(),
            Some(1),
            move || {
                opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(vec![Ok(UserRow(1))].into_iter()))
            },
        )
        .unwrap()
        .map_named("double", |row| UserRow(row.0 * 2))
        .unwrap()
        .filter_named("large", |row| row.0 > 2)
        .unwrap()
        .flat_map_named("expand", |row| vec![row.clone(), UserRow(row.0 + 1)])
        .unwrap()
        .output("result")
        .unwrap();
    pipeline
}

fn sdf_pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "events-source-v1".into(),
            0,
            |_| -> Result<
                ironbeam_core::TypedUnboundedSourceReader<UserRow>,
                ironbeam_core::Error,
            > { panic!("SDF preparation must not open the source reader") },
        )
        .unwrap();
    pipeline
}

struct SdfScript(std::collections::VecDeque<UnboundedPoll<Timestamped<UserRow>>>);

impl UnboundedSource<Timestamped<UserRow>> for SdfScript {
    fn poll(
        &mut self,
        _: usize,
    ) -> Result<UnboundedPoll<Timestamped<UserRow>>, ironbeam_core::Error> {
        Ok(self.0.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn sdf_polling_pipeline(opened: Arc<std::sync::Mutex<Vec<Option<Vec<u8>>>>>) -> Pipeline {
    sdf_polling_pipeline_with_pending(opened, false, 12)
}

fn sdf_polling_pipeline_with_branch(
    opened: Arc<std::sync::Mutex<Vec<Option<Vec<u8>>>>>,
    branch: bool,
) -> Pipeline {
    sdf_polling_pipeline_with_pending(opened, branch, 12)
}

fn sdf_polling_pipeline_with_pending(
    opened: Arc<std::sync::Mutex<Vec<Option<Vec<u8>>>>>,
    branch: bool,
    pending_polls: usize,
) -> Pipeline {
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "events-source-v1".into(),
            0,
            move |position| {
                opened.lock().unwrap().push(position.clone());
                let (value, next, completed) = match position.as_deref() {
                    None => (7, 1, false),
                    Some([1]) => (8, 2, true),
                    _ => return Err(ironbeam_core::Error::incompatible("unexpected SDF cursor")),
                };
                let ready = UnboundedPoll::Ready(UnboundedBatch {
                    elements: vec![Timestamped {
                        value: UserRow(value),
                        timestamp_ns: i64::from(next) * 1_000_000,
                    }],
                    progress: ProgressSignal {
                        source_key: "events".into(),
                        split_id: "split-a".into(),
                        checkpoint_position: Some(vec![next]),
                        event_time_ns: Some(i64::from(next) * 1_000_000),
                        completed,
                    },
                });
                let mut polls: std::collections::VecDeque<_> =
                    std::iter::repeat_with(|| UnboundedPoll::Pending)
                        .take(pending_polls)
                        .collect();
                polls.push_back(ready);
                Ok(Box::new(SdfScript(polls)) as TypedUnboundedSourceReader<UserRow>)
            },
        )
        .unwrap();
    source
        .map_named("double", |row| UserRow(row.0 * 2))
        .unwrap()
        .output("result")
        .unwrap();
    if branch {
        source
            .map_named("increment", |row| UserRow(row.0 + 1))
            .unwrap()
            .output("increment-result")
            .unwrap();
    }
    pipeline
}

async fn run_sdf_process_bundle(
    pipeline: &Pipeline,
    record: Vec<u8>,
    checkpoint: bool,
) -> (
    anyhow::Result<beam_proto::fn_execution_v1::ProcessBundleResponse>,
    Vec<Data>,
) {
    run_sdf_process_bundle_with_branch(pipeline, record, checkpoint, false).await
}

async fn run_sdf_process_bundle_with_branch(
    pipeline: &Pipeline,
    record: Vec<u8>,
    checkpoint: bool,
    branch: bool,
) -> (
    anyhow::Result<beam_proto::fn_execution_v1::ProcessBundleResponse>,
    Vec<Data>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(GroupedRunnerData {
                record,
                received: Arc::clone(&received),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let sized_pc = "runner-sized-events".to_string();
    let events_pc = collection_id("events");
    let result_pc = collection_id("double");
    let mut steps = vec![
        Step::Source {
            transform_id: "input".into(),
            port: port.clone(),
            output_pc: sized_pc.clone(),
            wire: SourceWire::Opaque,
        },
        Step::Sdf {
            stage: SdfStage::ProcessSizedElements,
            transform_id: "process".into(),
            node_ref: V5NodeRef {
                key: "events".into(),
                identity: identity.clone(),
                co_group_input: None,
            },
            input_pc: sized_pc,
            output_pc: events_pc.clone(),
        },
        Step::V5ParDo {
            transform_id: "double".into(),
            node_ref: V5NodeRef {
                key: "double".into(),
                identity: identity.clone(),
                co_group_input: None,
            },
            input_pc: events_pc.clone(),
            output_pc: result_pc.clone(),
            stateful_value: false,
        },
        Step::Sink {
            transform_id: "output".into(),
            port: port.clone(),
            input_pc: result_pc,
            wire: SinkWire::Opaque,
        },
    ];
    if branch {
        let increment_pc = collection_id("increment");
        steps.push(Step::V5ParDo {
            transform_id: "increment".into(),
            node_ref: V5NodeRef {
                key: "increment".into(),
                identity,
                co_group_input: None,
            },
            input_pc: events_pc,
            output_pc: increment_pc.clone(),
            stateful_value: false,
        });
        steps.push(Step::Sink {
            transform_id: "increment-output".into(),
            port,
            input_pc: increment_pc,
            wire: SinkWire::Opaque,
        });
    }
    let plan = BundlePlan {
        pbd_id: "sdf-process".into(),
        state_endpoint: None,
        side_inputs: HashMap::new(),
        steps,
    };
    let runner = Arc::new(V5BundleExecutor::new(pipeline, "test-build").unwrap());
    let response = if checkpoint {
        let running = Arc::clone(&runner);
        let task = tokio::spawn(async move { running.run("bundle", &plan).await });
        tokio::time::sleep(Duration::from_millis(35)).await;
        use beam_proto::fn_execution_v1::process_bundle_split_request::DesiredSplit;
        use beam_proto::fn_execution_v1::ProcessBundleSplitRequest;
        let refused = [
            HashMap::new(),
            HashMap::from([("other".into(), DesiredSplit::default())]),
            HashMap::from([(
                "input".into(),
                DesiredSplit {
                    fraction_of_remainder: 0.5,
                    ..Default::default()
                },
            )]),
            HashMap::from([(
                "input".into(),
                DesiredSplit {
                    allowed_split_points: vec![0],
                    ..Default::default()
                },
            )]),
        ];
        for desired_splits in refused {
            let split = runner
                .split_bundle(&ProcessBundleSplitRequest {
                    instruction_id: "bundle".into(),
                    desired_splits,
                })
                .await
                .unwrap();
            assert!(split.residual_roots.is_empty());
            tokio::time::sleep(Duration::from_millis(12)).await;
            assert!(!task.is_finished(), "unsupported split must not checkpoint");
        }
        let split = runner
            .split_bundle(&ProcessBundleSplitRequest {
                instruction_id: "bundle".into(),
                desired_splits: HashMap::from([(
                    "input".into(),
                    DesiredSplit {
                        estimated_input_elements: 1,
                        ..Default::default()
                    },
                )]),
            })
            .await
            .unwrap();
        assert!(split.residual_roots.is_empty());
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
    } else {
        tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
            .await
            .unwrap()
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    server.abort();
    (response, chunks)
}

#[tokio::test]
async fn fn_data_sdf_process_resumes_with_fresh_executor_and_graph() {
    let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
    let pipeline = sdf_polling_pipeline(Arc::clone(&opened));
    let initial = encode_sized(&SizedSourcePair {
        pair: SourcePair {
            element: Vec::new(),
            restriction: SourceRestriction::initial("events"),
        },
        size: 1.0,
    })
    .unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &initial, &mut record).unwrap();
    let (first, chunks) = run_sdf_process_bundle(&pipeline, record, false).await;
    let first = first.unwrap();
    let output = chunks
        .iter()
        .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .unwrap();
    let (windowed, _) = decode_windowed_value(&output.data).unwrap();
    let (body, _) = decode_length_prefix(windowed.element).unwrap();
    assert_eq!(UserRow::decode(body).unwrap(), UserRow(14));
    assert_eq!(first.residual_roots.len(), 1);
    let residual = first.residual_roots[0].application.as_ref().unwrap();
    assert_eq!(residual.transform_id, "process");
    assert_eq!(residual.input_id, "in");
    assert!(residual.output_watermarks.contains_key("out"));

    // Rebuild the graph and registry as a restarted worker would. The residual
    // must carry all progress needed to resume without the first executor.
    drop(pipeline);
    let restarted_pipeline = sdf_polling_pipeline(Arc::clone(&opened));
    let (second, chunks) =
        run_sdf_process_bundle(&restarted_pipeline, residual.element.clone(), false).await;
    let second = second.unwrap();
    let output = chunks
        .iter()
        .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .unwrap();
    let (windowed, _) = decode_windowed_value(&output.data).unwrap();
    let (body, _) = decode_length_prefix(windowed.element).unwrap();
    assert_eq!(UserRow::decode(body).unwrap(), UserRow(16));
    assert!(second.residual_roots.is_empty());
    assert_eq!(*opened.lock().unwrap(), [None, Some(vec![1])]);
}

#[tokio::test]
async fn fn_data_sdf_process_child_worker() {
    let Ok(input_path) = std::env::var("IRONBEAM_SDF_CHILD_INPUT") else {
        return;
    };
    let stage = std::env::var("IRONBEAM_SDF_CHILD_STAGE").unwrap();
    let branch = std::env::var_os("IRONBEAM_SDF_CHILD_BRANCH").is_some();
    let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
    let pipeline = sdf_polling_pipeline_with_branch(Arc::clone(&opened), branch);
    let pending = stage == "pending";
    let (response, chunks) = run_sdf_process_bundle_with_branch(
        &pipeline,
        std::fs::read(input_path).unwrap(),
        pending,
        branch,
    )
    .await;
    let response = response.unwrap();
    let output = chunks
        .iter()
        .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty());
    let increment_output = chunks
        .iter()
        .find(|chunk| chunk.transform_id == "increment-output" && !chunk.data.is_empty());
    match stage.as_str() {
        "pending" => {
            assert!(output.is_none());
            assert!(increment_output.is_none());
            assert_eq!(*opened.lock().unwrap(), [None]);
            assert_eq!(response.residual_roots.len(), 1);
            let residual = response.residual_roots[0].application.as_ref().unwrap();
            let (windowed, _) = decode_windowed_value(&residual.element).unwrap();
            assert!(decode_sized("events", windowed.element)
                .unwrap()
                .pair
                .restriction
                .progress
                .is_none());
        }
        "first" => {
            let output = output.unwrap();
            let (windowed, _) = decode_windowed_value(&output.data).unwrap();
            let (body, _) = decode_length_prefix(windowed.element).unwrap();
            assert_eq!(UserRow::decode(body).unwrap(), UserRow(14));
            if branch {
                assert!(increment_output.is_some());
            }
            assert_eq!(*opened.lock().unwrap(), [None]);
        }
        "second" => {
            let output = output.unwrap();
            let (windowed, _) = decode_windowed_value(&output.data).unwrap();
            let (body, _) = decode_length_prefix(windowed.element).unwrap();
            assert_eq!(UserRow::decode(body).unwrap(), UserRow(16));
            if branch {
                assert!(increment_output.is_some());
            }
            assert_eq!(*opened.lock().unwrap(), [Some(vec![1])]);
        }
        _ => panic!("unexpected child stage"),
    }
    std::fs::write(
        std::env::var("IRONBEAM_SDF_CHILD_RESPONSE").unwrap(),
        response.encode_to_vec(),
    )
    .unwrap();
    if let Some(output) = output {
        let (windowed, _) = decode_windowed_value(&output.data).unwrap();
        let (body, _) = decode_length_prefix(windowed.element).unwrap();
        let row = UserRow::decode(body).unwrap();
        std::fs::write(
            std::env::var("IRONBEAM_SDF_CHILD_RESULT").unwrap(),
            row.0.to_le_bytes(),
        )
        .unwrap();
    }
    if let Some(output) = increment_output {
        let (windowed, _) = decode_windowed_value(&output.data).unwrap();
        let (body, _) = decode_length_prefix(windowed.element).unwrap();
        let row = UserRow::decode(body).unwrap();
        std::fs::write(
            std::env::var("IRONBEAM_SDF_CHILD_BRANCH_RESULT").unwrap(),
            row.0.to_le_bytes(),
        )
        .unwrap();
    }
    std::fs::write(
        std::env::var("IRONBEAM_SDF_CHILD_PID").unwrap(),
        std::process::id().to_string(),
    )
    .unwrap();
}

#[test]
fn fn_data_sdf_split_and_fanout_survive_worker_process_restart() {
    fn child(
        directory: &Path,
        stage: &str,
        input: &[u8],
        branch: bool,
    ) -> (
        beam_proto::fn_execution_v1::ProcessBundleResponse,
        Option<i32>,
        Option<i32>,
        String,
    ) {
        let prefix = format!("{stage}-{branch}");
        let input_path = directory.join(format!("{prefix}-input"));
        let response_path = directory.join(format!("{prefix}-response"));
        let result_path = directory.join(format!("{prefix}-result"));
        let branch_result_path = directory.join(format!("{prefix}-branch-result"));
        let pid_path = directory.join(format!("{prefix}-pid"));
        std::fs::write(&input_path, input).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "fn_data_sdf_process_child_worker", "--nocapture"])
            .env("IRONBEAM_SDF_CHILD_STAGE", stage)
            .env("IRONBEAM_SDF_CHILD_INPUT", &input_path)
            .env("IRONBEAM_SDF_CHILD_RESPONSE", &response_path)
            .env("IRONBEAM_SDF_CHILD_RESULT", &result_path)
            .env("IRONBEAM_SDF_CHILD_BRANCH_RESULT", &branch_result_path)
            .env("IRONBEAM_SDF_CHILD_PID", &pid_path);
        if branch {
            command.env("IRONBEAM_SDF_CHILD_BRANCH", "1");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "child {stage} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let response = beam_proto::fn_execution_v1::ProcessBundleResponse::decode(
            std::fs::read(response_path).unwrap().as_slice(),
        )
        .unwrap();
        let row = result_path
            .exists()
            .then(|| i32::from_le_bytes(std::fs::read(result_path).unwrap().try_into().unwrap()));
        let branch_row = branch_result_path.exists().then(|| {
            i32::from_le_bytes(
                std::fs::read(branch_result_path)
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )
        });
        let pid = std::fs::read_to_string(pid_path).unwrap();
        (response, row, branch_row, pid)
    }

    let directory = tempfile::tempdir().unwrap();
    let initial = encode_sized(&SizedSourcePair {
        pair: SourcePair {
            element: Vec::new(),
            restriction: SourceRestriction::initial("events"),
        },
        size: 1.0,
    })
    .unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &initial, &mut record).unwrap();
    for branch in [false, true] {
        let (pending, pending_row, pending_branch_row, pending_pid) =
            child(directory.path(), "pending", &record, branch);
        assert!(pending_row.is_none());
        assert!(pending_branch_row.is_none());
        assert_eq!(pending.residual_roots.len(), 1);
        let pending_residual = pending.residual_roots[0].application.as_ref().unwrap();
        let (first, first_row, first_branch_row, first_pid) =
            child(directory.path(), "first", &pending_residual.element, branch);
        assert_eq!(first_row, Some(14));
        assert_eq!(first_branch_row, branch.then_some(8));
        assert_eq!(first.residual_roots.len(), 1);
        let residual = first.residual_roots[0].application.as_ref().unwrap();
        assert!(residual.output_watermarks.contains_key("out"));
        let (second, second_row, second_branch_row, second_pid) =
            child(directory.path(), "second", &residual.element, branch);
        assert_eq!(second_row, Some(16));
        assert_eq!(second_branch_row, branch.then_some(9));
        assert!(second.residual_roots.is_empty());
        assert_ne!(
            pending_pid, first_pid,
            "split and first batch ran in the same OS process"
        );
        assert_ne!(
            first_pid, second_pid,
            "both bundles ran in the same OS process"
        );
    }
}

#[tokio::test]
async fn fn_data_sdf_source_fans_out_to_two_sinks_across_residual_bundles() {
    let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
    let initial = encode_sized(&SizedSourcePair {
        pair: SourcePair {
            element: Vec::new(),
            restriction: SourceRestriction::initial("events"),
        },
        size: 1.0,
    })
    .unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &initial, &mut record).unwrap();
    let value_at = |chunks: &[Data], transform: &str| {
        let output = chunks
            .iter()
            .find(|chunk| chunk.transform_id == transform && !chunk.data.is_empty())
            .unwrap();
        let (windowed, _) = decode_windowed_value(&output.data).unwrap();
        let (body, _) = decode_length_prefix(windowed.element).unwrap();
        UserRow::decode(body).unwrap()
    };

    let first_graph = sdf_polling_pipeline_with_branch(Arc::clone(&opened), true);
    let (first, chunks) =
        run_sdf_process_bundle_with_branch(&first_graph, record, false, true).await;
    let first = first.unwrap();
    assert_eq!(value_at(&chunks, "output"), UserRow(14));
    assert_eq!(value_at(&chunks, "increment-output"), UserRow(8));
    assert_eq!(first.residual_roots.len(), 1);

    let residual = first.residual_roots[0]
        .application
        .as_ref()
        .unwrap()
        .element
        .clone();
    drop(first_graph);
    let second_graph = sdf_polling_pipeline_with_branch(Arc::clone(&opened), true);
    let (second, chunks) =
        run_sdf_process_bundle_with_branch(&second_graph, residual, false, true).await;
    assert!(second.unwrap().residual_roots.is_empty());
    assert_eq!(value_at(&chunks, "output"), UserRow(16));
    assert_eq!(value_at(&chunks, "increment-output"), UserRow(9));
    assert_eq!(*opened.lock().unwrap(), [None, Some(vec![1])]);
}

#[tokio::test]
async fn fn_data_sdf_rejects_wrong_restriction_before_opening_reader() {
    let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
    let pipeline = sdf_polling_pipeline(Arc::clone(&opened));
    let wrong = encode_sized(&SizedSourcePair {
        pair: SourcePair {
            element: Vec::new(),
            restriction: SourceRestriction::initial("other"),
        },
        size: 1.0,
    })
    .unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &wrong, &mut record).unwrap();
    let (result, chunks) = run_sdf_process_bundle(&pipeline, record, false).await;
    let error = result.unwrap_err();
    assert!(error.to_string().contains("restriction"), "{error:#}");
    assert!(opened.lock().unwrap().is_empty());
    assert!(chunks.iter().all(|chunk| chunk.data.is_empty()));
}

#[tokio::test]
async fn fn_data_sdf_split_request_checkpoints_a_pending_reader() {
    let opened = Arc::new(std::sync::Mutex::new(Vec::new()));
    let pipeline = sdf_polling_pipeline_with_pending(Arc::clone(&opened), false, 100);
    let initial = encode_sized(&SizedSourcePair {
        pair: SourcePair {
            element: Vec::new(),
            restriction: SourceRestriction::initial("events"),
        },
        size: 1.0,
    })
    .unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &initial, &mut record).unwrap();
    let (response, chunks) = run_sdf_process_bundle(&pipeline, record, true).await;
    let response = response.unwrap();
    assert_eq!(response.residual_roots.len(), 1);
    let root = &response.residual_roots[0];
    assert!(root.requested_time_delay.is_some());
    let (windowed, _) = decode_windowed_value(&root.application.as_ref().unwrap().element).unwrap();
    let sized = decode_sized("events", windowed.element).unwrap();
    assert!(sized.pair.restriction.progress.is_none());
    assert_eq!(*opened.lock().unwrap(), [None]);
    assert!(chunks.iter().all(|chunk| chunk.data.is_empty()));
}

#[tokio::test]
async fn fn_data_prepares_one_source_restriction_without_opening_reader() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received: Arc::clone(&received),
                impulse_count: 1,
                impulse_payload: Vec::new(),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let pipeline = sdf_pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let pair_pc = "runner-pair-events".to_string();
    let sized_pc = "runner-sized-events".to_string();
    let node_ref = V5NodeRef {
        key: "events".into(),
        identity,
        co_group_input: None,
    };
    let plan = BundlePlan {
        pbd_id: "sdf-preparation".into(),
        state_endpoint: None,
        side_inputs: HashMap::new(),
        steps: vec![
            Step::Source {
                transform_id: "input".into(),
                port: port.clone(),
                output_pc: "v5_impulse_pc".into(),
                wire: SourceWire::Opaque,
            },
            Step::Sdf {
                stage: SdfStage::PairWithRestriction,
                transform_id: "pair".into(),
                node_ref: node_ref.clone(),
                input_pc: "v5_impulse_pc".into(),
                output_pc: pair_pc.clone(),
            },
            Step::Sdf {
                stage: SdfStage::SplitAndSizeRestrictions,
                transform_id: "split".into(),
                node_ref,
                input_pc: pair_pc,
                output_pc: sized_pc.clone(),
            },
            Step::Sink {
                transform_id: "output".into(),
                port,
                input_pc: sized_pc,
                wire: SinkWire::Opaque,
            },
        ],
    };
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    server.abort();
    let output = chunks
        .iter()
        .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .expect("sized SDF output");
    let (windowed, _) = decode_windowed_value(&output.data).unwrap();
    let sized = decode_sized("events", windowed.element).unwrap();
    assert_eq!(sized.pair.element, Vec::<u8>::new());
    assert_eq!(sized.size, 1.0);
    assert!(sized.pair.restriction.progress.is_none());
    assert!(chunks
        .iter()
        .any(|chunk| chunk.transform_id == "output" && chunk.is_last));
}

#[tokio::test]
async fn fn_data_splits_a_pair_received_across_runner_boundary() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let pair = encode_pair(&SourcePair {
        element: Vec::new(),
        restriction: SourceRestriction::initial("events"),
    })
    .unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &pair, &mut record).unwrap();
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(GroupedRunnerData {
                record,
                received: Arc::clone(&received),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let pipeline = sdf_pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let pair_pc = "runner-pair-events".to_string();
    let sized_pc = "runner-sized-events".to_string();
    let plan = BundlePlan {
        pbd_id: "sdf-split".into(),
        state_endpoint: None,
        side_inputs: HashMap::new(),
        steps: vec![
            Step::Source {
                transform_id: "input".into(),
                port: port.clone(),
                output_pc: pair_pc.clone(),
                wire: SourceWire::Opaque,
            },
            Step::Sdf {
                stage: SdfStage::SplitAndSizeRestrictions,
                transform_id: "split".into(),
                node_ref: V5NodeRef {
                    key: "events".into(),
                    identity,
                    co_group_input: None,
                },
                input_pc: pair_pc,
                output_pc: sized_pc.clone(),
            },
            Step::Sink {
                transform_id: "output".into(),
                port,
                input_pc: sized_pc,
                wire: SinkWire::Opaque,
            },
        ],
    };
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    server.abort();
    let output = chunks
        .iter()
        .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .expect("sized SDF output");
    let (windowed, _) = decode_windowed_value(&output.data).unwrap();
    assert_eq!(decode_sized("events", windowed.element).unwrap().size, 1.0);
}

#[tokio::test]
async fn fn_data_drain_discards_unbounded_sized_restriction() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let sized = encode_sized(&SizedSourcePair {
        pair: SourcePair {
            element: Vec::new(),
            restriction: SourceRestriction::initial("events"),
        },
        size: 1.0,
    })
    .unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &sized, &mut record).unwrap();
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(GroupedRunnerData {
                record,
                received: Arc::clone(&received),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let pipeline = sdf_pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let plan = BundlePlan {
        pbd_id: "sdf-drain".into(),
        state_endpoint: None,
        side_inputs: HashMap::new(),
        steps: vec![
            Step::Source {
                transform_id: "input".into(),
                port: port.clone(),
                output_pc: "sized".into(),
                wire: SourceWire::Opaque,
            },
            Step::Sdf {
                stage: SdfStage::TruncateSizedRestrictions,
                transform_id: "truncate".into(),
                node_ref: V5NodeRef {
                    key: "events".into(),
                    identity,
                    co_group_input: None,
                },
                input_pc: "sized".into(),
                output_pc: "drained".into(),
            },
            Step::Sink {
                transform_id: "output".into(),
                port,
                input_pc: "drained".into(),
                wire: SinkWire::Opaque,
            },
        ],
    };
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    server.abort();
    assert!(chunks
        .iter()
        .any(|chunk| chunk.transform_id == "output" && chunk.is_last));
    assert!(!chunks
        .iter()
        .any(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty()));
}

#[tokio::test]
async fn fn_data_maps_interval_side_windows_to_global_identity_fixed_and_sliding_values() {
    for mapping in [
        WindowMapping::Global,
        WindowMapping::Identity,
        WindowMapping::Fixed {
            size_millis: 150,
            offset_millis: 0,
        },
        WindowMapping::Sliding {
            size_millis: 250,
            period_millis: 100,
            offset_millis: 0,
        },
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let received = Arc::new(Mutex::new(Vec::new()));
        let mut body = Vec::new();
        encode_length_prefix(b"scope-key", &mut body).unwrap();
        let key_len = body.len();
        encode_interval_window(100, 200, &mut body).unwrap();
        let mut record = Vec::new();
        encode_interval_windowed_value(150, 100, 200, &body, &mut record).unwrap();
        let server = tokio::spawn(
            Server::builder()
                .add_service(BeamFnDataServer::new(GroupedRunnerData {
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
        let plan = BundlePlan {
            pbd_id: "map-windows".into(),
            state_endpoint: None,
            side_inputs: HashMap::new(),
            steps: vec![
                Step::Source {
                    transform_id: "input".into(),
                    port: port.clone(),
                    output_pc: "before".into(),
                    wire: SourceWire::Opaque,
                },
                Step::MapWindows {
                    transform_id: "map".into(),
                    input_pc: "before".into(),
                    output_pc: "after".into(),
                    mapping,
                    outer_window: WindowCoder::Interval,
                    input_window: WindowCoder::Interval,
                },
                Step::Sink {
                    transform_id: "output".into(),
                    port,
                    input_pc: "after".into(),
                    wire: SinkWire::Opaque,
                },
            ],
        };
        let pipeline = Pipeline::default();
        let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
        tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let chunks = received.lock().await.clone();
        server.abort();
        let output = chunks
            .iter()
            .find(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
            .expect("mapped window output");
        let (mapped, header_len) = try_decode_windowed_value(&output.data, true)
            .unwrap()
            .expect("complete interval header");
        let mut expected = body[..key_len].to_vec();
        match mapping {
            WindowMapping::Global => {}
            WindowMapping::Identity => expected.extend_from_slice(&body[key_len..]),
            WindowMapping::Fixed { .. } => encode_interval_window(150, 300, &mut expected).unwrap(),
            WindowMapping::Sliding { .. } => encode_interval_window(0, 250, &mut expected).unwrap(),
        }
        assert_eq!(&mapped.element[..expected.len()], expected);
        assert_eq!(output.data.len(), header_len + expected.len());
        assert_eq!(mapped.interval_millis, Some((100, 200)));
        assert_eq!(mapped.timestamp_millis, 150);
        assert!(chunks
            .iter()
            .any(|chunk| chunk.transform_id == "output" && chunk.is_last));
    }
}

fn plan(pipeline: &Pipeline, endpoint: SocketAddr) -> BundlePlan {
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{endpoint}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let mut steps = vec![Step::Source {
        transform_id: "input".into(),
        port: port.clone(),
        output_pc: "impulse".into(),
        wire: SourceWire::Opaque,
    }];
    let mut previous_pc = "impulse".to_string();
    for key in ["source", "double", "large", "expand"] {
        let output_pc = collection_id(key);
        steps.push(Step::V5ParDo {
            transform_id: key.into(),
            node_ref: V5NodeRef {
                key: key.into(),
                identity: identity.clone(),
                co_group_input: None,
            },
            input_pc: previous_pc,
            output_pc: output_pc.clone(),
            stateful_value: false,
        });
        previous_pc = output_pc;
    }
    steps.push(Step::Sink {
        transform_id: "output".into(),
        port,
        input_pc: previous_pc,
        wire: SinkWire::Opaque,
    });
    BundlePlan {
        pbd_id: "v5-test".into(),
        state_endpoint: None,
        side_inputs: HashMap::new(),
        steps,
    }
}

#[tokio::test]
async fn fn_data_stream_round_trips_custom_rows_and_flat_map() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received: Arc::clone(&received),
                impulse_count: 1,
                impulse_payload: Vec::new(),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let driver = pipeline(vec![UserRow(1), UserRow(2), UserRow(3)]);
    let worker = pipeline(vec![UserRow(1), UserRow(2), UserRow(3)]);
    let runner = V5BundleExecutor::new(&worker, "test-build").unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        runner.run("bundle", &plan(&driver, addr)),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    server.abort();
    assert!(chunks
        .iter()
        .any(|chunk| chunk.transform_id == "output" && chunk.is_last));
    let actual = chunks
        .iter()
        .filter(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .map(|chunk| {
            let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
            let (body, _) = decode_length_prefix(windowed.element).unwrap();
            UserRow::decode(body).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, [UserRow(4), UserRow(5), UserRow(6), UserRow(7)]);
}

#[tokio::test]
async fn stateful_value_bundle_reads_prior_writes_by_key() {
    fn build() -> Pipeline {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("pairs", vec![(1_i32, 2_i32), (1, 3), (2, 4), (1, 5)])
            .unwrap()
            .stateful_value_map_named::<i32, i32>(
                "running",
                "example/running-total",
                Version { major: 1, minor: 0 },
                |_, value, prior| {
                    let total = prior.unwrap_or(0) + value;
                    Ok((Some(total), total))
                },
            )
            .unwrap()
            .output("result")
            .unwrap();
        pipeline
    }
    let driver = build();
    let worker = build();
    let (graph, registry) = driver.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let cells = Arc::new(Mutex::new(HashMap::new()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received: Arc::clone(&received),
                impulse_count: 1,
                impulse_payload: Vec::new(),
            }))
            .add_service(BeamFnStateServer::new(ValueRunnerState {
                cells: Arc::clone(&cells),
                requests: Arc::clone(&requests),
                state_id: "value",
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
    let plan = BundlePlan {
        pbd_id: "stateful-value".into(),
        state_endpoint: Some(format!("http://{addr}")),
        side_inputs: HashMap::new(),
        steps: vec![
            Step::Source {
                transform_id: "input".into(),
                port: port.clone(),
                output_pc: "impulse".into(),
                wire: SourceWire::Opaque,
            },
            Step::V5ParDo {
                transform_id: "source".into(),
                node_ref: V5NodeRef {
                    key: "pairs".into(),
                    identity: identity.clone(),
                    co_group_input: None,
                },
                input_pc: "impulse".into(),
                output_pc: collection_id("pairs"),
                stateful_value: false,
            },
            Step::V5ParDo {
                transform_id: "stateful".into(),
                node_ref: V5NodeRef {
                    key: "running".into(),
                    identity,
                    co_group_input: None,
                },
                input_pc: collection_id("pairs"),
                output_pc: collection_id("running"),
                stateful_value: true,
            },
            Step::Sink {
                transform_id: "output".into(),
                port,
                input_pc: collection_id("running"),
                wire: SinkWire::Opaque,
            },
        ],
    };
    let runner = V5BundleExecutor::new(&worker, "test-build").unwrap();
    tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    let actual = chunks
        .iter()
        .filter(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .map(|chunk| {
            let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
            let (body, _) = decode_length_prefix(windowed.element).unwrap();
            <(i32, i32)>::decode(body).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, [(1, 2), (1, 5), (2, 4), (1, 10)]);
    let requests = requests.lock().await.clone();
    assert_eq!(requests.len(), 12);
    for triplet in requests.as_chunks::<3>().0 {
        assert!(matches!(
            triplet[0].request,
            Some(state_request::Request::Get(_))
        ));
        assert!(matches!(
            triplet[1].request,
            Some(state_request::Request::Clear(_))
        ));
        assert!(matches!(
            triplet[2].request,
            Some(state_request::Request::Append(_))
        ));
        assert_eq!(triplet[0].state_key, triplet[1].state_key);
        assert_eq!(triplet[1].state_key, triplet[2].state_key);
    }
    let cells = cells.lock().await;
    assert_eq!(cells.len(), 2);
    for (key, expected) in [(1_i32, 10_i32), (2, 4)] {
        let mut encoded_key = Vec::new();
        encode_length_prefix(&Element::encode(&key).unwrap(), &mut encoded_key).unwrap();
        let encoded = &cells[&(Vec::new(), encoded_key)];
        let (body, used) = decode_length_prefix(encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(<i32 as Element>::decode(body).unwrap(), expected);
    }
    server.abort();
}

#[tokio::test]
async fn combining_bundle_merges_two_runner_accumulators_before_callback() {
    fn build() -> Pipeline {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("pairs", vec![(1_i32, 5_i32)])
            .unwrap()
            .stateful_combining_map_named::<i32, i32>(
                "running",
                "example/running-combining",
                Version { major: 1, minor: 0 },
                ironbeam_core::CombiningFunction {
                    semantic_id: "example/sum",
                    version: Version { major: 1, minor: 0 },
                },
                |left, right| Ok(left + right),
                |_, value, prior| {
                    Ok((
                        ironbeam_core::CombiningStateMutation::Add(value),
                        prior.copied().unwrap_or_default(),
                    ))
                },
            )
            .unwrap()
            .output("result")
            .unwrap();
        pipeline
    }
    let driver = build();
    let worker = build();
    let (graph, registry) = driver.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut encoded_key = Vec::new();
    encode_length_prefix(&Element::encode(&1_i32).unwrap(), &mut encoded_key).unwrap();
    let mut bag = Vec::new();
    for accumulator in [10_i32, 20] {
        encode_length_prefix(&Element::encode(&accumulator).unwrap(), &mut bag).unwrap();
    }
    let cells = Arc::new(Mutex::new(HashMap::from([(
        (Vec::new(), encoded_key.clone()),
        bag,
    )])));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received: Arc::clone(&received),
                impulse_count: 1,
                impulse_payload: Vec::new(),
            }))
            .add_service(BeamFnStateServer::new(ValueRunnerState {
                cells: Arc::clone(&cells),
                requests: Arc::clone(&requests),
                state_id: "combining",
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
    let plan = BundlePlan {
        pbd_id: "combining-state".into(),
        state_endpoint: Some(format!("http://{addr}")),
        side_inputs: HashMap::new(),
        steps: vec![
            Step::Source {
                transform_id: "input".into(),
                port: port.clone(),
                output_pc: "impulse".into(),
                wire: SourceWire::Opaque,
            },
            Step::V5ParDo {
                transform_id: "source".into(),
                node_ref: V5NodeRef {
                    key: "pairs".into(),
                    identity: identity.clone(),
                    co_group_input: None,
                },
                input_pc: "impulse".into(),
                output_pc: collection_id("pairs"),
                stateful_value: false,
            },
            Step::V5ParDo {
                transform_id: "stateful".into(),
                node_ref: V5NodeRef {
                    key: "running".into(),
                    identity,
                    co_group_input: None,
                },
                input_pc: collection_id("pairs"),
                output_pc: collection_id("running"),
                stateful_value: true,
            },
            Step::Sink {
                transform_id: "output".into(),
                port,
                input_pc: collection_id("running"),
                wire: SinkWire::Opaque,
            },
        ],
    };
    V5BundleExecutor::new(&worker, "test-build")
        .unwrap()
        .run("bundle", &plan)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    let actual = chunks
        .iter()
        .filter(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .map(|chunk| {
            let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
            let (body, _) = decode_length_prefix(windowed.element).unwrap();
            <(i32, i32)>::decode(body).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, [(1, 30)]);
    let requests = requests.lock().await.clone();
    assert_eq!(requests.len(), 3);
    assert!(matches!(
        requests[0].request,
        Some(state_request::Request::Get(_))
    ));
    assert!(matches!(
        requests[1].request,
        Some(state_request::Request::Clear(_))
    ));
    assert!(matches!(
        requests[2].request,
        Some(state_request::Request::Append(_))
    ));
    let cells = cells.lock().await;
    let encoded = &cells[&(Vec::new(), encoded_key)];
    let (body, used) = decode_length_prefix(encoded).unwrap();
    assert_eq!(used, encoded.len());
    assert_eq!(<i32 as Element>::decode(body).unwrap(), 35);
    server.abort();
}

#[tokio::test]
async fn stateful_value_bundle_decodes_runner_kv_boundary() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(7_i32, 4_i32), (7, 5)])
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "running",
            "example/running-total",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let total = prior.unwrap_or(0) + value;
                Ok((Some(total), total))
            },
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let mut record = Vec::new();
    for pair in [(7_i32, 4_i32), (7, 5)] {
        let (key, value) = registry
            .split_group_input("running", &Element::encode(&pair).unwrap())
            .unwrap();
        let mut body = Vec::new();
        encode_length_prefix(&key, &mut body).unwrap();
        encode_length_prefix(&value, &mut body).unwrap();
        encode_windowed_value(0, &body, &mut record).unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(GroupedRunnerData {
                record,
                received: Arc::clone(&received),
            }))
            .add_service(BeamFnStateServer::new(ValueRunnerState {
                cells: Arc::new(Mutex::new(HashMap::new())),
                requests: Arc::clone(&requests),
                state_id: "value",
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "kv".into(),
    };
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    let plan = BundlePlan {
        pbd_id: "stateful-kv-boundary".into(),
        state_endpoint: Some(format!("http://{addr}")),
        side_inputs: HashMap::new(),
        steps: vec![
            Step::Source {
                transform_id: "input".into(),
                port: port.clone(),
                output_pc: collection_id("pairs"),
                wire: SourceWire::KvLp,
            },
            Step::V5ParDo {
                transform_id: "stateful".into(),
                node_ref: V5NodeRef {
                    key: "running".into(),
                    identity,
                    co_group_input: None,
                },
                input_pc: collection_id("pairs"),
                output_pc: collection_id("running"),
                stateful_value: true,
            },
            Step::Sink {
                transform_id: "output".into(),
                port,
                input_pc: collection_id("running"),
                wire: SinkWire::Opaque,
            },
        ],
    };
    tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &plan))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let outputs = received.lock().await.clone();
    let actual = outputs
        .iter()
        .filter(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .map(|chunk| {
            let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
            let (body, _) = decode_length_prefix(windowed.element).unwrap();
            <(i32, i32)>::decode(body).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, [(7, 4), (7, 9)]);
    assert_eq!(requests.lock().await.len(), 6);
    server.abort();
}

#[tokio::test]
async fn concurrent_bundles_share_one_fn_data_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let connections = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(TwoBundleRunnerData {
                received: Arc::clone(&received),
                connections: Arc::clone(&connections),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let driver = pipeline(vec![UserRow(1), UserRow(2), UserRow(3)]);
    let worker = pipeline(vec![UserRow(1), UserRow(2), UserRow(3)]);
    let runner = V5BundleExecutor::new(&worker, "test-build").unwrap();
    let bundle = plan(&driver, addr);
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::try_join!(
            runner.run("bundle-a", &bundle),
            runner.run("bundle-b", &bundle)
        )
    })
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    server.abort();
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    for instruction_id in ["bundle-a", "bundle-b"] {
        let actual = chunks
            .iter()
            .filter(|chunk| {
                chunk.instruction_id == instruction_id
                    && chunk.transform_id == "output"
                    && !chunk.data.is_empty()
            })
            .map(|chunk| {
                let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
                let (body, _) = decode_length_prefix(windowed.element).unwrap();
                UserRow::decode(body).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, [UserRow(4), UserRow(5), UserRow(6), UserRow(7)]);
        assert!(chunks
            .iter()
            .any(|chunk| chunk.instruction_id == instruction_id
                && chunk.transform_id == "output"
                && chunk.is_last));
    }
}

#[tokio::test]
async fn one_impulse_drives_two_independent_user_sources() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let incoming = async_stream::stream! {
        loop {
            yield listener.accept().await.map(|(stream, _)| stream);
        }
    };
    let received = Arc::new(Mutex::new(Vec::new()));
    let server_received = Arc::clone(&received);
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received: server_received,
                impulse_count: 1,
                impulse_payload: Vec::new(),
            }))
            .serve_with_incoming(incoming)
            .await
    });

    let build = || {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source-left", vec![UserRow(1)])
            .unwrap()
            .map_named("left", |row| UserRow(row.0 * 2))
            .unwrap()
            .output("left-out")
            .unwrap();
        pipeline
            .from_vec("source-right", vec![UserRow(3)])
            .unwrap()
            .map_named("right", |row| UserRow(row.0 + 10))
            .unwrap()
            .output("right-out")
            .unwrap();
        pipeline
    };
    let driver = build();
    let worker = build();
    let (graph, registry) = driver.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let mut steps = vec![Step::Source {
        transform_id: "input".into(),
        port: port.clone(),
        output_pc: "impulse".into(),
        wire: SourceWire::Opaque,
    }];
    for (source, mapped) in [("source-left", "left"), ("source-right", "right")] {
        steps.push(Step::V5ParDo {
            transform_id: source.into(),
            node_ref: V5NodeRef {
                key: source.into(),
                identity: identity.clone(),
                co_group_input: None,
            },
            input_pc: "impulse".into(),
            output_pc: collection_id(source),
            stateful_value: false,
        });
        steps.push(Step::V5ParDo {
            transform_id: mapped.into(),
            node_ref: V5NodeRef {
                key: mapped.into(),
                identity: identity.clone(),
                co_group_input: None,
            },
            input_pc: collection_id(source),
            output_pc: collection_id(mapped),
            stateful_value: false,
        });
    }
    for (mapped, sink) in [("left", "output-left"), ("right", "output-right")] {
        steps.push(Step::Sink {
            transform_id: sink.into(),
            port: port.clone(),
            input_pc: collection_id(mapped),
            wire: SinkWire::Opaque,
        });
    }
    let bundle = BundlePlan {
        pbd_id: "two-sources".into(),
        state_endpoint: None,
        side_inputs: HashMap::new(),
        steps,
    };
    let runner = V5BundleExecutor::new(&worker, "test-build").unwrap();
    tokio::time::timeout(Duration::from_secs(5), runner.run("bundle", &bundle))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let chunks = received.lock().await.clone();
    server.abort();
    for (sink, expected) in [("output-left", UserRow(2)), ("output-right", UserRow(13))] {
        let actual = chunks
            .iter()
            .filter(|chunk| chunk.transform_id == sink && !chunk.data.is_empty())
            .map(|chunk| {
                let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
                let (body, _) = decode_length_prefix(windowed.element).unwrap();
                UserRow::decode(body).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, [expected]);
    }
}

#[tokio::test]
async fn worker_rejects_mismatched_graph_before_dialing_runner_data() {
    let driver = pipeline(vec![UserRow(1)]);
    let worker = pipeline(vec![UserRow(2)]);
    let runner = V5BundleExecutor::new(&worker, "test-build").unwrap();
    let unreachable = "127.0.0.1:1".parse().unwrap();
    let error = runner
        .run("bundle", &plan(&driver, unreachable))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("identity mismatch"), "{error}");
}

#[tokio::test]
async fn worker_rejects_disconnected_collection_before_dialing_runner_data() {
    let worker = pipeline(vec![UserRow(1)]);
    let runner = V5BundleExecutor::new(&worker, "test-build").unwrap();
    let unreachable = "127.0.0.1:1".parse().unwrap();
    let mut bundle = plan(&worker, unreachable);
    if let Step::V5ParDo { input_pc, .. } = &mut bundle.steps[2] {
        *input_pc = "wrong-collection".into();
    }
    let error = runner.run("bundle", &bundle).await.unwrap_err();
    assert!(error.to_string().contains("wiring"), "{error}");
}

#[tokio::test]
async fn empty_source_rooted_bundle_does_not_open_user_source() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received: Arc::clone(&received),
                impulse_count: 0,
                impulse_payload: Vec::new(),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let opened = Arc::new(AtomicUsize::new(0));
    let driver = counted_pipeline(Arc::clone(&opened));
    let worker = counted_pipeline(Arc::clone(&opened));
    let runner = V5BundleExecutor::new(&worker, "test-build").unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        runner.run("bundle", &plan(&driver, addr)),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    server.abort();
    assert_eq!(opened.load(Ordering::SeqCst), 0);
    assert!(!response.requires_finalization);
    assert_eq!(response.monitoring_infos[0].payload, vec![0]);
    assert!(received
        .lock()
        .await
        .iter()
        .all(|chunk| chunk.data.is_empty()));
}

#[tokio::test]
async fn duplicate_impulses_do_not_open_user_source() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received,
                impulse_count: 2,
                impulse_payload: Vec::new(),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let opened = Arc::new(AtomicUsize::new(0));
    let driver = counted_pipeline(Arc::clone(&opened));
    let worker = counted_pipeline(Arc::clone(&opened));
    let runner = V5BundleExecutor::new(&worker, "test-build").unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        runner.run("bundle", &plan(&driver, addr)),
    )
    .await
    .unwrap()
    .unwrap_err();
    server.abort();
    assert!(error.to_string().contains("exactly one Impulse"), "{error}");
    assert_eq!(opened.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn nonempty_impulse_does_not_open_user_source() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received,
                impulse_count: 1,
                impulse_payload: vec![1],
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let opened = Arc::new(AtomicUsize::new(0));
    let pipeline = counted_pipeline(Arc::clone(&opened));
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        runner.run("bundle", &plan(&pipeline, addr)),
    )
    .await
    .unwrap()
    .unwrap_err();
    server.abort();
    assert!(error.to_string().contains("empty payload"), "{error}");
    assert_eq!(opened.load(Ordering::SeqCst), 0);
}

struct CountingSink {
    written: Arc<AtomicUsize>,
    committed: Arc<AtomicUsize>,
}

impl TypedSink<UserRow> for CountingSink {
    fn write(&mut self, _value: UserRow) -> Result<(), ironbeam_core::Error> {
        self.written.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), ironbeam_core::Error> {
        self.committed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn abort(&mut self) -> Result<(), ironbeam_core::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn source_records_reach_the_sink_before_the_next_record_is_read() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received,
                impulse_count: 1,
                impulse_payload: Vec::new(),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let written = Arc::new(AtomicUsize::new(0));
    let committed = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source_written = Arc::clone(&written);
    let sink_written = Arc::clone(&written);
    let sink_committed = Arc::clone(&committed);
    pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "streaming-source-v1".into(),
            Some(3),
            move || {
                let written = Arc::clone(&source_written);
                Ok(Box::new((0..3).map(move |index| {
                    if written.load(Ordering::SeqCst) != index {
                        return Err(ironbeam_core::Error::incompatible(
                            "source was read ahead of sink",
                        ));
                    }
                    Ok(UserRow(index as i32))
                })))
            },
        )
        .unwrap()
        .map_named("copy", |row| row)
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            "counting-sink-v1".into(),
            move || {
                Ok(Box::new(CountingSink {
                    written: Arc::clone(&sink_written),
                    committed: Arc::clone(&sink_committed),
                }))
            },
        )
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let mut steps = vec![Step::Source {
        transform_id: "input".into(),
        port,
        output_pc: "impulse".into(),
        wire: SourceWire::Opaque,
    }];
    let mut previous_pc = "impulse".to_string();
    for key in ["source", "copy", "sink"] {
        let output_pc = if key == "sink" {
            String::new()
        } else {
            collection_id(key)
        };
        steps.push(Step::V5ParDo {
            transform_id: key.into(),
            node_ref: V5NodeRef {
                key: key.into(),
                identity: identity.clone(),
                co_group_input: None,
            },
            input_pc: previous_pc,
            output_pc: output_pc.clone(),
            stateful_value: false,
        });
        previous_pc = output_pc;
    }
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        runner.run(
            "bundle",
            &BundlePlan {
                pbd_id: "streaming".into(),
                state_endpoint: None,
                side_inputs: HashMap::new(),
                steps,
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    server.abort();
    assert_eq!(written.load(Ordering::SeqCst), 3);
    assert_eq!(committed.load(Ordering::SeqCst), 1);
}

struct SummingSink(Arc<AtomicUsize>);
impl TypedSink<UserRow> for SummingSink {
    fn write(&mut self, value: UserRow) -> Result<(), ironbeam_core::Error> {
        self.0.fetch_add(value.0 as usize, Ordering::SeqCst);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), ironbeam_core::Error> {
        Ok(())
    }
    fn abort(&mut self) -> Result<(), ironbeam_core::Error> {
        Ok(())
    }
}

#[tokio::test]
async fn branched_bundle_opens_source_once_and_writes_both_sinks() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let received = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(RunnerData {
                received,
                impulse_count: 1,
                impulse_payload: Vec::new(),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let opened = Arc::new(AtomicUsize::new(0));
    let left_sum = Arc::new(AtomicUsize::new(0));
    let right_sum = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source_opened = Arc::clone(&opened);
    let source = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "branch-source-v1".into(),
            Some(3),
            move || {
                source_opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(
                    vec![Ok(UserRow(1)), Ok(UserRow(2)), Ok(UserRow(3))].into_iter(),
                ))
            },
        )
        .unwrap();
    let spec = SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::AtLeastOnce,
        commit: true,
    };
    let left_capture = Arc::clone(&left_sum);
    source
        .map_named("left", |row| UserRow(row.0 * 2))
        .unwrap()
        .sink_named_with_config(
            "left-sink",
            spec.clone(),
            "left-destination-v1".into(),
            move || Ok(Box::new(SummingSink(Arc::clone(&left_capture)))),
        )
        .unwrap();
    let right_capture = Arc::clone(&right_sum);
    source
        .map_named("right", |row| UserRow(row.0 + 10))
        .unwrap()
        .sink_named_with_config(
            "right-sink",
            spec,
            "right-destination-v1".into(),
            move || Ok(Box::new(SummingSink(Arc::clone(&right_capture)))),
        )
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let mut steps = vec![Step::Source {
        transform_id: "input".into(),
        port,
        output_pc: "impulse".into(),
        wire: SourceWire::Opaque,
    }];
    for (key, input) in [
        ("source", "impulse"),
        ("left", "source"),
        ("right", "source"),
        ("left-sink", "left"),
        ("right-sink", "right"),
    ] {
        steps.push(Step::V5ParDo {
            transform_id: key.into(),
            node_ref: V5NodeRef {
                key: key.into(),
                identity: identity.clone(),
                co_group_input: None,
            },
            input_pc: if input == "impulse" {
                input.into()
            } else {
                collection_id(input)
            },
            output_pc: if key.ends_with("sink") {
                String::new()
            } else {
                collection_id(key)
            },
            stateful_value: false,
        });
    }
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        runner.run(
            "bundle",
            &BundlePlan {
                pbd_id: "branched".into(),
                state_endpoint: None,
                side_inputs: HashMap::new(),
                steps,
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    server.abort();
    assert_eq!(opened.load(Ordering::SeqCst), 1);
    assert_eq!(left_sum.load(Ordering::SeqCst), 12);
    assert_eq!(right_sum.load(Ordering::SeqCst), 36);
    for transform_id in ["left-sink", "right-sink"] {
        let metric = response
            .monitoring_infos
            .iter()
            .find(|info| {
                info.labels
                    .get("PTRANSFORM")
                    .is_some_and(|label| label == transform_id)
            })
            .unwrap();
        assert_eq!(metric.payload, vec![3]);
    }
}

#[tokio::test]
async fn state_backed_group_fetches_runner_suffix_before_mapping() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![("a".to_owned(), 1_i32)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .map_named("sum", |(_key, values)| UserRow(values.into_iter().sum()))
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let (key, first) = registry
        .split_group_input("group", &("a".to_owned(), 1_i32).encode().unwrap())
        .unwrap();
    let (_, second) = registry
        .split_group_input("group", &("a".to_owned(), 2_i32).encode().unwrap())
        .unwrap();
    let mut body = Vec::new();
    encode_length_prefix(&key, &mut body).unwrap();
    body.extend_from_slice(&(-1_i32).to_be_bytes());
    body.push(1);
    encode_length_prefix(&first, &mut body).unwrap();
    encode_varint(u64::MAX, &mut body).unwrap();
    encode_length_prefix(b"group-token", &mut body).unwrap();
    let mut record = Vec::new();
    encode_windowed_value(0, &body, &mut record).unwrap();
    let mut suffix = Vec::new();
    encode_length_prefix(&second, &mut suffix).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let incoming = async_stream::stream! {
        loop {
            yield listener.accept().await.map(|(stream, _)| stream);
        }
    };
    let received = Arc::new(Mutex::new(Vec::new()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(GroupedRunnerData {
                record,
                received: Arc::clone(&received),
            }))
            .add_service(BeamFnStateServer::new(GroupedRunnerState {
                suffix,
                requests: Arc::clone(&requests),
            }))
            .serve_with_incoming(incoming),
    );
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "grouped".into(),
    };
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        runner.run(
            "bundle",
            &BundlePlan {
                pbd_id: "state-backed-group".into(),
                state_endpoint: Some(format!("http://{addr}")),
                side_inputs: HashMap::new(),
                steps: vec![
                    Step::Source {
                        transform_id: "input".into(),
                        port: port.clone(),
                        output_pc: collection_id("group"),
                        wire: SourceWire::KvStateBackedIter,
                    },
                    Step::V5ParDo {
                        transform_id: "sum".into(),
                        node_ref: V5NodeRef {
                            key: "sum".into(),
                            identity,
                            co_group_input: None,
                        },
                        input_pc: collection_id("group"),
                        output_pc: collection_id("sum"),
                        stateful_value: false,
                    },
                    Step::Sink {
                        transform_id: "output".into(),
                        port,
                        input_pc: collection_id("sum"),
                        wire: SinkWire::Opaque,
                    },
                ],
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let requests = requests.lock().await.clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].instruction_id, "bundle");
    assert!(
        matches!(requests[0].state_key.as_ref().and_then(|key| key.r#type.as_ref()),
        Some(state_key::Type::Runner(key)) if key.key == b"group-token")
    );
    let chunks = received.lock().await.clone();
    server.abort();
    let values = chunks
        .iter()
        .filter(|chunk| chunk.transform_id == "output" && !chunk.data.is_empty())
        .map(|chunk| {
            let (windowed, _) = decode_windowed_value(&chunk.data).unwrap();
            let (body, _) = decode_length_prefix(windowed.element).unwrap();
            UserRow::decode(body).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, [UserRow(3)]);
}

struct BoundaryRunnerData {
    written: Arc<AtomicUsize>,
    first_was_processed: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl BeamFnData for BoundaryRunnerData {
    type DataStream = ReceiverStream<Result<Elements, Status>>;

    async fn data(
        &self,
        request: Request<Streaming<Elements>>,
    ) -> Result<Response<Self::DataStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        let written = Arc::clone(&self.written);
        let observed = Arc::clone(&self.first_was_processed);
        tokio::spawn(async move {
            let _outgoing = request.into_inner();
            for (index, value) in [UserRow(7), UserRow(9)].into_iter().enumerate() {
                if index == 1 {
                    for _ in 0..50 {
                        if written.load(Ordering::SeqCst) == 1 {
                            observed.store(1, Ordering::SeqCst);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
                let mut body = Vec::new();
                encode_length_prefix(&value.encode().unwrap(), &mut body).unwrap();
                let mut windowed = Vec::new();
                encode_windowed_value(0, &body, &mut windowed).unwrap();
                tx.send(Ok(Elements {
                    data: vec![Data {
                        instruction_id: "bundle".into(),
                        transform_id: "input".into(),
                        data: windowed,
                        is_last: false,
                    }],
                    ..Default::default()
                }))
                .await
                .unwrap();
            }
            tx.send(Ok(Elements {
                data: vec![Data {
                    instruction_id: "bundle".into(),
                    transform_id: "input".into(),
                    data: Vec::new(),
                    is_last: true,
                }],
                ..Default::default()
            }))
            .await
            .unwrap();
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::test]
async fn post_flatten_boundary_processes_records_before_input_closes() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let written = Arc::new(AtomicUsize::new(0));
    let committed = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let server = tokio::spawn(
        Server::builder()
            .add_service(BeamFnDataServer::new(BoundaryRunnerData {
                written: Arc::clone(&written),
                first_was_processed: Arc::clone(&observed),
            }))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(50)).await;

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![UserRow(1)]).unwrap();
    let left = source.map_named("left", |row| row).unwrap();
    let right = source.map_named("right", |row| row).unwrap();
    let sink_written = Arc::clone(&written);
    let sink_committed = Arc::clone(&committed);
    left.flatten_named("merged", &[right])
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            "boundary-sink-v1".into(),
            move || {
                Ok(Box::new(CountingSink {
                    written: Arc::clone(&sink_written),
                    committed: Arc::clone(&sink_committed),
                }))
            },
        )
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let identity = execution_identity(&graph, &registry, "test-build").unwrap();
    let port = RemoteGrpcPort {
        api_service_descriptor: Some(ApiServiceDescriptor {
            url: format!("http://{addr}"),
            ..Default::default()
        }),
        coder_id: "opaque".into(),
    };
    let runner = V5BundleExecutor::new(&pipeline, "test-build").unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        runner.run(
            "bundle",
            &BundlePlan {
                pbd_id: "post-flatten".into(),
                state_endpoint: None,
                side_inputs: HashMap::new(),
                steps: vec![
                    Step::Source {
                        transform_id: "input".into(),
                        port,
                        output_pc: collection_id("merged"),
                        wire: SourceWire::Opaque,
                    },
                    Step::V5ParDo {
                        transform_id: "sink".into(),
                        node_ref: V5NodeRef {
                            key: "sink".into(),
                            identity,
                            co_group_input: None,
                        },
                        input_pc: collection_id("merged"),
                        output_pc: String::new(),
                        stateful_value: false,
                    },
                ],
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    server.abort();
    assert_eq!(written.load(Ordering::SeqCst), 2);
    assert_eq!(committed.load(Ordering::SeqCst), 1);
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    let sink_count = response
        .monitoring_infos
        .iter()
        .find(|info| {
            info.labels
                .get("PTRANSFORM")
                .is_some_and(|transform| transform == "sink")
        })
        .unwrap();
    assert_eq!(sink_count.payload, vec![2]);
}
