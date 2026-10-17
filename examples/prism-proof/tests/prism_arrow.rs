//! Independent live Prism proof for native Arrow IPC input and final output.

use std::time::Duration;

use beam_proto::job_management_v1::{
    job_service_client::JobServiceClient, job_state, GetJobMetricsRequest, GetJobStateRequest,
    PrepareJobRequest, RunJobRequest,
};
use beam_proto::pipeline_v1::ProcessPayload;
use ironbeam_core::{Element, Error, Pipeline};
use ironbeam_dataflow::portable_publication::PortablePublicationPlan;
use ironbeam_dataflow::prepare_v5;
use ironbeam_direct::{DirectCollectExt, DirectRunner, JobConfig};
use prost_beam::Message;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    label: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "tests/prism-arrow-row";
    const CODER_ID: &'static str = "tests/prism-arrow-row-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.id.to_le_bytes().to_vec();
        bytes.extend_from_slice(self.label.as_bytes());
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let id = bytes
            .get(..4)
            .ok_or_else(|| Error::incompatible("short Arrow row"))?
            .try_into()
            .map_err(|_| Error::incompatible("short Arrow row"))?;
        let label = String::from_utf8(bytes[4..].to_vec())
            .map_err(|_| Error::incompatible("invalid Arrow row label"))?;
        Ok(Self {
            id: i32::from_le_bytes(id),
            label,
        })
    }
}

#[tokio::test]
async fn prism_reopens_native_arrow_rows_and_batches_after_finalization() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let input_path = dir.path().join("input.arrow");
    let stage = dir.path().join("fragments");
    let output_path = dir.path().join("final.arrow");
    let batch_stage = dir.path().join("batch-fragments");
    let batch_output_path = dir.path().join("final-batches.arrow");
    let journal_dir = dir.path().join("finalization-journal");
    let expected = vec![
        Row {
            id: 1,
            label: "a".into(),
        },
        Row {
            id: 2,
            label: "b".into(),
        },
        Row {
            id: 3,
            label: "c".into(),
        },
    ];
    let seed = Pipeline::default();
    let seed_rows = seed.from_vec("seed", expected.clone()).unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_arrow::arrow_ipc_sink(&seed_rows, "write-input", &input_path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();

    let pipeline = Pipeline::default();
    let rows =
        ironbeam_io_arrow::arrow_ipc_source::<Row>(&pipeline, "arrow-source", &input_path).unwrap();
    ironbeam_io_arrow::arrow_ipc_row_fragment_sink(
        &rows.reshuffle_named("arrow-boundary").unwrap(),
        "arrow-final",
        &stage,
        &stage.to_string_lossy(),
        "prism-arrow-fixture",
    )
    .unwrap();
    let batches =
        ironbeam_io_arrow::arrow_ipc_batches_source(&pipeline, "arrow-batch-source", &input_path)
            .unwrap();
    ironbeam_io_arrow::arrow_ipc_batch_fragment_sink(
        &batches.reshuffle_named("arrow-batch-boundary").unwrap(),
        "arrow-batch-final",
        &batch_stage,
        &batch_stage.to_string_lossy(),
        "prism-arrow-fixture",
    )
    .unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-arrow-fixture").unwrap();
    let publication_plan = PortablePublicationPlan::from_translated(&translated).unwrap();
    let environment = translated
        .pipeline
        .components
        .as_mut()
        .unwrap()
        .environments
        .values_mut()
        .next()
        .unwrap();
    environment.urn = "beam:env:process:v1".into();
    environment.payload = ProcessPayload {
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        command: env!("CARGO_BIN_EXE_v5_arrow_worker").into(),
        env: [
            (
                "IRONBEAM_V5_ARROW_INPUT".into(),
                input_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_ARROW_STAGING".into(),
                stage.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_ARROW_BATCH_STAGING".into(),
                batch_stage.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_FINALIZATION_JOURNAL_DIR".into(),
                journal_dir.to_string_lossy().into_owned(),
            ),
        ]
        .into(),
    }
    .encode_to_vec();

    let url = if endpoint.contains("://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    };
    let mut client = JobServiceClient::connect(url).await.unwrap();
    let prepared = client
        .prepare(PrepareJobRequest {
            pipeline: Some(translated.pipeline),
            pipeline_options: Some(prost_types::Struct::default()),
            job_name: format!("ironbeam-v5-arrow-{}", std::process::id()),
        })
        .await
        .unwrap()
        .into_inner();
    let running = client
        .run(RunJobRequest {
            preparation_id: prepared.preparation_id,
            retrieval_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut terminal = None;
    let mut last_state = None;
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        last_state = Some(state);
        if state == 3 || state == 4 || state == 5 || state == 6 {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Arrow Prism job did not reach DONE: {terminal:?}; last state: {last_state:?}"
    );
    let metrics = client
        .get_job_metrics(GetJobMetricsRequest {
            job_id: running.job_id,
        })
        .await
        .unwrap()
        .into_inner()
        .metrics
        .unwrap();
    let ready = publication_plan
        .complete_local(
            job_state::Enum::try_from(terminal.unwrap()).unwrap(),
            &metrics,
            &journal_dir,
        )
        .unwrap();
    assert_eq!(
        ready.sinks().collect::<Vec<_>>(),
        ["arrow-batch-final", "arrow-final"]
    );
    let committed = ready.committed_rows("arrow-final").unwrap();
    assert_eq!(committed, 3);
    let committed_batches = ready.committed_rows("arrow-batch-final").unwrap();
    assert_eq!(committed_batches, 1);
    let store = ironbeam_io_arrow::arrow_ipc_row_fragment_store::<Row>(
        &stage,
        &stage.to_string_lossy(),
        "prism-arrow-fixture",
        "arrow-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("arrow-final").unwrap())
    .unwrap();
    assert!(!output_path.exists());
    let published =
        ironbeam_io_arrow::publish_arrow_ipc_row_fragments(&store, committed, &output_path)
            .unwrap();
    assert_eq!(published.rows, 3);
    assert_eq!(
        ironbeam_io_arrow::publish_arrow_ipc_row_fragments(&store, committed, &output_path,)
            .unwrap(),
        published
    );
    let output = Pipeline::default();
    let reopened = ironbeam_io_arrow::arrow_ipc_source::<Row>(&output, "reopen", &output_path)
        .unwrap()
        .output("rows")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(reopened, expected);
    let batch_store = ironbeam_io_arrow::arrow_ipc_batch_fragment_store(
        &batch_stage,
        &batch_stage.to_string_lossy(),
        "prism-arrow-fixture",
        "arrow-batch-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("arrow-batch-final").unwrap())
    .unwrap();
    assert!(!batch_output_path.exists());
    let published_batches = ironbeam_io_arrow::publish_arrow_ipc_batch_fragments(
        &batch_store,
        committed_batches,
        &batch_output_path,
    )
    .unwrap();
    assert_eq!(published_batches.rows, 1);
    assert_eq!(
        ironbeam_io_arrow::publish_arrow_ipc_batch_fragments(
            &batch_store,
            committed_batches,
            &batch_output_path,
        )
        .unwrap(),
        published_batches
    );
    let batch_pipeline = Pipeline::default();
    let reopened_batches = ironbeam_io_arrow::arrow_ipc_batches_source(
        &batch_pipeline,
        "reopen-batches",
        &batch_output_path,
    )
    .unwrap()
    .output("batches")
    .unwrap()
    .collect_seq()
    .unwrap();
    assert_eq!(reopened_batches.len(), 1);
    assert_eq!(reopened_batches[0].0.num_rows(), 3);
}
