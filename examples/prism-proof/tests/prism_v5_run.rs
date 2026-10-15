//! Opt-in execution proof with a real local Prism process environment.

extern crate prost_v5 as prost;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use beam_proto::job_management_v1::{
    job_messages_response, job_service_client::JobServiceClient, job_state, CancelJobRequest,
    GetJobMetricsRequest, GetJobStateRequest, JobMessagesRequest, PrepareJobRequest, RunJobRequest,
};
use beam_proto::pipeline_v1::ProcessPayload;
use ironbeam_cloud_objects::{LocalObjectIO, ObjectIO, ObjectStores};
use ironbeam_core::{
    Element, Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress,
    TestStreamEvent, TypedSink, Version,
};
use ironbeam_dataflow::portable_publication::PortablePublicationPlan;
use ironbeam_dataflow::prepare_v5;
use ironbeam_direct::{DirectCollectExt, DirectRunner, JobConfig};
use ironbeam_harness::finalization_journal::FileFinalizationJournal;
use ironbeam_prism_proof::{
    accumulating_combined_count_fixture, accumulating_count_trigger_stream_fixture,
    accumulating_late_count_fixture, accumulating_late_join_fixture,
    accumulating_latest_count_trigger_stream_fixture, allowed_lateness_fixture,
    bounded_batch_helpers_fixture, bounded_join_cogroup_fixture,
    bounded_join_cogroup_output_time_fixture, bounded_map_filter_fixture,
    bounded_multi_source_fixture, bounded_multi_wait_fixture, bounded_session_cogroup_fixture,
    bounded_session_join_fixture, bounded_side_input_fixture, bounded_ten_way_cogroup_fixture,
    bounded_three_way_cogroup_fixture, bounded_wait_on_fixture, combine_output_time_fixture,
    combined_count_fixture, completion_signal, count_trigger_stream_fixture, dead_letter_fixture,
    dual_count_or_time_cogroup_fixture, dual_late_cogroup_collection, dual_late_cogroup_fixture,
    dual_late_count_cogroup_collection, dual_late_count_cogroup_fixture,
    dual_positive_late_cogroup_fixture, dual_positive_late_full_join_fixture,
    dual_positive_late_join_time_fixture, dual_processing_time_cogroup_collection,
    dual_processing_time_cogroup_fixture, dual_stream_windowed_side_collection,
    dual_stream_windowed_side_fixture, dual_stream_windowed_wait_collection,
    dual_stream_windowed_wait_fixture, dual_unbounded_cogroup_collection,
    dual_unbounded_cogroup_fixture, dual_unbounded_fanin_output_time_collection,
    dual_unbounded_fanin_output_time_fixture, dual_unbounded_flatten_collection,
    dual_unbounded_flatten_fixture, dual_unbounded_join_collection, dual_unbounded_join_fixture,
    dual_unbounded_join_kind_collection, dual_unbounded_join_kind_fixture,
    dual_unbounded_late_join_collection, dual_unbounded_late_join_fixture,
    dual_unbounded_map_side_collection, dual_unbounded_map_side_fixture,
    dual_unbounded_session_cogroup_collection, dual_unbounded_session_cogroup_fixture,
    dual_unbounded_session_join_collection, dual_unbounded_session_join_fixture,
    explicit_custom_batches_fixture, failing_wait_on_fixture, finalization_failure_fixture,
    finalization_probe_sink, finalization_probe_store,
    global_accumulating_early_cogroup_collection, global_accumulating_early_cogroup_fixture,
    global_count_collection, global_count_fixture, global_fold_fixture,
    global_keyed_batches_fixture, global_keyed_batches_with_upstream_trigger_fixture,
    global_keyed_combine_collection, global_keyed_combine_fixture,
    global_vector_fixture, group_end_time_fixture, group_latest_time_fixture, group_time_fixture,
    heterogeneous_late_count_cogroup_collection, heterogeneous_late_count_cogroup_fixture,
    heterogeneous_windowed_wait_collection, heterogeneous_windowed_wait_fixture,
    homogeneous_twelve_way_cogroup_fixture, homogeneous_twelve_way_session_fixture,
    invalid_dynop_fixture, invalid_keyed_batch_fixture, large_group_rows, large_group_summary,
    late_count_fixture, late_count_join_fixture, late_count_join_kind_collection,
    late_count_join_kind_fixture, late_keyed_batches_fixture, latest_tie_duplicate_fixture, latest_tie_empty_fixture,
    latest_tie_fixture, mixed_boundedness_join_cogroup_fixture, mixed_flatten_collection,
    mixed_flatten_fixture, mixed_homogeneous_cogroup_collection, mixed_homogeneous_cogroup_fixture,
    offset_window_group_fixture, ordered_batch_duplicate_fixture, ordered_global_duplicate_fixture,
    ordered_global_fixture, ordered_keyed_duplicate_fixture, ordered_keyed_fixture,
    pane_metadata_fixture_for, pane_metadata_fixture_with_accumulating_count,
    pane_metadata_fixture_with_trigger, panicking_global_combine_fixture,
    panicking_keyed_fold_fixture, panicking_map_fixture, panicking_side_input_fixture,
    partition_fixture, portable_object_avro_collection, portable_object_avro_fixture,
    portable_object_parquet_collection, portable_object_parquet_fixture,
    processing_time_trigger_collection, processing_time_trigger_fixture,
    production_join_cogroup_fixture, production_stream_fixture, session_group_fixture,
    shared_accumulating_early_cogroup_collection, shared_accumulating_early_cogroup_fixture,
    shared_accumulating_late_cogroup_collection, shared_accumulating_late_cogroup_fixture,
    shared_unbounded_homogeneous_cogroup_collection, shared_unbounded_homogeneous_cogroup_fixture,
    side_view_shapes_fixture, sliding_allowed_lateness_fixture, sliding_late_join_fixture,
    sliding_window_group_fixture, streaming_group_fixture, streaming_ordered_batch_fixture,
    streaming_ordered_keyed_fixture, streaming_partition_fixture, streaming_session_group_fixture,
    streaming_sliding_group_fixture, streaming_windowed_side_input_collection,
    streaming_windowed_side_input_fixture, timed_keyed_batches_fixture,
    triple_stream_windowed_wait_collection, triple_stream_windowed_wait_fixture,
    triple_unbounded_flatten_collection, triple_unbounded_flatten_fixture,
    triple_unbounded_homogeneous_cogroup_collection, triple_unbounded_homogeneous_cogroup_fixture,
    triple_unbounded_session_homogeneous_collection, triple_unbounded_session_homogeneous_fixture,
    validation_fail_fixture, validation_skip_fixture, windowed_duplicate_map_side_fixture,
    windowed_keyed_batches_fixture, windowed_side_input_fixture, windowed_side_view_shapes_fixture,
    ExpandSingleton, PaneFixtureWindow, ProductionAvroRow, ProductionProtoRow,
    PRODUCTION_AVRO_SCHEMA,
};
#[cfg(feature = "database-proofs")]
use ironbeam_prism_proof::{mongodb_round_trip_fixture, sqlite_round_trip_fixture, MongoRow};
use prost_beam::Message;
use sha2::{Digest, Sha256};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct UserRow {
    id: i32,
    label: String,
}

impl Element for UserRow {
    const TYPE_ID: &'static str = "tests/prism-run-user-row";
    const CODER_ID: &'static str = "tests/prism-run-user-row-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = self.id.to_le_bytes().to_vec();
        bytes.extend_from_slice(self.label.as_bytes());
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let id: [u8; 4] = bytes
            .get(..4)
            .ok_or_else(|| Error::incompatible("short row"))?
            .try_into()
            .map_err(|_| Error::incompatible("short row"))?;
        let label = String::from_utf8(bytes[4..].to_vec())
            .map_err(|_| Error::incompatible("invalid row label"))?;
        Ok(Self {
            id: i32::from_le_bytes(id),
            label,
        })
    }
}

fn decode_final_rows(path: &Path, expected_rows: u64) -> Result<Vec<String>, Error> {
    let bytes = std::fs::read(path).map_err(|error| Error::incompatible(error.to_string()))?;
    let mut offset = 0;
    let mut rows = Vec::new();
    while offset < bytes.len() {
        let end = offset
            .checked_add(8)
            .ok_or_else(|| Error::incompatible("frame offset overflow"))?;
        let length: [u8; 8] = bytes
            .get(offset..end)
            .ok_or_else(|| Error::incompatible("truncated frame length"))?
            .try_into()
            .map_err(|_| Error::incompatible("invalid frame length"))?;
        offset = end;
        let length = usize::try_from(u64::from_le_bytes(length))
            .map_err(|_| Error::incompatible("frame length overflow"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::incompatible("frame offset overflow"))?;
        let row = UserRow::decode(
            bytes
                .get(offset..end)
                .ok_or_else(|| Error::incompatible("truncated frame payload"))?,
        )?;
        offset = end;
        rows.push(format!("{}:{}", row.id, row.label));
    }
    if rows.len() as u64 != expected_rows {
        return Err(Error::incompatible(
            "final result row count does not match accepted fragments",
        ));
    }
    Ok(rows)
}

fn decode_final_i32_rows(path: &Path, expected_rows: u64) -> Result<Vec<i32>, Error> {
    let bytes = std::fs::read(path).map_err(|error| Error::incompatible(error.to_string()))?;
    let mut offset = 0;
    let mut rows = Vec::new();
    while offset < bytes.len() {
        let length_end = offset
            .checked_add(8)
            .ok_or_else(|| Error::incompatible("final row offset overflow"))?;
        let length: [u8; 8] = bytes
            .get(offset..length_end)
            .ok_or_else(|| Error::incompatible("truncated final row length"))?
            .try_into()
            .map_err(|_| Error::incompatible("invalid final row length"))?;
        offset = length_end;
        let length = usize::try_from(u64::from_le_bytes(length))
            .map_err(|_| Error::incompatible("final row length overflow"))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::incompatible("final row offset overflow"))?;
        rows.push(<i32 as Element>::decode(
            bytes
                .get(offset..end)
                .ok_or_else(|| Error::incompatible("truncated final row"))?,
        )?);
        offset = end;
    }
    if rows.len() as u64 != expected_rows {
        return Err(Error::incompatible("final row count mismatch"));
    }
    Ok(rows)
}

#[test]
fn final_result_validator_rejects_incomplete_frames_and_wrong_count() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), 12_u64.to_le_bytes()).unwrap();
    assert!(decode_final_rows(file.path(), 1).is_err());
    let encoded = UserRow {
        id: 7,
        label: "ok".into(),
    }
    .encode()
    .unwrap();
    let mut framed = (encoded.len() as u64).to_le_bytes().to_vec();
    framed.extend_from_slice(&encoded);
    std::fs::write(file.path(), &framed).unwrap();
    assert_eq!(decode_final_rows(file.path(), 1).unwrap(), ["7:ok"]);
    assert!(decode_final_rows(file.path(), 2).is_err());
}

#[derive(Clone, PartialEq, prost_v5::Message)]
struct ProtoRow {
    #[prost(int32, tag = "1")]
    id: i32,
    #[prost(string, tag = "2")]
    label: String,
}

impl Element for ProtoRow {
    const TYPE_ID: &'static str = "tests/prism-protobuf-row";
    const CODER_ID: &'static str = "tests/prism-protobuf-row-prost14";
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

struct CaptureSink(Vec<UserRow>);
impl TypedSink<UserRow> for CaptureSink {
    fn write(&mut self, value: UserRow) -> Result<(), Error> {
        self.0.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.0.clear();
        Ok(())
    }
}
struct NullTimeSink;
impl TypedSink<(i64, UserRow)> for NullTimeSink {
    fn write(&mut self, _: (i64, UserRow)) -> Result<(), Error> {
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

fn prism_job_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn run_shared_fixture(
    endpoint: &str,
    pipeline: &Pipeline,
    name: &str,
    env: HashMap<String, String>,
) -> (i32, Vec<String>) {
    let mut translated = prepare_v5(pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env,
    }
    .encode_to_vec();
    let url = if endpoint.contains("://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    let mut client = JobServiceClient::connect(url).await.unwrap();
    let prepared = client
        .prepare(PrepareJobRequest {
            pipeline: Some(translated.pipeline),
            pipeline_options: Some(prost_types::Struct::default()),
            job_name: format!("ironbeam-v5-{name}-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            let mut messages = Vec::new();
            if state == 4 {
                let mut stream = client
                    .get_message_stream(JobMessagesRequest {
                        job_id: running.job_id.clone(),
                    })
                    .await
                    .unwrap()
                    .into_inner();
                while let Some(response) =
                    tokio::time::timeout(Duration::from_secs(3), stream.message())
                        .await
                        .unwrap()
                        .unwrap()
                {
                    if let Some(job_messages_response::Response::MessageResponse(message)) =
                        response.response
                    {
                        messages.push(message.message_text);
                    }
                }
            }
            return (state, messages);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Prism {name} job did not reach a terminal state");
}

#[cfg(feature = "database-proofs")]
#[tokio::test]
async fn prism_reads_and_writes_sqlite_through_the_v5_worker() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    sqlx::any::install_default_drivers();
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory.path().join("rows.db").display()
    );
    let pool = sqlx::AnyPool::connect(&url).await.unwrap();
    for statement in [
        "CREATE TABLE source_rows (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE direct_out (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "CREATE TABLE portable_out (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        "INSERT INTO source_rows (id, name) VALUES (1, 'Ada'), (2, 'Lin'), (3, 'Max')",
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }

    let direct_pipeline = Pipeline::default();
    let direct_sink = sqlite_round_trip_fixture(&direct_pipeline, &url, "direct_out").unwrap();
    tokio::task::spawn_blocking(move || DirectRunner.run_sink(&direct_sink, &JobConfig::default()))
        .await
        .unwrap()
        .unwrap();

    let portable_pipeline = Pipeline::default();
    sqlite_round_trip_fixture(&portable_pipeline, &url, "portable_out").unwrap();
    let environment: HashMap<String, String> = [
        ("IRONBEAM_V5_PRISM_SQLITE_URL".into(), url.clone()),
        (
            "IRONBEAM_V5_PRISM_SQLITE_OUTPUT".into(),
            "portable_out".into(),
        ),
    ]
    .into();
    for attempt in 0..2 {
        let (state, messages) = run_shared_fixture(
            &endpoint,
            &portable_pipeline,
            &format!("sqlite-{attempt}"),
            environment.clone(),
        )
        .await;
        assert_eq!(state, 3, "portable SQLite job failed: {messages:?}");
    }

    let expected = vec![
        (10_i64, "ADA".to_owned()),
        (20, "LIN".to_owned()),
        (30, "MAX".to_owned()),
    ];
    for table in ["direct_out", "portable_out"] {
        let query = match table {
            "direct_out" => "SELECT id, name FROM direct_out ORDER BY id",
            "portable_out" => "SELECT id, name FROM portable_out ORDER BY id",
            _ => unreachable!(),
        };
        let rows: Vec<(i64, String)> = sqlx::query_as(query).fetch_all(&pool).await.unwrap();
        assert_eq!(rows, expected, "{table} differs");
    }
    pool.close().await;
}

#[cfg(feature = "database-proofs")]
#[tokio::test]
async fn prism_reads_and_writes_mongodb_through_the_v5_worker() {
    let (Ok(endpoint), Ok(uri)) = (
        std::env::var("IRONBEAM_PRISM_ENDPOINT"),
        std::env::var("IRONBEAM_PRISM_MONGODB_URI"),
    ) else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let database = format!("ironbeam_v5_prism_{}", std::process::id());
    let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
    let source = client
        .database(&database)
        .collection::<MongoRow>("source_rows");
    source
        .insert_many([
            MongoRow {
                id: 1,
                name: "Ada".into(),
            },
            MongoRow {
                id: 2,
                name: "Lin".into(),
            },
            MongoRow {
                id: 3,
                name: "Max".into(),
            },
        ])
        .await
        .unwrap();

    let direct_pipeline = Pipeline::default();
    let direct_sink =
        mongodb_round_trip_fixture(&direct_pipeline, &uri, &database, "direct_out").unwrap();
    tokio::task::spawn_blocking(move || DirectRunner.run_sink(&direct_sink, &JobConfig::default()))
        .await
        .unwrap()
        .unwrap();

    let portable_pipeline = Pipeline::default();
    mongodb_round_trip_fixture(&portable_pipeline, &uri, &database, "portable_out").unwrap();
    let environment: HashMap<String, String> = [
        ("IRONBEAM_V5_PRISM_MONGODB_URI".into(), uri.clone()),
        (
            "IRONBEAM_V5_PRISM_MONGODB_DATABASE".into(),
            database.clone(),
        ),
        (
            "IRONBEAM_V5_PRISM_MONGODB_OUTPUT".into(),
            "portable_out".into(),
        ),
    ]
    .into();
    for attempt in 0..2 {
        let (state, messages) = run_shared_fixture(
            &endpoint,
            &portable_pipeline,
            &format!("mongodb-{attempt}"),
            environment.clone(),
        )
        .await;
        assert_eq!(state, 3, "portable MongoDB job failed: {messages:?}");
    }

    let expected = vec![
        MongoRow {
            id: 10,
            name: "ADA".into(),
        },
        MongoRow {
            id: 20,
            name: "LIN".into(),
        },
        MongoRow {
            id: 30,
            name: "MAX".into(),
        },
    ];
    for collection in ["direct_out", "portable_out"] {
        let mut cursor = client
            .database(&database)
            .collection::<MongoRow>(collection)
            .find(bson::doc! {})
            .sort(bson::doc! {"_id": 1})
            .await
            .unwrap();
        let mut rows = Vec::new();
        while cursor.advance().await.unwrap() {
            rows.push(cursor.deserialize_current().unwrap());
        }
        assert_eq!(rows, expected, "{collection} differs");
    }
    client.database(&database).drop().await.unwrap();
}

#[tokio::test]
async fn prism_abandons_fragment_when_bundle_fails_after_staging() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let capture = tempfile::tempdir().unwrap();
    let fragments = capture.path().join("fragments");
    let journal_dir = capture.path().join("journal");
    let output = capture.path().join("result.bin");
    let pipeline = Pipeline::default();
    finalization_failure_fixture(&pipeline, &fragments).unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-run-fixture").unwrap();
    let identity = translated.identity.clone();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [
            (
                "IRONBEAM_V5_PRISM_FAIL_AFTER_STAGE".into(),
                fragments.to_string_lossy().into_owned(),
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
            job_name: format!("ironbeam-v5-failed-fragment-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(terminal, Some(4), "failed sink must fail the Prism job");
    assert_eq!(std::fs::read_dir(&fragments).unwrap().count(), 0);
    let store = finalization_probe_store(&fragments, "failure-finalization").unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    let journal = FileFinalizationJournal::new(&journal_dir, identity).unwrap();
    assert!(journal.snapshot().unwrap().is_empty());
    assert!(!output.exists());
    assert!(store.publish_concatenated(1, &output).is_err());
}

#[tokio::test]
async fn prism_runs_production_unbounded_source_with_residual_resume() {
    run_production_unbounded_source(false, false, false).await;
}

#[tokio::test]
async fn prism_fails_closed_on_supervised_worker_loss_with_active_bundles() {
    run_production_unbounded_source(true, false, false).await;
}

#[tokio::test]
async fn prism_recovers_killed_execution_child_in_same_job() {
    run_production_unbounded_source(true, false, true).await;
}

#[tokio::test]
async fn prism_rejects_worker_semantic_version_drift() {
    run_production_unbounded_source(false, true, false).await;
}

async fn run_production_unbounded_source(
    restart_worker: bool,
    semantic_version_drift: bool,
    supervised: bool,
) {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let capture = tempfile::tempdir().unwrap();
    let directory = capture.path().to_string_lossy().into_owned();
    let journal_dir = capture.path().join("finalization-journal");
    let restart_trigger = capture.path().join("restart-trigger");
    let kill_witness = capture.path().join("kill-witness");
    let pipeline = Pipeline::default();
    production_stream_fixture(&pipeline, &directory).unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-run-fixture").unwrap();
    let publication_plan = PortablePublicationPlan::from_translated(&translated).unwrap();
    let finalization_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-finalization")
        .map(|(id, _)| id.clone())
        .unwrap();
    let jsonl_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-jsonl-final")
        .map(|(id, _)| id.clone())
        .unwrap();
    let protobuf_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-protobuf-final")
        .map(|(id, _)| id.clone())
        .unwrap();
    let avro_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-avro-final")
        .map(|(id, _)| id.clone())
        .unwrap();
    let tfrecord_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-tfrecord-final")
        .map(|(id, _)| id.clone())
        .unwrap();
    let msgpack_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-msgpack-final")
        .map(|(id, _)| id.clone())
        .unwrap();
    let cbor_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-cbor-final")
        .map(|(id, _)| id.clone())
        .unwrap();
    let fold_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-capture-fold")
        .map(|(id, _)| id.clone())
        .unwrap();
    let global_fold_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-capture-global-fold")
        .map(|(id, _)| id.clone())
        .unwrap();
    let shared_group_transform = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .find(|(_, transform)| transform.unique_name == "production-capture-fold-shared")
        .map(|(id, _)| id.clone())
        .unwrap();
    let sink_input_pcollections = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .transforms
        .iter()
        .filter_map(|(id, transform)| {
            transform
                .inputs
                .values()
                .next()
                .map(|input| (id.clone(), input.clone()))
        })
        .collect::<HashMap<_, _>>();
    let worker = env!("CARGO_BIN_EXE_v5_prism_worker");
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
    let mut process = ProcessPayload {
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        command: if restart_worker && !supervised {
            env!("CARGO_BIN_EXE_v5_prism_worker_killer").into()
        } else {
            worker.into()
        },
        env: [
            (
                "IRONBEAM_V5_PRISM_PRODUCTION_CAPTURE".into(),
                directory.clone(),
            ),
            (
                "IRONBEAM_V5_FINALIZATION_JOURNAL_DIR".into(),
                journal_dir.to_string_lossy().into_owned(),
            ),
        ]
        .into(),
    };
    if restart_worker {
        if supervised {
            process
                .env
                .insert("IRONBEAM_V5_SUPERVISE_EXECUTION".into(), "1".into());
        } else {
            process
                .env
                .insert("IRONBEAM_V5_SUPERVISOR_WORKER".into(), worker.into());
        }
        process.env.insert(
            "IRONBEAM_V5_SUPERVISOR_TRIGGER".into(),
            restart_trigger.to_string_lossy().into_owned(),
        );
        process.env.insert(
            "IRONBEAM_V5_SUPERVISOR_KILLED".into(),
            kill_witness.to_string_lossy().into_owned(),
        );
    }
    if semantic_version_drift {
        process.env.insert(
            "IRONBEAM_V5_PRISM_SEMANTIC_VERSION_DRIFT".into(),
            "1".into(),
        );
    }
    environment.payload = process.encode_to_vec();
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
            job_name: format!("ironbeam-v5-production-{}", std::process::id()),
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
    let mut last_state = job_state::Enum::Unspecified as i32;
    for _ in 0..300 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        last_state = state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if (restart_worker && !supervised) || semantic_version_drift {
        if restart_worker {
            let trigger_pid = std::fs::read_to_string(&restart_trigger).unwrap();
            assert_eq!(std::fs::read_to_string(&kill_witness).unwrap(), trigger_pid);
            // Prism may report FAILED or leave a disconnected worker's job
            // RUNNING. Neither state authorizes coordinator publication.
            assert_ne!(terminal, Some(job_state::Enum::Done as i32));
        } else {
            assert_eq!(
                terminal,
                Some(job_state::Enum::Failed as i32),
                "semantic version drift must fail the active Prism job"
            );
        }
        if semantic_version_drift {
            let mut messages = client
                .get_message_stream(JobMessagesRequest {
                    job_id: running.job_id.clone(),
                })
                .await
                .unwrap()
                .into_inner();
            let mut diagnostics = Vec::new();
            while let Some(response) =
                tokio::time::timeout(Duration::from_secs(3), messages.message())
                    .await
                    .unwrap()
                    .unwrap()
            {
                if let Some(job_messages_response::Response::MessageResponse(message)) =
                    response.response
                {
                    diagnostics.push(message.message_text);
                }
            }
            assert!(
                diagnostics
                    .iter()
                    .any(|message| message.contains("driver/worker execution identity mismatch")),
                "Prism did not report semantic version rejection: {diagnostics:?}"
            );
        }
        assert!(publication_plan
            .complete_local(
                job_state::Enum::try_from(last_state).unwrap(),
                &beam_proto::job_management_v1::MetricResults::default(),
                &journal_dir,
            )
            .is_err());
        assert!(!capture.path().join("published.bin").exists());
        assert!(!capture
            .path()
            .join("object-results/output/production.jsonl")
            .exists());
        if terminal.is_none() {
            let _ = tokio::time::timeout(
                Duration::from_secs(3),
                client.cancel(CancelJobRequest {
                    job_id: running.job_id,
                }),
            )
            .await;
        }
        return;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism production source job did not reach DONE: {terminal:?}"
    );
    if supervised {
        assert_eq!(
            std::fs::read_to_string(&kill_witness).unwrap(),
            std::fs::read_to_string(&restart_trigger).unwrap(),
            "the failed child must be replaced inside this Prism job"
        );
    }
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-14")).unwrap(),
        "1000000:14:0:2000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-18")).unwrap(),
        "1000000:18:0:2000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-16")).unwrap(),
        "2000000:16:2000000:4000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-8")).unwrap(),
        "1000000:8:0:2000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-10")).unwrap(),
        "1000000:10:0:2000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-9")).unwrap(),
        "2000000:9:2000000:4000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-30116")).unwrap(),
        "1000000:30116:0:2000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-30008")).unwrap(),
        "2000000:30008:2000000:4000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-50016")).unwrap(),
        "1000000:50016:0:2000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-50008")).unwrap(),
        "2000000:50008:2000000:4000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-40116")).unwrap(),
        "1000000:40116:0:2000000\n"
    );
    assert_eq!(
        std::fs::read_to_string(capture.path().join("value-40008")).unwrap(),
        "2000000:40008:2000000:4000000\n",
    );
    let first_group = std::fs::read_to_string(capture.path().join("value-22116")).unwrap();
    let second_group = std::fs::read_to_string(capture.path().join("value-21008")).unwrap();
    assert!(first_group.ends_with(":22116:0:2000000\n"), "{first_group}");
    assert!(
        second_group.ends_with(":21008:2000000:4000000\n"),
        "{second_group}"
    );
    let names = std::fs::read_dir(capture.path())
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("value-")
        })
        .count();
    assert_eq!(names, 14, "idempotent sinks wrote unexpected value files");
    let mut metrics = client
        .get_job_metrics(GetJobMetricsRequest {
            job_id: running.job_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .metrics
        .unwrap();
    // Prism can report DONE before its committed counters for the final
    // residual bundle have converged, especially while other local jobs run.
    // Keep the exact count requirement with a bounded convergence window.
    let expected_sink_counts = [
        (&finalization_transform, 3),
        (&jsonl_transform, 3),
        (&protobuf_transform, 3),
        (&avro_transform, 3),
        (&tfrecord_transform, 3),
        (&msgpack_transform, 3),
        (&cbor_transform, 3),
        (&fold_transform, 2),
        (&global_fold_transform, 2),
        (&shared_group_transform, 2),
    ];
    for _ in 0..100 {
        if expected_sink_counts.iter().all(|(id, expected)| {
            ironbeam_dataflow::portable_metrics::committed_sink_input_count(&metrics, id).ok()
                == Some(*expected)
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        metrics = client
            .get_job_metrics(GetJobMetricsRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .metrics
            .unwrap();
    }
    let ready = publication_plan
        .complete_local(
            job_state::Enum::try_from(terminal.unwrap()).unwrap(),
            &metrics,
            &journal_dir,
        )
        .unwrap();
    assert_eq!(ready.sinks().count(), 8);
    let committed = ironbeam_dataflow::portable_metrics::committed_sink_input_count(
        &metrics,
        &finalization_transform,
    )
    .unwrap();
    let sink_count_diagnostics = expected_sink_counts
        .iter()
        .map(|(id, expected)| {
            let input = sink_input_pcollections.get(*id);
            (
                id.as_str(),
                *expected,
                ironbeam_dataflow::portable_metrics::committed_sink_input_count(&metrics, id),
                input.and_then(|pc| {
                    ironbeam_dataflow::portable_metrics::committed_element_count(&metrics, pc).ok()
                }),
            )
        })
        .collect::<Vec<_>>();
    let store =
        finalization_probe_store(capture.path().join("fragments"), "production-finalization")
            .unwrap()
            .require_bundle_attempts(ready.bundle_attempts("production-finalization").unwrap())
            .unwrap();
    let fragments = store.accepted_fragments().unwrap();
    assert_eq!(
        committed, 3,
        "sink and input counts: {sink_count_diagnostics:?}; accepted finalization fragments: {fragments:?}"
    );
    assert_eq!(
        ironbeam_dataflow::portable_metrics::committed_sink_input_count(&metrics, &fold_transform,)
            .unwrap(),
        2
    );
    assert_eq!(
        ironbeam_dataflow::portable_metrics::committed_sink_input_count(
            &metrics,
            &global_fold_transform,
        )
        .unwrap(),
        2
    );
    assert_eq!(
        ironbeam_dataflow::portable_metrics::committed_sink_input_count(
            &metrics,
            &shared_group_transform,
        )
        .unwrap(),
        2
    );
    assert!(
        fragments.len() >= 2,
        "production source residual did not create multiple finalized bundles: {fragments:?}"
    );
    assert_eq!(
        fragments.iter().map(|fragment| fragment.rows).sum::<u64>(),
        committed
    );
    let published = capture.path().join("published.bin");
    assert!(
        !published.exists(),
        "a final result appeared before coordinator publication"
    );
    let receipt = store
        .publish_validated_concatenated(committed, &published, |path| {
            decode_final_i32_rows(path, committed).map(|_| ())
        })
        .unwrap();
    assert_eq!(receipt.rows, committed);
    assert_eq!(
        store
            .publish_validated_concatenated(committed, &published, |path| {
                decode_final_i32_rows(path, committed).map(|_| ())
            })
            .unwrap(),
        receipt
    );
    let mut values = decode_final_i32_rows(&published, committed).unwrap();
    values.sort_unstable();
    assert_eq!(values, [7, 8, 9]);
    let jsonl_committed =
        ironbeam_dataflow::portable_metrics::committed_sink_input_count(&metrics, &jsonl_transform)
            .unwrap();
    assert_eq!(jsonl_committed, 3);
    let jsonl_staging = capture.path().join("jsonl-fragments");
    let jsonl_store = ironbeam_io_jsonl::jsonl_fragment_store(
        &jsonl_staging,
        &jsonl_staging.to_string_lossy(),
        "prism-run-fixture",
        "production-jsonl-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("production-jsonl-final").unwrap())
    .unwrap();
    let jsonl_fragments = jsonl_store.accepted_fragments().unwrap();
    assert!(
        jsonl_fragments.len() >= 2,
        "production JSONL output did not finalize multiple bundles: {jsonl_fragments:?}"
    );
    assert_eq!(
        jsonl_fragments
            .iter()
            .map(|fragment| fragment.rows)
            .sum::<u64>(),
        jsonl_committed
    );
    let jsonl_published = capture.path().join("published.jsonl");
    assert!(!jsonl_published.exists());
    let jsonl_receipt = ironbeam_io_jsonl::publish_jsonl_fragments::<i32>(
        &jsonl_store,
        jsonl_committed,
        &jsonl_published,
    )
    .unwrap();
    assert_eq!(jsonl_receipt.rows, jsonl_committed);
    assert_eq!(
        ironbeam_io_jsonl::publish_jsonl_fragments::<i32>(
            &jsonl_store,
            jsonl_committed,
            &jsonl_published,
        )
        .unwrap(),
        jsonl_receipt
    );
    let mut jsonl_values = ironbeam_io_jsonl::jsonl_source::<i32>(
        &Pipeline::default(),
        "verify-production-jsonl",
        &jsonl_published,
    )
    .unwrap()
    .output("verify-production-jsonl-out")
    .unwrap()
    .collect_seq()
    .unwrap();
    jsonl_values.sort_unstable();
    assert_eq!(jsonl_values, [7, 8, 9]);
    let object_storage: Arc<dyn ObjectIO> =
        Arc::new(LocalObjectIO::new(capture.path().join("object-results")).unwrap());
    let object_store = ironbeam_io_jsonl::cloud_jsonl_fragment_store(
        Arc::clone(&object_storage),
        ("stage", "production/jsonl"),
        ("output", "production.jsonl"),
        &directory,
        "prism-run-fixture",
        "production-object-jsonl-final",
    )
    .unwrap()
    .require_bundle_attempts(
        ready
            .bundle_attempts("production-object-jsonl-final")
            .unwrap(),
    )
    .unwrap();
    let object_rows = ready
        .committed_rows("production-object-jsonl-final")
        .unwrap();
    assert_eq!(object_rows, 3);
    assert!(!object_storage
        .object_exists("output", "production.jsonl")
        .unwrap());
    let object_receipt =
        ironbeam_io_jsonl::publish_cloud_jsonl_fragments::<i32>(&object_store, object_rows)
            .unwrap();
    assert_eq!(object_receipt.rows, object_rows);
    assert_eq!(
        ironbeam_io_jsonl::publish_cloud_jsonl_fragments::<i32>(&object_store, object_rows)
            .unwrap(),
        object_receipt
    );
    let mut object_values = object_storage
        .get_object("output", "production.jsonl")
        .unwrap()
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<i32>(line).unwrap())
        .collect::<Vec<_>>();
    object_values.sort_unstable();
    assert_eq!(object_values, [7, 8, 9]);
    let protobuf_committed = ironbeam_dataflow::portable_metrics::committed_sink_input_count(
        &metrics,
        &protobuf_transform,
    )
    .unwrap();
    let protobuf_staging = capture.path().join("protobuf-fragments");
    let protobuf_store = ironbeam_io_protobuf::protobuf_fragment_store(
        &protobuf_staging,
        &protobuf_staging.to_string_lossy(),
        "prism-run-fixture",
        "production-protobuf-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("production-protobuf-final").unwrap())
    .unwrap();
    let protobuf_fragments = protobuf_store.accepted_fragments().unwrap();
    assert_eq!(
        protobuf_committed, 3,
        "sink and input counts: {sink_count_diagnostics:?}; accepted protobuf fragments: {protobuf_fragments:?}"
    );
    // Prism may coalesce downstream format writes after the source residual
    // bundles. The source and primary sink above retain the split witness;
    // each format must still publish the exact committed rows.
    assert!(!protobuf_fragments.is_empty());
    assert_eq!(
        protobuf_fragments
            .iter()
            .map(|fragment| fragment.rows)
            .sum::<u64>(),
        protobuf_committed
    );
    let protobuf_published = capture.path().join("published.protobuf");
    assert!(!protobuf_published.exists());
    let protobuf_receipt = ironbeam_io_protobuf::publish_protobuf_fragments::<ProductionProtoRow>(
        &protobuf_store,
        protobuf_committed,
        &protobuf_published,
    )
    .unwrap();
    assert_eq!(protobuf_receipt.rows, protobuf_committed);
    assert_eq!(
        ironbeam_io_protobuf::publish_protobuf_fragments::<ProductionProtoRow>(
            &protobuf_store,
            protobuf_committed,
            &protobuf_published,
        )
        .unwrap(),
        protobuf_receipt
    );
    let mut protobuf_values = ironbeam_io_protobuf::protobuf_source::<ProductionProtoRow>(
        &Pipeline::default(),
        "verify-production-protobuf",
        &protobuf_published,
    )
    .unwrap()
    .output("verify-production-protobuf-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .into_iter()
    .map(|row| row.value)
    .collect::<Vec<_>>();
    protobuf_values.sort_unstable();
    assert_eq!(protobuf_values, [7, 8, 9]);
    let avro_committed =
        ironbeam_dataflow::portable_metrics::committed_sink_input_count(&metrics, &avro_transform)
            .unwrap();
    let avro_staging = capture.path().join("avro-fragments");
    let avro_store = ironbeam_io_avro::avro_fragment_store(
        &avro_staging,
        &avro_staging.to_string_lossy(),
        "prism-run-fixture",
        "production-avro-final",
        PRODUCTION_AVRO_SCHEMA,
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("production-avro-final").unwrap())
    .unwrap();
    let avro_fragments = avro_store.accepted_fragments().unwrap();
    assert_eq!(
        avro_committed, 3,
        "sink and input counts: {sink_count_diagnostics:?}; accepted Avro fragments: {avro_fragments:?}"
    );
    assert!(
        !avro_fragments.is_empty(),
        "production Avro output did not finalize: {avro_fragments:?}"
    );
    assert_eq!(
        avro_fragments
            .iter()
            .map(|fragment| fragment.rows)
            .sum::<u64>(),
        avro_committed
    );
    let avro_published = capture.path().join("published.avro");
    assert!(!avro_published.exists());
    let avro_receipt =
        ironbeam_io_avro::publish_avro_fragments(&avro_store, avro_committed, &avro_published)
            .unwrap();
    assert_eq!(avro_receipt.rows, avro_committed);
    assert_eq!(
        ironbeam_io_avro::publish_avro_fragments(&avro_store, avro_committed, &avro_published,)
            .unwrap(),
        avro_receipt
    );
    let mut avro_rows = ironbeam_io_avro::avro_source::<ProductionAvroRow>(
        &Pipeline::default(),
        "verify-production-avro",
        &avro_published,
    )
    .unwrap()
    .output("verify-production-avro-out")
    .unwrap()
    .collect_seq()
    .unwrap();
    avro_rows.sort_by_key(|row| row.id);
    assert_eq!(
        avro_rows,
        [
            ProductionAvroRow {
                id: 7,
                label: "production-7".into()
            },
            ProductionAvroRow {
                id: 8,
                label: "production-8".into()
            },
            ProductionAvroRow {
                id: 9,
                label: "production-9".into()
            },
        ]
    );
    let tfrecord_committed = ironbeam_dataflow::portable_metrics::committed_sink_input_count(
        &metrics,
        &tfrecord_transform,
    )
    .unwrap();
    let tfrecord_staging = capture.path().join("tfrecord-fragments");
    let tfrecord_store = ironbeam_io_tfrecord::tfrecord_fragment_store(
        &tfrecord_staging,
        &tfrecord_staging.to_string_lossy(),
        "prism-run-fixture",
        "production-tfrecord-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("production-tfrecord-final").unwrap())
    .unwrap();
    let tfrecord_fragments = tfrecord_store.accepted_fragments().unwrap();
    assert_eq!(
        tfrecord_committed, 3,
        "sink and input counts: {sink_count_diagnostics:?}; accepted TFRecord fragments: {tfrecord_fragments:?}"
    );
    assert!(
        !tfrecord_fragments.is_empty(),
        "production TFRecord output did not finalize: {tfrecord_fragments:?}"
    );
    assert_eq!(
        tfrecord_fragments
            .iter()
            .map(|fragment| fragment.rows)
            .sum::<u64>(),
        tfrecord_committed
    );
    let tfrecord_published = capture.path().join("published.tfrecord");
    assert!(!tfrecord_published.exists());
    let tfrecord_receipt = ironbeam_io_tfrecord::publish_tfrecord_fragments(
        &tfrecord_store,
        tfrecord_committed,
        &tfrecord_published,
    )
    .unwrap();
    assert_eq!(tfrecord_receipt.rows, tfrecord_committed);
    assert_eq!(
        ironbeam_io_tfrecord::publish_tfrecord_fragments(
            &tfrecord_store,
            tfrecord_committed,
            &tfrecord_published,
        )
        .unwrap(),
        tfrecord_receipt
    );
    let mut tfrecord_values = ironbeam_io_tfrecord::tfrecord_source(
        &Pipeline::default(),
        "verify-production-tfrecord",
        &tfrecord_published,
    )
    .unwrap()
    .output("verify-production-tfrecord-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .into_iter()
    .map(|bytes| {
        <ProductionProtoRow as prost_v5::Message>::decode(bytes.as_slice())
            .unwrap()
            .value
    })
    .collect::<Vec<_>>();
    tfrecord_values.sort_unstable();
    assert_eq!(tfrecord_values, [7, 8, 9]);
    let msgpack_committed = ironbeam_dataflow::portable_metrics::committed_sink_input_count(
        &metrics,
        &msgpack_transform,
    )
    .unwrap();
    let msgpack_staging = capture.path().join("msgpack-fragments");
    let msgpack_store = ironbeam_io_msgpack::msgpack_fragment_store(
        &msgpack_staging,
        &msgpack_staging.to_string_lossy(),
        "prism-run-fixture",
        "production-msgpack-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("production-msgpack-final").unwrap())
    .unwrap();
    let msgpack_fragments = msgpack_store.accepted_fragments().unwrap();
    assert_eq!(
        msgpack_committed, 3,
        "sink and input counts: {sink_count_diagnostics:?}; accepted MessagePack fragments: {msgpack_fragments:?}"
    );
    assert!(
        !msgpack_fragments.is_empty(),
        "production MessagePack output did not finalize: {msgpack_fragments:?}"
    );
    assert_eq!(
        msgpack_fragments
            .iter()
            .map(|fragment| fragment.rows)
            .sum::<u64>(),
        msgpack_committed
    );
    let msgpack_published = capture.path().join("published.msgpack");
    assert!(!msgpack_published.exists());
    let msgpack_receipt = ironbeam_io_msgpack::publish_msgpack_fragments::<i32>(
        &msgpack_store,
        msgpack_committed,
        &msgpack_published,
    )
    .unwrap();
    assert_eq!(msgpack_receipt.rows, msgpack_committed);
    assert_eq!(
        ironbeam_io_msgpack::publish_msgpack_fragments::<i32>(
            &msgpack_store,
            msgpack_committed,
            &msgpack_published,
        )
        .unwrap(),
        msgpack_receipt
    );
    let mut msgpack_values = ironbeam_io_msgpack::msgpack_source::<i32>(
        &Pipeline::default(),
        "verify-production-msgpack",
        &msgpack_published,
    )
    .unwrap()
    .output("verify-production-msgpack-out")
    .unwrap()
    .collect_seq()
    .unwrap();
    msgpack_values.sort_unstable();
    assert_eq!(msgpack_values, [7, 8, 9]);
    let cbor_committed =
        ironbeam_dataflow::portable_metrics::committed_sink_input_count(&metrics, &cbor_transform)
            .unwrap();
    let cbor_staging = capture.path().join("cbor-fragments");
    let cbor_store = ironbeam_io_cbor::cbor_fragment_store(
        &cbor_staging,
        &cbor_staging.to_string_lossy(),
        "prism-run-fixture",
        "production-cbor-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("production-cbor-final").unwrap())
    .unwrap();
    let cbor_fragments = cbor_store.accepted_fragments().unwrap();
    assert_eq!(
        cbor_committed, 3,
        "sink and input counts: {sink_count_diagnostics:?}; accepted CBOR fragments: {cbor_fragments:?}"
    );
    assert!(
        !cbor_fragments.is_empty(),
        "production CBOR output did not finalize: {cbor_fragments:?}"
    );
    assert_eq!(
        cbor_fragments
            .iter()
            .map(|fragment| fragment.rows)
            .sum::<u64>(),
        cbor_committed
    );
    let cbor_published = capture.path().join("published.cbor");
    assert!(!cbor_published.exists());
    let cbor_receipt = ironbeam_io_cbor::publish_cbor_fragments::<i32>(
        &cbor_store,
        cbor_committed,
        &cbor_published,
    )
    .unwrap();
    assert_eq!(cbor_receipt.rows, cbor_committed);
    assert_eq!(
        ironbeam_io_cbor::publish_cbor_fragments::<i32>(
            &cbor_store,
            cbor_committed,
            &cbor_published,
        )
        .unwrap(),
        cbor_receipt
    );
    let mut cbor_values = ironbeam_io_cbor::cbor_source::<i32>(
        &Pipeline::default(),
        "verify-production-cbor",
        &cbor_published,
    )
    .unwrap()
    .output("verify-production-cbor-out")
    .unwrap()
    .collect_seq()
    .unwrap();
    cbor_values.sort_unstable();
    assert_eq!(cbor_values, [7, 8, 9]);
}

#[tokio::test]
async fn prism_runs_timestamped_unbounded_test_stream() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let capture = tempfile::NamedTempFile::new().unwrap();
    let path = capture.path().to_string_lossy().into_owned();
    let pipeline = Pipeline::default();
    pipeline
        .test_stream_source(
            "stream-events",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceProcessingTime(1_000_000),
                TestStreamEvent::Elements(vec![(2_i32, 3_000_000)]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
            ],
        )
        .unwrap()
        .apply_transform_with_semantic_named(
            "stream-double",
            1,
            "prism-proof/stream-double",
            Version { major: 1, minor: 0 },
            |values| values.into_iter().map(|value| value * 2).collect(),
        )
        .unwrap()
        .map_batches_named("stream-batch-adjust", 1, |values| vec![values[0] + 1])
        .unwrap()
        .apply_dyn_transform_with_semantic_named::<i32>(
            "stream-expand",
            1,
            "prism-proof/stream-expand",
            Version { major: 1, minor: 0 },
            Arc::new(ExpandSingleton),
        )
        .unwrap()
        .map_named("stream-rows", |id| UserRow {
            id: id + 10,
            label: "stream".into(),
        })
        .unwrap()
        .fixed_windows("stream-windows", 2_000_000)
        .unwrap()
        .with_window_named("stream-window-context")
        .unwrap()
        .map_named("stream-label", |(window, mut row)| {
            row.label = format!("{}..{}", window.start_ns, window.end_ns);
            row
        })
        .unwrap()
        .sink_named_with_config(
            "stream-capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{path}"),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-run-fixture").unwrap();
    let worker = env!("CARGO_BIN_EXE_v5_prism_worker");
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
        command: worker.into(),
        env: [("IRONBEAM_V5_PRISM_STREAM_CAPTURE".into(), path.clone())].into(),
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
            job_name: format!("ironbeam-v5-stream-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism test-stream job did not reach DONE: {terminal:?}"
    );
    let mut rows = std::fs::read_to_string(capture.path())
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(
        rows,
        [
            "13:0..2000000",
            "15:2000000..4000000",
            "23:0..2000000",
            "25:2000000..4000000",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_explicit_custom_batch_sizes() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-custom-batches.txt");
    let portable_path = directory.path().join("portable-custom-batches.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink =
        explicit_custom_batches_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_lengths = |path: &Path| {
        let mut lengths = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| line.parse::<i32>().unwrap())
            .collect::<Vec<_>>();
        lengths.sort_unstable();
        lengths
    };
    assert_eq!(read_lengths(&direct_path), [1, 2, 2]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    explicit_custom_batches_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "explicit-custom-batches",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_CUSTOM_BATCHES".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism custom batch state: {terminal:?}");
    assert_eq!(read_lengths(&portable_path), [1, 2, 2]);
}

#[tokio::test]
async fn prism_matches_direct_dead_letter_fanout_and_merge() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-dead-letter.txt");
    let portable_path = directory.path().join("portable-dead-letter.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = dead_letter_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "error:bad:invalid integer",
        "error:x:invalid integer",
        "good:3",
        "good:7",
    ];
    assert_eq!(read_rows(&direct_path), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    dead_letter_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "dead-letter-contract",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DEAD_LETTER".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism dead-letter state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_validation_skip_and_fail_fast() {
    let directory = tempfile::tempdir().unwrap();
    let direct_skip = directory.path().join("direct-validation-skip.txt");
    let direct_fail = directory.path().join("direct-validation-fail.txt");
    let portable_skip = directory.path().join("portable-validation-skip.txt");
    let portable_fail = directory.path().join("portable-validation-fail.txt");

    let direct_pipeline = Pipeline::default();
    let skip_sink = validation_skip_fixture(&direct_pipeline, direct_skip.clone()).unwrap();
    DirectRunner
        .run_sink(&skip_sink, &JobConfig::default())
        .unwrap();
    let mut direct_rows = std::fs::read_to_string(&direct_skip)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    direct_rows.sort();
    assert_eq!(direct_rows, ["ok:1:one", "ok:2:two"]);

    let direct_pipeline = Pipeline::default();
    let fail_sink = validation_fail_fixture(&direct_pipeline, direct_fail.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&fail_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("validation-check"));
    assert!(
        error.message.contains("user transform panicked"),
        "{error:?}"
    );
    assert!(!direct_fail.exists());

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    validation_skip_fixture(&portable_pipeline, portable_skip.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "validation-skip",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_VALIDATION_SKIP".into(),
            portable_skip.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism validation skip state: {terminal:?}");
    let mut portable_rows = std::fs::read_to_string(&portable_skip)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    portable_rows.sort();
    assert_eq!(portable_rows, direct_rows);

    let portable_pipeline = Pipeline::default();
    validation_fail_fixture(&portable_pipeline, portable_fail.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "validation-fail",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_VALIDATION_FAIL".into(),
            portable_fail.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 4, "Prism validation fail state: {terminal:?}");
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("Execution at Some(\"validation-check\")")
                && message.contains("user transform panicked: Validation failed")
        }),
        "Prism did not report the validation failure: {:?}",
        terminal.1
    );
    assert!(!portable_fail.exists());
}

#[tokio::test]
async fn prism_matches_direct_ten_way_cogroup() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-ten-cogroup.txt");
    let portable_path = directory.path().join("portable-ten-cogroup.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        bounded_ten_way_cogroup_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "x|[1, 1, 1, 1, 0, 1, 1, 1, 1, 1]",
        "y|[0, 0, 0, 0, 0, 0, 0, 0, 0, 1]",
    ];
    assert_eq!(read_rows(&direct_path), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    bounded_ten_way_cogroup_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "ten-cogroup",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TEN_COGROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism ten-way co-group state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_twelve_way_homogeneous_cogroup() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-homogeneous-cogroup.txt");
    let portable_path = directory.path().join("portable-homogeneous-cogroup.txt");
    let direct = Pipeline::default();
    let sink = homogeneous_twelve_way_cogroup_fixture(&direct, direct_path.clone()).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let mut a = (0..12).map(|index| vec![index]).collect::<Vec<_>>();
    a[0] = vec![0, 100];
    a[4].clear();
    let mut z = vec![Vec::<i32>::new(); 12];
    z[11].push(99);
    let mut expected = vec![
        format!("0..10000000:1000000:a:{a:?}"),
        format!("0..10000000:0:z:{z:?}"),
    ];
    expected.sort();
    assert_eq!(rows(&direct_path), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable = Pipeline::default();
    homogeneous_twelve_way_cogroup_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "homogeneous-twelve-cogroup",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_HOMOGENEOUS_COGROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism twelve-way co-group state: {terminal:?}"
    );
    assert_eq!(rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_twelve_branch_session_cogroup() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-homogeneous-session.txt");
    let portable_path = directory.path().join("portable-homogeneous-session.txt");
    let direct = Pipeline::default();
    let sink = homogeneous_twelve_way_session_fixture(&direct, direct_path.clone()).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let mut buckets = vec![Vec::<i32>::new(); 12];
    buckets[0] = vec![10, 80];
    buckets[11] = vec![40];
    let expected = format!("0..13000000:a:{buckets:?}");
    assert_eq!(
        std::fs::read_to_string(&direct_path).unwrap().trim(),
        expected
    );

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable = Pipeline::default();
    homogeneous_twelve_way_session_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "homogeneous-twelve-session",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_HOMOGENEOUS_SESSION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism session co-group state: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&portable_path).unwrap().trim(),
        expected
    );
}

#[tokio::test]
async fn prism_matches_direct_three_way_windowed_cogroup() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-three-cogroup.txt");
    let portable_path = directory.path().join("portable-three-cogroup.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        bounded_three_way_cogroup_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "a|[1]|[10]|[100]|1000000|0|4000000",
        "b|[2]|[]|[]|5000000|4000000|8000000",
        "c|[]|[30]|[300]|6000000|4000000|8000000",
    ];
    assert_eq!(read_rows(&direct_path), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    bounded_three_way_cogroup_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "three-cogroup",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_THREE_COGROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism three-way co-group state: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_matches_direct_bounded_runner_contract() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct.txt");
    let portable_path = directory.path().join("portable.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = bounded_map_filter_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();

    let portable_pipeline = Pipeline::default();
    bounded_map_filter_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_BOUNDED_CONTRACT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-bounded-contract-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism bounded contract did not reach DONE: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| line.parse::<i32>().unwrap())
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    assert_eq!(read_rows(&direct_path), [4, 5, 8, 9]);
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_matches_direct_singleton_map_and_multimap_side_views() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-side-shapes.txt");
    let portable_path = directory.path().join("portable-side-shapes.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = side_view_shapes_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "map:a:10",
            "map:b:20",
            "map:missing:0",
            "multi:a:[1, 2]",
            "multi:b:[3]",
            "multi:missing:[]",
            "single:a:100",
            "single:b:100",
            "single:missing:100",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    side_view_shapes_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "side-view-shapes",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SIDE_VIEW_SHAPES".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism side view state: {terminal:?}");
    assert_eq!(rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_windowed_singleton_map_and_multimap_views() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-windowed-side-shapes.txt");
    let portable_path = directory.path().join("portable-windowed-side-shapes.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        windowed_side_view_shapes_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "map:a:1:10",
            "map:a:2:20",
            "map:b:3:30",
            "multi:a:1:[1, 2]",
            "multi:a:2:[3]",
            "multi:b:3:[4]",
            "single:a:1:10",
            "single:a:2:20",
            "single:b:3:30",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    windowed_side_view_shapes_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "windowed-side-view-shapes",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_WINDOWED_SIDE_VIEW_SHAPES".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism windowed side view state: {terminal:?}"
    );
    assert_eq!(rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_and_direct_reject_duplicate_windowed_map_keys_before_commit() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-windowed-duplicate-map.txt");
    let portable_path = directory.path().join("portable-windowed-duplicate-map.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        windowed_duplicate_map_side_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .expect("duplicate map keys must fail Direct");
    assert!(error.to_string().contains("duplicate key"), "{error}");
    assert!(!direct_path.exists());
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    windowed_duplicate_map_side_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "windowed-duplicate-map",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_WINDOWED_DUPLICATE_MAP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 4, "Prism duplicate map state: {terminal:?}");
    assert!(
        terminal
            .1
            .iter()
            .any(|message| message.contains("duplicate key")),
        "Prism duplicate map messages: {terminal:?}"
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_matches_direct_windowed_main_with_global_side_view() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-side.txt");
    let portable_path = directory.path().join("portable-side.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = bounded_side_input_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();

    let portable_pipeline = Pipeline::default();
    bounded_side_input_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_SIDE_INPUT_CONTRACT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-side-input-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(terminal, Some(3), "Prism side input state: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(
        read_rows(&direct_path),
        ["0..2000000:31", "0..2000000:32", "2000000..4000000:33"]
    );
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_matches_direct_per_window_side_view_and_main_time() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-window-side.txt");
    let portable_path = directory.path().join("portable-window-side.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = windowed_side_input_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();

    let portable_pipeline = Pipeline::default();
    windowed_side_input_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "window-side",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_WINDOWED_SIDE_CONTRACT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism windowed side state: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(
        read_rows(&direct_path),
        [
            "0..2000000:0:11",
            "0..2000000:1000000:12",
            "2000000..4000000:2000000:23",
            "4000000..6000000:4000000:4",
        ]
    );
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_matches_direct_two_unbounded_windowed_join() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dual-join.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = dual_unbounded_join_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "dual-join-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-direct-result-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    dual_unbounded_join_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-join",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_JOIN".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism dual join state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(rows, direct);
    assert_eq!(
        rows,
        [
            "0..4000000:1000000:a:Some(1):Some(10)",
            "4000000..8000000:5000000:b:None:Some(20)",
            "4000000..8000000:5000000:c:Some(3):None",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_two_unbounded_merged_session_join() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable-dual-session.txt");
    let direct_pipeline = Pipeline::default();
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_sink = dual_unbounded_session_join_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "dual-session-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "direct-dual-session-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort_unstable();
    assert_eq!(direct, ["0..13000000:a:1:10", "0..13000000:a:2:10",]);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let pipeline = Pipeline::default();
    dual_unbounded_session_join_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-session-join",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_SESSION_JOIN".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism dual session join state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct);
}

#[tokio::test]
async fn prism_matches_direct_two_unbounded_merged_session_cogroup() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable-dual-session-cogroup.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = dual_unbounded_session_cogroup_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "dual-session-cogroup-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "direct-dual-session-cogroup-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort_unstable();
    assert_eq!(
        direct,
        ["0..13000000:a:[1, 2]:[10]", "0..5000000:b:[99]:[]"]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let pipeline = Pipeline::default();
    dual_unbounded_session_cogroup_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-session-cogroup",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_SESSION_COGROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism dual session co-group state: {terminal:?}"
    );
    let mut portable = std::fs::read_to_string(portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    portable.sort_unstable();
    assert_eq!(portable, direct);
}

#[tokio::test]
async fn prism_matches_direct_each_two_unbounded_windowed_join_kind() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for kind in ["inner", "left", "right", "full"] {
        let path = directory.path().join(format!("dual-{kind}.txt"));
        let direct_rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let direct_sink = dual_unbounded_join_kind_collection(&direct_pipeline, kind)
            .unwrap()
            .sink_named_with_config(
                "dual-kind-result",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("dual-kind-{kind}-direct-result-v1"),
                {
                    let direct_rows = Arc::clone(&direct_rows);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&direct_rows),
                            accepted: Arc::clone(&accepted),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let portable_pipeline = Pipeline::default();
        dual_unbounded_join_kind_fixture(&portable_pipeline, kind, path.clone()).unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &portable_pipeline,
            &format!("dual-{kind}-join"),
            HashMap::from([
                ("IRONBEAM_V5_PRISM_DUAL_JOIN_KIND".into(), kind.into()),
                (
                    "IRONBEAM_V5_PRISM_DUAL_JOIN_KIND_PATH".into(),
                    path.to_string_lossy().into_owned(),
                ),
            ]),
        )
        .await;
        assert_eq!(terminal.0, 3, "Prism {kind} join state: {terminal:?}");
        let mut rows = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        let mut direct = direct_rows.lock().unwrap().clone();
        direct.sort();
        let expected: &[&str] = match kind {
            "inner" => &["I|a|1|10|1000000|0|4000000"],
            "left" => &[
                "L|a|1|Some(10)|1000000|0|4000000",
                "L|c|3|None|5000000|4000000|8000000",
            ],
            "right" => &[
                "R|a|Some(1)|10|1000000|0|4000000",
                "R|b|None|20|5000000|4000000|8000000",
            ],
            "full" => &[
                "F|a|Some(1)|Some(10)|1000000|0|4000000",
                "F|b|None|Some(20)|5000000|4000000|8000000",
                "F|c|Some(3)|None|5000000|4000000|8000000",
            ],
            _ => unreachable!(),
        };
        assert_eq!(direct, expected, "Direct {kind} join rows");
        assert_eq!(rows, direct, "Prism {kind} join rows");
    }
}

#[tokio::test]
async fn prism_matches_direct_late_two_unbounded_join() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late-join.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = dual_unbounded_late_join_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "late-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "late-direct-result-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    dual_unbounded_late_join_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-late-join",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_LATE_JOIN".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism late join state: {terminal:?}");
    assert_eq!(*direct_rows.lock().unwrap(), ["1:10:20"]);
    assert_eq!(std::fs::read_to_string(path).unwrap(), "1:10:20\n");
}

#[tokio::test]
async fn prism_heterogeneous_cogroup_retains_late_pane() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("heterogeneous-late.txt");
    let pipeline = Pipeline::default();
    dual_positive_late_cogroup_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "heterogeneous-late-cogroup",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_POSITIVE_LATE_COGROUP".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "heterogeneous late co-group state: {terminal:?}"
    );
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "1:([2], [\"right\"]):OnTime:true:false:0:0",
            "1:([3], []):Late:false:false:1:1",
        ]
    );
}

#[tokio::test]
async fn prism_heterogeneous_late_count_panes() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let read_rows = |path: &std::path::Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let expected = [
        "1:([2], [\"right\"]):OnTime:true:false:0:0",
        "1:([3, 5], []):Late:false:false:1:1",
    ];
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = heterogeneous_late_count_cogroup_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "heterogeneous-late-count-direct",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "heterogeneous-late-count-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort_unstable();
    assert_eq!(direct, expected);
    let portable_path = directory.path().join("heterogeneous-prism.txt");
    let portable_pipeline = Pipeline::default();
    heterogeneous_late_count_cogroup_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "heterogeneous-late-count-cogroup",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_HETEROGENEOUS_LATE_COUNT_COGROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "heterogeneous late-count state: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_typed_join_fires_late_count_and_partial_expiry_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late-count-join.txt");
    let pipeline = Pipeline::default();
    late_count_join_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "late-count-join",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_LATE_COUNT_JOIN".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "late count join state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    let mut expected = vec![
        "1:(Some(2), Some(4)):OnTime:true:false:0:0",
        "1:(Some(3), None):Late:false:false:1:1",
        "1:(Some(5), None):Late:false:false:1:1",
        "1:(Some(7), None):Late:false:true:2:2",
    ];
    expected.sort_unstable();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn prism_accumulating_typed_join_retains_on_time_rows_in_late_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("accumulating-late-join.txt");
    let pipeline = Pipeline::default();
    accumulating_late_join_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "accumulating-late-join",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_ACCUMULATING_LATE_JOIN".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism accumulating join state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "1:(Some(2), Some(4)):Late:false:true:1:1",
            "1:(Some(2), Some(4)):OnTime:true:false:0:0",
            "1:(Some(3), Some(4)):Late:false:true:1:1",
            "1:(Some(5), Some(4)):Late:false:true:1:1",
            "1:(Some(7), Some(4)):Late:false:true:1:1",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_late_count_for_every_typed_join_kind() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for (kind, expected) in [
        ("inner", vec!["1:(2, 4):OnTime:true:false:0:0"]),
        (
            "left",
            vec![
                "1:(2, Some(4)):OnTime:true:false:0:0",
                "1:(3, None):Late:false:false:1:1",
                "1:(5, None):Late:false:false:1:1",
                "1:(7, None):Late:false:true:2:2",
            ],
        ),
        ("right", vec!["1:(Some(2), 4):OnTime:true:false:0:0"]),
        (
            "full",
            vec![
                "1:(Some(2), Some(4)):OnTime:true:false:0:0",
                "1:(Some(3), None):Late:false:false:1:1",
                "1:(Some(5), None):Late:false:false:1:1",
                "1:(Some(7), None):Late:false:true:2:2",
            ],
        ),
    ] {
        let direct_rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let sink = late_count_join_kind_collection(&direct_pipeline, kind)
            .unwrap()
            .sink_named_with_config(
                "late-count-kind-direct",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("late-count-kind-{kind}-direct-v1"),
                {
                    let rows = Arc::clone(&direct_rows);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&rows),
                            accepted: Arc::clone(&accepted),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let mut direct = direct_rows.lock().unwrap().clone();
        direct.sort_unstable();
        let mut expected = expected;
        expected.sort_unstable();
        assert_eq!(direct, expected, "Direct {kind}");

        let path = directory.path().join(format!("late-count-{kind}.txt"));
        let portable_pipeline = Pipeline::default();
        late_count_join_kind_fixture(&portable_pipeline, kind, path.clone()).unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &portable_pipeline,
            &format!("late-count-{kind}-join"),
            HashMap::from([
                ("IRONBEAM_V5_PRISM_LATE_COUNT_JOIN_KIND".into(), kind.into()),
                (
                    "IRONBEAM_V5_PRISM_LATE_COUNT_JOIN_KIND_PATH".into(),
                    path.to_string_lossy().into_owned(),
                ),
            ]),
        )
        .await;
        assert_eq!(terminal.0, 3, "Prism {kind} state: {terminal:?}");
        let mut portable = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        portable.sort_unstable();
        assert_eq!(portable, expected, "Prism {kind}");
    }
}

#[tokio::test]
async fn prism_sliding_late_join_characterizes_final_pane_timing() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sliding-late-join.txt");
    let pipeline = Pipeline::default();
    sliding_late_join_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "sliding-late-join",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SLIDING_LATE_JOIN".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "sliding late join state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    let mut expected = vec![
        "0..10000000:1:(Some(2), Some(4)):OnTime:true:false:0:0",
        "0..10000000:1:(Some(3), None):Late:false:true:1:1",
        "5000000..15000000:1:(Some(2), Some(4)):OnTime:true:false:0:0",
        "5000000..15000000:1:(Some(3), Some(4)):OnTime:true:false:0:0",
    ];
    expected.sort_unstable();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn prism_late_full_join_uses_each_panes_output_time_policy() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    for (policy, environment, on_time, late) in [
        (
            ironbeam_core::WindowOutputTime::EndOfWindow,
            "IRONBEAM_V5_PRISM_DUAL_POSITIVE_LATE_END_TIME",
            9_000_000,
            9_000_000,
        ),
        (
            ironbeam_core::WindowOutputTime::LatestInPane,
            "IRONBEAM_V5_PRISM_DUAL_POSITIVE_LATE_LATEST_TIME",
            4_000_000,
            3_000_000,
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("late-join-time.txt");
        let pipeline = Pipeline::default();
        dual_positive_late_join_time_fixture(&pipeline, path.clone(), policy).unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            "positive-late-join-output-time",
            HashMap::from([(environment.into(), path.to_string_lossy().into_owned())]),
        )
        .await;
        assert_eq!(terminal.0, 3, "late join time state: {terminal:?}");
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        let mut expected = vec![
            format!("{on_time}:1:(Some(2), Some(4)):OnTime:true:false:0:0"),
            format!("{late}:1:(Some(3), None):Late:false:false:1:1"),
        ];
        expected.sort_unstable();
        assert_eq!(rows, expected, "policy: {policy:?}");
    }
}

#[tokio::test]
async fn prism_full_join_preserves_late_pane_and_drops_expired_row() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("positive-late-join.txt");
    let pipeline = Pipeline::default();
    dual_positive_late_full_join_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "positive-late-full-join",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_POSITIVE_LATE_JOIN".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "positive late join state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "1:(Some(2), Some(4)):OnTime:true:false:0:0",
            "1:(Some(3), None):Late:false:false:1:1",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_two_source_late_cogroup_panes() {
    assert_two_source_late_cogroup_panes(false).await;
}

#[tokio::test]
async fn prism_matches_direct_two_source_late_count_panes() {
    assert_two_source_late_cogroup_panes(true).await;
}

async fn assert_two_source_late_cogroup_panes(count_trigger: bool) {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("portable.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_collection = if count_trigger {
        dual_late_count_cogroup_collection(&direct_pipeline)
    } else {
        dual_late_cogroup_collection(&direct_pipeline)
    };
    let direct_sink = direct_collection
        .unwrap()
        .sink_named_with_config(
            "dual-late-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-late-direct-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let portable_pipeline = Pipeline::default();
    if count_trigger {
        dual_late_count_cogroup_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    } else {
        dual_late_cogroup_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    }
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        if count_trigger {
            "dual-late-count-cogroup"
        } else {
            "dual-late-cogroup"
        },
        HashMap::from([(
            if count_trigger {
                "IRONBEAM_V5_PRISM_DUAL_LATE_COUNT_COGROUP"
            } else {
                "IRONBEAM_V5_PRISM_DUAL_LATE_COGROUP"
            }
            .into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "dual late co-group state: {terminal:?}");
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort_unstable();
    if count_trigger {
        assert_eq!(
            direct,
            [
                "1:[[2], [4]]:OnTime:true:false:0:0",
                "1:[[3, 5], []]:Late:false:false:1:1",
                "1:[[7], []]:Late:false:true:2:2",
            ]
        );
    } else {
        assert_eq!(
            direct,
            [
                "1:[[2], [4]]:OnTime:true:false:0:0",
                "1:[[3], []]:Late:false:false:1:1",
            ]
        );
    }
    let mut portable = std::fs::read_to_string(&portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    portable.sort_unstable();
    assert_eq!(portable, direct);
}

#[tokio::test]
async fn prism_matches_direct_two_unbounded_windowed_cogroup() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dual-cogroup.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = dual_unbounded_cogroup_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "dual-cogroup-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-cogroup-direct-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    dual_unbounded_cogroup_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-cogroup",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_COGROUP".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism dual co-group state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(rows, direct);
    assert_eq!(
        rows,
        [
            "0..4000000:1000000:a:[1]:[10]",
            "4000000..8000000:5000000:b:[]:[20]",
            "4000000..8000000:5000000:c:[3]:[]",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_two_and_three_unbounded_windowed_flatten() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dual-flatten.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = dual_unbounded_flatten_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "dual-flatten-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-flatten-direct-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    dual_unbounded_flatten_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-flatten",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_FLATTEN".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism dual flatten state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(rows, direct);
    assert_eq!(
        rows,
        [
            "0..4000000:1000000:a:1",
            "0..4000000:2000000:a:10",
            "4000000..8000000:5000000:b:20",
            "4000000..8000000:5000000:c:3",
        ]
    );

    let triple_path = directory.path().join("triple-flatten.txt");
    let triple_direct_rows = Arc::new(Mutex::new(Vec::new()));
    let triple_accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = triple_unbounded_flatten_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "triple-flatten-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "triple-flatten-direct-v1".into(),
            {
                let rows = Arc::clone(&triple_direct_rows);
                let accepted = Arc::clone(&triple_accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    triple_unbounded_flatten_fixture(&pipeline, triple_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "triple-flatten",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TRIPLE_FLATTEN".into(),
            triple_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism triple flatten state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&triple_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct = triple_direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(rows, direct);
    assert_eq!(
        rows,
        [
            "0..4000000:1000000:a:1",
            "0..4000000:2000000:a:10",
            "0..4000000:2000000:d:30",
            "4000000..8000000:5000000:b:20",
            "4000000..8000000:5000000:c:3",
            "4000000..8000000:5000000:e:40",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_three_unbounded_homogeneous_cogroup() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("triple-homogeneous.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = triple_unbounded_homogeneous_cogroup_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "triple-homogeneous-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "triple-homogeneous-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut expected = vec![
        "0..4000000:1000000:a:[[1], [10], [30]]".to_owned(),
        "4000000..8000000:5000000:b:[[], [20], [40]]".to_owned(),
        "4000000..8000000:5000000:c:[[3], [], []]".to_owned(),
    ];
    expected.sort();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(direct, expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable = Pipeline::default();
    triple_unbounded_homogeneous_cogroup_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "triple-homogeneous",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TRIPLE_HOMOGENEOUS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism triple co-group state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn prism_matches_direct_shared_unbounded_homogeneous_cogroup() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("shared-homogeneous.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = shared_unbounded_homogeneous_cogroup_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "shared-homogeneous-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-homogeneous-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let expected = ["0..10000000:1000000:a:[[10], [20], [30]]".to_owned()];
    assert_eq!(*direct_rows.lock().unwrap(), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable = Pipeline::default();
    shared_unbounded_homogeneous_cogroup_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "shared-homogeneous",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SHARED_HOMOGENEOUS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism shared co-group state: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(&portable_path).unwrap().trim(),
        expected[0]
    );
}

#[tokio::test]
async fn prism_matches_direct_shared_accumulating_late_cogroup_values_and_indices() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = shared_accumulating_late_cogroup_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "shared-accumulating-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-accumulating-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let expected = ["a:[[2], [4], []]:OnTime:0", "a:[[2, 3, 5], [4], []]:Late:1"];
    assert_eq!(*direct_rows.lock().unwrap(), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("shared-accumulating-late.txt");
    let portable = Pipeline::default();
    shared_accumulating_late_cogroup_fixture(&portable, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "shared-accumulating-late",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SHARED_ACCUMULATING_LATE".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism accumulating fan-in state: {terminal:?}"
    );
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn prism_characterizes_shared_accumulating_early_cogroup_panes() {
    assert_accumulating_early_cogroup_panes(false).await;
}

#[tokio::test]
async fn prism_matches_global_accumulating_early_cogroup_pane_identities() {
    assert_accumulating_early_cogroup_panes(true).await;
}

async fn assert_accumulating_early_cogroup_panes(global: bool) {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct = Pipeline::default();
    let collection = if global {
        global_accumulating_early_cogroup_collection(&direct)
    } else {
        shared_accumulating_early_cogroup_collection(&direct)
    };
    let sink = collection
        .unwrap()
        .sink_named_with_config(
            "shared-early-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-early-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let direct_rows = direct_rows.lock().unwrap().clone();
    assert_eq!(
        direct_rows,
        [
            "a:[[2], []]:Early:0",
            "a:[[2], [4]]:Early:1",
            "a:[[2], [4]]:OnTime:2",
        ]
    );

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let path = directory.path().join(if global {
        "prism-global-early.txt"
    } else {
        "prism-early.txt"
    });
    let portable = Pipeline::default();
    if global {
        global_accumulating_early_cogroup_fixture(&portable, path.clone()).unwrap();
    } else {
        shared_accumulating_early_cogroup_fixture(&portable, path.clone()).unwrap();
    }
    let env_key = if global {
        "IRONBEAM_V5_PRISM_GLOBAL_ACCUMULATING_EARLY"
    } else {
        "IRONBEAM_V5_PRISM_SHARED_ACCUMULATING_EARLY"
    };
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        if global {
            "global-accumulating-early"
        } else {
            "shared-accumulating-early"
        },
        HashMap::from([(env_key.into(), path.to_string_lossy().into_owned())]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism early fan-in state: {terminal:?}");
    let mut prism_rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    prism_rows.sort();
    let mut direct_rows_sorted = direct_rows;
    direct_rows_sorted.sort();
    assert_eq!(prism_rows, direct_rows_sorted);
}

#[tokio::test]
async fn prism_reads_provider_bound_avro_with_a_defaulted_reader_field() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let seed_path = directory.path().join("seed.avro");
    let seed = Pipeline::default()
        .from_vec(
            "object-avro-seed",
            vec![
                ProductionAvroRow {
                    id: 6,
                    label: "first".into(),
                },
                ProductionAvroRow {
                    id: 7,
                    label: "second".into(),
                },
            ],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_avro::avro_sink(
                &seed,
                "object-avro-seed-sink",
                &seed_path,
                PRODUCTION_AVRO_SCHEMA,
            )
            .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    stores
        .write(
            "file://bucket/input/rows.avro",
            &std::fs::read(seed_path).unwrap(),
        )
        .unwrap();
    let direct_pipeline = Pipeline::default();
    let direct = portable_object_avro_collection(&direct_pipeline, &object_root)
        .unwrap()
        .output("object-avro-direct-output")
        .unwrap()
        .collect_seq()
        .unwrap();
    assert_eq!(direct, ["6:first:5", "7:second:5"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let path = directory.path().join("object-avro-result.txt");
    let portable = Pipeline::default();
    portable_object_avro_fixture(&portable, &object_root, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "provider-bound-avro",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_AVRO_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_AVRO_RESULT".into(),
                path.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "portable object Avro job: {terminal:?}");
    assert_eq!(
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["6:first:5", "7:second:5"]
    );
}

#[tokio::test]
async fn prism_reads_provider_bound_parquet_rows() {
    let directory = tempfile::tempdir().unwrap();
    let object_root = directory.path().join("objects");
    let seed_path = directory.path().join("seed.parquet");
    let seed = Pipeline::default()
        .from_vec(
            "object-parquet-seed",
            vec![
                ProductionAvroRow {
                    id: 8,
                    label: "third".into(),
                },
                ProductionAvroRow {
                    id: 9,
                    label: "fourth".into(),
                },
            ],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_parquet::parquet_sink(&seed, "object-parquet-seed-sink", &seed_path)
                .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    stores
        .write(
            "file://bucket/input/a.parquet",
            &std::fs::read(seed_path).unwrap(),
        )
        .unwrap();
    let second_path = directory.path().join("second.parquet");
    let second = Pipeline::default()
        .from_vec(
            "object-parquet-second-seed",
            vec![ProductionAvroRow {
                id: 10,
                label: "fifth".into(),
            }],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_parquet::parquet_sink(
                &second,
                "object-parquet-second-seed-sink",
                &second_path,
            )
            .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    stores
        .write(
            "file://bucket/input/b.parquet",
            &std::fs::read(second_path).unwrap(),
        )
        .unwrap();
    let direct_pipeline = Pipeline::default();
    let mut direct = portable_object_parquet_collection(&direct_pipeline, &object_root)
        .unwrap()
        .output("object-parquet-direct-output")
        .unwrap()
        .collect_seq()
        .unwrap();
    direct.sort();
    assert_eq!(direct, ["10:fifth", "8:third", "9:fourth"]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let path = directory.path().join("object-parquet-result.txt");
    let portable = Pipeline::default();
    portable_object_parquet_fixture(&portable, &object_root, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "provider-bound-parquet",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_OBJECT_PARQUET_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_PARQUET_RESULT".into(),
                path.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "portable object Parquet job: {terminal:?}");
    let result = std::fs::read_to_string(path).unwrap();
    let mut portable_rows = result.lines().collect::<Vec<_>>();
    portable_rows.sort_unstable();
    assert_eq!(portable_rows, ["10:fifth", "8:third", "9:fourth"]);
}

#[tokio::test]
async fn prism_matches_direct_three_unbounded_merged_sessions() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("triple-session.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = triple_unbounded_session_homogeneous_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "triple-session-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "triple-session-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut expected = vec![
        "0..13000000:a:[[1, 2], [10], []]".to_owned(),
        "0..5000000:b:[[99], [], []]".to_owned(),
        "0..5000000:c:[[], [], [77]]".to_owned(),
    ];
    expected.sort();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(direct, expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable = Pipeline::default();
    triple_unbounded_session_homogeneous_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "triple-session",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TRIPLE_SESSION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism triple session state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn prism_matches_direct_mixed_homogeneous_cogroup() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let expected = [
        "0..10000000:a:[[1], [10], []]".to_owned(),
        "0..10000000:b:[[], [20], []]".to_owned(),
    ];
    let direct_pipeline = Pipeline::default();
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_sink = mixed_homogeneous_cogroup_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "mixed-group-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "mixed-group-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(direct, expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed-homogeneous.txt");
    let portable = Pipeline::default();
    mixed_homogeneous_cogroup_fixture(&portable, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "mixed-homogeneous",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_MIXED_HOMOGENEOUS".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism mixed group state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn prism_matches_direct_mixed_bounded_and_unbounded_flatten() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let expected = [
        "0..2000000:1000000:10".to_owned(),
        "2000000..4000000:3000000:20".to_owned(),
        "4000000..6000000:4000000:30".to_owned(),
    ];
    let direct_pipeline = Pipeline::default();
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_sink = mixed_flatten_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "mixed-flat-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "mixed-flat-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(direct, expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed-flat.txt");
    let portable = Pipeline::default();
    mixed_flatten_fixture(&portable, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "mixed-flat",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_MIXED_FLATTEN".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism mixed Flatten state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn prism_runs_unbounded_main_with_per_window_side_view() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("stream-window-side.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = streaming_windowed_side_input_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "stream-window-side-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "direct-window-side-result-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    streaming_windowed_side_input_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "stream-window-side",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAM_WINDOWED_SIDE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism streaming side state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct_rows = direct_rows.lock().unwrap().clone();
    direct_rows.sort();
    assert_eq!(
        rows,
        [
            "0..2000000:1000000:11",
            "2000000..4000000:3000000:22",
            "4000000..6000000:5000000:3",
        ]
    );
    assert_eq!(rows, direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_two_unbounded_windowed_side_views_at_closure() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("dual-window-side.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = dual_stream_windowed_side_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "dual-window-side-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-window-side-direct-v1".into(),
            {
                let direct_rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&direct_rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    dual_stream_windowed_side_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-window-side",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_WINDOWED_SIDE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism dual window side state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct_rows = direct_rows.lock().unwrap().clone();
    direct_rows.sort();
    assert_eq!(rows, direct_rows);
    assert_eq!(
        rows,
        [
            "0..2000000:1000000:11",
            "2000000..4000000:3000000:43",
            "4000000..6000000:5000000:3",
        ]
    );

    let typed_path = directory.path().join("dual-typed-map-side.txt");
    let typed_direct_rows = Arc::new(Mutex::new(Vec::new()));
    let typed_accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = dual_unbounded_map_side_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "dual-typed-side-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-typed-side-direct-v1".into(),
            {
                let rows = Arc::clone(&typed_direct_rows);
                let accepted = Arc::clone(&typed_accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    dual_unbounded_map_side_fixture(&pipeline, typed_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-typed-map-side",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_TYPED_MAP_SIDE".into(),
            typed_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism dual typed side state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&typed_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct = typed_direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(rows, direct);
    assert_eq!(
        rows,
        ["0..4000000:1000000:a:11", "4000000..8000000:5000000:c:3"]
    );
}

#[tokio::test]
async fn prism_matches_direct_two_stream_windowed_wait_on() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("two-stream-wait.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = dual_stream_windowed_wait_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "stream-wait-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "stream-wait-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    dual_stream_windowed_wait_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "stream-wait-two-sources",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_WINDOWED_WAIT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism stream wait state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(rows, direct);
    assert_eq!(
        rows,
        [
            "0..2000000:1000000:1",
            "2000000..4000000:3000000:2",
            "4000000..6000000:5000000:3",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_three_stream_windowed_wait_on() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("three-stream-wait.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = triple_stream_windowed_wait_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "triple-wait-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "triple-wait-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    triple_stream_windowed_wait_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "stream-wait-three-sources",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TRIPLE_WINDOWED_WAIT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism stream wait state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(rows, direct);
    assert_eq!(
        rows,
        [
            "0..2000000:1000000:1",
            "2000000..4000000:3000000:2",
            "4000000..6000000:5000000:3",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_heterogeneous_windowed_wait_on() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let portable_path = directory.path().join("heterogeneous-wait.txt");
    let direct_rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let direct_pipeline = Pipeline::default();
    let direct_sink = heterogeneous_windowed_wait_collection(&direct_pipeline)
        .unwrap()
        .sink_named_with_config(
            "heterogeneous-wait-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "heterogeneous-wait-direct-v1".into(),
            {
                let rows = Arc::clone(&direct_rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(DirectRows {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let pipeline = Pipeline::default();
    heterogeneous_windowed_wait_fixture(&pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "stream-wait-heterogeneous-signals",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_HETEROGENEOUS_WINDOWED_WAIT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism stream wait state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&portable_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct = direct_rows.lock().unwrap().clone();
    direct.sort();
    assert_eq!(rows, direct);
    assert_eq!(rows, ["0..2000000:1000000:1", "2000000..4000000:3000000:2"]);
}

#[tokio::test]
async fn prism_matches_direct_bounded_wait_on() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-wait.txt");
    let direct_marker = directory.path().join("direct-ready");
    let portable_path = directory.path().join("portable-wait.txt");
    let portable_marker = directory.path().join("portable-ready");

    let direct_pipeline = Pipeline::default();
    let direct_sink =
        bounded_wait_on_fixture(&direct_pipeline, direct_path.clone(), direct_marker.clone())
            .unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    assert!(direct_marker.exists());

    let portable_pipeline = Pipeline::default();
    bounded_wait_on_fixture(
        &portable_pipeline,
        portable_path.clone(),
        portable_marker.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "wait-on",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_WAIT_CONTRACT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_WAIT_MARKER".into(),
                portable_marker.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism wait-on state: {terminal:?}");
    assert!(portable_marker.exists());
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(
        read_rows(&direct_path),
        ["0..2000000:1", "2000000..4000000:2"]
    );
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_matches_direct_bounded_multi_signal_wait() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-multi-wait.txt");
    let direct_first = directory.path().join("direct-first-ready");
    let direct_second = directory.path().join("direct-second-ready");
    let portable_path = directory.path().join("portable-multi-wait.txt");
    let portable_first = directory.path().join("portable-first-ready");
    let portable_second = directory.path().join("portable-second-ready");

    let direct = Pipeline::default();
    let sink = bounded_multi_wait_fixture(
        &direct,
        direct_path.clone(),
        direct_first.clone(),
        direct_second.clone(),
    )
    .unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert!(direct_first.exists() && direct_second.exists());

    let portable = Pipeline::default();
    bounded_multi_wait_fixture(
        &portable,
        portable_path.clone(),
        portable_first.clone(),
        portable_second.clone(),
    )
    .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "multi-wait-on",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_MULTI_WAIT_CONTRACT".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_MULTI_WAIT_FIRST".into(),
                portable_first.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_MULTI_WAIT_SECOND".into(),
                portable_second.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism multi-signal wait state: {terminal:?}");
    assert!(portable_first.exists() && portable_second.exists());
    let sorted_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(sorted_rows(&direct_path), ["1", "2"]);
    assert_eq!(sorted_rows(&portable_path), sorted_rows(&direct_path));
}

#[tokio::test]
async fn prism_and_direct_abort_when_wait_signal_fails() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-wait-failure.txt");
    let portable_path = directory.path().join("portable-wait-failure.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = failing_wait_on_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(error.message.contains("injected signal read failure"));
    assert!(!direct_path.exists());

    let portable_pipeline = Pipeline::default();
    failing_wait_on_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "wait-signal-failure",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_WAIT_FAILURE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 4, "Prism wait failure state: {terminal:?}");
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("Execution at Some(\"wait-failing-signal\")")
                && message.contains("injected signal read failure")
        }),
        "Prism did not report the expected typed signal error: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_and_direct_abort_on_dynamic_output_type_mismatch() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-invalid-dynop.txt");
    let portable_path = directory.path().join("portable-invalid-dynop.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = invalid_dynop_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("invalid-dynop-output"));
    assert!(error.message.contains("partition output type mismatch"));
    assert!(!direct_path.exists());

    let portable_pipeline = Pipeline::default();
    invalid_dynop_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "invalid-dynop-output",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_BAD_DYNOP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 4,
        "Prism dynamic type mismatch state: {terminal:?}"
    );
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("invalid-dynop-output")
                && message.contains("partition output type mismatch")
        }),
        "Prism did not report the dynamic type error: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_and_direct_abort_on_user_map_panic_without_commit() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-map-panic.txt");
    let portable_path = directory.path().join("portable-map-panic.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = panicking_map_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("panicking-map"));
    assert!(error.message.contains("user transform panicked"));
    assert!(!direct_path.exists());

    let portable_pipeline = Pipeline::default();
    panicking_map_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "panicking-map",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_PANICKING_MAP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 4, "Prism map panic state: {terminal:?}");
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("panicking-map") && message.contains("user transform panicked")
        }),
        "Prism did not report the typed map panic: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_and_direct_abort_on_side_input_panic_without_commit() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-side-panic.txt");
    let portable_path = directory.path().join("portable-side-panic.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = panicking_side_input_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("panicking-side-apply"));
    assert!(error.message.contains("user aggregate panicked"));
    assert!(!direct_path.exists());

    let portable_pipeline = Pipeline::default();
    panicking_side_input_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "panicking-side",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_PANICKING_SIDE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 4, "Prism side panic state: {terminal:?}");
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("Execution at Some(\"panicking-side-apply\")")
                && message.contains("user aggregate panicked")
                && message.contains("injected side-view failure")
        }),
        "Prism did not report the typed side-view panic: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_and_direct_abort_on_global_combine_panic_without_commit() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-global-panic.txt");
    let portable_path = directory.path().join("portable-global-panic.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink =
        panicking_global_combine_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("panicking-global-combine"));
    assert!(error.message.contains("user aggregate panicked"));
    assert!(!direct_path.exists());

    let portable_pipeline = Pipeline::default();
    panicking_global_combine_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "panicking-global",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_PANICKING_GLOBAL".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 4,
        "Prism global combine panic state: {terminal:?}"
    );
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("Execution at Some(\"panicking-global-combine\")")
                && message.contains("user aggregate panicked")
                && message.contains("injected global combine failure")
        }),
        "Prism did not report the typed global combine panic: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_and_direct_abort_on_keyed_fold_panic_without_commit() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-fold-panic.txt");
    let portable_path = directory.path().join("portable-fold-panic.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = panicking_keyed_fold_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("panicking-keyed-fold"));
    assert!(error.message.contains("user fold panicked"));
    assert!(!direct_path.exists());

    let portable_pipeline = Pipeline::default();
    panicking_keyed_fold_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "panicking-fold",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_PANICKING_FOLD".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 4, "Prism keyed fold panic state: {terminal:?}");
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("Execution at Some(\"panicking-keyed-fold\")")
                && message.contains("user fold panicked")
                && message.contains("injected keyed fold failure")
        }),
        "Prism did not report the typed keyed fold panic: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_matches_direct_byte_and_keyed_batch_helpers() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-batches.txt");
    let portable_path = directory.path().join("portable-batches.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = bounded_batch_helpers_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let expected = [
        "bytes:aa",
        "bytes:aa+aa",
        "bytes:aa+aa",
        "key-bytes:3:a",
        "key-bytes:3:a+a",
        "key-bytes:3:bbbb",
        "key:1:1",
        "key:1:1+1",
        "key:2:9+9",
        "time-bytes:4:1000000:x+y",
        "time-bytes:4:8000000:z",
        "time:4:1000000:x+y",
        "time:4:8000000:z",
    ];
    assert_eq!(read_rows(&direct_path), expected);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    bounded_batch_helpers_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "batch-helper-contract",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_BATCH_HELPERS".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism batch helper state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), expected);
}

#[tokio::test]
async fn prism_and_direct_reject_keyed_batch_length_change() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-invalid-keyed-batch.txt");
    let portable_path = directory.path().join("portable-invalid-keyed-batch.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = invalid_keyed_batch_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("invalid-keyed-batch"));
    assert!(error.message.contains("batch value map changed length"));
    assert!(!direct_path.exists());

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    invalid_keyed_batch_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "invalid-keyed-batch",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_BAD_KEYED_BATCH".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 4,
        "Prism keyed batch failure state: {terminal:?}"
    );
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("invalid-keyed-batch")
                && message.contains("batch value map changed length")
        }),
        "Prism did not report the keyed batch error: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_matches_direct_multi_source_flatten_and_group() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct.txt");
    let portable_path = directory.path().join("portable.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = bounded_multi_source_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| line.parse::<i32>().unwrap())
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    assert_eq!(read_rows(&direct_path), [107, 203, 307]);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;

    let portable_pipeline = Pipeline::default();
    bounded_multi_source_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_MULTI_SOURCE_CONTRACT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-multi-source-contract-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism multi-source contract did not reach DONE: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_matches_direct_all_windowed_joins_and_cogroup() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct.txt");
    let portable_path = directory.path().join("portable.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = bounded_join_cogroup_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "C|a|[1, 2]|[10]|0|0|4000000",
            "C|b|[3]|[20]|4000000|4000000|8000000",
            "C|c|[]|[30]|7000000|4000000|8000000",
            "C|d|[4]|[]|6000000|4000000|8000000",
            "I|a|1|10|0|0|4000000",
            "I|a|2|10|0|0|4000000",
            "I|b|3|20|4000000|4000000|8000000",
            "J|a|Some(1)|Some(10)|0|0|4000000",
            "J|a|Some(2)|Some(10)|0|0|4000000",
            "J|b|Some(3)|Some(20)|4000000|4000000|8000000",
            "J|c|None|Some(30)|7000000|4000000|8000000",
            "J|d|Some(4)|None|6000000|4000000|8000000",
            "L|a|1|Some(10)|0|0|4000000",
            "L|a|2|Some(10)|0|0|4000000",
            "L|b|3|Some(20)|4000000|4000000|8000000",
            "L|d|4|None|6000000|4000000|8000000",
            "R|a|Some(1)|10|0|0|4000000",
            "R|a|Some(2)|10|0|0|4000000",
            "R|b|Some(3)|20|4000000|4000000|8000000",
            "R|c|None|30|7000000|4000000|8000000",
        ]
    );

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    bounded_join_cogroup_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_JOIN_COGROUP_CONTRACT".into(),
            portable_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-join-cogroup-contract-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism join/co-group contract did not reach DONE: {terminal:?}"
    );
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_negative_time_offset_windows() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-offset.txt");
    let portable_path = directory.path().join("portable-offset.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = offset_window_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "a|[1, 2]|-1000000|-3000000|1000000",
            "a|[3, 4]|1000000|1000000|5000000",
            "a|[5]|7000000|5000000|9000000",
        ]
    );

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    offset_window_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "offset-window",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_OFFSET_WINDOW".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism offset window state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_overlapping_sliding_windows() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-sliding.txt");
    let portable_path = directory.path().join("portable-sliding.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = sliding_window_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "a|[1, 2, 3]|-1000000|-2000000|2000000",
            "a|[1]|-1000000|-4000000|0",
            "a|[2, 3]|0|0|4000000",
            "a|[4, 5]|4000000|4000000|8000000",
            "a|[4]|4000000|2000000|6000000",
            "a|[5]|7000000|6000000|10000000",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    sliding_window_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "sliding-window",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SLIDING_WINDOW".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism sliding window state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_merging_keyed_sessions() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-session.txt");
    let portable_path = directory.path().join("portable-session.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = session_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "a|[1, 2, 3, 4]|0|0|18000000",
            "b|[5]|4000000|4000000|9000000",
            "c|[6, 7]|-4000000|-4000000|4000000",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    session_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "session-window",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SESSION_WINDOW".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism session state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_bounded_join_of_merged_sessions() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-session-join.txt");
    let portable_path = directory.path().join("portable-session-join.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = bounded_session_join_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "a:1:10:0..13000000",
            "a:2:10:0..13000000",
            "b:3:12:30000000..36000000",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    bounded_session_join_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "session-join",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SESSION_JOIN".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism session join state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_partition_routes_and_time_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-partition.txt");
    let portable_path = directory.path().join("portable-partition.txt");
    let direct_pipeline = Pipeline::default();
    let sink = partition_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let mut expected = (0_i32..9)
        .map(|value| {
            let start = i64::from(value / 5) * 10_000_000;
            let stamp = i64::from(value) * 2_000_000;
            format!(
                "{}:{value}:{stamp}:{start}..{}",
                value.rem_euclid(3),
                start + 10_000_000
            )
        })
        .collect::<Vec<_>>();
    expected.sort_unstable();
    let direct_rows = read_rows(&direct_path);
    assert_eq!(direct_rows, expected);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    partition_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "partition",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_PARTITION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism partition state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

async fn assert_default_pane_metadata(
    window: PaneFixtureWindow,
    expected_direct: &[&str],
    expected_prism: &[&str],
) {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-panes.txt");
    let portable_path = directory.path().join("portable-panes.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        pane_metadata_fixture_for(&direct_pipeline, direct_path.clone(), window).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = rows(&direct_path);
    assert_eq!(direct_rows, expected_direct);
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    pane_metadata_fixture_for(&portable_pipeline, portable_path.clone(), window).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        &format!("panes-{window:?}"),
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_PANES".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_PANES_WINDOW".into(),
                format!("{window:?}").to_ascii_lowercase(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism pane metadata state: {terminal:?}");
    assert_eq!(rows(&portable_path), expected_prism);
}

#[tokio::test]
async fn prism_matches_direct_default_pane_metadata_after_native_grouping() {
    assert_default_pane_metadata(
        PaneFixtureWindow::Fixed,
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
    )
    .await;
}

#[tokio::test]
async fn prism_matches_direct_default_panes_in_overlapping_sliding_windows() {
    assert_default_pane_metadata(
        PaneFixtureWindow::Sliding,
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
    )
    .await;
}

#[tokio::test]
async fn prism_reports_early_default_pane_for_merged_session() {
    // The pinned Prism runner emits one Early pane for this merged session.
    // Beam's default trigger describes an on-time firing at window closure;
    // keep the runner difference visible until its cause is resolved.
    assert_default_pane_metadata(
        PaneFixtureWindow::Session,
        &[
            "group:a:[2, 3]:OnTime:true:true:0:0",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
        &[
            "group:a:[2, 3]:Early:false:false:0:-1",
            "source:a:2:Unknown:true:true:0:0",
            "source:a:3:Unknown:true:true:0:0",
        ],
    )
    .await;
}

#[tokio::test]
async fn prism_count_trigger_fires_one_early_pane_for_a_bounded_bundle() {
    // The same public graph runs through Direct and the pinned Prism runner.
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-count-trigger.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = pane_metadata_fixture_with_trigger(
        &direct_pipeline,
        direct_path.clone(),
        PaneFixtureWindow::Fixed,
        true,
    )
    .unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let path = directory.path().join("count-trigger.txt");
    let pipeline = Pipeline::default();
    pane_metadata_fixture_with_trigger(&pipeline, path.clone(), PaneFixtureWindow::Fixed, true)
        .unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "count-trigger-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_BOUNDED".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "count trigger job state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut direct_rows = std::fs::read_to_string(direct_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    direct_rows.sort();
    assert_eq!(direct_rows, rows);
    assert_eq!(
        rows,
        [
            "group:a:[2, 3]:Early:true:false:0:-1".to_owned(),
            "source:a:2:Unknown:true:true:0:0".to_owned(),
            "source:a:3:Unknown:true:true:0:0".to_owned(),
        ]
    );
}

#[tokio::test]
async fn prism_and_direct_count_trigger_match_overlapping_sliding_windows() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for accumulating in [false, true] {
        let mode = if accumulating {
            "accumulating"
        } else {
            "discarding"
        };
        let direct_path = directory.path().join(format!("direct-sliding-{mode}.txt"));
        let direct_pipeline = Pipeline::default();
        let direct_sink = if accumulating {
            pane_metadata_fixture_with_accumulating_count(
                &direct_pipeline,
                direct_path.clone(),
                PaneFixtureWindow::Sliding,
            )
        } else {
            pane_metadata_fixture_with_trigger(
                &direct_pipeline,
                direct_path.clone(),
                PaneFixtureWindow::Sliding,
                true,
            )
        }
        .unwrap();
        DirectRunner
            .run_sink(&direct_sink, &JobConfig::default())
            .unwrap();
        let path = directory.path().join(format!("sliding-{mode}.txt"));
        let pipeline = Pipeline::default();
        if accumulating {
            pane_metadata_fixture_with_accumulating_count(
                &pipeline,
                path.clone(),
                PaneFixtureWindow::Sliding,
            )
        } else {
            pane_metadata_fixture_with_trigger(
                &pipeline,
                path.clone(),
                PaneFixtureWindow::Sliding,
                true,
            )
        }
        .unwrap();
        let environment = if accumulating {
            HashMap::from([(
                "IRONBEAM_V5_PRISM_COUNT_ACCUMULATING_BOUNDED".into(),
                path.to_string_lossy().into_owned(),
            )])
        } else {
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_COUNT_BOUNDED".into(),
                    path.to_string_lossy().into_owned(),
                ),
                ("IRONBEAM_V5_PRISM_COUNT_WINDOW".into(), "sliding".into()),
            ])
        };
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            &format!("sliding-{mode}-count-trigger-probe"),
            environment,
        )
        .await;
        assert_eq!(
            terminal.0, 3,
            "sliding {mode} count job state: {terminal:?}"
        );
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort();
        let mut direct_rows = std::fs::read_to_string(direct_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        direct_rows.sort();
        assert_eq!(direct_rows, rows, "sliding {mode} panes differ");
    }
}

#[tokio::test]
async fn prism_matches_direct_processing_time_early_panes() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for accumulating in [false, true] {
        let portable_path = directory
            .path()
            .join(format!("portable-{accumulating}.txt"));
        let direct_rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let direct_sink = processing_time_trigger_collection(&direct_pipeline, accumulating)
            .unwrap()
            .sink_named_with_config(
                "processing-result",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("processing-direct-{accumulating}-v1"),
                {
                    let rows = Arc::clone(&direct_rows);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&rows),
                            accepted: Arc::clone(&accepted),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let pipeline = Pipeline::default();
        processing_time_trigger_fixture(&pipeline, portable_path.clone(), accumulating).unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            &format!("processing-time-{accumulating}"),
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_PROCESSING_TIME".into(),
                    portable_path.to_string_lossy().into_owned(),
                ),
                (
                    "IRONBEAM_V5_PRISM_PROCESSING_ACCUMULATING".into(),
                    if accumulating { "1" } else { "0" }.into(),
                ),
            ]),
        )
        .await;
        assert_eq!(terminal.0, 3, "processing-time job state: {terminal:?}");
        let mut direct = direct_rows.lock().unwrap().clone();
        let mut portable = std::fs::read_to_string(&portable_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        direct.sort();
        portable.sort();
        assert_eq!(portable, direct, "processing-time panes differ");
    }
}

#[tokio::test]
async fn prism_matches_direct_two_source_processing_time_panes() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        accepted: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    for accumulating in [false, true] {
        let portable_path = directory
            .path()
            .join(format!("portable-dual-processing-{accumulating}.txt"));
        let direct_rows = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let direct_sink = dual_processing_time_cogroup_collection(&direct_pipeline, accumulating)
            .unwrap()
            .sink_named_with_config(
                "dual-processing-result",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                format!("dual-processing-direct-{accumulating}-v1"),
                {
                    let rows = Arc::clone(&direct_rows);
                    let accepted = Arc::clone(&accepted);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&rows),
                            accepted: Arc::clone(&accepted),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let portable_pipeline = Pipeline::default();
        dual_processing_time_cogroup_fixture(
            &portable_pipeline,
            portable_path.clone(),
            accumulating,
        )
        .unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &portable_pipeline,
            &format!("dual-processing-time-{accumulating}"),
            HashMap::from([
                (
                    "IRONBEAM_V5_PRISM_DUAL_PROCESSING_TIME".into(),
                    portable_path.to_string_lossy().into_owned(),
                ),
                (
                    "IRONBEAM_V5_PRISM_DUAL_PROCESSING_ACCUMULATING".into(),
                    if accumulating { "1" } else { "0" }.into(),
                ),
            ]),
        )
        .await;
        assert_eq!(
            terminal.0, 3,
            "dual processing-time job state: {terminal:?}"
        );
        let mut direct = direct_rows.lock().unwrap().clone();
        let mut portable = std::fs::read_to_string(&portable_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        direct.sort();
        portable.sort();
        assert_eq!(direct.len(), if accumulating { 2 } else { 1 });
        assert_eq!(portable, direct, "two-source processing-time panes differ");
    }
}

#[tokio::test]
async fn prism_characterizes_two_source_accumulating_count_or_time_panes() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("dual-count-time.txt");
    let pipeline = Pipeline::default();
    dual_count_or_time_cogroup_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "dual-count-time",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_DUAL_COUNT_OR_TIME".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "two-source count-or-time job: {terminal:?}");
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "1:[[2, 3], [4]]:Early:2",
            "1:[[2, 3], [4]]:OnTime:3",
            "1:[[2], [4]]:Early:0",
            "1:[[2], [4]]:Early:1",
        ]
    );
}

#[tokio::test]
async fn prism_count_trigger_stream_advances_early_pane_indices() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("count-stream.txt");
    let pipeline = Pipeline::default();
    count_trigger_stream_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "count-trigger-stream-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_STREAM".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "count stream job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "group:a:[2]:Early:true:false:0:-1".to_owned(),
            "group:a:[3]:Early:false:false:1:-1".to_owned(),
        ]
    );
}

#[tokio::test]
async fn prism_accumulating_count_stream_retains_prior_values() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("accumulating-count-stream.txt");
    let pipeline = Pipeline::default();
    accumulating_count_trigger_stream_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "accumulating-count-stream-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_ACCUMULATING".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "accumulating count stream job state: {terminal:?}"
    );
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "group:a:[2]:Early:true:false:0:-1".to_owned(),
            "group:a:[2, 3]:Early:false:false:1:-1".to_owned(),
            "group:a:[2, 3]:OnTime:false:true:2:0".to_owned(),
        ]
    );
}

#[tokio::test]
async fn prism_accumulating_count_stream_uses_latest_output_time() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("latest-count-stream.txt");
    let pipeline = Pipeline::default();
    accumulating_latest_count_trigger_stream_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "latest-count-stream-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COUNT_LATEST".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "latest count job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "group:a:[2]:1000000:Early:true:false:0:-1".to_owned(),
            "group:a:[2, 3]:3000000:Early:false:false:1:-1".to_owned(),
            "group:a:[2, 3]:3000000:OnTime:false:true:2:0".to_owned(),
        ]
    );
}

#[tokio::test]
async fn prism_emits_late_pane_before_allowed_lateness_expires() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("allowed-lateness.txt");
    let pipeline = Pipeline::default();
    allowed_lateness_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "allowed-lateness-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_ALLOWED_LATENESS".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "late pane job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[3]:3000000:Late:false:false:1:1",
        ]
    );
}

#[tokio::test]
async fn prism_repeats_late_count_and_flushes_partial_pane() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late-count.txt");
    let pipeline = Pipeline::default();
    late_count_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "late-count-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_LATE_COUNT".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "late count job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[3, 5]:3000000:Late:false:false:1:1",
            "a:[7]:7000000:Late:false:true:2:2",
        ]
    );
}

#[tokio::test]
async fn prism_combines_early_and_late_count_firings() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("combined-count.txt");
    let pipeline = Pipeline::default();
    combined_count_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "combined-count-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COMBINED_COUNT".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "combined count job state: {terminal:?}");
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2, 3]:2000000:Early:true:false:0:-1",
            "a:[4]:4000000:OnTime:false:false:1:0",
            "a:[5, 7]:5000000:Late:false:false:2:1",
            "a:[11]:8000000:Late:false:true:3:2",
        ]
    );
}

#[tokio::test]
async fn prism_characterizes_accumulating_early_and_late_count_schedule() {
    let directory = tempfile::tempdir().unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let direct_path = directory.path().join("combined-accumulating-direct");
    let direct = Pipeline::default();
    let direct_sink = accumulating_combined_count_fixture(&direct, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let direct_rows = read_rows(&direct_path);

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_path = directory.path().join("combined-accumulating-prism");
    let portable = Pipeline::default();
    accumulating_combined_count_fixture(&portable, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable,
        "combined-accumulating-count",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_COMBINED_COUNT_ACCUMULATING".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "combined accumulating job: {terminal:?}");
    let portable_rows = read_rows(&portable_path);
    let mut expected_direct = vec![
        "a:[2, 3]:2000000:Early:true:false:0:-1",
        "a:[2, 3]:2000000:OnTime:false:false:1:0",
        "a:[2, 3, 5, 7]:2000000:Late:false:false:2:1",
    ];
    expected_direct.sort();
    assert_eq!(direct_rows, expected_direct);
    // Pinned Prism restarts the accumulating early/on-time trigger for the
    // late bundle before firing its final late pane on this TestStream schedule.
    let mut expected_prism = vec![
        "a:[2, 3]:2000000:Early:true:false:0:-1",
        "a:[2, 3]:2000000:OnTime:false:false:1:0",
        "a:[2, 3, 5, 7]:2000000:Early:false:false:2:-1",
        "a:[2, 3, 5, 7]:2000000:OnTime:false:false:3:0",
        "a:[2, 3, 5, 7]:2000000:Late:false:true:4:1",
    ];
    expected_prism.sort();
    assert_eq!(portable_rows, expected_prism);
}

#[tokio::test]
async fn prism_accumulating_late_count_coalesces_until_expiry() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("accumulating-late-count.txt");
    let pipeline = Pipeline::default();
    accumulating_late_count_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "accumulating-late-count-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_LATE_COUNT_ACCUMULATING".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    // Pinned Prism holds accumulating late input until expiry on this schedule.
    // Direct emits a threshold pane earlier, but the final values agree.
    assert_eq!(
        terminal.0, 3,
        "accumulating late count job state: {terminal:?}"
    );
    let rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [
            "a:[2]:2000000:OnTime:true:false:0:0",
            "a:[2, 3, 5, 7]:2000000:Late:false:true:1:1",
        ]
    );
}

#[tokio::test]
async fn prism_sliding_windows_keep_independent_late_pane_indices() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sliding-lateness.txt");
    let pipeline = Pipeline::default();
    sliding_allowed_lateness_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "sliding-lateness-probe",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SLIDING_LATENESS".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "sliding late job state: {terminal:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    let mut expected = vec![
        "0..10000000:a:[2]:6000000:OnTime:true:false:0:0",
        "0..10000000:a:[3]:7000000:Late:false:true:1:1",
        "5000000..15000000:a:[2, 3]:6000000:OnTime:true:false:0:0",
        "5000000..15000000:a:[4]:8000000:Late:false:false:1:1",
    ];
    expected.sort();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn prism_matches_direct_three_input_merged_session_cogroup() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-session-cogroup.txt");
    let portable_path = directory.path().join("portable-session-cogroup.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        bounded_session_cogroup_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "a:[1, 2]:[\"bridge\"]:[true]:0..17000000",
            "a:[]:[\"isolated\"]:[]:20000000..25000000",
            "b:[]:[]:[true]:30000000..35000000",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    bounded_session_cogroup_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "session-cogroup",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_SESSION_COGROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism session co-group state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_runs_mixed_boundedness_windowed_join_and_cogroup() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed.txt");
    let pipeline = Pipeline::default();
    mixed_boundedness_join_cogroup_fixture(&pipeline, path.clone()).unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_MIXED_JOIN_COGROUP".into(),
            path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-mixed-group-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(terminal, Some(3), "mixed-input Prism job: {terminal:?}");
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "C|a|[1]|[10]|0|0|4000000",
            "J|a|Some(1)|Some(10)|0|0|4000000",
        ]
    );
}

#[tokio::test]
async fn prism_runs_production_source_windowed_join_and_cogroup() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let rows_path = directory.path().join("rows");
    let pipeline = Pipeline::default();
    production_join_cogroup_fixture(&pipeline, rows_path.clone()).unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_PRODUCTION_JOIN_COGROUP".into(),
            rows_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-production-join-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(terminal, Some(3), "production join job: {terminal:?}");
    let mut rows = std::fs::read_dir(&rows_path)
        .unwrap()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "C|a|[7, 9]|[10]|0|0|2000000",
            "C|a|[8]|[20]|2000000|2000000|4000000",
            "J|a|Some(7)|Some(10)|0|0|2000000",
            "J|a|Some(8)|Some(20)|2000000|2000000|4000000",
            "J|a|Some(9)|Some(10)|0|0|2000000",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_earliest_group_timestamp() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct.txt");
    let portable_path = directory.path().join("portable.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = group_time_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();

    let portable_pipeline = Pipeline::default();
    group_time_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_GROUP_TIME".into(),
            portable_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-group-time-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism grouped-time job did not reach DONE: {terminal:?}"
    );
    assert_eq!(std::fs::read_to_string(&direct_path).unwrap(), "12000014\n");
    assert_eq!(
        std::fs::read_to_string(&portable_path).unwrap(),
        std::fs::read_to_string(&direct_path).unwrap()
    );
}

#[tokio::test]
async fn prism_matches_direct_fanin_output_time_policies() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    for (policy, variable) in [
        (
            ironbeam_core::WindowOutputTime::EndOfWindow,
            "IRONBEAM_V5_PRISM_JOIN_COGROUP_END_TIME",
        ),
        (
            ironbeam_core::WindowOutputTime::LatestInPane,
            "IRONBEAM_V5_PRISM_JOIN_COGROUP_LATEST_TIME",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let direct_path = directory.path().join("direct.txt");
        let portable_path = directory.path().join("portable.txt");
        let direct_pipeline = Pipeline::default();
        let direct_sink = bounded_join_cogroup_output_time_fixture(
            &direct_pipeline,
            direct_path.clone(),
            Some(policy),
        )
        .unwrap();
        DirectRunner
            .run_sink(&direct_sink, &JobConfig::default())
            .unwrap();
        let pipeline = Pipeline::default();
        bounded_join_cogroup_output_time_fixture(&pipeline, portable_path.clone(), Some(policy))
            .unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            "fan-in-output-time",
            HashMap::from([(
                variable.into(),
                portable_path.to_string_lossy().into_owned(),
            )]),
        )
        .await;
        assert_eq!(terminal.0, 3, "fan-in output-time job state: {terminal:?}");
        let read_rows = |path: &Path| {
            let mut rows = std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            rows.sort_unstable();
            rows
        };
        assert_eq!(
            read_rows(&portable_path),
            read_rows(&direct_path),
            "policy: {policy:?}"
        );
    }
}

#[tokio::test]
async fn prism_matches_direct_unbounded_fanin_output_time_policies() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        committed: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.committed.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    for (policy, variable, first_time, second_time) in [
        (
            ironbeam_core::WindowOutputTime::EndOfWindow,
            "IRONBEAM_V5_PRISM_DUAL_FANIN_END_TIME",
            3_000_000,
            7_000_000,
        ),
        (
            ironbeam_core::WindowOutputTime::LatestInPane,
            "IRONBEAM_V5_PRISM_DUAL_FANIN_LATEST_TIME",
            2_000_000,
            5_000_000,
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let portable_path = directory.path().join("portable.txt");
        let direct_rows = Arc::new(Mutex::new(Vec::new()));
        let committed = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let direct_sink = dual_unbounded_fanin_output_time_collection(&direct_pipeline, policy)
            .unwrap()
            .sink_named_with_config(
                "dual-time-result",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "dual-time-direct-result-v1".into(),
                {
                    let direct_rows = Arc::clone(&direct_rows);
                    let committed = Arc::clone(&committed);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&direct_rows),
                            committed: Arc::clone(&committed),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let pipeline = Pipeline::default();
        dual_unbounded_fanin_output_time_fixture(&pipeline, portable_path.clone(), policy).unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            "dual-fanin-output-time",
            HashMap::from([(
                variable.into(),
                portable_path.to_string_lossy().into_owned(),
            )]),
        )
        .await;
        assert_eq!(
            terminal.0, 3,
            "dual fan-in output-time job state: {terminal:?}"
        );
        let read_rows = |path: &Path| {
            let mut rows = std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            rows.sort_unstable();
            rows
        };
        let mut direct = direct_rows.lock().unwrap().clone();
        direct.sort_unstable();
        assert_eq!(
            direct,
            vec![
                format!("C:a:{first_time}:([1], [10])"),
                format!("C:b:{second_time}:([], [20])"),
                format!("C:c:{second_time}:([3], [])"),
                format!("J:a:{first_time}:(Some(1), Some(10))"),
                format!("J:b:{second_time}:(None, Some(20))"),
                format!("J:c:{second_time}:(Some(3), None)"),
            ]
        );
        assert_eq!(read_rows(&portable_path), direct, "policy: {policy:?}");
    }
}

#[tokio::test]
async fn prism_matches_direct_combine_output_time_policies() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    for (policy, variable, expected) in [
        (
            ironbeam_core::WindowOutputTime::EndOfWindow,
            "IRONBEAM_V5_PRISM_COMBINE_END_TIME",
            9_000_000,
        ),
        (
            ironbeam_core::WindowOutputTime::LatestInPane,
            "IRONBEAM_V5_PRISM_COMBINE_LATEST_TIME",
            7_000_000,
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let direct_path = directory.path().join("direct.txt");
        let portable_path = directory.path().join("portable.txt");
        let direct_pipeline = Pipeline::default();
        let direct_sink =
            combine_output_time_fixture(&direct_pipeline, direct_path.clone(), policy).unwrap();
        DirectRunner
            .run_sink(&direct_sink, &JobConfig::default())
            .unwrap();
        let pipeline = Pipeline::default();
        combine_output_time_fixture(&pipeline, portable_path.clone(), policy).unwrap();
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            "combine-output-time",
            HashMap::from([(
                variable.into(),
                portable_path.to_string_lossy().into_owned(),
            )]),
        )
        .await;
        assert_eq!(terminal.0, 3, "combine output-time job state: {terminal:?}");
        let read_rows = |path: &Path| {
            let mut rows = std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            rows.sort_unstable();
            rows
        };
        let direct = read_rows(&direct_path);
        assert_eq!(
            direct,
            vec![
                format!("G:{expected}:9"),
                format!("K:{expected}:1:9"),
                format!("V:{expected}:9")
            ]
        );
        assert_eq!(read_rows(&portable_path), direct, "policy: {policy:?}");
    }
}

#[tokio::test]
async fn prism_emits_full_and_final_partial_unbounded_keyed_batches() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    for (windowed, upstream_trigger, variable) in [
        (false, false, "IRONBEAM_V5_PRISM_GLOBAL_KEYED_BATCHES"),
        (true, false, "IRONBEAM_V5_PRISM_WINDOWED_KEYED_BATCHES"),
        (false, true, "IRONBEAM_V5_PRISM_GLOBAL_KEYED_BATCHES_UPSTREAM_TRIGGER"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("batches.txt");
        let pipeline = Pipeline::default();
        if upstream_trigger {
            global_keyed_batches_with_upstream_trigger_fixture(&pipeline, path.clone()).unwrap();
        } else if windowed {
            windowed_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
        } else {
            global_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
        }
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            "keyed-batches",
            HashMap::from([(variable.into(), path.to_string_lossy().into_owned())]),
        )
        .await;
        assert_eq!(terminal.0, 3, "keyed batch job state: {terminal:?}");
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        rows.sort_unstable();
        assert_eq!(
            rows,
            [
                "a:[2, 3]:Early:true:false:0:-1",
                "a:[4]:OnTime:false:true:1:0",
            ],
            "windowed: {windowed}; upstream trigger: {upstream_trigger}"
        );
    }
}

#[tokio::test]
async fn prism_flushes_late_keyed_batches_at_the_batch_count() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("late-batches.txt");
    let pipeline = Pipeline::default();
    late_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "late-keyed-batches",
        HashMap::from([("IRONBEAM_V5_PRISM_LATE_KEYED_BATCHES".into(), path.to_string_lossy().into_owned())]),
    ).await;
    assert_eq!(terminal.0, 3, "late keyed batch job state: {terminal:?}");
    let rows = std::fs::read_to_string(path).unwrap().lines().map(str::to_owned).collect::<Vec<_>>();
    assert_eq!(rows, ["a:[2, 3]:Early:true:false:0:-1", "a:[4, 5]:Late:false:false:1:0"]);
}

#[tokio::test]
async fn prism_flushes_keyed_batches_on_time_and_count() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("timed-batches.txt");
    let pipeline = Pipeline::default();
    timed_keyed_batches_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "timed-keyed-batches",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_TIMED_KEYED_BATCHES".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "timed batch job state: {terminal:?}");
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        rows,
        [
            "a:[2]:Early:true:false:0:-1",
            "a:[3, 4]:Early:false:false:1:-1",
        ]
    );
}

#[tokio::test]
async fn prism_matches_direct_global_count_group_and_combine_panes() {
    struct DirectRows {
        rows: Arc<Mutex<Vec<String>>>,
        committed: Arc<Mutex<HashSet<String>>>,
        staged: Vec<String>,
        bundle: String,
    }
    impl TypedSink<String> for DirectRows {
        fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
            self.bundle = id.to_owned();
            Ok(())
        }
        fn write(&mut self, row: String) -> Result<(), Error> {
            self.staged.push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.committed.lock().unwrap().insert(self.bundle.clone()) {
                self.rows.lock().unwrap().append(&mut self.staged);
            }
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.staged.clear();
            Ok(())
        }
    }
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    for (accumulating, combine, variable) in [
        (false, false, "IRONBEAM_V5_PRISM_GLOBAL_COUNT_DISCARDING"),
        (true, false, "IRONBEAM_V5_PRISM_GLOBAL_COUNT_ACCUMULATING"),
        (false, true, "IRONBEAM_V5_PRISM_GLOBAL_COMBINE_DISCARDING"),
        (true, true, "IRONBEAM_V5_PRISM_GLOBAL_COMBINE_ACCUMULATING"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let portable_path = directory.path().join("portable.txt");
        let rows = Arc::new(Mutex::new(Vec::new()));
        let committed = Arc::new(Mutex::new(HashSet::new()));
        let direct_pipeline = Pipeline::default();
        let direct_collection = if combine {
            global_keyed_combine_collection(&direct_pipeline, accumulating).unwrap()
        } else {
            global_count_collection(&direct_pipeline, accumulating).unwrap()
        };
        let direct_sink = direct_collection
            .sink_named_with_config(
                "global-count-capture",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "global-count-direct-v1".into(),
                {
                    let rows = Arc::clone(&rows);
                    let committed = Arc::clone(&committed);
                    move || {
                        Ok(Box::new(DirectRows {
                            rows: Arc::clone(&rows),
                            committed: Arc::clone(&committed),
                            staged: Vec::new(),
                            bundle: String::new(),
                        }))
                    }
                },
            )
            .unwrap();
        DirectRunner
            .start_stream_sink(&direct_sink, &JobConfig::default())
            .unwrap()
            .wait()
            .unwrap();
        let pipeline = Pipeline::default();
        if combine {
            global_keyed_combine_fixture(&pipeline, portable_path.clone(), accumulating).unwrap();
        } else {
            global_count_fixture(&pipeline, portable_path.clone(), accumulating).unwrap();
        }
        let terminal = run_shared_fixture(
            &endpoint,
            &pipeline,
            "global-count",
            HashMap::from([(
                variable.into(),
                portable_path.to_string_lossy().into_owned(),
            )]),
        )
        .await;
        assert_eq!(terminal.0, 3, "global count job state: {terminal:?}");
        let mut direct = rows.lock().unwrap().clone();
        direct.sort_unstable();
        let mut portable = std::fs::read_to_string(portable_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        portable.sort_unstable();
        assert_eq!(
            portable, direct,
            "accumulating: {accumulating}, combine: {combine}"
        );
    }
}

#[tokio::test]
async fn prism_matches_direct_end_of_window_group_timestamp() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-end-time.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = group_end_time_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let path = directory.path().join("end-time.txt");
    let pipeline = Pipeline::default();
    group_end_time_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "end-of-window-group-time",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_GROUP_END_TIME".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "end-of-window job state: {terminal:?}");
    assert_eq!(std::fs::read_to_string(&direct_path).unwrap(), "19000014\n");
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        std::fs::read_to_string(direct_path).unwrap()
    );
}

#[tokio::test]
async fn prism_matches_direct_latest_in_pane_group_timestamp() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-latest-time.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink = group_latest_time_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();
    let path = directory.path().join("latest-time.txt");
    let pipeline = Pipeline::default();
    group_latest_time_fixture(&pipeline, path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &pipeline,
        "latest-in-pane-group-time",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_GROUP_LATEST_TIME".into(),
            path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "latest-in-pane job state: {terminal:?}");
    assert_eq!(std::fs::read_to_string(&direct_path).unwrap(), "17000014\n");
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        std::fs::read_to_string(direct_path).unwrap()
    );
}

#[tokio::test]
async fn prism_runs_global_combine_with_empty_identity() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let nonempty = directory.path().join("nonempty.txt");
    let empty = directory.path().join("empty.txt");
    let pipeline = Pipeline::default();
    global_fold_fixture(&pipeline, nonempty.clone(), empty.clone()).unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [
            (
                "IRONBEAM_V5_PRISM_GLOBAL_FOLD_NONEMPTY".into(),
                nonempty.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_GLOBAL_FOLD_EMPTY".into(),
                empty.to_string_lossy().into_owned(),
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
            job_name: format!("ironbeam-v5-global-fold-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism global-fold job did not reach DONE: {terminal:?}"
    );
    assert_eq!(std::fs::read_to_string(nonempty).unwrap(), "5\n");
    assert_eq!(std::fs::read_to_string(empty).unwrap(), "0\n");
}

#[tokio::test]
async fn prism_runs_vector_global_combine_with_empty_identity() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let nonempty = directory.path().join("nonempty.txt");
    let empty = directory.path().join("empty.txt");
    let pipeline = Pipeline::default();
    global_vector_fixture(&pipeline, nonempty.clone(), empty.clone()).unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [
            (
                "IRONBEAM_V5_PRISM_GLOBAL_VECTOR_NONEMPTY".into(),
                nonempty.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_GLOBAL_VECTOR_EMPTY".into(),
                empty.to_string_lossy().into_owned(),
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
            job_name: format!("ironbeam-v5-global-vector-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism global-vector job did not reach DONE: {terminal:?}"
    );
    assert_eq!(std::fs::read_to_string(nonempty).unwrap(), "5\n");
    assert_eq!(std::fs::read_to_string(empty).unwrap(), "0\n");
}

#[tokio::test]
async fn prism_matches_direct_ordered_keyed_fold_values_and_metadata() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct.txt");
    let portable_path = directory.path().join("portable.txt");

    let direct_pipeline = Pipeline::default();
    let direct_sink = ordered_keyed_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .unwrap();

    let portable_pipeline = Pipeline::default();
    ordered_keyed_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_ORDERED_KEYED".into(),
            portable_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-ordered-keyed-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism ordered-fold job did not reach DONE: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| line.parse::<i32>().unwrap())
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    assert_eq!(read_rows(&direct_path), [52_010, 71_045, 10_111_067]);
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_matches_direct_unbounded_partition_fanout() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-partition");
    let portable_path = directory.path().join("portable-partition");
    let direct_pipeline = Pipeline::default();
    let direct_sink = streaming_partition_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .flat_map(|entry| {
                std::fs::read_to_string(entry.unwrap().path())
                    .unwrap()
                    .lines()
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "0:2:20000000:0..100000000",
            "0:4:110000000:100000000..200000000",
            "1:1:10000000:0..100000000",
            "1:3:30000000:0..100000000",
        ]
    );
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    streaming_partition_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "stream-partition",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAM_PARTITION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism stream partition state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_unbounded_fixed_window_groups() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct");
    let portable_path = directory.path().join("portable");
    let direct_pipeline = Pipeline::default();
    let direct_sink = streaming_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let portable_pipeline = Pipeline::default();
    streaming_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "stream-group",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAM_GROUP".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism group job failed: {terminal:?}");
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(
        read_rows(&direct_path),
        [
            "10000000:104:0:100000000\n",
            "110000000:4:100000000:200000000\n",
            "20000000:2:0:100000000\n",
        ]
    );
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_matches_direct_unbounded_sliding_windows_and_late_drop() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-slide");
    let portable_path = directory.path().join("portable-slide");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        streaming_sliding_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "0|[2]|20000000|-50000000|50000000",
            "0|[2]|20000000|0|100000000",
            "0|[4]|110000000|100000000|200000000",
            "0|[4]|110000000|50000000|150000000",
            "1|[1, 3]|10000000|-50000000|50000000",
            "1|[1, 3]|10000000|0|100000000",
        ]
    );
    let portable_pipeline = Pipeline::default();
    streaming_sliding_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "stream-slide",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAM_SLIDE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism streaming slide state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_matches_direct_unbounded_merging_sessions() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-session");
    let portable_path = directory.path().join("portable-session");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        streaming_session_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    let direct_rows = read_rows(&direct_path);
    assert_eq!(
        direct_rows,
        [
            "1|[1, 2, 3]|0|0|13000000",
            "2|[7]|4000000|4000000|9000000",
            "3|[11]|-10000000|-10000000|-5000000",
        ]
    );
    let portable_pipeline = Pipeline::default();
    streaming_session_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "stream-session",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAM_SESSION".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism streaming session state: {terminal:?}");
    assert_eq!(read_rows(&portable_path), direct_rows);
}

#[tokio::test]
async fn prism_supervisor_discards_output_from_a_crashed_attempt() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct");
    let portable_path = directory.path().join("portable");
    let witness = directory.path().join("killed-after-output");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        streaming_sliding_group_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let portable_pipeline = Pipeline::default();
    streaming_sliding_group_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "supervised-output-replay",
        HashMap::from([
            (
                "IRONBEAM_V5_PRISM_STREAM_SLIDE".into(),
                portable_path.to_string_lossy().into_owned(),
            ),
            ("IRONBEAM_V5_SUPERVISE_EXECUTION".into(), "1".into()),
            (
                "IRONBEAM_TEST_KILL_AFTER_REPLAY_OUTPUT".into(),
                witness.to_string_lossy().into_owned(),
            ),
        ]),
    )
    .await;
    assert_eq!(terminal.0, 3, "Prism output replay failed: {terminal:?}");
    assert!(
        witness.exists(),
        "the first execution child did not exit after output"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_matches_direct_unbounded_ordered_value_batches() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-batches");
    let portable_path = directory.path().join("portable-batches");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        streaming_ordered_batch_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    DirectRunner
        .start_stream_sink(&direct_sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    let portable_pipeline = Pipeline::default();
    streaming_ordered_batch_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "stream-ordered-batches",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_STREAM_ORDERED_BATCH".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 3,
        "Prism ordered batch job failed: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(
        read_rows(&direct_path),
        [
            "10000000:112:0:100000000\n",
            "110000000:41:100000000:200000000\n",
            "20000000:21:0:100000000\n",
            "30000000:132:0:100000000\n",
        ]
    );
    assert_eq!(read_rows(&portable_path), read_rows(&direct_path));
}

#[tokio::test]
async fn prism_runs_unbounded_ordered_keyed_fold_across_windows() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ordered-stream.txt");
    let pipeline = Pipeline::default();
    streaming_ordered_keyed_fixture(&pipeline, path.clone()).unwrap();
    let (state, messages) = run_shared_fixture(
        &endpoint,
        &pipeline,
        "ordered-stream",
        [(
            "IRONBEAM_V5_PRISM_ORDERED_STREAM".into(),
            path.to_string_lossy().into_owned(),
        )]
        .into(),
    )
    .await;
    assert_eq!(state, job_state::Enum::Done as i32, "{messages:?}");
    let mut rows = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| line.parse::<i32>().unwrap())
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(rows, [52_011, 71_056, 10_111_078]);
}

#[tokio::test]
async fn prism_matches_direct_ordered_global_fold_and_empty_identity() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_nonempty = directory.path().join("direct-nonempty.txt");
    let direct_empty = directory.path().join("direct-empty.txt");
    let portable_nonempty = directory.path().join("portable-nonempty.txt");
    let portable_empty = directory.path().join("portable-empty.txt");

    let direct_pipeline = Pipeline::default();
    let (nonempty_sink, empty_sink) = ordered_global_fixture(
        &direct_pipeline,
        direct_nonempty.clone(),
        direct_empty.clone(),
    )
    .unwrap();
    DirectRunner
        .run_sink(&nonempty_sink, &JobConfig::default())
        .unwrap();
    DirectRunner
        .run_sink(&empty_sink, &JobConfig::default())
        .unwrap();

    let portable_pipeline = Pipeline::default();
    ordered_global_fixture(
        &portable_pipeline,
        portable_nonempty.clone(),
        portable_empty.clone(),
    )
    .unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [
            (
                "IRONBEAM_V5_PRISM_ORDERED_GLOBAL_NONEMPTY".into(),
                portable_nonempty.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_ORDERED_GLOBAL_EMPTY".into(),
                portable_empty.to_string_lossy().into_owned(),
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
            job_name: format!("ironbeam-v5-ordered-global-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism ordered-global job did not reach DONE: {terminal:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&direct_nonempty).unwrap(),
        "70023\n"
    );
    assert_eq!(
        std::fs::read_to_string(&portable_nonempty).unwrap(),
        std::fs::read_to_string(&direct_nonempty).unwrap()
    );
    assert_eq!(std::fs::read_to_string(&direct_empty).unwrap(), "0\n");
    assert_eq!(
        std::fs::read_to_string(&portable_empty).unwrap(),
        std::fs::read_to_string(&direct_empty).unwrap()
    );
}

#[tokio::test]
async fn prism_and_direct_reject_duplicate_global_order_keys() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct.txt");
    let portable_path = directory.path().join("portable.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ordered_global_duplicate_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(error.message.contains("duplicate ordered fold key"));
    assert!(!direct_path.exists());

    let portable_pipeline = Pipeline::default();
    ordered_global_duplicate_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_ORDERED_DUPLICATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-ordered-duplicate-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(4),
        "Prism should fail a duplicate order-key job: {terminal:?}"
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_and_direct_reject_duplicate_keyed_order_keys() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct.txt");
    let portable_path = directory.path().join("portable.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ordered_keyed_duplicate_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(error.message.contains("duplicate ordered fold key"));
    assert!(!direct_path.exists());

    let portable_pipeline = Pipeline::default();
    ordered_keyed_duplicate_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [(
            "IRONBEAM_V5_PRISM_ORDERED_KEYED_DUPLICATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]
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
            job_name: format!("ironbeam-v5-ordered-keyed-duplicate-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(4),
        "Prism should fail a duplicate keyed order-key job: {terminal:?}"
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_and_direct_reject_duplicate_ordered_batch_keys() {
    let directory = tempfile::tempdir().unwrap();
    let direct_path = directory.path().join("direct-ordered-batch-duplicate.txt");
    let portable_path = directory
        .path()
        .join("portable-ordered-batch-duplicate.txt");
    let direct_pipeline = Pipeline::default();
    let direct_sink =
        ordered_batch_duplicate_fixture(&direct_pipeline, direct_path.clone()).unwrap();
    let error = DirectRunner
        .run_sink(&direct_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("ordered-batch-duplicate"));
    assert!(error.message.contains("duplicate ordered batch key"));
    assert!(!direct_path.exists());

    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let portable_pipeline = Pipeline::default();
    ordered_batch_duplicate_fixture(&portable_pipeline, portable_path.clone()).unwrap();
    let terminal = run_shared_fixture(
        &endpoint,
        &portable_pipeline,
        "ordered-batch-duplicate",
        HashMap::from([(
            "IRONBEAM_V5_PRISM_ORDERED_BATCH_DUPLICATE".into(),
            portable_path.to_string_lossy().into_owned(),
        )]),
    )
    .await;
    assert_eq!(
        terminal.0, 4,
        "Prism ordered batch failure state: {terminal:?}"
    );
    assert!(
        terminal.1.iter().any(|message| {
            message.contains("ordered-batch-duplicate")
                && message.contains("duplicate ordered batch key")
        }),
        "Prism did not report the duplicate ordered batch key: {:?}",
        terminal.1
    );
    assert!(!portable_path.exists());
}

#[tokio::test]
async fn prism_matches_direct_latest_tie_keys_and_time_metadata() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let direct_keyed = directory.path().join("direct-keyed.txt");
    let direct_global = directory.path().join("direct-global.txt");
    let portable_keyed = directory.path().join("portable-keyed.txt");
    let portable_global = directory.path().join("portable-global.txt");

    let direct_pipeline = Pipeline::default();
    let (keyed_sink, global_sink) = latest_tie_fixture(
        &direct_pipeline,
        direct_keyed.clone(),
        direct_global.clone(),
    )
    .unwrap();
    DirectRunner
        .run_sink(&keyed_sink, &JobConfig::default())
        .unwrap();
    DirectRunner
        .run_sink(&global_sink, &JobConfig::default())
        .unwrap();

    let portable_pipeline = Pipeline::default();
    latest_tie_fixture(
        &portable_pipeline,
        portable_keyed.clone(),
        portable_global.clone(),
    )
    .unwrap();
    let mut translated =
        prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
        command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
        env: [
            (
                "IRONBEAM_V5_PRISM_LATEST_TIE_KEYED".into(),
                portable_keyed.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_LATEST_TIE_GLOBAL".into(),
                portable_global.to_string_lossy().into_owned(),
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
            job_name: format!("ironbeam-v5-latest-tie-{}", std::process::id()),
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
    for _ in 0..100 {
        let state = client
            .get_state(GetJobStateRequest {
                job_id: running.job_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .state;
        if matches!(state, 3..=6) {
            terminal = Some(state);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        terminal,
        Some(3),
        "Prism latest-tie job did not reach DONE: {terminal:?}"
    );
    let read_rows = |path: &Path| {
        let mut rows = std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|line| line.parse::<i32>().unwrap())
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };
    assert_eq!(read_rows(&direct_keyed), [91_303, 92_209, 10_121_205]);
    assert_eq!(read_rows(&portable_keyed), read_rows(&direct_keyed));
    assert_eq!(read_rows(&direct_global), [90_303, 10_120_205]);
    assert_eq!(read_rows(&portable_global), read_rows(&direct_global));
}

#[tokio::test]
async fn prism_and_direct_reject_duplicate_or_empty_latest_tie_keys() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let directory = tempfile::tempdir().unwrap();
    let url = if endpoint.contains("://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    };
    let mut client = JobServiceClient::connect(url).await.unwrap();
    for (variant, env_name) in [
        ("keyed", "IRONBEAM_V5_PRISM_LATEST_TIE_DUPLICATE_KEYED"),
        ("global", "IRONBEAM_V5_PRISM_LATEST_TIE_DUPLICATE_GLOBAL"),
        ("empty", "IRONBEAM_V5_PRISM_LATEST_TIE_EMPTY"),
    ] {
        let direct_path = directory.path().join(format!("direct-{variant}.txt"));
        let portable_path = directory.path().join(format!("portable-{variant}.txt"));
        let direct_pipeline = Pipeline::default();
        let direct_sink = if variant == "empty" {
            latest_tie_empty_fixture(&direct_pipeline, direct_path.clone())
        } else {
            latest_tie_duplicate_fixture(&direct_pipeline, direct_path.clone(), variant == "keyed")
        }
        .unwrap();
        let error = DirectRunner
            .run_sink(&direct_sink, &JobConfig::default())
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
        let expected = if variant == "empty" {
            "latest on empty input"
        } else {
            "duplicate latest tie key"
        };
        assert!(error.message.contains(expected));
        assert!(!direct_path.exists());

        let portable_pipeline = Pipeline::default();
        if variant == "empty" {
            latest_tie_empty_fixture(&portable_pipeline, portable_path.clone()).unwrap();
        } else {
            latest_tie_duplicate_fixture(
                &portable_pipeline,
                portable_path.clone(),
                variant == "keyed",
            )
            .unwrap();
        }
        let mut translated =
            prepare_v5(&portable_pipeline, "local-process", "prism-run-fixture").unwrap();
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
            command: env!("CARGO_BIN_EXE_v5_prism_worker").into(),
            env: [(
                env_name.into(),
                portable_path.to_string_lossy().into_owned(),
            )]
            .into(),
        }
        .encode_to_vec();
        let prepared = client
            .prepare(PrepareJobRequest {
                pipeline: Some(translated.pipeline),
                pipeline_options: Some(prost_types::Struct::default()),
                job_name: format!("ironbeam-v5-latest-tie-{variant}-{}", std::process::id()),
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
        for _ in 0..100 {
            let state = client
                .get_state(GetJobStateRequest {
                    job_id: running.job_id.clone(),
                })
                .await
                .unwrap()
                .into_inner()
                .state;
            if matches!(state, 3..=6) {
                terminal = Some(state);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            terminal,
            Some(4),
            "Prism should reject {variant} latest tie keys: {terminal:?}"
        );
        assert!(!portable_path.exists());
    }
}

#[tokio::test]
async fn prism_runs_multi_source_graph_jsonl_and_grouping() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let _prism_job = prism_job_lock().lock().await;
    let capture =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-{}.txt", std::process::id()));
    let capture_right = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-right-{}.txt",
        std::process::id()
    ));
    let capture_merged = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-merged-{}.txt",
        std::process::id()
    ));
    let capture_time =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-time-{}.txt", std::process::id()));
    let capture_window = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-window-{}.txt",
        std::process::id()
    ));
    let capture_window_grouped = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-window-grouped-{}.txt",
        std::process::id()
    ));
    let capture_jsonl = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-jsonl-{}.jsonl",
        std::process::id()
    ));
    let capture_object = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-object-{}.txt",
        std::process::id()
    ));
    let capture_avro =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-avro-{}.txt", std::process::id()));
    let capture_csv =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-csv-{}.txt", std::process::id()));
    let capture_xml =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-xml-{}.txt", std::process::id()));
    let capture_parquet = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-parquet-{}.txt",
        std::process::id()
    ));
    let capture_proto = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-proto-{}.txt",
        std::process::id()
    ));
    let capture_tfrecord = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-tfrecord-{}.txt",
        std::process::id()
    ));
    let capture_reshuffle = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-reshuffle-{}.txt",
        std::process::id()
    ));
    let capture_grouped = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-grouped-{}.txt",
        std::process::id()
    ));
    let capture_latest = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-latest-{}.txt",
        std::process::id()
    ));
    let capture_large_group = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-large-group-{}.txt",
        std::process::id()
    ));
    let capture_wait =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-wait-{}.txt", std::process::id()));
    let wait_marker = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-wait-complete-{}.txt",
        std::process::id()
    ));
    let capture_join_window = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-join-window-{}.txt",
        std::process::id()
    ));
    let capture_side =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-side-{}.txt", std::process::id()));
    let capture_join =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-join-{}.txt", std::process::id()));
    let capture_cogrouped = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-cogrouped-{}.txt",
        std::process::id()
    ));
    let capture_cogroup_three = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-cogroup-three-{}.txt",
        std::process::id()
    ));
    let capture_cogroup_window = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-cogroup-window-{}.txt",
        std::process::id()
    ));
    let capture_cogroup_ten = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-cogroup-ten-{}.txt",
        std::process::id()
    ));
    let capture_keyed_fanout = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-keyed-fanout-{}.txt",
        std::process::id()
    ));
    let source_opens = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-opens-{}.txt",
        std::process::id()
    ));
    let finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-finalization-{}",
        std::process::id()
    ));
    let finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.bin",
        std::process::id()
    ));
    let tfrecord_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-tfrecord-finalization-{}",
        std::process::id()
    ));
    let tfrecord_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.tfrecord",
        std::process::id()
    ));
    let avro_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-avro-finalization-{}",
        std::process::id()
    ));
    let avro_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.avro",
        std::process::id()
    ));
    let msgpack_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-msgpack-finalization-{}",
        std::process::id()
    ));
    let msgpack_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.msgpack",
        std::process::id()
    ));
    let jsonl_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-jsonl-finalization-{}",
        std::process::id()
    ));
    let jsonl_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.jsonl",
        std::process::id()
    ));
    let csv_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-csv-finalization-{}",
        std::process::id()
    ));
    let csv_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.csv",
        std::process::id()
    ));
    let xml_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-xml-finalization-{}",
        std::process::id()
    ));
    let xml_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.xml",
        std::process::id()
    ));
    let parquet_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-parquet-finalization-{}",
        std::process::id()
    ));
    let parquet_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.parquet",
        std::process::id()
    ));
    let cbor_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-cbor-finalization-{}",
        std::process::id()
    ));
    let cbor_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.cbor",
        std::process::id()
    ));
    let protobuf_finalization_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-protobuf-finalization-{}",
        std::process::id()
    ));
    let protobuf_finalization_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-final-result-{}.pb",
        std::process::id()
    ));
    let finalization_journal_dir = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-finalization-journal-{}",
        std::process::id()
    ));
    let input = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-input-{}.txt",
        std::process::id()
    ));
    let jsonl_input = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-input-{}.jsonl",
        std::process::id()
    ));
    let avro_input = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-input-{}.avro",
        std::process::id()
    ));
    let csv_input = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-input-{}.csv",
        std::process::id()
    ));
    let xml_input = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-input-{}.xml",
        std::process::id()
    ));
    let parquet_input = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-input-{}.parquet",
        std::process::id()
    ));
    let proto_input = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-input-{}.proto",
        std::process::id()
    ));
    let proto_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-output-{}.proto",
        std::process::id()
    ));
    let tfrecord_input = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-input-{}.tfrecord",
        std::process::id()
    ));
    let tfrecord_output = std::env::temp_dir().join(format!(
        "ironbeam-v5-prism-output-{}.tfrecord",
        std::process::id()
    ));
    let object_root =
        std::env::temp_dir().join(format!("ironbeam-v5-prism-objects-{}", std::process::id()));
    let _ = std::fs::remove_file(&capture);
    let _ = std::fs::remove_file(&capture_right);
    let _ = std::fs::remove_file(&capture_merged);
    let _ = std::fs::remove_file(&capture_time);
    let _ = std::fs::remove_file(&capture_window);
    let _ = std::fs::remove_file(&capture_window_grouped);
    let _ = std::fs::remove_file(&capture_jsonl);
    let _ = std::fs::remove_file(&capture_object);
    let _ = std::fs::remove_file(&capture_avro);
    let _ = std::fs::remove_file(&capture_csv);
    let _ = std::fs::remove_file(&capture_xml);
    let _ = std::fs::remove_file(&capture_parquet);
    let _ = std::fs::remove_file(&capture_proto);
    let _ = std::fs::remove_file(&proto_output);
    let _ = std::fs::remove_file(&capture_tfrecord);
    let _ = std::fs::remove_file(&tfrecord_output);
    let _ = std::fs::remove_file(&capture_reshuffle);
    let _ = std::fs::remove_file(&capture_grouped);
    let _ = std::fs::remove_file(&capture_latest);
    let _ = std::fs::remove_file(&capture_large_group);
    let _ = std::fs::remove_file(&capture_wait);
    let _ = std::fs::remove_file(&wait_marker);
    let _ = std::fs::remove_file(&capture_join_window);
    let _ = std::fs::remove_file(&capture_side);
    let _ = std::fs::remove_file(&capture_join);
    let _ = std::fs::remove_file(&capture_cogrouped);
    let _ = std::fs::remove_file(&capture_cogroup_three);
    let _ = std::fs::remove_file(&capture_cogroup_window);
    let _ = std::fs::remove_file(&capture_cogroup_ten);
    let _ = std::fs::remove_file(&capture_keyed_fanout);
    let _ = std::fs::remove_file(&source_opens);
    let _ = std::fs::remove_dir_all(&finalization_dir);
    let _ = std::fs::remove_file(&finalization_output);
    let _ = std::fs::remove_dir_all(&tfrecord_finalization_dir);
    let _ = std::fs::remove_file(&tfrecord_finalization_output);
    let _ = std::fs::remove_dir_all(&avro_finalization_dir);
    let _ = std::fs::remove_file(&avro_finalization_output);
    let _ = std::fs::remove_dir_all(&msgpack_finalization_dir);
    let _ = std::fs::remove_file(&msgpack_finalization_output);
    let _ = std::fs::remove_dir_all(&jsonl_finalization_dir);
    let _ = std::fs::remove_file(&jsonl_finalization_output);
    let _ = std::fs::remove_dir_all(&csv_finalization_dir);
    let _ = std::fs::remove_file(&csv_finalization_output);
    let _ = std::fs::remove_dir_all(&xml_finalization_dir);
    let _ = std::fs::remove_file(&xml_finalization_output);
    let _ = std::fs::remove_dir_all(&parquet_finalization_dir);
    let _ = std::fs::remove_file(&parquet_finalization_output);
    let _ = std::fs::remove_dir_all(&cbor_finalization_dir);
    let _ = std::fs::remove_file(&cbor_finalization_output);
    let _ = std::fs::remove_dir_all(&protobuf_finalization_dir);
    let _ = std::fs::remove_file(&protobuf_finalization_output);
    let _ = std::fs::remove_dir_all(&finalization_journal_dir);
    std::fs::write(&input, "1,a\n2,b\n3,c\n").unwrap();
    std::fs::write(&jsonl_input, "{\"id\":5,\"label\":\"jsonl\"}\n").unwrap();
    std::fs::write(&csv_input, "id,label\n8,csv\n").unwrap();
    let xml_seed = Pipeline::default()
        .from_vec(
            "xml-seed",
            vec![UserRow {
                id: 11,
                label: "xml".into(),
            }],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_xml::xml_sink(&xml_seed, "write-xml", &xml_input).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let parquet_seed = Pipeline::default()
        .from_vec(
            "parquet-seed",
            vec![UserRow {
                id: 12,
                label: "parquet".into(),
            }],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_parquet::parquet_sink(&parquet_seed, "write-parquet", &parquet_input)
                .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    std::fs::write(
        &proto_input,
        prost_v5::Message::encode_length_delimited_to_vec(&ProtoRow {
            id: 9,
            label: "proto".into(),
        }),
    )
    .unwrap();
    let tfrecord_seed = Pipeline::default()
        .from_vec(
            "tfrecord-seed",
            vec![prost_v5::Message::encode_to_vec(&ProtoRow {
                id: 10,
                label: "tfrecord".into(),
            })],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_tfrecord::tfrecord_sink(&tfrecord_seed, "write-tfrecord", &tfrecord_input)
                .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let avro_schema = r#"{"type":"record","name":"UserRow","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#;
    let avro_seed = Pipeline::default()
        .from_vec(
            "avro-seed",
            vec![UserRow {
                id: 7,
                label: "avro".into(),
            }],
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &ironbeam_io_avro::avro_sink(&avro_seed, "write-avro", &avro_input, avro_schema)
                .unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let mut objects = ObjectStores::new();
    objects
        .register("file", LocalObjectIO::new(&object_root).unwrap())
        .unwrap();
    objects
        .write(
            "file://bucket/input/row.jsonl",
            b"{\"id\":6,\"label\":\"object\"}\n",
        )
        .unwrap();
    let digest = format!(
        "sha256-v1:{}",
        hex::encode(Sha256::digest(std::fs::read(&input).unwrap()))
    );
    let pipeline = Pipeline::default();
    let source_path = input.clone();
    let source = pipeline
        .bounded_source_with_semantic(
            "source",
            "tests/prism-file-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            digest,
            Some(3),
            move || {
                let text = std::fs::read_to_string(&source_path)
                    .map_err(|error| Error::incompatible(error.to_string()))?;
                let rows = text
                    .lines()
                    .map(|line| {
                        let (id, label) = line
                            .split_once(',')
                            .ok_or_else(|| Error::incompatible("invalid input row"))?;
                        let id = id
                            .parse()
                            .map_err(|_| Error::incompatible("invalid row id"))?;
                        Ok(UserRow {
                            id,
                            label: label.into(),
                        })
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                Ok(Box::new(rows.into_iter().map(Ok)))
            },
        )
        .unwrap();
    finalization_probe_sink(
        &source.reshuffle_named("finalization-boundary").unwrap(),
        "capture-finalization",
        &finalization_dir,
    )
    .unwrap();
    let tfrecord_final = source
        .map_named("tfrecord-final-encode", |row| {
            row.encode().expect("fixture row coder")
        })
        .unwrap()
        .reshuffle_named("tfrecord-finalization-boundary")
        .unwrap();
    ironbeam_io_tfrecord::tfrecord_fragment_sink(
        &tfrecord_final,
        "capture-tfrecord-final",
        &tfrecord_finalization_dir,
        &tfrecord_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
    )
    .unwrap();
    ironbeam_io_avro::avro_fragment_sink(
        &source
            .reshuffle_named("avro-finalization-boundary")
            .unwrap(),
        "capture-avro-final",
        &avro_finalization_dir,
        &avro_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        avro_schema,
    )
    .unwrap();
    ironbeam_io_msgpack::msgpack_fragment_sink(
        &source
            .reshuffle_named("msgpack-finalization-boundary")
            .unwrap(),
        "capture-msgpack-final",
        &msgpack_finalization_dir,
        &msgpack_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
    )
    .unwrap();
    ironbeam_io_jsonl::jsonl_fragment_sink(
        &source
            .reshuffle_named("jsonl-finalization-boundary")
            .unwrap(),
        "capture-jsonl-final",
        &jsonl_finalization_dir,
        &jsonl_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
    )
    .unwrap();
    ironbeam_io_csv::csv_fragment_sink(
        &source.reshuffle_named("csv-finalization-boundary").unwrap(),
        "capture-csv-final",
        &csv_finalization_dir,
        &csv_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
    )
    .unwrap();
    ironbeam_io_xml::xml_fragment_sink(
        &source.reshuffle_named("xml-finalization-boundary").unwrap(),
        "capture-xml-final",
        &xml_finalization_dir,
        &xml_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
    )
    .unwrap();
    ironbeam_io_parquet::parquet_fragment_sink(
        &source
            .reshuffle_named("parquet-finalization-boundary")
            .unwrap(),
        "capture-parquet-final",
        &parquet_finalization_dir,
        &parquet_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
    )
    .unwrap();
    ironbeam_io_cbor::cbor_fragment_sink(
        &source
            .reshuffle_named("cbor-finalization-boundary")
            .unwrap(),
        "capture-cbor-final",
        &cbor_finalization_dir,
        &cbor_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
    )
    .unwrap();
    let protobuf_final = source
        .map_named("protobuf-final-encode", |row| ProtoRow {
            id: row.id,
            label: row.label,
        })
        .unwrap()
        .reshuffle_named("protobuf-finalization-boundary")
        .unwrap();
    ironbeam_io_protobuf::protobuf_fragment_sink(
        &protobuf_final,
        "capture-protobuf-final",
        &protobuf_finalization_dir,
        &protobuf_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
    )
    .unwrap();
    let signal = completion_signal(&pipeline, wait_marker.clone())
        .unwrap()
        .assign_timestamps_named("wait-signal-time", |_| 1_000_000)
        .unwrap()
        .fixed_windows_with_offset("wait-signal-window", 2_000_000, 0)
        .unwrap();
    source
        .assign_timestamps_named("wait-main-time", |row| i64::from(row.id) * 1_000_000)
        .unwrap()
        .fixed_windows_with_offset("wait-main-window", 2_000_000, 1_000_000)
        .unwrap()
        .wait_on_named("wait-for-signal", &signal)
        .unwrap()
        .with_window_named("wait-window-context")
        .unwrap()
        .map_named("wait-row", |(window, mut row)| {
            row.label = format!("{}..{}:{}", window.start_ns, window.end_ns, row.label);
            row
        })
        .unwrap()
        .sink_named_with_config(
            "capture-wait",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_wait.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let extra = pipeline
        .from_vec(
            "source-extra",
            vec![UserRow {
                id: 4,
                label: "d".into(),
            }],
        )
        .unwrap();
    let jsonl = ironbeam_io_jsonl::jsonl_source::<UserRow>(&pipeline, "jsonl-source", &jsonl_input)
        .unwrap();
    let jsonl_output = jsonl
        .map_named("jsonl-add-ten", |mut row| {
            row.id += 10;
            row
        })
        .unwrap();
    ironbeam_io_jsonl::jsonl_append_sink(&jsonl_output, "capture-jsonl", &capture_jsonl).unwrap();
    let proto_source =
        ironbeam_io_protobuf::protobuf_source::<ProtoRow>(&pipeline, "proto-source", &proto_input)
            .unwrap();
    ironbeam_io_protobuf::protobuf_append_sink(&proto_source, "write-proto-append", &proto_output)
        .unwrap();
    proto_source
        .map_named("proto-to-user", |row| UserRow {
            id: row.id + 10,
            label: row.label,
        })
        .unwrap()
        .sink_named_with_config(
            "capture-proto",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_proto.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let tfrecord_rows =
        ironbeam_io_tfrecord::tfrecord_source(&pipeline, "tfrecord-source", &tfrecord_input)
            .unwrap();
    ironbeam_io_tfrecord::tfrecord_append_sink(
        &tfrecord_rows,
        "write-tfrecord-append",
        &tfrecord_output,
    )
    .unwrap();
    tfrecord_rows
        .map_named("tfrecord-to-user", |bytes| {
            let row = <ProtoRow as prost_v5::Message>::decode(bytes.as_slice()).unwrap();
            UserRow {
                id: row.id + 40,
                label: row.label,
            }
        })
        .unwrap()
        .sink_named_with_config(
            "capture-tfrecord",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_tfrecord.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    ironbeam_io_jsonl::cloud_jsonl_uri_source::<UserRow>(
        &pipeline,
        "object-source",
        &objects,
        "file://bucket/input/row.jsonl",
    )
    .unwrap()
    .map_named("object-add-ten", |mut row| {
        row.id += 10;
        row
    })
    .unwrap()
    .sink_named_with_config(
        "capture-object",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("file:{}", capture_object.display()),
        || Ok(Box::new(CaptureSink(Vec::new()))),
    )
    .unwrap();
    ironbeam_io_avro::avro_source::<UserRow>(&pipeline, "avro-source", &avro_input)
        .unwrap()
        .map_named("avro-add-twenty", |mut row| {
            row.id += 20;
            row
        })
        .unwrap()
        .sink_named_with_config(
            "capture-avro",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_avro.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    ironbeam_io_csv::csv_source::<UserRow>(&pipeline, "csv-source", &csv_input)
        .unwrap()
        .map_named("csv-add-thirty", |mut row| {
            row.id += 30;
            row
        })
        .unwrap()
        .sink_named_with_config(
            "capture-csv",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_csv.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    ironbeam_io_xml::xml_source::<UserRow>(&pipeline, "xml-source", &xml_input)
        .unwrap()
        .map_named("xml-add-fifty", |mut row| {
            row.id += 50;
            row
        })
        .unwrap()
        .sink_named_with_config(
            "capture-xml",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_xml.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    ironbeam_io_parquet::parquet_source::<UserRow>(&pipeline, "parquet-source", &parquet_input)
        .unwrap()
        .map_named("parquet-add-sixty", |mut row| {
            row.id += 60;
            row
        })
        .unwrap()
        .sink_named_with_config(
            "capture-parquet",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_parquet.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let pairs = pipeline
        .from_vec(
            "group-pairs",
            vec![
                ("a".to_owned(), 1_i32),
                ("b".to_owned(), 2),
                ("a".to_owned(), 3),
            ],
        )
        .unwrap();
    pairs
        .group_by_key_named("group-pairs-by-key")
        .unwrap()
        .map_named("group-count", |(label, values)| UserRow {
            id: values.len() as i32,
            label,
        })
        .unwrap()
        .sink_named_with_config(
            "capture-grouped",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_grouped.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    pairs
        .assign_timestamps_named("latest-time", |(_, value)| i64::from(*value) * 1_000_000)
        .unwrap()
        .fixed_windows("latest-window", 4_000_000)
        .unwrap()
        .latest_per_key_named("latest-per-key")
        .unwrap()
        .reify_timestamps_named("latest-result-time")
        .unwrap()
        .with_window_named("latest-result-window")
        .unwrap()
        .map_named(
            "latest-result-row",
            |(window, (timestamp, (key, value)))| UserRow {
                id: value,
                label: format!("{key}:{timestamp}:{}..{}", window.start_ns, window.end_ns),
            },
        )
        .unwrap()
        .sink_named_with_config(
            "capture-latest",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_latest.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    pipeline
        .from_vec("large-group-pairs", large_group_rows())
        .unwrap()
        .group_by_key_named("large-group-by-key")
        .unwrap()
        .map_named("large-group-summary", |(key, values)| {
            let (id, bytes, digest) = large_group_summary(values);
            UserRow {
                id,
                label: format!("{key}:{bytes}:{digest}"),
            }
        })
        .unwrap()
        .sink_named_with_config(
            "capture-large-group",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_large_group.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    pairs
        .map_named("keyed-fanout", |(label, id)| UserRow { id, label })
        .unwrap()
        .sink_named_with_config(
            "capture-keyed-fanout",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_keyed_fanout.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let join_right = pipeline
        .from_vec(
            "join-right-source",
            vec![
                ("a".to_owned(), 10_i32),
                ("a".to_owned(), 20),
                ("c".to_owned(), 30),
            ],
        )
        .unwrap();
    let join_inner = pairs
        .join_inner_named("join-inner", &join_right)
        .unwrap()
        .map_named("join-inner-row", |(key, (left, right))| {
            let (id, label) = (left + right, format!("inner:{key}:{left}:{right}"));
            UserRow { id, label }
        })
        .unwrap();
    let join_left = pairs
        .join_left_named("join-left", &join_right)
        .unwrap()
        .map_named("join-left-row", |(key, (left, right))| {
            let (id, label) = (
                left + right.unwrap_or_default(),
                format!("left:{key}:{left}:{right:?}"),
            );
            UserRow { id, label }
        })
        .unwrap();
    let join_right_rows = pairs
        .join_right_named("join-right", &join_right)
        .unwrap()
        .map_named("join-right-row", |(key, (left, right))| {
            let (id, label) = (
                left.unwrap_or_default() + right,
                format!("right:{key}:{left:?}:{right}"),
            );
            UserRow { id, label }
        })
        .unwrap();
    let join_full = pairs
        .join_full_named("join-full", &join_right)
        .unwrap()
        .map_named("join-full-row", |(key, (left, right))| {
            let (id, label) = (
                left.unwrap_or_default() + right.unwrap_or_default(),
                format!("full:{key}:{left:?}:{right:?}"),
            );
            UserRow { id, label }
        })
        .unwrap();
    join_inner
        .flatten_named("join-all", &[join_left, join_right_rows, join_full])
        .unwrap()
        .sink_named_with_config(
            "capture-join",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_join.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let join_window_left = pairs
        .assign_timestamps_named("join-window-left-time", |pair| {
            i64::from(pair.1) * 1_000_000
        })
        .unwrap()
        .fixed_windows_with_offset("join-window-left", 2_000_000, 1_000_000)
        .unwrap();
    let join_window_right = join_right
        .assign_timestamps_named("join-window-right-time", |pair| {
            if pair.1 == 10 {
                1_000_000
            } else {
                3_000_000
            }
        })
        .unwrap()
        .fixed_windows_with_offset("join-window-right", 2_000_000, 1_000_000)
        .unwrap();
    let join_window_rows = join_window_left
        .join_full_named("join-window", &join_window_right)
        .unwrap()
        .reify_timestamps_named("join-window-stamp")
        .unwrap()
        .with_window_named("join-window-context")
        .unwrap()
        .map_named(
            "join-window-row",
            |(window, (timestamp, (key, (left, right))))| UserRow {
                id: left.unwrap_or_default() + right.unwrap_or_default(),
                label: format!(
                    "{key}:{left:?}:{right:?}:{timestamp}:{}..{}",
                    window.start_ns, window.end_ns
                ),
            },
        )
        .unwrap();
    join_window_rows
        .sink_named_with_config(
            "capture-join-window",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_join_window.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let side_values = pipeline
        .from_vec("side-values", vec![10_i32, 20])
        .unwrap()
        .assign_timestamps_named("side-time", |value| {
            (i64::from(*value) / 10 * 2 - 1) * 1_000_000
        })
        .unwrap()
        .fixed_windows_with_offset("side-window", 2_000_000, 1_000_000)
        .unwrap();
    let side_main = pairs
        .assign_timestamps_named("side-main-time", |pair| i64::from(pair.1) * 1_000_000)
        .unwrap()
        .fixed_windows_with_offset("side-main-window", 2_000_000, 1_000_000)
        .unwrap();
    let side_rows = side_main
        .map_with_side_named("add-side", &side_values, |(label, id), values| UserRow {
            id: id + values.iter().sum::<i32>(),
            label,
        })
        .unwrap();
    let mut direct_side_rows = side_rows
        .collect_seq()
        .unwrap()
        .into_iter()
        .map(|row| format!("{}:{}", row.id, row.label))
        .collect::<Vec<_>>();
    direct_side_rows.sort_unstable();
    side_rows
        .sink_named_with_config(
            "capture-side",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_side.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let cogroup_right = pipeline
        .from_vec(
            "cogroup-right",
            vec![("a".to_owned(), true), ("b".to_owned(), false)],
        )
        .unwrap();
    ironbeam_core::cogroup_by_key_2_named("cogroup-pairs", &pairs, &cogroup_right)
        .unwrap()
        .map_named("cogroup-count", |(key, (left, right))| UserRow {
            id: left.iter().sum(),
            label: format!("{key}:{}:{}", left.len(), right.len()),
        })
        .unwrap()
        .sink_named_with_config(
            "capture-cogrouped",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_cogrouped.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let cogroup_third = pipeline
        .from_vec(
            "cogroup-third",
            vec![
                ("a".to_owned(), "x".to_owned()),
                ("a".to_owned(), "y".to_owned()),
                ("b".to_owned(), "z".to_owned()),
            ],
        )
        .unwrap();
    ironbeam_core::cogroup_by_key_3_named("cogroup-three", &pairs, &cogroup_right, &cogroup_third)
        .unwrap()
        .map_named("cogroup-three-count", |(key, (left, right, third))| {
            UserRow {
                id: left.iter().sum::<i32>() + right.len() as i32 + third.len() as i32,
                label: format!("{key}:{}:{}:{}", left.len(), right.len(), third.len()),
            }
        })
        .unwrap()
        .sink_named_with_config(
            "capture-cogroup-three",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_cogroup_three.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let cogroup_window_left = pairs
        .assign_timestamps_named("cogroup-window-left-time", |pair| {
            i64::from(pair.1) * 1_000_000
        })
        .unwrap()
        .fixed_windows_with_offset("cogroup-window-left", 2_000_000, 1_000_000)
        .unwrap();
    let cogroup_window_right = cogroup_right
        .assign_timestamps_named("cogroup-window-right-time", |pair| {
            if pair.0 == "a" {
                1_000_000
            } else {
                3_000_000
            }
        })
        .unwrap()
        .fixed_windows_with_offset("cogroup-window-right", 2_000_000, 1_000_000)
        .unwrap();
    ironbeam_core::cogroup_by_key_2_named(
        "cogroup-window",
        &cogroup_window_left,
        &cogroup_window_right,
    )
    .unwrap()
    .with_window_named("cogroup-window-metadata")
    .unwrap()
    .map_named("cogroup-window-row", |(window, (key, (left, right)))| {
        UserRow {
            id: left.iter().sum(),
            label: format!(
                "{key}:{}:{}:{}..{}",
                left.len(),
                right.len(),
                window.start_ns,
                window.end_ns
            ),
        }
    })
    .unwrap()
    .sink_named_with_config(
        "capture-cogroup-window",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("file:{}", capture_cogroup_window.display()),
        || Ok(Box::new(CaptureSink(Vec::new()))),
    )
    .unwrap();
    let ten_inputs = (0..10)
        .map(|index| {
            pipeline
                .from_vec(
                    &format!("cogroup-ten-source-{index}"),
                    vec![("a".to_owned(), index)],
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    ironbeam_core::cogroup_by_key_10_named(
        "cogroup-ten",
        &ten_inputs[0],
        &ten_inputs[1],
        &ten_inputs[2],
        &ten_inputs[3],
        &ten_inputs[4],
        &ten_inputs[5],
        &ten_inputs[6],
        &ten_inputs[7],
        &ten_inputs[8],
        &ten_inputs[9],
    )
    .unwrap()
    .map_named(
        "cogroup-ten-sum",
        |(key, (a, b, c, d, e, f, g, h, i, j))| UserRow {
            id: [a, b, c, d, e, f, g, h, i, j].into_iter().flatten().sum(),
            label: key,
        },
    )
    .unwrap()
    .sink_named_with_config(
        "capture-cogroup-ten",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        format!("file:{}", capture_cogroup_ten.display()),
        || Ok(Box::new(CaptureSink(Vec::new()))),
    )
    .unwrap();
    let doubled = source
        .map_named("double", |mut row| {
            row.id *= 2;
            row
        })
        .unwrap();
    source
        .reshuffle_named("redistribute")
        .unwrap()
        .map_named("reshuffle-mark", |mut row| {
            row.label.push_str("-shuffled");
            row
        })
        .unwrap()
        .sink_named_with_config(
            "capture-reshuffle",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_reshuffle.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    doubled
        .sink_named_with_config(
            "capture",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let added = source
        .map_named("add-hundred", |mut row| {
            row.id += 100;
            row
        })
        .unwrap();
    added
        .sink_named_with_config(
            "capture-right",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_right.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    doubled
        .flatten_named("merged", &[added, extra])
        .unwrap()
        .sink_named_with_config(
            "capture-merged",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_merged.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    source
        .assign_timestamps_named("event-time", |row| i64::from(row.id) * 1_000_000)
        .unwrap()
        .reify_timestamps_named("reify-time")
        .unwrap()
        .sink_named_with_config(
            "capture-time",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_time.display()),
            || Ok(Box::new(NullTimeSink)),
        )
        .unwrap();
    source
        .assign_timestamps_named("window-time", |row| i64::from(row.id) * 1_000_000)
        .unwrap()
        .fixed_windows_with_offset("fixed-window", 2_000_000, 1_000_000)
        .unwrap()
        .reshuffle_named("window-redistribute")
        .unwrap()
        .with_window_named("window-metadata")
        .unwrap()
        .map_named("window-row", |(window, row)| UserRow {
            id: row.id,
            label: format!("{}..{}:{}", window.start_ns, window.end_ns, row.label),
        })
        .unwrap()
        .sink_named_with_config(
            "capture-window",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_window.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    source
        .map_named("group-window-pairs", |row| ("all".to_owned(), row.id))
        .unwrap()
        .assign_timestamps_named("group-window-time", |pair| i64::from(pair.1) * 1_000_000)
        .unwrap()
        .fixed_windows_with_offset("group-window-set", 2_000_000, 1_000_000)
        .unwrap()
        .group_by_key_named("group-window-gbk")
        .unwrap()
        .with_window_named("group-window-metadata")
        .unwrap()
        .map_named("group-window-row", |(window, (key, values))| UserRow {
            id: values.len() as i32,
            label: format!("{}..{}:{key}", window.start_ns, window.end_ns),
        })
        .unwrap()
        .sink_named_with_config(
            "capture-window-grouped",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            format!("file:{}", capture_window_grouped.display()),
            || Ok(Box::new(CaptureSink(Vec::new()))),
        )
        .unwrap();
    let mut translated = prepare_v5(&pipeline, "local-process", "prism-run-fixture").unwrap();
    let publication_plan = PortablePublicationPlan::from_translated(&translated).unwrap();
    let extra_source_pc = translated
        .pipeline
        .components
        .as_ref()
        .unwrap()
        .pcollections
        .iter()
        .find(|(_, collection)| collection.unique_name == "source-extra")
        .unwrap()
        .0
        .clone();
    let worker = env!("CARGO_BIN_EXE_v5_prism_worker");
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
        command: worker.into(),
        env: [
            (
                "IRONBEAM_V5_PRISM_CAPTURE".into(),
                capture.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_RIGHT".into(),
                capture_right.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_MERGED".into(),
                capture_merged.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_TIME".into(),
                capture_time.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_WINDOW".into(),
                capture_window.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_WINDOW_GROUPED".into(),
                capture_window_grouped.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_JSONL".into(),
                capture_jsonl.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_OBJECT".into(),
                capture_object.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_AVRO".into(),
                capture_avro.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_CSV".into(),
                capture_csv.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_XML".into(),
                capture_xml.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_PARQUET".into(),
                capture_parquet.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_PROTO".into(),
                capture_proto.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_TFRECORD".into(),
                capture_tfrecord.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_RESHUFFLE".into(),
                capture_reshuffle.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_GROUPED".into(),
                capture_grouped.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_LATEST".into(),
                capture_latest.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_LARGE_GROUP".into(),
                capture_large_group.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_WAIT".into(),
                capture_wait.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_WAIT_MARKER".into(),
                wait_marker.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_JOIN_WINDOW".into(),
                capture_join_window.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_SIDE".into(),
                capture_side.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_JOIN".into(),
                capture_join.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_COGROUPED".into(),
                capture_cogrouped.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_COGROUP_THREE".into(),
                capture_cogroup_three.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_COGROUP_WINDOW".into(),
                capture_cogroup_window.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_COGROUP_TEN".into(),
                capture_cogroup_ten.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CAPTURE_KEYED_FANOUT".into(),
                capture_keyed_fanout.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_SOURCE_OPENS".into(),
                source_opens.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_FINALIZATION_DIR".into(),
                finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_TFRECORD_FINALIZATION_DIR".into(),
                tfrecord_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_AVRO_FINALIZATION_DIR".into(),
                avro_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_MSGPACK_FINALIZATION_DIR".into(),
                msgpack_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_JSONL_FINALIZATION_DIR".into(),
                jsonl_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CSV_FINALIZATION_DIR".into(),
                csv_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_XML_FINALIZATION_DIR".into(),
                xml_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_PARQUET_FINALIZATION_DIR".into(),
                parquet_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CBOR_FINALIZATION_DIR".into(),
                cbor_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_PROTOBUF_FINALIZATION_DIR".into(),
                protobuf_finalization_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_FINALIZATION_JOURNAL_DIR".into(),
                finalization_journal_dir.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_INPUT".into(),
                input.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_JSONL_INPUT".into(),
                jsonl_input.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_OBJECT_ROOT".into(),
                object_root.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_AVRO_INPUT".into(),
                avro_input.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_CSV_INPUT".into(),
                csv_input.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_XML_INPUT".into(),
                xml_input.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_PARQUET_INPUT".into(),
                parquet_input.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_PROTO_INPUT".into(),
                proto_input.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_PROTO_OUTPUT".into(),
                proto_output.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_TFRECORD_INPUT".into(),
                tfrecord_input.to_string_lossy().into_owned(),
            ),
            (
                "IRONBEAM_V5_PRISM_TFRECORD_OUTPUT".into(),
                tfrecord_output.to_string_lossy().into_owned(),
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
            job_name: format!("ironbeam-v5-run-{}", std::process::id()),
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
        "Prism job did not reach DONE: {terminal:?}; last state: {last_state:?}"
    );
    let metrics = client
        .get_job_metrics(GetJobMetricsRequest {
            job_id: running.job_id.clone(),
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
            &finalization_journal_dir,
        )
        .unwrap();
    assert_eq!(ready.sinks().count(), 10);
    assert!(!ready
        .bundle_attempts("capture-finalization")
        .unwrap()
        .is_empty());
    assert_eq!(
        ironbeam_dataflow::portable_metrics::committed_element_count(&metrics, &extra_source_pc,)
            .unwrap_or_else(|error| {
                let committed_collections = metrics
                    .committed
                    .iter()
                    .filter(|info| info.urn == "beam:metric:element_count:v1")
                    .filter_map(|info| info.labels.get("PCOLLECTION"))
                    .collect::<Vec<_>>();
                panic!("{error}; committed PCollection metrics: {committed_collections:?}");
            }),
        1
    );
    let committed_finalization_rows = ready.committed_rows("capture-finalization").unwrap();
    assert_eq!(committed_finalization_rows, 3);
    let store = finalization_probe_store(&finalization_dir, "capture-finalization")
        .unwrap()
        .require_bundle_attempts(ready.bundle_attempts("capture-finalization").unwrap())
        .unwrap();
    let finalized = std::fs::read_dir(&finalization_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert!(!finalized.is_empty());
    assert!(finalized
        .iter()
        .all(|path| path.extension().is_some_and(|part| part == "accepted")));
    let accepted_fragments = store.accepted_fragments().unwrap();
    let accepted_rows = accepted_fragments
        .iter()
        .map(|fragment| fragment.rows)
        .sum::<u64>();
    assert_eq!(accepted_rows, committed_finalization_rows);
    assert!(!finalization_output.exists());
    let published = store
        .publish_validated_concatenated(committed_finalization_rows, &finalization_output, |path| {
            decode_final_rows(path, committed_finalization_rows).map(|_| ())
        })
        .unwrap();
    assert_eq!(published.rows, 3);
    assert_eq!(
        store
            .publish_validated_concatenated(
                committed_finalization_rows,
                &finalization_output,
                |path| decode_final_rows(path, committed_finalization_rows).map(|_| ()),
            )
            .unwrap(),
        published
    );
    let mut final_rows =
        decode_final_rows(&finalization_output, committed_finalization_rows).unwrap();
    final_rows.sort_unstable();
    assert_eq!(final_rows, ["1:a", "2:b", "3:c"]);
    let committed_tfrecord_rows = ready.committed_rows("capture-tfrecord-final").unwrap();
    assert_eq!(committed_tfrecord_rows, 3);
    let tfrecord_store = ironbeam_io_tfrecord::tfrecord_fragment_store(
        &tfrecord_finalization_dir,
        &tfrecord_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-tfrecord-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-tfrecord-final").unwrap())
    .unwrap();
    assert!(!tfrecord_finalization_output.exists());
    let published_tfrecord = ironbeam_io_tfrecord::publish_tfrecord_fragments(
        &tfrecord_store,
        committed_tfrecord_rows,
        &tfrecord_finalization_output,
    )
    .unwrap();
    assert_eq!(published_tfrecord.rows, 3);
    assert_eq!(
        ironbeam_io_tfrecord::publish_tfrecord_fragments(
            &tfrecord_store,
            committed_tfrecord_rows,
            &tfrecord_finalization_output,
        )
        .unwrap(),
        published_tfrecord
    );
    let tfrecord_values = ironbeam_io_tfrecord::tfrecord_source(
        &Pipeline::default(),
        "verify-final-tfrecord",
        &tfrecord_finalization_output,
    )
    .unwrap()
    .output("verify-final-tfrecord-out")
    .unwrap()
    .collect_seq()
    .unwrap();
    let mut tfrecord_rows = tfrecord_values
        .iter()
        .map(|bytes| UserRow::decode(bytes).unwrap())
        .map(|row| format!("{}:{}", row.id, row.label))
        .collect::<Vec<_>>();
    tfrecord_rows.sort_unstable();
    assert_eq!(tfrecord_rows, ["1:a", "2:b", "3:c"]);
    let committed_avro_rows = ready.committed_rows("capture-avro-final").unwrap();
    assert_eq!(committed_avro_rows, 3);
    let avro_store = ironbeam_io_avro::avro_fragment_store(
        &avro_finalization_dir,
        &avro_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-avro-final",
        avro_schema,
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-avro-final").unwrap())
    .unwrap();
    assert!(!avro_finalization_output.exists());
    let published_avro = ironbeam_io_avro::publish_avro_fragments(
        &avro_store,
        committed_avro_rows,
        &avro_finalization_output,
    )
    .unwrap();
    assert_eq!(published_avro.rows, 3);
    assert_eq!(
        ironbeam_io_avro::publish_avro_fragments(
            &avro_store,
            committed_avro_rows,
            &avro_finalization_output,
        )
        .unwrap(),
        published_avro
    );
    let mut avro_rows = ironbeam_io_avro::avro_source::<UserRow>(
        &Pipeline::default(),
        "verify-final-avro",
        &avro_finalization_output,
    )
    .unwrap()
    .output("verify-final-avro-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .iter()
    .map(|row| format!("{}:{}", row.id, row.label))
    .collect::<Vec<_>>();
    avro_rows.sort_unstable();
    assert_eq!(avro_rows, ["1:a", "2:b", "3:c"]);
    let committed_msgpack_rows = ready.committed_rows("capture-msgpack-final").unwrap();
    assert_eq!(committed_msgpack_rows, 3);
    let msgpack_store = ironbeam_io_msgpack::msgpack_fragment_store(
        &msgpack_finalization_dir,
        &msgpack_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-msgpack-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-msgpack-final").unwrap())
    .unwrap();
    assert!(!msgpack_finalization_output.exists());
    let published_msgpack = ironbeam_io_msgpack::publish_msgpack_fragments::<UserRow>(
        &msgpack_store,
        committed_msgpack_rows,
        &msgpack_finalization_output,
    )
    .unwrap();
    assert_eq!(published_msgpack.rows, 3);
    assert_eq!(
        ironbeam_io_msgpack::publish_msgpack_fragments::<UserRow>(
            &msgpack_store,
            committed_msgpack_rows,
            &msgpack_finalization_output,
        )
        .unwrap(),
        published_msgpack
    );
    let mut msgpack_rows = ironbeam_io_msgpack::msgpack_source::<UserRow>(
        &Pipeline::default(),
        "verify-final-msgpack",
        &msgpack_finalization_output,
    )
    .unwrap()
    .output("verify-final-msgpack-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .iter()
    .map(|row| format!("{}:{}", row.id, row.label))
    .collect::<Vec<_>>();
    msgpack_rows.sort_unstable();
    assert_eq!(msgpack_rows, ["1:a", "2:b", "3:c"]);
    let committed_jsonl_rows = ready.committed_rows("capture-jsonl-final").unwrap();
    assert_eq!(committed_jsonl_rows, 3);
    let jsonl_store = ironbeam_io_jsonl::jsonl_fragment_store(
        &jsonl_finalization_dir,
        &jsonl_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-jsonl-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-jsonl-final").unwrap())
    .unwrap();
    assert!(!jsonl_finalization_output.exists());
    let published_jsonl = ironbeam_io_jsonl::publish_jsonl_fragments::<UserRow>(
        &jsonl_store,
        committed_jsonl_rows,
        &jsonl_finalization_output,
    )
    .unwrap();
    assert_eq!(published_jsonl.rows, 3);
    assert_eq!(
        ironbeam_io_jsonl::publish_jsonl_fragments::<UserRow>(
            &jsonl_store,
            committed_jsonl_rows,
            &jsonl_finalization_output,
        )
        .unwrap(),
        published_jsonl
    );
    let mut jsonl_rows = ironbeam_io_jsonl::jsonl_source::<UserRow>(
        &Pipeline::default(),
        "verify-final-jsonl",
        &jsonl_finalization_output,
    )
    .unwrap()
    .output("verify-final-jsonl-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .iter()
    .map(|row| format!("{}:{}", row.id, row.label))
    .collect::<Vec<_>>();
    jsonl_rows.sort_unstable();
    assert_eq!(jsonl_rows, ["1:a", "2:b", "3:c"]);
    let committed_csv_rows = ready.committed_rows("capture-csv-final").unwrap();
    assert_eq!(committed_csv_rows, 3);
    let csv_store = ironbeam_io_csv::csv_fragment_store(
        &csv_finalization_dir,
        &csv_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-csv-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-csv-final").unwrap())
    .unwrap();
    assert!(!csv_finalization_output.exists());
    let published_csv = ironbeam_io_csv::publish_csv_fragments::<UserRow>(
        &csv_store,
        committed_csv_rows,
        &csv_finalization_output,
    )
    .unwrap();
    assert_eq!(published_csv.rows, 3);
    assert_eq!(
        ironbeam_io_csv::publish_csv_fragments::<UserRow>(
            &csv_store,
            committed_csv_rows,
            &csv_finalization_output,
        )
        .unwrap(),
        published_csv
    );
    assert_eq!(
        std::fs::read_to_string(&csv_finalization_output)
            .unwrap()
            .matches("id,label")
            .count(),
        1
    );
    let mut csv_rows = ironbeam_io_csv::csv_source::<UserRow>(
        &Pipeline::default(),
        "verify-final-csv",
        &csv_finalization_output,
    )
    .unwrap()
    .output("verify-final-csv-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .iter()
    .map(|row| format!("{}:{}", row.id, row.label))
    .collect::<Vec<_>>();
    csv_rows.sort_unstable();
    assert_eq!(csv_rows, ["1:a", "2:b", "3:c"]);
    let committed_xml_rows = ready.committed_rows("capture-xml-final").unwrap();
    assert_eq!(committed_xml_rows, 3);
    let xml_store = ironbeam_io_xml::xml_fragment_store(
        &xml_finalization_dir,
        &xml_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-xml-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-xml-final").unwrap())
    .unwrap();
    assert!(!xml_finalization_output.exists());
    let published_xml = ironbeam_io_xml::publish_xml_fragments::<UserRow>(
        &xml_store,
        committed_xml_rows,
        &xml_finalization_output,
    )
    .unwrap();
    assert_eq!(published_xml.rows, 3);
    assert_eq!(
        ironbeam_io_xml::publish_xml_fragments::<UserRow>(
            &xml_store,
            committed_xml_rows,
            &xml_finalization_output,
        )
        .unwrap(),
        published_xml
    );
    let mut xml_rows = ironbeam_io_xml::xml_source::<UserRow>(
        &Pipeline::default(),
        "verify-final-xml",
        &xml_finalization_output,
    )
    .unwrap()
    .output("verify-final-xml-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .iter()
    .map(|row| format!("{}:{}", row.id, row.label))
    .collect::<Vec<_>>();
    xml_rows.sort_unstable();
    assert_eq!(xml_rows, ["1:a", "2:b", "3:c"]);
    let committed_parquet_rows = ready.committed_rows("capture-parquet-final").unwrap();
    assert_eq!(committed_parquet_rows, 3);
    let parquet_store = ironbeam_io_parquet::parquet_fragment_store::<UserRow>(
        &parquet_finalization_dir,
        &parquet_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-parquet-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-parquet-final").unwrap())
    .unwrap();
    assert!(!parquet_finalization_output.exists());
    let published_parquet = ironbeam_io_parquet::publish_parquet_fragments::<UserRow>(
        &parquet_store,
        committed_parquet_rows,
        &parquet_finalization_output,
    )
    .unwrap();
    assert_eq!(published_parquet.rows, 3);
    assert_eq!(
        ironbeam_io_parquet::publish_parquet_fragments::<UserRow>(
            &parquet_store,
            committed_parquet_rows,
            &parquet_finalization_output,
        )
        .unwrap(),
        published_parquet
    );
    let mut parquet_rows = ironbeam_io_parquet::parquet_source::<UserRow>(
        &Pipeline::default(),
        "verify-final-parquet",
        &parquet_finalization_output,
    )
    .unwrap()
    .output("verify-final-parquet-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .iter()
    .map(|row| format!("{}:{}", row.id, row.label))
    .collect::<Vec<_>>();
    parquet_rows.sort_unstable();
    assert_eq!(parquet_rows, ["1:a", "2:b", "3:c"]);
    let committed_cbor_rows = ready.committed_rows("capture-cbor-final").unwrap();
    assert_eq!(committed_cbor_rows, 3);
    let cbor_store = ironbeam_io_cbor::cbor_fragment_store(
        &cbor_finalization_dir,
        &cbor_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-cbor-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-cbor-final").unwrap())
    .unwrap();
    assert!(!cbor_finalization_output.exists());
    let published_cbor = ironbeam_io_cbor::publish_cbor_fragments::<UserRow>(
        &cbor_store,
        committed_cbor_rows,
        &cbor_finalization_output,
    )
    .unwrap();
    assert_eq!(published_cbor.rows, 3);
    assert_eq!(
        ironbeam_io_cbor::publish_cbor_fragments::<UserRow>(
            &cbor_store,
            committed_cbor_rows,
            &cbor_finalization_output,
        )
        .unwrap(),
        published_cbor
    );
    let mut cbor_rows = ironbeam_io_cbor::cbor_source::<UserRow>(
        &Pipeline::default(),
        "verify-final-cbor",
        &cbor_finalization_output,
    )
    .unwrap()
    .output("verify-final-cbor-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .iter()
    .map(|row| format!("{}:{}", row.id, row.label))
    .collect::<Vec<_>>();
    cbor_rows.sort_unstable();
    assert_eq!(cbor_rows, ["1:a", "2:b", "3:c"]);
    let committed_protobuf_rows = ready.committed_rows("capture-protobuf-final").unwrap();
    assert_eq!(committed_protobuf_rows, 3);
    let protobuf_store = ironbeam_io_protobuf::protobuf_fragment_store(
        &protobuf_finalization_dir,
        &protobuf_finalization_dir.to_string_lossy(),
        "prism-run-fixture",
        "capture-protobuf-final",
    )
    .unwrap()
    .require_bundle_attempts(ready.bundle_attempts("capture-protobuf-final").unwrap())
    .unwrap();
    assert!(!protobuf_finalization_output.exists());
    let published_protobuf = ironbeam_io_protobuf::publish_protobuf_fragments::<ProtoRow>(
        &protobuf_store,
        committed_protobuf_rows,
        &protobuf_finalization_output,
    )
    .unwrap();
    assert_eq!(published_protobuf.rows, 3);
    assert_eq!(
        ironbeam_io_protobuf::publish_protobuf_fragments::<ProtoRow>(
            &protobuf_store,
            committed_protobuf_rows,
            &protobuf_finalization_output,
        )
        .unwrap(),
        published_protobuf
    );
    let mut protobuf_rows = ironbeam_io_protobuf::protobuf_source::<ProtoRow>(
        &Pipeline::default(),
        "verify-final-protobuf",
        &protobuf_finalization_output,
    )
    .unwrap()
    .output("verify-final-protobuf-out")
    .unwrap()
    .collect_seq()
    .unwrap()
    .iter()
    .map(|row| format!("{}:{}", row.id, row.label))
    .collect::<Vec<_>>();
    protobuf_rows.sort_unstable();
    assert_eq!(protobuf_rows, ["1:a", "2:b", "3:c"]);
    let mut values = std::fs::read_to_string(&capture)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    values.sort_unstable();
    assert_eq!(values, ["2:a", "4:b", "6:c"]);
    let mut right_values = std::fs::read_to_string(&capture_right)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    right_values.sort_unstable();
    assert_eq!(right_values, ["101:a", "102:b", "103:c"]);
    let mut merged_values = std::fs::read_to_string(&capture_merged)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    merged_values.sort_unstable();
    assert_eq!(
        merged_values,
        ["101:a", "102:b", "103:c", "2:a", "4:b", "4:d", "6:c"]
    );
    let mut time_values = std::fs::read_to_string(&capture_time)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    time_values.sort_unstable();
    assert_eq!(time_values, ["1000000:1:a", "2000000:2:b", "3000000:3:c"]);
    let mut window_values = std::fs::read_to_string(&capture_window)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    window_values.sort_unstable();
    assert_eq!(
        window_values,
        [
            "1:1000000..3000000:a",
            "2:1000000..3000000:b",
            "3:3000000..5000000:c",
        ]
    );
    let mut grouped_window_values = std::fs::read_to_string(&capture_window_grouped)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    grouped_window_values.sort_unstable();
    assert_eq!(
        grouped_window_values,
        ["1:3000000..5000000:all", "2:1000000..3000000:all"]
    );
    assert_eq!(
        std::fs::read_to_string(&capture_jsonl).unwrap(),
        "{\"id\":15,\"label\":\"jsonl\"}\n"
    );
    assert_eq!(
        std::fs::read_to_string(&capture_object).unwrap(),
        "16:object\n"
    );
    assert_eq!(std::fs::read_to_string(&capture_avro).unwrap(), "27:avro\n");
    assert_eq!(std::fs::read_to_string(&capture_csv).unwrap(), "38:csv\n");
    assert_eq!(std::fs::read_to_string(&capture_xml).unwrap(), "61:xml\n");
    assert_eq!(
        std::fs::read_to_string(&capture_parquet).unwrap(),
        "72:parquet\n"
    );
    assert_eq!(
        std::fs::read_to_string(&capture_proto).unwrap(),
        "19:proto\n"
    );
    let portable_proto = ironbeam_io_protobuf::protobuf_source::<ProtoRow>(
        &Pipeline::default(),
        "verify-portable-proto",
        &proto_output,
    )
    .unwrap()
    .output("verify-portable-proto-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&portable_proto, &JobConfig::default())
            .unwrap()
            .0,
        vec![ProtoRow {
            id: 9,
            label: "proto".into()
        }]
    );
    assert_eq!(
        std::fs::read_to_string(&capture_tfrecord).unwrap(),
        "50:tfrecord\n"
    );
    let portable_tfrecord = ironbeam_io_tfrecord::tfrecord_source(
        &Pipeline::default(),
        "verify-portable-tfrecord",
        &tfrecord_output,
    )
    .unwrap()
    .output("verify-portable-tfrecord-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&portable_tfrecord, &JobConfig::default())
            .unwrap()
            .0,
        vec![prost_v5::Message::encode_to_vec(&ProtoRow {
            id: 10,
            label: "tfrecord".into()
        })]
    );
    let mut reshuffled = std::fs::read_to_string(&capture_reshuffle)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    reshuffled.sort_unstable();
    assert_eq!(reshuffled, ["1:a-shuffled", "2:b-shuffled", "3:c-shuffled"]);
    let mut grouped = std::fs::read_to_string(&capture_grouped)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    grouped.sort_unstable();
    assert_eq!(grouped, ["1:b", "2:a"]);
    let mut latest = std::fs::read_to_string(&capture_latest)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    latest.sort_unstable();
    assert_eq!(latest, ["2:b:2000000:0..4000000", "3:a:3000000:0..4000000"]);
    assert_eq!(
        std::fs::read_to_string(&capture_large_group).unwrap(),
        "512:large:2097152:405c963eb75f7845d80282d761ae7b5cec94ae72c6af900d74a63a0c8f42ac4e\n"
    );
    assert_eq!(std::fs::read_to_string(&wait_marker).unwrap(), "complete");
    let mut waited = std::fs::read_to_string(&capture_wait)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    waited.sort_unstable();
    assert_eq!(
        waited,
        [
            "1:1000000..3000000:a",
            "2:1000000..3000000:b",
            "3:3000000..5000000:c",
        ]
    );
    let mut joined = std::fs::read_to_string(&capture_join)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    joined.sort_unstable();
    let mut expected_join = Vec::new();
    for (left, right) in [(1, 10), (1, 20), (3, 10), (3, 20)] {
        expected_join.push(format!("{}:inner:a:{left}:{right}", left + right));
        expected_join.push(format!("{}:left:a:{left}:Some({right})", left + right));
        expected_join.push(format!("{}:right:a:Some({left}):{right}", left + right));
        expected_join.push(format!(
            "{}:full:a:Some({left}):Some({right})",
            left + right
        ));
    }
    expected_join.extend([
        "2:left:b:2:None".to_owned(),
        "2:full:b:Some(2):None".to_owned(),
        "30:right:c:None:30".to_owned(),
        "30:full:c:None:Some(30)".to_owned(),
    ]);
    expected_join.sort_unstable();
    assert_eq!(joined, expected_join);
    let mut joined_window = std::fs::read_to_string(&capture_join_window)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    joined_window.sort_unstable();
    assert_eq!(
        joined_window,
        [
            "11:a:Some(1):Some(10):1000000:1000000..3000000",
            "23:a:Some(3):Some(20):3000000:3000000..5000000",
            "2:b:Some(2):None:2000000:1000000..3000000",
            "30:c:None:Some(30):3000000:3000000..5000000",
        ]
    );
    let mut side_rows = std::fs::read_to_string(&capture_side)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    side_rows.sort_unstable();
    assert_eq!(side_rows, ["31:a", "32:b", "33:a"]);
    assert_eq!(side_rows, direct_side_rows);
    let mut cogrouped = std::fs::read_to_string(&capture_cogrouped)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    cogrouped.sort_unstable();
    assert_eq!(cogrouped, ["2:b:1:1", "4:a:2:1"]);
    let mut cogroup_three = std::fs::read_to_string(&capture_cogroup_three)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    cogroup_three.sort_unstable();
    assert_eq!(cogroup_three, ["4:b:1:1:1", "7:a:2:1:2"]);
    let mut cogroup_window = std::fs::read_to_string(&capture_cogroup_window)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    cogroup_window.sort_unstable();
    assert_eq!(
        cogroup_window,
        [
            "0:b:0:1:3000000..5000000",
            "1:a:1:1:1000000..3000000",
            "2:b:1:0:1000000..3000000",
            "3:a:1:0:3000000..5000000",
        ]
    );
    assert_eq!(
        std::fs::read_to_string(&capture_cogroup_ten).unwrap(),
        "45:a\n"
    );
    let mut keyed_fanout = std::fs::read_to_string(&capture_keyed_fanout)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    keyed_fanout.sort_unstable();
    assert_eq!(keyed_fanout, ["1:a", "2:b", "3:a"]);
    assert_eq!(std::fs::read_to_string(&source_opens).unwrap(), "open\n");
    let _ = std::fs::remove_file(capture);
    let _ = std::fs::remove_file(capture_right);
    let _ = std::fs::remove_file(capture_merged);
    let _ = std::fs::remove_file(capture_time);
    let _ = std::fs::remove_file(capture_window);
    let _ = std::fs::remove_file(capture_window_grouped);
    let _ = std::fs::remove_file(capture_jsonl);
    let _ = std::fs::remove_file(capture_object);
    let _ = std::fs::remove_file(capture_avro);
    let _ = std::fs::remove_file(capture_csv);
    let _ = std::fs::remove_file(capture_xml);
    let _ = std::fs::remove_file(capture_parquet);
    let _ = std::fs::remove_file(capture_proto);
    let _ = std::fs::remove_file(capture_tfrecord);
    let _ = std::fs::remove_file(capture_reshuffle);
    let _ = std::fs::remove_file(capture_grouped);
    let _ = std::fs::remove_file(capture_latest);
    let _ = std::fs::remove_file(capture_large_group);
    let _ = std::fs::remove_file(capture_wait);
    let _ = std::fs::remove_file(wait_marker);
    let _ = std::fs::remove_file(capture_join_window);
    let _ = std::fs::remove_file(capture_side);
    let _ = std::fs::remove_file(capture_join);
    let _ = std::fs::remove_file(capture_cogrouped);
    let _ = std::fs::remove_file(capture_cogroup_three);
    let _ = std::fs::remove_file(capture_cogroup_window);
    let _ = std::fs::remove_file(capture_cogroup_ten);
    let _ = std::fs::remove_file(capture_keyed_fanout);
    let _ = std::fs::remove_file(source_opens);
    let _ = std::fs::remove_dir_all(finalization_dir);
    let _ = std::fs::remove_file(finalization_output);
    let _ = std::fs::remove_dir_all(tfrecord_finalization_dir);
    let _ = std::fs::remove_file(tfrecord_finalization_output);
    let _ = std::fs::remove_dir_all(avro_finalization_dir);
    let _ = std::fs::remove_file(avro_finalization_output);
    let _ = std::fs::remove_dir_all(msgpack_finalization_dir);
    let _ = std::fs::remove_file(msgpack_finalization_output);
    let _ = std::fs::remove_dir_all(jsonl_finalization_dir);
    let _ = std::fs::remove_file(jsonl_finalization_output);
    let _ = std::fs::remove_dir_all(csv_finalization_dir);
    let _ = std::fs::remove_file(csv_finalization_output);
    let _ = std::fs::remove_dir_all(xml_finalization_dir);
    let _ = std::fs::remove_file(xml_finalization_output);
    let _ = std::fs::remove_dir_all(parquet_finalization_dir);
    let _ = std::fs::remove_file(parquet_finalization_output);
    let _ = std::fs::remove_dir_all(cbor_finalization_dir);
    let _ = std::fs::remove_file(cbor_finalization_output);
    let _ = std::fs::remove_dir_all(protobuf_finalization_dir);
    let _ = std::fs::remove_file(protobuf_finalization_output);
    let _ = std::fs::remove_dir_all(finalization_journal_dir);
    let _ = std::fs::remove_file(input);
    let _ = std::fs::remove_file(jsonl_input);
    let _ = std::fs::remove_file(avro_input);
    let _ = std::fs::remove_file(csv_input);
    let _ = std::fs::remove_file(xml_input);
    let _ = std::fs::remove_file(parquet_input);
    let _ = std::fs::remove_file(proto_input);
    let _ = std::fs::remove_file(proto_output);
    let _ = std::fs::remove_file(tfrecord_input);
    let _ = std::fs::remove_file(tfrecord_output);
    let _ = std::fs::remove_dir_all(object_root);
}
