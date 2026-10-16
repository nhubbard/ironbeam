//! End-to-end driver flow against a mocked Dataflow REST surface.
//!
//! Stages via [`MemoryStager`] — no GCS — and points [`Driver`] at a wiremock
//! server that answers `jobs.create` and `jobs.get`. Exercises the full
//! stage→rewrite→submit→poll path including artifact rewrite, Job JSON
//! construction, and the terminal-state polling loop.

use std::time::Duration;

use beam_proto::pipeline_v1::{ArtifactUrlPayload, Pipeline};
use ironbeam_core::{Error, Pipeline as IronbeamPipeline, Replay, SinkMode, SinkSpec, TypedSink};
use ironbeam_dataflow::{
    prepare_v5, DataflowOptions, Driver, MemoryStager, PollConfig, StagedArtifact,
    StaticTokenSource,
};
use ironbeam_graphx::urns;
use prost::Message;
use wiremock::matchers::{method, path as pathm};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CONTAINER_IMAGE: &str = "gcr.io/cloud-dataflow/v1beta3/beam_go_sdk:test";

struct NoopSink;

impl TypedSink<(String, u64)> for NoopSink {
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

fn build_wordcount() -> IronbeamPipeline {
    let p = IronbeamPipeline::default();
    p.from_vec("lines", vec!["a b".to_string(), "a c".to_string()])
        .unwrap()
        .flat_map_named("words", |line| {
            line.split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap()
        .map_named("ones", |word| (word, 1_u64))
        .unwrap()
        .group_by_key_named("group-words")
        .unwrap()
        .map_named("counts", |(word, values)| (word, values.into_iter().sum()))
        .unwrap()
        .sink_named_with_config(
            "result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            "test:wordcount:no-op:v1".into(),
            || Ok(Box::new(NoopSink)),
        )
        .unwrap();
    p
}

fn opts(job_name: &str) -> DataflowOptions {
    DataflowOptions::new(
        "proj",
        "us-central1",
        job_name,
        "gs://my-bkt/staging",
        "gs://my-bkt/tmp",
    )
}

fn translate(ib: &IronbeamPipeline) -> Pipeline {
    prepare_v5(ib, CONTAINER_IMAGE, "driver-e2e-v5")
        .expect("translate succeeds")
        .pipeline
}

fn pending(id: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "currentState": "JOB_STATE_PENDING"})
}

fn running(id: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "currentState": "JOB_STATE_RUNNING"})
}

fn done(id: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "currentState": "JOB_STATE_DONE"})
}

#[tokio::test]
async fn full_driver_flow_reaches_done() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(pending("job-xyz")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(pathm(
            "/v1b3/projects/proj/locations/us-central1/jobs/job-xyz",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(done("job-xyz")))
        .mount(&server)
        .await;

    let stager = MemoryStager::new("my-bkt");
    let driver = Driver::new(stager.clone(), StaticTokenSource::new("ya29.test"))
        .with_dataflow_base_url(server.uri())
        .with_poll_config(PollConfig {
            interval: Duration::from_millis(5),
            timeout: Some(Duration::from_secs(5)),
            tail_messages: false,
        });

    let outcome = driver
        .submit_pipeline(translate(&build_wordcount()), &opts("wc-job"))
        .await
        .expect("driver returns ok");

    assert_eq!(outcome.job_id, "job-xyz");
    assert_eq!(outcome.final_state, "JOB_STATE_DONE");

    // Worker binary + pipeline.pb both staged.
    let stored = stager.stored();
    let keys: Vec<String> = stored.iter().map(|(k, _)| k.clone()).collect();
    assert!(
        keys.iter().any(|k| k == "wc-job/ironbeam_worker"),
        "worker staged: {keys:?}"
    );
    assert!(
        keys.iter().any(|k| k == "wc-job/pipeline.pb"),
        "pipeline.pb staged: {keys:?}"
    );

    // The staged pipeline.pb must include the artifact rewrite (URL payload,
    // not file payload, pointing at the staged worker binary).
    let pipeline_bytes = stored
        .iter()
        .find(|(k, _)| k == "wc-job/pipeline.pb")
        .map(|(_, b)| b.clone())
        .unwrap();
    let pipeline = Pipeline::decode(pipeline_bytes.as_slice()).unwrap();
    let env = pipeline
        .components
        .as_ref()
        .unwrap()
        .environments
        .values()
        .next()
        .unwrap();
    assert_eq!(env.dependencies.len(), 1);
    assert_eq!(env.dependencies[0].type_urn, urns::ARTIFACT_TYPE_URL);
    assert_eq!(
        env.dependencies[0].role_urn,
        urns::ARTIFACT_ROLE_GO_WORKER_BINARY
    );
    let url_payload =
        ArtifactUrlPayload::decode(env.dependencies[0].type_payload.as_slice()).unwrap();
    assert_eq!(url_payload.url, "gs://my-bkt/wc-job/ironbeam_worker");
    assert!(
        !url_payload.sha256.is_empty(),
        "sha256 should be computed at stage time"
    );
}

#[tokio::test]
async fn poll_loop_tolerates_transient_states_before_terminal() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(pending("jx")))
        .mount(&server)
        .await;

    // First two GETs return RUNNING, then DONE. `up_to_n_times` drops a mock
    // after N uses; ResponseTemplate::set_body_json is fine to repeat.
    Mock::given(method("GET"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs/jx"))
        .respond_with(ResponseTemplate::new(200).set_body_json(running("jx")))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs/jx"))
        .respond_with(ResponseTemplate::new(200).set_body_json(done("jx")))
        .mount(&server)
        .await;

    let stager = MemoryStager::new("bkt");
    let driver = Driver::new(stager, StaticTokenSource::new("t"))
        .with_dataflow_base_url(server.uri())
        .with_poll_config(PollConfig {
            interval: Duration::from_millis(5),
            timeout: Some(Duration::from_secs(5)),
            tail_messages: false,
        });

    let outcome = driver
        .submit_pipeline(translate(&build_wordcount()), &opts("jx-job"))
        .await
        .unwrap();
    assert_eq!(outcome.final_state, "JOB_STATE_DONE");
}

#[tokio::test]
async fn submit_job_propagates_http_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let stager = MemoryStager::new("bkt");
    let driver = Driver::new(stager, StaticTokenSource::new("t"))
        .with_dataflow_base_url(server.uri())
        .with_poll_config(PollConfig {
            interval: Duration::from_millis(5),
            timeout: Some(Duration::from_secs(1)),
            tail_messages: false,
        });

    let err = driver
        .submit_pipeline(translate(&build_wordcount()), &opts("err-job"))
        .await
        .expect_err("403 must surface as an error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("403") || msg.contains("forbidden"),
        "got: {msg}"
    );
}

#[tokio::test]
async fn list_messages_pages_and_tracks_latest_time() {
    use ironbeam_dataflow::list_messages;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs/j-msg/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jobMessages": [
                {"id": "m1", "time": "2026-04-21T00:00:01Z", "messageImportance": "JOB_MESSAGE_BASIC", "messageText": "starting"},
                {"id": "m2", "time": "2026-04-21T00:00:02Z", "messageImportance": "JOB_MESSAGE_WARNING", "messageText": "slow step"},
                {"id": "m3", "time": "2026-04-21T00:00:03Z", "messageImportance": "JOB_MESSAGE_ERROR", "messageText": "retrying"},
            ]
        })))
        .mount(&server)
        .await;

    let token = StaticTokenSource::new("t");
    let (msgs, latest) = list_messages(&server.uri(), &token, &opts("j-msg"), "j-msg", None)
        .await
        .unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(latest.as_deref(), Some("2026-04-21T00:00:03Z"));
}

#[tokio::test]
async fn regional_job_metrics_preserve_names_and_reject_bad_counts() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(pathm(
            "/v1b3/projects/proj/locations/us-central1/jobs/j-metrics/metrics",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metricTime": "2026-10-15T20:00:00Z",
            "metrics": [
                {
                    "name": {"origin": "dataflow", "name": "element_count", "context": {"step": "s1"}},
                    "kind": "Sum",
                    "cumulative": true,
                    "scalar": "3"
                },
                {"name": {"name": "fraction", "context": {}}, "scalar": 0.5},
                {"name": {"name": "negative", "context": {}}, "scalar": -1},
                {"name": {"name": "distribution", "context": {}}, "distribution": {"count": 2}}
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;
    let driver = Driver::new(MemoryStager::new("bkt"), StaticTokenSource::new("t"))
        .with_dataflow_base_url(server.uri());
    let metrics = driver
        .job_metrics(&opts("j-metrics"), "j-metrics")
        .await
        .unwrap();
    assert_eq!(metrics.metric_time.as_deref(), Some("2026-10-15T20:00:00Z"));
    assert_eq!(
        metrics.metrics[0].name.as_ref().unwrap().name,
        "element_count"
    );
    assert_eq!(
        metrics.metrics[0].name.as_ref().unwrap().context["step"],
        "s1"
    );
    assert_eq!(metrics.metrics[0].scalar_u64().unwrap(), Some(3));
    assert!(metrics.metrics[1].scalar_u64().is_err());
    assert!(metrics.metrics[2].scalar_u64().is_err());
    assert_eq!(metrics.metrics[3].scalar_u64().unwrap(), None);
    assert!(metrics.metrics[3].other.contains_key("distribution"));
}

#[tokio::test]
async fn regional_job_metrics_surface_permission_and_shape_errors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(pathm(
            "/v1b3/projects/proj/locations/us-central1/jobs/forbidden/metrics",
        ))
        .respond_with(ResponseTemplate::new(403).set_body_string("metrics denied"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(pathm(
            "/v1b3/projects/proj/locations/us-central1/jobs/malformed/metrics",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "metricTime": "2026-10-15T20:00:00Z"
        })))
        .mount(&server)
        .await;
    let driver = Driver::new(MemoryStager::new("bkt"), StaticTokenSource::new("t"))
        .with_dataflow_base_url(server.uri());
    let forbidden = driver
        .job_metrics(&opts("forbidden"), "forbidden")
        .await
        .unwrap_err();
    assert!(forbidden.to_string().contains("403"));
    assert!(driver
        .job_metrics(&opts("malformed"), "malformed")
        .await
        .is_err());
}

#[tokio::test]
async fn poll_with_tail_messages_hits_messages_endpoint() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(pending("j-tail")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(pathm(
            "/v1b3/projects/proj/locations/us-central1/jobs/j-tail",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(done("j-tail")))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs/j-tail/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jobMessages": [
                {"id": "m1", "time": "2026-04-21T00:00:01Z", "messageImportance": "JOB_MESSAGE_BASIC", "messageText": "hello"},
            ]
        })))
        .expect(1..)
        .mount(&server)
        .await;

    let stager = MemoryStager::new("bkt");
    let driver = Driver::new(stager, StaticTokenSource::new("t"))
        .with_dataflow_base_url(server.uri())
        .with_poll_config(PollConfig {
            interval: Duration::from_millis(5),
            timeout: Some(Duration::from_secs(5)),
            tail_messages: true,
        });

    let outcome = driver
        .submit_pipeline(translate(&build_wordcount()), &opts("j-tail"))
        .await
        .unwrap();
    assert_eq!(outcome.final_state, "JOB_STATE_DONE");
    // `.expect(1..)` on the messages mock fails the server on drop if unhit.
}

#[tokio::test]
async fn submit_only_uses_passed_worker_artifact() {
    // Exercises the poll-free entry point: caller supplies the staged worker
    // directly, and we skip the current_exe() lookup.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(pathm("/v1b3/projects/proj/locations/us-central1/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(pending("j-only")))
        .expect(1)
        .mount(&server)
        .await;

    let stager = MemoryStager::new("bkt");
    let driver = Driver::new(stager.clone(), StaticTokenSource::new("t"))
        .with_dataflow_base_url(server.uri());

    let submitted = driver
        .submit_only(
            translate(&build_wordcount()),
            &opts("only-job"),
            &StagedArtifact {
                url: "gs://bkt/preexisting/ironbeam_worker".into(),
                sha256: "deadbeef".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(submitted.id, "j-only");
    // Only one upload: pipeline.pb. Worker wasn't uploaded here — caller did it.
    let keys: Vec<String> = stager.stored().into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec!["only-job/pipeline.pb"]);
}
