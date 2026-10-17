//! Opt-in local Prism job-service contract check for the v5 Beam protobuf.
//! Set IRONBEAM_PRISM_ENDPOINT to a running Prism gRPC endpoint, such as
//! `localhost:8073`, to exercise this test.

use beam_proto::job_management_v1::{
    job_service_client::JobServiceClient, GetJobStateRequest, PrepareJobRequest,
};
use ironbeam_core::{Element, Error, Pipeline};
use ironbeam_dataflow::prepare_v5;

#[derive(Clone)]
struct UserRow(i32);
impl Element for UserRow {
    const TYPE_ID: &'static str = "tests/prism-user-row";
    const CODER_ID: &'static str = "tests/prism-user-row-i32-le";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(self.0.to_le_bytes().to_vec())
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let raw: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Error::incompatible("invalid Prism user row"))?;
        Ok(Self(i32::from_le_bytes(raw)))
    }
}

#[tokio::test]
async fn prism_accepts_v5_pipeline_protobuf_at_prepare() {
    let Ok(endpoint) = std::env::var("IRONBEAM_PRISM_ENDPOINT") else {
        return;
    };
    let pipeline = Pipeline::default();
    let source = pipeline
        .from_vec("source", vec![UserRow(1), UserRow(2), UserRow(3)])
        .unwrap();
    source
        .map_named("double", |value| UserRow(value.0 * 2))
        .unwrap()
        .filter_named("positive", |value| value.0 > 0)
        .unwrap()
        .output("result")
        .unwrap();
    let translated = prepare_v5(
        &pipeline,
        "example.test/ironbeam:v5",
        "prism-prepare-fixture",
    )
    .expect("v5 graph contract");
    let url = if endpoint.contains("://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    };
    let mut client = JobServiceClient::connect(url)
        .await
        .expect("Prism connection");
    let prepared = client
        .prepare(PrepareJobRequest {
            pipeline: Some(translated.pipeline),
            pipeline_options: Some(prost_types::Struct::default()),
            job_name: format!("ironbeam-v5-prepare-{}", std::process::id()),
        })
        .await
        .expect("Prism accepts the v5 protobuf")
        .into_inner();
    assert!(!prepared.preparation_id.is_empty());
    let state = client
        .get_state(GetJobStateRequest {
            job_id: prepared.preparation_id,
        })
        .await
        .expect("Prism tracks the prepared job")
        .into_inner();
    assert_eq!(state.state, 1, "prepared job remains stopped");
}
