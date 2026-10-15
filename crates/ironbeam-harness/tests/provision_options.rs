use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use beam_proto::fn_execution_v1::{
    provision_service_server::{ProvisionService, ProvisionServiceServer},
    GetProvisionInfoRequest, GetProvisionInfoResponse, ProvisionInfo,
};
use ironbeam_harness::provision::{
    get_provision_info, get_provision_info_for_worker, pipeline_option_str, pipeline_option_u64,
};
use prost_types::{value::Kind, Struct, Value};
use tonic::{transport::Server, Request, Response, Status};

fn provision_value(kind: Kind) -> ProvisionInfo {
    ProvisionInfo {
        pipeline_options: Some(Struct {
            fields: BTreeMap::from([("option".into(), Value { kind: Some(kind) })]),
        }),
        ..Default::default()
    }
}

#[test]
fn numeric_pipeline_option_requires_a_finite_nonnegative_integer() {
    for (number, expected) in [
        (0.0, Some(0)),
        (42.0, Some(42)),
        (9_007_199_254_740_991.0, Some(9_007_199_254_740_991)),
        (9_007_199_254_740_992.0, None),
        (-1.0, None),
        (1.5, None),
        (f64::NAN, None),
        (f64::INFINITY, None),
        (18_446_744_073_709_551_616.0, None),
    ] {
        let info = provision_value(Kind::NumberValue(number));
        assert_eq!(pipeline_option_u64(&info, "option"), expected);
        assert_eq!(pipeline_option_str(&info, "option"), None);
    }
}

#[test]
fn string_pipeline_option_parses_only_valid_unsigned_numbers() {
    let info = provision_value(Kind::StringValue(u64::MAX.to_string()));
    assert_eq!(pipeline_option_u64(&info, "option"), Some(u64::MAX));
    assert_eq!(
        pipeline_option_str(&info, "option"),
        Some("18446744073709551615")
    );
    assert_eq!(pipeline_option_u64(&info, "missing"), None);

    for text in ["-1", "1.0", "18446744073709551616", "garbage"] {
        let info = provision_value(Kind::StringValue(text.into()));
        assert_eq!(pipeline_option_u64(&info, "option"), None);
        assert_eq!(pipeline_option_str(&info, "option"), Some(text));
    }
    let info = ProvisionInfo::default();
    assert_eq!(pipeline_option_u64(&info, "option"), None);
    assert_eq!(pipeline_option_str(&info, "option"), None);
}

struct ProvisionFixture {
    worker_ids: Arc<Mutex<Vec<Option<String>>>>,
}

#[tonic::async_trait]
impl ProvisionService for ProvisionFixture {
    async fn get_provision_info(
        &self,
        request: Request<GetProvisionInfoRequest>,
    ) -> Result<Response<GetProvisionInfoResponse>, Status> {
        let id = request
            .metadata()
            .get("worker_id")
            .map(|value| value.to_str().unwrap().to_owned());
        self.worker_ids.lock().unwrap().push(id.clone());
        Ok(Response::new(GetProvisionInfoResponse {
            info: (id.as_deref() != Some("missing")).then(|| ProvisionInfo {
                retrieval_token: "token".into(),
                ..Default::default()
            }),
        }))
    }
}

#[tokio::test]
async fn provision_rpc_passes_worker_identity_and_rejects_missing_info() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let worker_ids = Arc::new(Mutex::new(Vec::new()));
    let fixture = ProvisionFixture {
        worker_ids: Arc::clone(&worker_ids),
    };
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(ProvisionServiceServer::new(fixture))
            .serve(address)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let endpoint = address.to_string();
    let info = get_provision_info_for_worker(&endpoint, "worker-7")
        .await
        .unwrap();
    assert_eq!(info.retrieval_token, "token");
    assert_eq!(
        get_provision_info(&endpoint).await.unwrap().retrieval_token,
        "token"
    );
    assert!(get_provision_info_for_worker(&endpoint, "missing")
        .await
        .is_err());
    assert_eq!(
        *worker_ids.lock().unwrap(),
        [Some("worker-7".into()), None, Some("missing".into())]
    );
    server.abort();
}
