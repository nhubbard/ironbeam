// Unit tests for src/state.rs.
use super::*;
use beam_proto::fn_execution_v1::{state_key, StateGetResponse};

fn test_client() -> (StateClient, mpsc::UnboundedReceiver<StateRequest>) {
    let (outbound, receiver) = mpsc::unbounded_channel();
    (
        StateClient {
            outbound,
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
        },
        receiver,
    )
}

fn runner_key() -> StateKey {
    StateKey {
        r#type: Some(state_key::Type::Runner(state_key::Runner {
            key: b"ref".to_vec(),
        })),
    }
}

#[tokio::test]
async fn gets_all_pages_with_instruction_and_continuation() {
    let (client, mut requests) = test_client();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for (expected_token, data, next_token) in [
            (b"".as_slice(), b"first".as_slice(), b"next".as_slice()),
            (b"next".as_slice(), b"second".as_slice(), b"".as_slice()),
        ] {
            let request = requests.recv().await.unwrap();
            assert_eq!(request.instruction_id, "bundle-1");
            assert_eq!(request.state_key, Some(runner_key()));
            let Some(state_request::Request::Get(get)) = request.request else {
                panic!("expected StateGetRequest");
            };
            assert_eq!(get.continuation_token, expected_token);
            let sender = pending.lock().unwrap().remove(&request.id).unwrap();
            sender
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(state_response::Response::Get(StateGetResponse {
                        continuation_token: next_token.to_vec(),
                        data: data.to_vec(),
                    })),
                })
                .unwrap();
        }
    });
    assert_eq!(
        client.get_all("bundle-1", runner_key()).await.unwrap(),
        b"firstsecond"
    );
    responder.await.unwrap();
}

#[tokio::test]
async fn drain_waits_for_all_pages_and_reports_later_failure() {
    let (client, mut requests) = test_client();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        let first = requests.recv().await.unwrap();
        pending
            .lock()
            .unwrap()
            .remove(&first.id)
            .unwrap()
            .send(StateResponse {
                id: first.id,
                error: String::new(),
                response: Some(state_response::Response::Get(StateGetResponse {
                    continuation_token: b"next".to_vec(),
                    data: b"ignored".to_vec(),
                })),
            })
            .unwrap();
        let second = requests.recv().await.unwrap();
        let Some(state_request::Request::Get(get)) = second.request else {
            panic!("expected continuation request");
        };
        assert_eq!(get.continuation_token, b"next");
        pending
            .lock()
            .unwrap()
            .remove(&second.id)
            .unwrap()
            .send(StateResponse {
                id: second.id,
                error: "signal failed".into(),
                response: None,
            })
            .unwrap();
    });
    assert!(client
        .drain("bundle", runner_key())
        .await
        .unwrap_err()
        .to_string()
        .contains("signal failed"));
    responder.await.unwrap();
}

#[tokio::test]
async fn runner_error_and_repeated_token_fail() {
    let (client, mut requests) = test_client();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        let first = requests.recv().await.unwrap();
        pending
            .lock()
            .unwrap()
            .remove(&first.id)
            .unwrap()
            .send(StateResponse {
                id: first.id,
                error: "denied".into(),
                response: None,
            })
            .unwrap();
    });
    assert!(client
        .get_all("bundle", runner_key())
        .await
        .unwrap_err()
        .to_string()
        .contains("denied"));
    responder.await.unwrap();

    let (client, mut requests) = test_client();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for _ in 0..2 {
            let request = requests.recv().await.unwrap();
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(state_response::Response::Get(StateGetResponse {
                        continuation_token: b"same".to_vec(),
                        data: Vec::new(),
                    })),
                })
                .unwrap();
        }
    });
    assert!(client
        .get_all("bundle", runner_key())
        .await
        .unwrap_err()
        .to_string()
        .contains("repeated a continuation token"));
    responder.await.unwrap();
}
