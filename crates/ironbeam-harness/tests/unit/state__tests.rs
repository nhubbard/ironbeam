// Unit tests for src/state.rs.
use super::*;
use beam_proto::fn_execution_v1::{
    state_key, StateAppendResponse, StateClearResponse, StateGetResponse,
};

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
async fn cancelled_state_request_releases_only_its_pending_response() {
    let (client, mut requests) = test_client();
    let cancelled = {
        let client = client.clone();
        tokio::spawn(async move { client.get_all("cancelled", runner_key()).await })
    };
    let first = requests.recv().await.unwrap();
    let surviving = {
        let client = client.clone();
        tokio::spawn(async move { client.get_all("surviving", runner_key()).await })
    };
    let second = requests.recv().await.unwrap();
    assert_eq!(client.pending.lock().unwrap().len(), 2);
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    {
        let mut pending = client.pending.lock().unwrap();
        assert!(!pending.contains_key(&first.id));
        pending.remove(&second.id).unwrap().send(StateResponse {
            id: second.id,
            error: String::new(),
            response: Some(state_response::Response::Get(StateGetResponse {
                continuation_token: Vec::new(),
                data: b"retained".to_vec(),
            })),
        }).unwrap();
    }
    assert_eq!(surviving.await.unwrap().unwrap(), b"retained");
    assert!(client.pending.lock().unwrap().is_empty());
}

#[test]
fn user_state_scope_builds_each_beam_fn_state_key_shape() {
    let scope = UserStateScope::new("transform", "cell", Vec::new(), vec![7]).unwrap();
    let Some(state_key::Type::BagUserState(bag)) = scope.bag().r#type else {
        panic!("expected bag user state key");
    };
    assert_eq!(bag.transform_id, "transform");
    assert_eq!(bag.user_state_id, "cell");
    assert!(bag.window.is_empty()); // GlobalWindow has a zero-byte encoding.
    assert_eq!(bag.key, vec![7]);
    let Some(state_key::Type::MultimapKeysUserState(keys)) = scope.multimap_keys().r#type else {
        panic!("expected multimap keys user state key");
    };
    assert_eq!(keys.user_state_id, "cell");
    assert_eq!(keys.key, vec![7]);
    let Some(state_key::Type::MultimapEntriesUserState(entries)) = scope.multimap_entries().r#type
    else {
        panic!("expected multimap entries user state key");
    };
    assert_eq!(entries.transform_id, "transform");
    let Some(state_key::Type::MultimapUserState(value)) = scope.multimap_value(vec![9]).r#type
    else {
        panic!("expected multimap value user state key");
    };
    assert_eq!(value.key, vec![7]);
    assert_eq!(value.map_key, vec![9]);
    let Some(state_key::Type::OrderedListUserState(ordered)) =
        scope.ordered_list(-5, 10).unwrap().r#type
    else {
        panic!("expected ordered list user state key");
    };
    let range = ordered.range.unwrap();
    assert_eq!((range.start, range.end), (-5, 10));
    assert!(scope.ordered_list(10, 10).is_err());
    assert!(scope.ordered_list(11, 10).is_err());
    assert!(UserStateScope::new("", "cell", Vec::new(), Vec::new()).is_err());
    assert!(UserStateScope::new("transform", "", Vec::new(), Vec::new()).is_err());
}

#[tokio::test]
async fn appends_and_clears_state_with_matching_runner_acknowledgements() {
    let (client, mut requests) = test_client();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        let append = requests.recv().await.unwrap();
        assert_eq!(append.instruction_id, "bundle-1");
        assert_eq!(append.state_key, Some(runner_key()));
        let Some(state_request::Request::Append(write)) = append.request else {
            panic!("expected StateAppendRequest");
        };
        assert_eq!(write.data, b"value");
        pending
            .lock()
            .unwrap()
            .remove(&append.id)
            .unwrap()
            .send(StateResponse {
                id: append.id,
                error: String::new(),
                response: Some(state_response::Response::Append(StateAppendResponse {})),
            })
            .unwrap();
        let clear = requests.recv().await.unwrap();
        assert_eq!(clear.instruction_id, "bundle-1");
        assert_eq!(clear.state_key, Some(runner_key()));
        assert!(matches!(
            clear.request,
            Some(state_request::Request::Clear(_))
        ));
        pending
            .lock()
            .unwrap()
            .remove(&clear.id)
            .unwrap()
            .send(StateResponse {
                id: clear.id,
                error: String::new(),
                response: Some(state_response::Response::Clear(StateClearResponse {})),
            })
            .unwrap();
    });
    client
        .append("bundle-1", runner_key(), b"value".to_vec())
        .await
        .unwrap();
    client.clear("bundle-1", runner_key()).await.unwrap();
    responder.await.unwrap();
}

#[tokio::test]
async fn state_mutations_reject_wrong_response_and_runner_error() {
    let (client, mut requests) = test_client();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        let append = requests.recv().await.unwrap();
        pending
            .lock()
            .unwrap()
            .remove(&append.id)
            .unwrap()
            .send(StateResponse {
                id: append.id,
                error: String::new(),
                response: Some(state_response::Response::Clear(StateClearResponse {})),
            })
            .unwrap();
        let clear = requests.recv().await.unwrap();
        pending
            .lock()
            .unwrap()
            .remove(&clear.id)
            .unwrap()
            .send(StateResponse {
                id: clear.id,
                error: "state denied".into(),
                response: None,
            })
            .unwrap();
    });
    assert!(client
        .append("bundle-1", runner_key(), vec![1])
        .await
        .unwrap_err()
        .to_string()
        .contains("non-Append"));
    assert!(client
        .clear("bundle-1", runner_key())
        .await
        .unwrap_err()
        .to_string()
        .contains("state denied"));
    responder.await.unwrap();
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
