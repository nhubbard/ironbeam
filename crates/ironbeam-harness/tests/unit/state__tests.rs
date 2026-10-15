// Unit tests for src/state.rs.
use super::*;
use beam_proto::fn_execution_v1::{
    state_key, StateAppendResponse, StateClearResponse, StateGetResponse,
};
use ironbeam_core::{StatefulParDoContract, UserStateKind, Version};
use std::collections::BTreeMap;

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

fn user_contract() -> StatefulParDoContract {
    StatefulParDoContract {
        semantic_id: "tests/stateful".into(),
        version: Version { major: 1, minor: 0 },
        key_coder_id: "beam:coder:bytes:v1".into(),
        window_coder_id: "beam:coder:global_window:v1".into(),
        states: BTreeMap::from([
            (
                "value".into(),
                UserStateKind::Value {
                    coder_id: "beam:coder:bytes:v1".into(),
                },
            ),
            (
                "bag".into(),
                UserStateKind::Bag {
                    element_coder_id: "beam:coder:bytes:v1".into(),
                },
            ),
            (
                "set".into(),
                UserStateKind::Set {
                    element_coder_id: "beam:coder:bytes:v1".into(),
                },
            ),
            (
                "map".into(),
                UserStateKind::Map {
                    key_coder_id: "beam:coder:bytes:v1".into(),
                    value_coder_id: "beam:coder:bytes:v1".into(),
                },
            ),
            (
                "ordered".into(),
                UserStateKind::OrderedList {
                    element_coder_id: "beam:coder:bytes:v1".into(),
                },
            ),
        ]),
        timers: BTreeMap::new(),
        on_window_expiration_timer: None,
    }
}

#[tokio::test]
async fn ordered_list_callback_reads_sorted_entries_and_replaces_one_bundle_cell() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "ordered-bundle",
        "ordered-transform",
        &contract,
        vec![3],
        vec![7],
    )
    .unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for step in 0..3 {
            let request = requests.recv().await.unwrap();
            assert_eq!(request.instruction_id, "ordered-bundle");
            let Some(state_key::Type::OrderedListUserState(scope)) =
                request.state_key.as_ref().unwrap().r#type.as_ref()
            else {
                panic!("expected ordered-list state key");
            };
            assert_eq!(scope.transform_id, "ordered-transform");
            assert_eq!(scope.user_state_id, "ordered");
            let response = match step {
                0 => {
                    let range = scope.range.as_ref().unwrap();
                    assert_eq!((range.start, range.end), (i64::MIN, i64::MAX));
                    assert!(matches!(
                        request.request,
                        Some(state_request::Request::Get(_))
                    ));
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: vec![20, 1, 2, 10, 1, 1],
                    })
                }
                1 => {
                    let range = scope.range.as_ref().unwrap();
                    assert_eq!((range.start, range.end), (i64::MIN, i64::MAX));
                    assert!(matches!(
                        request.request,
                        Some(state_request::Request::Clear(_))
                    ));
                    state_response::Response::Clear(StateClearResponse {})
                }
                _ => {
                    assert!(scope.range.is_none());
                    let Some(state_request::Request::Append(append)) = request.request else {
                        panic!("expected ordered-list append");
                    };
                    assert_eq!(append.data, [10, 1, 1, 15, 1, 3]);
                    state_response::Response::Append(StateAppendResponse {})
                }
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    let result = access
        .apply_ordered_state("ordered", |current| {
            assert_eq!(current, br"[[10,[1]],[20,[2]]]");
            Ok((19, Some(br"[[10,[1]],[15,[3]]]".to_vec())))
        })
        .await
        .unwrap();
    assert_eq!(result, 19);
    responder.await.unwrap();
}

#[tokio::test]
async fn ordered_list_append_only_writes_new_entries_including_duplicates() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "append-ordered-bundle",
        "ordered-transform",
        &contract,
        Vec::new(),
        vec![7],
    )
    .unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for step in 0..2 {
            let request = requests.recv().await.unwrap();
            let Some(state_key::Type::OrderedListUserState(scope)) =
                request.state_key.as_ref().unwrap().r#type.as_ref()
            else {
                panic!("expected ordered-list state key");
            };
            let response = if step == 0 {
                assert!(scope.range.is_some());
                assert!(matches!(
                    request.request,
                    Some(state_request::Request::Get(_))
                ));
                state_response::Response::Get(StateGetResponse {
                    continuation_token: Vec::new(),
                    data: vec![10, 1, 1],
                })
            } else {
                assert!(scope.range.is_none());
                let Some(state_request::Request::Append(append)) = request.request else {
                    panic!("append-only update must not clear the cell");
                };
                assert_eq!(append.data, [10, 1, 1, 15, 1, 3]);
                state_response::Response::Append(StateAppendResponse {})
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    access
        .apply_ordered_state("ordered", |current| {
            assert_eq!(current, br"[[10,[1]]]");
            Ok(((), Some(br"[[10,[1]],[10,[1]],[15,[3]]]".to_vec())))
        })
        .await
        .unwrap();
    responder.await.unwrap();
}

#[tokio::test]
async fn ordered_list_first_append_does_not_clear_missing_runner_cell() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "first-ordered-bundle",
        "ordered-transform",
        &contract,
        Vec::new(),
        vec![7],
    )
    .unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for step in 0..2 {
            let request = requests.recv().await.unwrap();
            let Some(state_key::Type::OrderedListUserState(scope)) =
                request.state_key.as_ref().unwrap().r#type.as_ref()
            else {
                panic!("expected ordered-list state key");
            };
            let response = if step == 0 {
                assert!(scope.range.is_some());
                assert!(matches!(
                    request.request,
                    Some(state_request::Request::Get(_))
                ));
                state_response::Response::Get(StateGetResponse {
                    continuation_token: Vec::new(),
                    data: Vec::new(),
                })
            } else {
                assert!(scope.range.is_none());
                let Some(state_request::Request::Append(append)) = request.request else {
                    panic!("first ordered-list mutation should append without clear");
                };
                assert_eq!(append.data, [10, 1, 1]);
                state_response::Response::Append(StateAppendResponse {})
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    access
        .apply_ordered_state("ordered", |current| {
            assert_eq!(current, b"[]");
            Ok(((), Some(br"[[10,[1]]]".to_vec())))
        })
        .await
        .unwrap();
    responder.await.unwrap();
}

#[tokio::test]
async fn scoped_value_and_map_replacement_clear_before_append() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "replace-bundle",
        "replace-transform",
        &contract,
        vec![3],
        vec![7],
    )
    .unwrap();
    assert!(access.write_value("bag", vec![1]).await.is_err());
    assert!(access
        .write_map_value("bag", vec![4], vec![1])
        .await
        .is_err());
    assert!(access.clear_value("map").await.is_err());
    assert!(requests.try_recv().is_err());

    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        let mut prior_key = None;
        for (index, expected_id) in ["value", "value", "map", "map"].iter().enumerate() {
            let request = requests.recv().await.unwrap();
            assert_eq!(request.instruction_id, "replace-bundle");
            let key = request.state_key.unwrap();
            match &key.r#type {
                Some(state_key::Type::BagUserState(scope)) if *expected_id == "value" => {
                    assert_eq!(scope.transform_id, "replace-transform");
                    assert_eq!(scope.user_state_id, "value");
                    assert_eq!(
                        (scope.window.as_slice(), scope.key.as_slice()),
                        (&[3][..], &[7][..])
                    );
                }
                Some(state_key::Type::MultimapUserState(scope)) if *expected_id == "map" => {
                    assert_eq!(scope.user_state_id, "map");
                    assert_eq!(scope.map_key, [4]);
                }
                _ => panic!("replacement used the wrong state key"),
            }
            if index % 2 == 0 {
                assert!(matches!(
                    request.request,
                    Some(state_request::Request::Clear(_))
                ));
                prior_key = Some(key.clone());
            } else {
                assert_eq!(prior_key.take(), Some(key));
                let Some(state_request::Request::Append(append)) = request.request else {
                    panic!("replacement must append after clearing");
                };
                assert_eq!(append.data, if index == 1 { &[9][..] } else { &[8][..] });
            }
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(if index % 2 == 0 {
                        state_response::Response::Clear(StateClearResponse {})
                    } else {
                        state_response::Response::Append(StateAppendResponse {})
                    }),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    access.write_value("value", vec![9]).await.unwrap();
    access
        .write_map_value("map", vec![4], vec![8])
        .await
        .unwrap();
    responder.await.unwrap();
}

#[tokio::test]
async fn failed_value_clear_does_not_append() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access =
        UserStateAccess::new(&client, "bundle", "transform", &contract, vec![], vec![7]).unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        let clear = requests.recv().await.unwrap();
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
                error: "runner rejected clear".into(),
                response: None,
            })
            .unwrap();
        assert!(requests.try_recv().is_err());
    });
    assert!(access.write_value("value", vec![9]).await.is_err());
    responder.await.unwrap();
}

#[tokio::test]
async fn set_state_callback_replaces_multimap_keys_under_one_instruction() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "set-bundle",
        "set-transform",
        &contract,
        vec![3],
        vec![7],
    )
    .unwrap();
    let old = [10_u8, 20]
        .into_iter()
        .flat_map(|value| [1, value])
        .collect::<Vec<_>>();
    let next = [10_u8, 30]
        .into_iter()
        .flat_map(|value| [1, value])
        .collect::<Vec<_>>();
    let old_for_runner = old.clone();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for expected in ["get", "clear", "append-10", "append-30"] {
            let request = requests.recv().await.unwrap();
            assert_eq!(request.instruction_id, "set-bundle");
            let key = request.state_key.as_ref().unwrap();
            let response = match (expected, &key.r#type, request.request) {
                (
                    "get",
                    Some(state_key::Type::MultimapKeysUserState(scope)),
                    Some(state_request::Request::Get(_)),
                ) => {
                    assert_eq!(scope.user_state_id, "set");
                    assert_eq!(scope.window, [3]);
                    assert_eq!(scope.key, [7]);
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: old_for_runner.clone(),
                    })
                }
                (
                    "clear",
                    Some(state_key::Type::MultimapKeysUserState(scope)),
                    Some(state_request::Request::Clear(_)),
                ) => {
                    assert_eq!(scope.user_state_id, "set");
                    state_response::Response::Clear(StateClearResponse {})
                }
                (
                    label,
                    Some(state_key::Type::MultimapUserState(scope)),
                    Some(state_request::Request::Append(append)),
                ) if label.starts_with("append") => {
                    assert_eq!(scope.user_state_id, "set");
                    assert_eq!(
                        scope.map_key,
                        [1, if label == "append-10" { 10 } else { 30 }]
                    );
                    assert!(append.data.is_empty());
                    state_response::Response::Append(StateAppendResponse {})
                }
                _ => panic!("unexpected set-state RPC"),
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    let result = access
        .apply_set_state("set", |current| {
            assert_eq!(current, old);
            Ok((19, Some(next)))
        })
        .await
        .unwrap();
    assert_eq!(result, 19);
    responder.await.unwrap();
}

#[tokio::test]
async fn set_state_callback_keeps_equal_keys_and_rejects_failed_mutations() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "set-bundle",
        "set-transform",
        &contract,
        vec![],
        vec![7],
    )
    .unwrap();
    assert!(access
        .apply_set_state("bag", |_| Ok(((), None)))
        .await
        .is_err());
    assert!(requests.try_recv().is_err());
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for _ in 0..3 {
            let request = requests.recv().await.unwrap();
            assert!(matches!(
                request.state_key.as_ref().unwrap().r#type,
                Some(state_key::Type::MultimapKeysUserState(_))
            ));
            assert!(matches!(
                request.request,
                Some(state_request::Request::Get(_))
            ));
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: vec![1, 10],
                    })),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    assert_eq!(
        access
            .apply_set_state("set", |_| Ok((7, Some(vec![1, 10]))))
            .await
            .unwrap(),
        7
    );
    assert!(access
        .apply_set_state("set", |_| -> anyhow::Result<((), Option<Vec<u8>>)> {
            anyhow::bail!("callback failed")
        })
        .await
        .is_err());
    assert!(access
        .apply_set_state("set", |_| -> anyhow::Result<((), Option<Vec<u8>>)> {
            panic!("callback panicked")
        })
        .await
        .is_err());
    responder.await.unwrap();
}

#[tokio::test]
async fn map_state_callback_reads_entries_and_replaces_keys() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "map-bundle",
        "map-transform",
        &contract,
        vec![3],
        vec![7],
    )
    .unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for step in 0..5 {
            let request = requests.recv().await.unwrap();
            let response = match (step, request.state_key.as_ref().unwrap().r#type.as_ref()) {
                (0, Some(state_key::Type::MultimapKeysUserState(scope))) => {
                    assert_eq!(scope.user_state_id, "map");
                    assert_eq!(scope.window, [3]);
                    assert_eq!(scope.key, [7]);
                    assert!(matches!(
                        request.request,
                        Some(state_request::Request::Get(_))
                    ));
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: vec![1, 10],
                    })
                }
                (1, Some(state_key::Type::MultimapUserState(scope))) => {
                    assert_eq!(scope.map_key, [1, 10]);
                    assert!(matches!(
                        request.request,
                        Some(state_request::Request::Get(_))
                    ));
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: vec![1, 20],
                    })
                }
                (2, Some(state_key::Type::MultimapKeysUserState(_))) => {
                    assert!(matches!(
                        request.request,
                        Some(state_request::Request::Clear(_))
                    ));
                    state_response::Response::Clear(StateClearResponse {})
                }
                (3 | 4, Some(state_key::Type::MultimapUserState(scope))) => {
                    let Some(state_request::Request::Append(append)) = request.request else {
                        panic!("expected map append");
                    };
                    if step == 3 {
                        assert_eq!(scope.map_key, [1, 10]);
                        assert_eq!(append.data, [1, 30]);
                    } else {
                        assert_eq!(scope.map_key, [1, 40]);
                        assert_eq!(append.data, [1, 50]);
                    }
                    state_response::Response::Append(StateAppendResponse {})
                }
                _ => panic!("unexpected map-state RPC"),
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    let result = access
        .apply_map_state("map", |current| {
            assert_eq!(current, br"[[[10],[20]]]");
            Ok((19, Some(br"[[[10],[30]],[[40],[50]]]".to_vec())))
        })
        .await
        .unwrap();
    assert_eq!(result, 19);
    responder.await.unwrap();
}

#[tokio::test]
async fn multimap_callback_reads_duplicate_values_and_replaces_keys() {
    let (client, mut requests) = test_client();
    let mut contract = user_contract();
    contract.states.insert(
        "multimap".into(),
        UserStateKind::Multimap {
            key_coder_id: "beam:coder:bytes:v1".into(),
            value_coder_id: "beam:coder:bytes:v1".into(),
        },
    );
    let access = UserStateAccess::new(
        &client,
        "multimap-bundle",
        "multimap-transform",
        &contract,
        vec![3],
        vec![7],
    )
    .unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for step in 0..5 {
            let request = requests.recv().await.unwrap();
            let response = match (step, request.state_key.as_ref().unwrap().r#type.as_ref()) {
                (0, Some(state_key::Type::MultimapKeysUserState(scope))) => {
                    assert_eq!(scope.user_state_id, "multimap");
                    assert!(matches!(
                        request.request,
                        Some(state_request::Request::Get(_))
                    ));
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: vec![1, 10],
                    })
                }
                (1, Some(state_key::Type::MultimapUserState(scope))) => {
                    assert_eq!(scope.map_key, [1, 10]);
                    assert!(matches!(
                        request.request,
                        Some(state_request::Request::Get(_))
                    ));
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: vec![1, 20, 1, 20],
                    })
                }
                (2, Some(state_key::Type::MultimapKeysUserState(_))) => {
                    assert!(matches!(
                        request.request,
                        Some(state_request::Request::Clear(_))
                    ));
                    state_response::Response::Clear(StateClearResponse {})
                }
                (3 | 4, Some(state_key::Type::MultimapUserState(scope))) => {
                    assert_eq!(scope.map_key, [1, 10]);
                    let Some(state_request::Request::Append(append)) = request.request else {
                        panic!("expected multimap append");
                    };
                    assert_eq!(append.data, [1, if step == 3 { 20 } else { 30 }]);
                    state_response::Response::Append(StateAppendResponse {})
                }
                _ => panic!("unexpected multimap RPC"),
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    let result = access
        .apply_multimap_state("multimap", |current| {
            assert_eq!(current, br"[[[10],[[20],[20]]]]");
            Ok((19, Some(br"[[[10],[[20],[30]]]]".to_vec())))
        })
        .await
        .unwrap();
    assert_eq!(result, 19);
    responder.await.unwrap();
}

#[tokio::test]
async fn bag_state_callback_reads_and_replaces_declared_cell() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "bag-bundle",
        "bag-transform",
        &contract,
        vec![3],
        vec![7],
    )
    .unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for expected in ["get", "clear", "append"] {
            let request = requests.recv().await.unwrap();
            assert_eq!(request.instruction_id, "bag-bundle");
            let Some(state_key::Type::BagUserState(scope)) =
                &request.state_key.as_ref().unwrap().r#type
            else {
                panic!("bag callback used a non-bag key");
            };
            assert_eq!(scope.user_state_id, "bag");
            assert_eq!(scope.window, [3]);
            assert_eq!(scope.key, [7]);
            let response = match (expected, request.request) {
                ("get", Some(state_request::Request::Get(_))) => {
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: b"prior".to_vec(),
                    })
                }
                ("clear", Some(state_request::Request::Clear(_))) => {
                    state_response::Response::Clear(StateClearResponse {})
                }
                ("append", Some(state_request::Request::Append(append))) => {
                    assert_eq!(append.data, b"next");
                    state_response::Response::Append(StateAppendResponse {})
                }
                _ => panic!("bag callback sent an unexpected request"),
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    let output = access
        .apply_bag_state("bag", |current| {
            assert_eq!(current, b"prior");
            Ok((17, Some(b"next".to_vec())))
        })
        .await
        .unwrap();
    assert_eq!(output, 17);
    responder.await.unwrap();
}

#[tokio::test]
async fn bag_state_callback_keeps_unchanged_bytes_and_rejects_failed_mutations() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "bag-bundle",
        "bag-transform",
        &contract,
        vec![],
        vec![7],
    )
    .unwrap();
    assert!(access
        .apply_bag_state("value", |_| Ok(((), None)))
        .await
        .is_err());
    assert!(requests.try_recv().is_err());
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for _ in 0..3 {
            let request = requests.recv().await.unwrap();
            assert!(matches!(
                request.request,
                Some(state_request::Request::Get(_))
            ));
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: b"unchanged".to_vec(),
                    })),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    let kept = access
        .apply_bag_state("bag", |current| Ok((current.len(), Some(current.to_vec()))))
        .await
        .unwrap();
    assert_eq!(kept, b"unchanged".len());
    assert!(access
        .apply_bag_state("bag", |_| -> anyhow::Result<((), Option<Vec<u8>>)> {
            anyhow::bail!("callback failed")
        })
        .await
        .is_err());
    assert!(access
        .apply_bag_state("bag", |_| -> anyhow::Result<((), Option<Vec<u8>>)> {
            panic!("callback panicked")
        })
        .await
        .is_err());
    responder.await.unwrap();
}

#[tokio::test]
async fn value_state_callback_reads_then_replaces_under_one_bundle_scope() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "callback-bundle",
        "callback-transform",
        &contract,
        vec![3],
        vec![7],
    )
    .unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        let mut prior_key = None;
        for (index, expected) in ["get", "clear", "append"].iter().enumerate() {
            let request = requests.recv().await.unwrap();
            assert_eq!(request.instruction_id, "callback-bundle");
            let key = request.state_key.unwrap();
            let Some(state_key::Type::BagUserState(scope)) = &key.r#type else {
                panic!("value callback used a non-bag key");
            };
            assert_eq!(scope.transform_id, "callback-transform");
            assert_eq!(scope.user_state_id, "value");
            assert_eq!(
                (scope.window.as_slice(), scope.key.as_slice()),
                (&[3][..], &[7][..])
            );
            if let Some(previous) = prior_key.replace(key) {
                assert_eq!(previous, prior_key.as_ref().unwrap().clone());
            }
            let response = match (expected, request.request) {
                (&"get", Some(state_request::Request::Get(_))) => {
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: b"prior".to_vec(),
                    })
                }
                (&"clear", Some(state_request::Request::Clear(_))) => {
                    state_response::Response::Clear(StateClearResponse {})
                }
                (&"append", Some(state_request::Request::Append(append))) => {
                    assert_eq!(append.data, b"next");
                    state_response::Response::Append(StateAppendResponse {})
                }
                _ => panic!("value callback sent the wrong state request at {index}"),
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    let output = access
        .apply_value_state("value", |current| {
            assert_eq!(current, b"prior");
            Ok((
                b"result".to_vec(),
                ValueStateMutation::Set(b"next".to_vec()),
            ))
        })
        .await
        .unwrap();
    assert_eq!(output, b"result");
    responder.await.unwrap();
}

#[tokio::test]
async fn value_state_callback_error_and_panic_submit_no_mutation() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access =
        UserStateAccess::new(&client, "bundle", "transform", &contract, vec![], vec![7]).unwrap();
    assert!(access
        .apply_value_state("bag", |_| Ok(((), ValueStateMutation::Keep)))
        .await
        .is_err());
    assert!(requests.try_recv().is_err());
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for _ in 0..2 {
            let request = requests.recv().await.unwrap();
            assert!(matches!(
                request.request,
                Some(state_request::Request::Get(_))
            ));
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: b"unchanged".to_vec(),
                    })),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    assert!(access
        .apply_value_state("value", |_| -> anyhow::Result<((), ValueStateMutation)> {
            anyhow::bail!("callback failed")
        })
        .await
        .is_err());
    assert!(access
        .apply_value_state("value", |_| -> anyhow::Result<((), ValueStateMutation)> {
            panic!("callback failed")
        })
        .await
        .is_err());
    responder.await.unwrap();
}

#[tokio::test]
async fn value_state_callback_keep_and_clear_use_distinct_runner_sequences() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access =
        UserStateAccess::new(&client, "bundle", "transform", &contract, vec![], vec![7]).unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for expected in ["get", "get", "clear"] {
            let request = requests.recv().await.unwrap();
            let response = match (expected, request.request) {
                ("get", Some(state_request::Request::Get(_))) => {
                    state_response::Response::Get(StateGetResponse {
                        continuation_token: Vec::new(),
                        data: b"value".to_vec(),
                    })
                }
                ("clear", Some(state_request::Request::Clear(_))) => {
                    state_response::Response::Clear(StateClearResponse {})
                }
                _ => panic!("unexpected value-state mutation request"),
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
        assert!(requests.try_recv().is_err());
    });
    let kept = access
        .apply_value_state("value", |current| {
            assert_eq!(current, b"value");
            Ok((1, ValueStateMutation::Keep))
        })
        .await
        .unwrap();
    let cleared = access
        .apply_value_state("value", |current| {
            assert_eq!(current, b"value");
            Ok((2, ValueStateMutation::Clear))
        })
        .await
        .unwrap();
    assert_eq!((kept, cleared), (1, 2));
    responder.await.unwrap();
}

#[tokio::test]
async fn scoped_user_state_routes_only_declared_protocol_families() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "bundle-1",
        "transform-1",
        &contract,
        vec![],
        vec![7],
    )
    .unwrap();
    assert!(access.read_bag("missing").await.is_err());
    assert!(access.read_bag("map").await.is_err());
    assert!(access.read_map_keys("bag").await.is_err());
    assert!(access.read_ordered("bag", 0, 10).await.is_err());
    assert!(access.read_ordered("ordered", 10, 10).await.is_err());
    assert!(requests.try_recv().is_err());

    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        let bag = requests.recv().await.unwrap();
        assert_eq!(bag.instruction_id, "bundle-1");
        let Some(state_key::Type::BagUserState(key)) = bag.state_key.unwrap().r#type else {
            panic!("expected bag state key");
        };
        assert_eq!(
            (key.transform_id.as_str(), key.user_state_id.as_str()),
            ("transform-1", "bag")
        );
        assert_eq!(key.key, vec![7]);
        pending
            .lock()
            .unwrap()
            .remove(&bag.id)
            .unwrap()
            .send(StateResponse {
                id: bag.id,
                error: String::new(),
                response: Some(state_response::Response::Get(StateGetResponse {
                    continuation_token: Vec::new(),
                    data: b"stored".to_vec(),
                })),
            })
            .unwrap();

        let map = requests.recv().await.unwrap();
        let Some(state_key::Type::MultimapUserState(key)) = map.state_key.unwrap().r#type else {
            panic!("expected multimap value state key");
        };
        assert_eq!(key.user_state_id, "map");
        assert_eq!(key.map_key, vec![9]);
        let Some(state_request::Request::Append(write)) = map.request else {
            panic!("expected map append");
        };
        assert_eq!(write.data, vec![8]);
        pending
            .lock()
            .unwrap()
            .remove(&map.id)
            .unwrap()
            .send(StateResponse {
                id: map.id,
                error: String::new(),
                response: Some(state_response::Response::Append(StateAppendResponse {})),
            })
            .unwrap();

        let ordered = requests.recv().await.unwrap();
        let Some(state_key::Type::OrderedListUserState(key)) = ordered.state_key.unwrap().r#type
        else {
            panic!("expected ordered-list state key");
        };
        let range = key.range.unwrap();
        assert_eq!((range.start, range.end), (0, 10));
        pending
            .lock()
            .unwrap()
            .remove(&ordered.id)
            .unwrap()
            .send(StateResponse {
                id: ordered.id,
                error: String::new(),
                response: Some(state_response::Response::Clear(StateClearResponse {})),
            })
            .unwrap();
    });

    assert_eq!(access.read_bag("bag").await.unwrap(), b"stored");
    access
        .append_map_value("map", vec![9], vec![8])
        .await
        .unwrap();
    access.clear_ordered("ordered", 0, 10).await.unwrap();
    responder.await.unwrap();
}

#[tokio::test]
async fn scoped_user_state_routes_remaining_cell_operations() {
    let (client, mut requests) = test_client();
    let contract = user_contract();
    let access = UserStateAccess::new(
        &client,
        "bundle-2",
        "transform-2",
        &contract,
        vec![3],
        vec![7],
    )
    .unwrap();
    let pending = Arc::clone(&client.pending);
    let responder = tokio::spawn(async move {
        for expected in [
            "bag-append",
            "bag-clear",
            "map-keys",
            "map-entries",
            "map-value",
            "map-clear",
            "ordered-read",
            "ordered-append",
        ] {
            let request = requests.recv().await.unwrap();
            assert_eq!(request.instruction_id, "bundle-2");
            let state_key = request.state_key.as_ref().unwrap();
            let actual = match (&state_key.r#type, &request.request) {
                (
                    Some(state_key::Type::BagUserState(key)),
                    Some(state_request::Request::Append(_)),
                ) if key.user_state_id == "bag" => "bag-append",
                (
                    Some(state_key::Type::BagUserState(key)),
                    Some(state_request::Request::Clear(_)),
                ) if key.user_state_id == "bag" => "bag-clear",
                (
                    Some(state_key::Type::MultimapKeysUserState(key)),
                    Some(state_request::Request::Get(_)),
                ) if key.user_state_id == "map" => "map-keys",
                (
                    Some(state_key::Type::MultimapEntriesUserState(key)),
                    Some(state_request::Request::Get(_)),
                ) if key.user_state_id == "map" => "map-entries",
                (
                    Some(state_key::Type::MultimapUserState(key)),
                    Some(state_request::Request::Get(_)),
                ) if key.user_state_id == "map" && key.map_key == [9] => "map-value",
                (
                    Some(state_key::Type::MultimapUserState(key)),
                    Some(state_request::Request::Clear(_)),
                ) if key.user_state_id == "map" && key.map_key == [9] => "map-clear",
                (
                    Some(state_key::Type::OrderedListUserState(key)),
                    Some(state_request::Request::Get(_)),
                ) if key.user_state_id == "ordered" && key.range.is_some() => "ordered-read",
                (
                    Some(state_key::Type::OrderedListUserState(key)),
                    Some(state_request::Request::Append(_)),
                ) if key.user_state_id == "ordered" && key.range.is_none() => "ordered-append",
                _ => panic!("wrong scoped state request"),
            };
            assert_eq!(actual, expected);
            let response = match request.request.unwrap() {
                state_request::Request::Get(_) => state_response::Response::Get(StateGetResponse {
                    continuation_token: Vec::new(),
                    data: b"ok".to_vec(),
                }),
                state_request::Request::Append(_) => {
                    state_response::Response::Append(StateAppendResponse {})
                }
                state_request::Request::Clear(_) => {
                    state_response::Response::Clear(StateClearResponse {})
                }
            };
            pending
                .lock()
                .unwrap()
                .remove(&request.id)
                .unwrap()
                .send(StateResponse {
                    id: request.id,
                    error: String::new(),
                    response: Some(response),
                })
                .unwrap();
        }
    });
    access.append_bag("bag", vec![1]).await.unwrap();
    access.clear_bag("bag").await.unwrap();
    assert_eq!(access.read_map_keys("map").await.unwrap(), b"ok");
    assert_eq!(access.read_map_entries("map").await.unwrap(), b"ok");
    assert_eq!(access.read_map_value("map", vec![9]).await.unwrap(), b"ok");
    access.clear_map_value("map", vec![9]).await.unwrap();
    assert_eq!(access.read_ordered("ordered", 0, 10).await.unwrap(), b"ok");
    access.append_ordered("ordered", vec![2]).await.unwrap();
    responder.await.unwrap();
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
        pending
            .remove(&second.id)
            .unwrap()
            .send(StateResponse {
                id: second.id,
                error: String::new(),
                response: Some(state_response::Response::Get(StateGetResponse {
                    continuation_token: Vec::new(),
                    data: b"retained".to_vec(),
                })),
            })
            .unwrap();
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
    let Some(state_key::Type::OrderedListUserState(append_key)) = scope.ordered_list_all().r#type
    else {
        panic!("expected ordered-list append key");
    };
    assert!(append_key.range.is_none());
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
