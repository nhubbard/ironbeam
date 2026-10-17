use ironbeam_core::{
    BagStateMutation, Element, Envelope, PaneInfo, Pipeline, UserStateKind, Version, Window,
};

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_bag_map_named::<i32, i32>(
            "bag",
            "tests/bag-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let previous = i32::try_from(prior.len()).unwrap();
                let mutation = match value {
                    -1 => BagStateMutation::Keep,
                    -2 => BagStateMutation::Clear,
                    -3 => BagStateMutation::Replace(vec![7, 9]),
                    value => BagStateMutation::Append(vec![value]),
                };
                Ok((mutation, previous))
            },
        )
        .unwrap();
    pipeline
}

fn apply(registry: &ironbeam_core::Registry, value: i32) -> i32 {
    let output = registry
        .apply_many(
            "bag",
            Envelope {
                value: Box::new((1_i32, value)),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None,
                pane: PaneInfo::NO_FIRING,
            },
        )
        .unwrap();
    let [row] = output.try_into().ok().unwrap();
    row.value.downcast::<(i32, i32)>().unwrap().1
}

#[test]
fn bag_mutations_and_coded_restart_keep_keyed_contents() {
    let pipeline = pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node = graph.node("bag").unwrap();
    let ironbeam_core::Operation::StatefulParDo { contract, .. } = &node.operation else {
        panic!("bag node must be stateful");
    };
    assert!(matches!(
        contract.states.get("bag"),
        Some(UserStateKind::Bag { .. })
    ));
    assert_eq!(apply(&registry, 10), 0);
    assert_eq!(apply(&registry, 20), 1);
    assert_eq!(apply(&registry, -1), 2);
    assert_eq!(apply(&registry, -3), 2);
    let snapshot = registry.checkpoint_stateful_value("bag").unwrap();
    let (_, recovered) = pipeline.snapshot().unwrap();
    recovered.restore_stateful_value("bag", &snapshot).unwrap();
    assert_eq!(apply(&recovered, 30), 2);
    assert_eq!(apply(&recovered, -2), 3);
    assert_eq!(apply(&recovered, 40), 0);
}

#[test]
fn malformed_bag_contents_fail_restore_without_changing_state() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(apply(&registry, 10), 0);
    let malformed = serde_json::json!({
        "version": 1,
        "entries": [[null, 1_i32.encode().unwrap(), [1, 2, 3]]],
    });
    assert!(
        registry
            .restore_stateful_value("bag", &serde_json::to_vec(&malformed).unwrap())
            .is_err()
    );
    assert_eq!(apply(&registry, 20), 1);
}

#[test]
fn runner_owned_bag_callback_rejects_wrong_payload_and_invalid_prior() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    let input = |value: Box<dyn std::any::Any + Send>| Envelope {
        value,
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
    };
    assert!(
        registry
            .apply_stateful_value("bag", input(Box::new(1_i32)), None)
            .is_err()
    );
    assert!(
        registry
            .apply_stateful_value("bag", input(Box::new((1_i32, 1_i32))), Some(&[1]))
            .is_err()
    );
    let malformed_item = serde_json::to_vec(&vec![vec![1_u8]]).unwrap();
    assert!(
        registry
            .apply_stateful_value(
                "bag",
                input(Box::new((1_i32, 1_i32))),
                Some(&malformed_item)
            )
            .is_err()
    );
    let (next, output) = registry
        .apply_stateful_value("bag", input(Box::new((1_i32, 4_i32))), None)
        .unwrap();
    assert_eq!(*output.value.downcast::<(i32, i32)>().unwrap(), (1, 0));
    let encoded: Vec<Vec<u8>> = serde_json::from_slice(&next.unwrap()).unwrap();
    assert_eq!(i32::decode(&encoded[0]).unwrap(), 4);
}

#[test]
fn bag_registration_rejects_invalid_identity_and_window_policy() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    assert!(
        pairs
            .stateful_bag_map_named::<i32, i32>(
                "empty-id",
                "",
                Version { major: 1, minor: 0 },
                |_, _, _: Vec<i32>| Ok((BagStateMutation::Keep, 0)),
            )
            .is_err()
    );
    assert!(
        pairs
            .stateful_bag_map_named::<i32, i32>(
                "zero-version",
                "tests/bag",
                Version { major: 0, minor: 0 },
                |_, _, _: Vec<i32>| Ok((BagStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let early = pairs
        .fixed_windows("fixed", 10)
        .unwrap()
        .early_count_trigger_named("early", 1)
        .unwrap();
    assert!(
        early
            .stateful_bag_map_named::<i32, i32>(
                "early-bag",
                "tests/early-bag",
                Version { major: 1, minor: 0 },
                |_, _, _: Vec<i32>| Ok((BagStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let late = pairs
        .fixed_windows("late-fixed", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 1_000_000)
        .unwrap();
    assert!(
        late.stateful_bag_map_named::<i32, i32>(
            "late-bag",
            "tests/late-bag",
            Version { major: 1, minor: 0 },
            |_, _, _: Vec<i32>| Ok((BagStateMutation::Keep, 0)),
        )
        .is_err()
    );
}
