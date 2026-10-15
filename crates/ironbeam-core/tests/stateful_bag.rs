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
