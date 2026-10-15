use ironbeam_core::{
    Element, Envelope, Error, Operation, OrderedListStateMutation, PaneInfo, Pipeline,
    UserStateKind, Version, Window,
};

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_ordered_list_map_named::<i32, Vec<i32>>(
            "ordered",
            "tests/ordered-list-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let seen = prior.iter().map(|(_, value)| *value).collect();
                let mutation = match value {
                    -1 => OrderedListStateMutation::ClearRange {
                        start_ms: 10,
                        end_ms: 20,
                    },
                    -2 => OrderedListStateMutation::Replace(vec![(20, 7), (10, 8), (10, 8)]),
                    -3 => OrderedListStateMutation::Clear,
                    -4 => OrderedListStateMutation::Keep,
                    -5 => return Err(Error::incompatible("ordered callback failed")),
                    -6 => OrderedListStateMutation::Append(i64::MAX, 1),
                    -7 => OrderedListStateMutation::ClearRange {
                        start_ms: 20,
                        end_ms: 20,
                    },
                    value => OrderedListStateMutation::Append(i64::from(value), value),
                };
                Ok((mutation, seen))
            },
        )
        .unwrap();
    pipeline
}

fn input(value: i32) -> Envelope {
    Envelope {
        value: Box::new((1_i32, value)),
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
    }
}

fn apply(registry: &ironbeam_core::Registry, value: i32) -> Vec<i32> {
    let output = registry.apply_many("ordered", input(value)).unwrap();
    let [row] = output.try_into().ok().unwrap();
    row.value.downcast::<(i32, Vec<i32>)>().unwrap().1
}

#[test]
fn ordered_list_sorts_repeats_and_restores_coded_state() {
    let pipeline = pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node = graph.node("ordered").unwrap();
    let Operation::StatefulParDo { contract, .. } = &node.operation else {
        panic!("ordered-list node must be stateful");
    };
    assert!(matches!(
        contract.states.get("ordered_list"),
        Some(UserStateKind::OrderedList { .. })
    ));
    assert_eq!(apply(&registry, 20), Vec::<i32>::new());
    assert_eq!(apply(&registry, 10), [20]);
    assert_eq!(apply(&registry, 10), [10, 20]);
    let snapshot = registry.checkpoint_stateful_value("ordered").unwrap();
    let (_, recovered) = pipeline.snapshot().unwrap();
    recovered
        .restore_stateful_value("ordered", &snapshot)
        .unwrap();
    assert_eq!(apply(&recovered, -4), [10, 10, 20]);
    assert_eq!(apply(&recovered, -1), [10, 10, 20]);
    assert_eq!(apply(&recovered, -2), [20]);
    assert_eq!(apply(&recovered, -3), [8, 8, 7]);
    assert_eq!(apply(&recovered, -4), Vec::<i32>::new());
}

#[test]
fn ordered_list_rejects_invalid_updates_without_mutating_cell() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(apply(&registry, 10), Vec::<i32>::new());
    let before = registry.checkpoint_stateful_value("ordered").unwrap();
    for bad in [-5, -6, -7] {
        assert!(registry.apply_many("ordered", input(bad)).is_err());
        assert_eq!(
            registry.checkpoint_stateful_value("ordered").unwrap(),
            before
        );
    }
    let invalid = serde_json::to_vec(&vec![(10_i64, vec![1_u8])]).unwrap();
    assert!(
        registry
            .apply_stateful_value("ordered", input(-4), Some(&invalid))
            .is_err()
    );
    assert!(
        registry
            .apply_stateful_value("ordered", input(-4), Some(&[1]))
            .is_err()
    );
    let max = serde_json::to_vec(&vec![(i64::MAX, 1_i32.encode().unwrap())]).unwrap();
    assert!(
        registry
            .apply_stateful_value("ordered", input(-4), Some(&max))
            .is_err()
    );
    assert_eq!(apply(&registry, -4), [10]);
}
