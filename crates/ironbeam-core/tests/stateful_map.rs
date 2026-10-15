use std::collections::HashMap;

use ironbeam_core::{
    Element, Envelope, Error, MapStateMutation, PaneInfo, Pipeline, UserStateKind, Version, Window,
};

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_map_map_named::<i32, i32, (i32, i32)>(
            "map",
            "tests/map-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observed = (
                    prior.get(&10).copied().unwrap_or(-1),
                    i32::try_from(prior.len()).unwrap(),
                );
                let mutation = match value {
                    -1 => MapStateMutation::Keep,
                    -2 => MapStateMutation::Remove(10),
                    -3 => MapStateMutation::Replace(HashMap::from([(7, 70), (9, 90)])),
                    -4 => MapStateMutation::Clear,
                    -5 => return Err(Error::incompatible("map callback failed")),
                    value => MapStateMutation::Put(value, value * 10),
                };
                Ok((mutation, observed))
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

fn apply(registry: &ironbeam_core::Registry, value: i32) -> (i32, i32) {
    let output = registry.apply_many("map", input(value)).unwrap();
    let [row] = output.try_into().ok().unwrap();
    row.value.downcast::<(i32, (i32, i32))>().unwrap().1
}

#[test]
fn map_state_put_remove_replace_clear_and_checkpoint() {
    let pipeline = pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node = graph.node("map").unwrap();
    let ironbeam_core::Operation::StatefulParDo { contract, .. } = &node.operation else {
        panic!("map node must be stateful");
    };
    assert!(matches!(
        contract.states.get("map"),
        Some(UserStateKind::Map { .. })
    ));
    assert_eq!(apply(&registry, 10), (-1, 0));
    assert_eq!(apply(&registry, 20), (100, 1));
    assert_eq!(apply(&registry, -1), (100, 2));
    let snapshot = registry.checkpoint_stateful_value("map").unwrap();
    let (_, recovered) = pipeline.snapshot().unwrap();
    recovered.restore_stateful_value("map", &snapshot).unwrap();
    assert_eq!(apply(&recovered, -2), (100, 2));
    assert_eq!(apply(&recovered, 10), (-1, 1));
    assert_eq!(apply(&recovered, -3), (100, 2));
    assert_eq!(apply(&recovered, 7), (-1, 2));
    assert_eq!(apply(&recovered, -4), (-1, 2));
    assert_eq!(apply(&recovered, 30), (-1, 0));
}

#[test]
fn map_state_rejects_bad_contents_and_preserves_cell_on_error() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(apply(&registry, 10), (-1, 0));
    assert!(registry.apply_many("map", input(-5)).is_err());
    assert_eq!(apply(&registry, -1), (100, 1));
    let duplicate = serde_json::to_vec(&vec![
        (10_i32.encode().unwrap(), 1_i32.encode().unwrap()),
        (10_i32.encode().unwrap(), 2_i32.encode().unwrap()),
    ])
    .unwrap();
    assert!(
        registry
            .apply_stateful_value("map", input(-1), Some(&duplicate))
            .is_err()
    );
    assert!(
        registry
            .apply_stateful_value("map", input(-1), Some(&[1]))
            .is_err()
    );
}
