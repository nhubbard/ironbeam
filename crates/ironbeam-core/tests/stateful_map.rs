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
    let malformed_key = serde_json::to_vec(&vec![(vec![1_u8], 20_i32.encode().unwrap())]).unwrap();
    assert!(
        registry
            .apply_stateful_value("map", input(-1), Some(&malformed_key))
            .is_err()
    );
    let malformed_value =
        serde_json::to_vec(&vec![(10_i32.encode().unwrap(), vec![1_u8])]).unwrap();
    assert!(
        registry
            .apply_stateful_value("map", input(-1), Some(&malformed_value))
            .is_err()
    );
    let wrong_input = Envelope {
        value: Box::new(1_i32),
        ..input(-1)
    };
    assert!(
        registry
            .apply_stateful_value("map", wrong_input, None)
            .is_err()
    );
}

#[derive(Eq, Hash, PartialEq)]
struct CollidingKey(i32);

impl Element for CollidingKey {
    const TYPE_ID: &'static str = "tests/colliding-map-key";
    const CODER_ID: &'static str = "tests/colliding-map-key-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(vec![0])
    }
    fn decode(_: &[u8]) -> Result<Self, Error> {
        Ok(Self(0))
    }
}

#[derive(Eq, PartialEq)]
struct FailingValue;

impl Element for FailingValue {
    const TYPE_ID: &'static str = "tests/failing-map-value";
    const CODER_ID: &'static str = "tests/failing-map-value-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Err(Error::incompatible("map value encode failed"))
    }
    fn decode(_: &[u8]) -> Result<Self, Error> {
        Ok(Self)
    }
}

#[derive(Eq, Hash, PartialEq)]
struct FailingKey;

impl Element for FailingKey {
    const TYPE_ID: &'static str = "tests/failing-map-key";
    const CODER_ID: &'static str = "tests/failing-map-key-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Err(Error::incompatible("map key encode failed"))
    }
    fn decode(_: &[u8]) -> Result<Self, Error> {
        Ok(Self)
    }
}

#[derive(Eq, Hash, PartialEq)]
struct Uncoded;

impl Element for Uncoded {
    const TYPE_ID: &'static str = "tests/uncoded-map-key";
    const CODER_ID: &'static str = "tests/uncoded-map-key-v1";
}

#[test]
fn map_state_rejects_coder_collisions_and_failed_value_encoding_atomically() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_map_map_named::<CollidingKey, i32, i32>(
            "colliding-map",
            "tests/colliding-map",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                Ok((
                    MapStateMutation::Put(CollidingKey(value), value),
                    i32::try_from(prior.len()).unwrap(),
                ))
            },
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.apply_many("colliding-map", input(1)).is_ok());
    let before = registry.checkpoint_stateful_value("colliding-map").unwrap();
    assert!(registry.apply_many("colliding-map", input(2)).is_err());
    assert_eq!(
        registry.checkpoint_stateful_value("colliding-map").unwrap(),
        before
    );

    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_map_map_named::<i32, FailingValue, i32>(
            "failing-value-map",
            "tests/failing-value-map",
            Version { major: 1, minor: 0 },
            |_, _, _| Ok((MapStateMutation::Put(1, FailingValue), 0)),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let before = registry
        .checkpoint_stateful_value("failing-value-map")
        .unwrap();
    assert!(registry.apply_many("failing-value-map", input(1)).is_err());
    assert_eq!(
        registry
            .checkpoint_stateful_value("failing-value-map")
            .unwrap(),
        before
    );

    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_map_map_named::<FailingKey, i32, i32>(
            "failing-key-map",
            "tests/failing-key-map",
            Version { major: 1, minor: 0 },
            |_, _, _| Ok((MapStateMutation::Put(FailingKey, 1), 0)),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let before = registry
        .checkpoint_stateful_value("failing-key-map")
        .unwrap();
    assert!(registry.apply_many("failing-key-map", input(1)).is_err());
    assert_eq!(
        registry
            .checkpoint_stateful_value("failing-key-map")
            .unwrap(),
        before
    );
}

#[test]
fn map_state_requires_coders_and_default_window_policy() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    assert!(
        pairs
            .stateful_map_map_named::<Uncoded, i32, i32>(
                "uncoded-map",
                "tests/uncoded-map",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((MapStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let session = pairs.session_windows_named("sessions", 10_000_000).unwrap();
    assert!(
        session
            .stateful_map_map_named::<i32, i32, i32>(
                "session-map",
                "tests/session-map",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((MapStateMutation::Keep, 0)),
            )
            .is_err()
    );
}
