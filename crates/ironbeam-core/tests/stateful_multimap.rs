use std::collections::HashMap;

use ironbeam_core::{
    Element, Envelope, Error, MultimapStateMutation, PaneInfo, Pipeline, UserStateKind, Version,
    Window,
};

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_multimap_map_named::<i32, i32, (i32, i32)>(
            "multimap",
            "tests/multimap-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    i32::try_from(prior.get(&10).map_or(0, Vec::len)).unwrap(),
                    i32::try_from(prior.values().map(Vec::len).sum::<usize>()).unwrap(),
                );
                let mutation = match value {
                    -1 => MultimapStateMutation::Remove(10),
                    -2 => MultimapStateMutation::Replace(HashMap::from([(10, vec![7, 7, 9])])),
                    -3 => MultimapStateMutation::Clear,
                    -4 => MultimapStateMutation::Keep,
                    -5 => return Err(Error::incompatible("multimap callback failed")),
                    value => MultimapStateMutation::Append(10, value),
                };
                Ok((mutation, observation))
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
    let output = registry.apply_many("multimap", input(value)).unwrap();
    let [row] = output.try_into().ok().unwrap();
    row.value.downcast::<(i32, (i32, i32))>().unwrap().1
}

#[test]
fn multimap_keeps_duplicate_values_and_restores_coded_state() {
    let pipeline = pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node = graph.node("multimap").unwrap();
    let ironbeam_core::Operation::StatefulParDo { contract, .. } = &node.operation else {
        panic!("multimap node must be stateful");
    };
    assert!(matches!(
        contract.states.get("multimap"),
        Some(UserStateKind::Multimap { .. })
    ));
    assert_eq!(apply(&registry, 10), (0, 0));
    assert_eq!(apply(&registry, 10), (1, 1));
    assert_eq!(apply(&registry, 20), (2, 2));
    assert_eq!(apply(&registry, -4), (3, 3));
    let snapshot = registry.checkpoint_stateful_value("multimap").unwrap();
    let (_, recovered) = pipeline.snapshot().unwrap();
    recovered
        .restore_stateful_value("multimap", &snapshot)
        .unwrap();
    assert_eq!(apply(&recovered, -1), (3, 3));
    assert_eq!(apply(&recovered, 10), (0, 0));
    assert_eq!(apply(&recovered, -2), (1, 1));
    assert_eq!(apply(&recovered, -3), (3, 3));
    assert_eq!(apply(&recovered, 10), (0, 0));
}

#[test]
fn multimap_rejects_bad_state_and_callback_errors_without_changing_cell() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(apply(&registry, 10), (0, 0));
    let before = registry.checkpoint_stateful_value("multimap").unwrap();
    assert!(registry.apply_many("multimap", input(-5)).is_err());
    assert_eq!(
        registry.checkpoint_stateful_value("multimap").unwrap(),
        before
    );
    let duplicate = serde_json::to_vec(&vec![
        (10_i32.encode().unwrap(), vec![1_i32.encode().unwrap()]),
        (10_i32.encode().unwrap(), vec![2_i32.encode().unwrap()]),
    ])
    .unwrap();
    assert!(
        registry
            .apply_stateful_value("multimap", input(-4), Some(&duplicate))
            .is_err()
    );
    let invalid = serde_json::to_vec(&vec![(10_i32.encode().unwrap(), vec![vec![1_u8]])]).unwrap();
    assert!(
        registry
            .apply_stateful_value("multimap", input(-4), Some(&invalid))
            .is_err()
    );
    let invalid_key =
        serde_json::to_vec(&vec![(vec![1_u8], vec![10_i32.encode().unwrap()])]).unwrap();
    assert!(
        registry
            .apply_stateful_value("multimap", input(-4), Some(&invalid_key))
            .is_err()
    );
    assert!(
        registry
            .apply_stateful_value("multimap", input(-4), Some(&[1]))
            .is_err()
    );
    let wrong_input = Envelope {
        value: Box::new(1_i32),
        ..input(-4)
    };
    assert!(
        registry
            .apply_stateful_value("multimap", wrong_input, None)
            .is_err()
    );
    assert_eq!(apply(&registry, -4), (1, 1));
}

#[derive(Eq, Hash, PartialEq)]
struct Uncoded;

impl Element for Uncoded {
    const TYPE_ID: &'static str = "tests/uncoded-multimap";
    const CODER_ID: &'static str = "tests/uncoded-multimap-v1";
}

#[derive(Eq, Hash, PartialEq)]
struct FailingCoder;

impl Element for FailingCoder {
    const TYPE_ID: &'static str = "tests/failing-multimap-coder";
    const CODER_ID: &'static str = "tests/failing-multimap-coder-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        Err(Error::incompatible("multimap encode failed"))
    }
    fn decode(_: &[u8]) -> Result<Self, Error> {
        Ok(Self)
    }
}

#[derive(Eq, Hash, PartialEq)]
struct CollidingKey(i32);

impl Element for CollidingKey {
    const TYPE_ID: &'static str = "tests/colliding-multimap-key";
    const CODER_ID: &'static str = "tests/colliding-multimap-key-v1";
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

#[test]
fn multimap_requires_coders_and_default_window_policy() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    assert!(
        pairs
            .stateful_multimap_map_named::<Uncoded, i32, i32>(
                "uncoded-key",
                "tests/uncoded-key",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((MultimapStateMutation::Keep, 0)),
            )
            .is_err()
    );
    assert!(
        pairs
            .stateful_multimap_map_named::<i32, Uncoded, i32>(
                "uncoded-value",
                "tests/uncoded-value",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((MultimapStateMutation::Keep, 0)),
            )
            .is_err()
    );
    assert!(
        pairs
            .stateful_multimap_map_named::<i32, i32, Uncoded>(
                "uncoded-output",
                "tests/uncoded-output",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((MultimapStateMutation::Keep, Uncoded)),
            )
            .is_err()
    );
    let session = pairs.session_windows_named("sessions", 10_000_000).unwrap();
    assert!(
        session
            .stateful_multimap_map_named::<i32, i32, i32>(
                "session-multimap",
                "tests/session-multimap",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((MultimapStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let fixed = pairs.fixed_windows("fixed", 10_000_000).unwrap();
    fixed
        .stateful_multimap_map_named::<i32, i32, i32>(
            "fixed-multimap",
            "tests/fixed-multimap",
            Version { major: 1, minor: 0 },
            |_, _, _| Ok((MultimapStateMutation::Keep, 0)),
        )
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let ironbeam_core::Operation::StatefulParDo { contract, .. } =
        &graph.node("fixed-multimap").unwrap().operation
    else {
        panic!("fixed multimap must be stateful");
    };
    assert_eq!(contract.window_coder_id, "c_iw");
}

#[test]
fn multimap_rejects_encoder_failures_and_key_collisions_atomically() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_multimap_map_named::<i32, FailingCoder, i32>(
            "bad-value",
            "tests/bad-value",
            Version { major: 1, minor: 0 },
            |_, _, _| Ok((MultimapStateMutation::Append(1, FailingCoder), 0)),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let before = registry.checkpoint_stateful_value("bad-value").unwrap();
    assert!(registry.apply_many("bad-value", input(1)).is_err());
    assert_eq!(
        registry.checkpoint_stateful_value("bad-value").unwrap(),
        before
    );

    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_multimap_map_named::<FailingCoder, i32, i32>(
            "bad-key",
            "tests/bad-key",
            Version { major: 1, minor: 0 },
            |_, _, _| Ok((MultimapStateMutation::Append(FailingCoder, 1), 0)),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.apply_many("bad-key", input(1)).is_err());

    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_multimap_map_named::<CollidingKey, i32, i32>(
            "collision",
            "tests/collision",
            Version { major: 1, minor: 0 },
            |_, value, _| Ok((MultimapStateMutation::Append(CollidingKey(value), value), 0)),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.apply_many("collision", input(1)).is_ok());
    let before = registry.checkpoint_stateful_value("collision").unwrap();
    assert!(registry.apply_many("collision", input(2)).is_err());
    assert_eq!(
        registry.checkpoint_stateful_value("collision").unwrap(),
        before
    );
}
