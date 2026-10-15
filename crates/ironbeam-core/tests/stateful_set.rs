use ironbeam_core::{
    Element, Envelope, Error, PaneInfo, Pipeline, SetStateMutation, UserStateKind, Version, Window,
    WindowInterval,
};

#[derive(Eq, Hash, PartialEq)]
struct Uncoded;

impl Element for Uncoded {
    const TYPE_ID: &'static str = "tests/uncoded-set-element";
    const CODER_ID: &'static str = "tests/uncoded-set-element-v1";
}

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_set_map_named::<i32, (i32, i32)>(
            "set",
            "tests/set-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let observation = (
                    i32::from(prior.contains(&value)),
                    i32::try_from(prior.len()).unwrap(),
                );
                let mutation = match value {
                    -5 => return Err(Error::incompatible("set callback failed")),
                    -1 => SetStateMutation::Keep,
                    -2 => SetStateMutation::Remove(10),
                    -3 => SetStateMutation::Replace(vec![7, 7, 9]),
                    -4 => SetStateMutation::Clear,
                    value => SetStateMutation::Insert(value),
                };
                Ok((mutation, observation))
            },
        )
        .unwrap();
    pipeline
}

fn apply(registry: &ironbeam_core::Registry, value: i32) -> (i32, i32) {
    let output = registry
        .apply_many(
            "set",
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
    row.value.downcast::<(i32, (i32, i32))>().unwrap().1
}

#[test]
fn set_state_deduplicates_mutates_and_restores_coded_contents() {
    let pipeline = pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let node = graph.node("set").unwrap();
    let ironbeam_core::Operation::StatefulParDo { contract, .. } = &node.operation else {
        panic!("set node must be stateful");
    };
    assert!(matches!(
        contract.states.get("set"),
        Some(UserStateKind::Set { .. })
    ));
    assert_eq!(apply(&registry, 10), (0, 0));
    assert_eq!(apply(&registry, 10), (1, 1));
    assert_eq!(apply(&registry, 20), (0, 1));
    assert_eq!(apply(&registry, -1), (0, 2));
    let snapshot = registry.checkpoint_stateful_value("set").unwrap();
    let (_, recovered) = pipeline.snapshot().unwrap();
    recovered.restore_stateful_value("set", &snapshot).unwrap();
    assert_eq!(apply(&recovered, -2), (0, 2));
    assert_eq!(apply(&recovered, 10), (0, 1));
    assert_eq!(apply(&recovered, -3), (0, 2));
    assert_eq!(apply(&recovered, 7), (1, 2));
    assert_eq!(apply(&recovered, -4), (0, 2));
    assert_eq!(apply(&recovered, 40), (0, 0));
}

#[test]
fn malformed_set_contents_fail_restore_and_runner_callback() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(apply(&registry, 10), (0, 0));
    let malformed = serde_json::json!({
        "version": 1,
        "entries": [[null, 1_i32.encode().unwrap(), [1, 2, 3]]],
    });
    assert!(
        registry
            .restore_stateful_value("set", &serde_json::to_vec(&malformed).unwrap())
            .is_err()
    );
    assert_eq!(apply(&registry, 10), (1, 1));
    let input = |value: Box<dyn std::any::Any + Send>| Envelope {
        value,
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
    };
    assert!(
        registry
            .apply_stateful_value("set", input(Box::new(1_i32)), None)
            .is_err()
    );
    assert!(registry.apply_many("set", input(Box::new(1_i32))).is_err());
    assert!(
        registry
            .apply_stateful_value("set", input(Box::new((1_i32, 1_i32))), Some(&[1]))
            .is_err()
    );
    let malformed_item = serde_json::to_vec(&vec![vec![1_u8]]).unwrap();
    assert!(
        registry
            .apply_stateful_value(
                "set",
                input(Box::new((1_i32, 1_i32))),
                Some(&malformed_item)
            )
            .is_err()
    );
}

#[test]
fn set_callback_error_keeps_the_previous_cell() {
    let pipeline = pipeline();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(apply(&registry, 10), (0, 0));
    assert!(
        registry
            .apply_many(
                "set",
                Envelope {
                    value: Box::new((1_i32, -5_i32)),
                    timestamp_ns: 0,
                    window: Window::Global,
                    assigned_window: None,
                    pane: PaneInfo::NO_FIRING,
                }
            )
            .is_err()
    );
    assert_eq!(apply(&registry, 10), (1, 1));
}

#[test]
fn set_registration_rejects_invalid_identity_and_window_policy() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    assert!(
        pairs
            .stateful_set_map_named::<i32, i32>(
                "empty-id",
                "",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((SetStateMutation::Keep, 0)),
            )
            .is_err()
    );
    assert!(
        pairs
            .stateful_set_map_named::<i32, i32>(
                "zero-version",
                "tests/set",
                Version { major: 0, minor: 0 },
                |_, _, _| Ok((SetStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let early = pairs
        .fixed_windows("fixed", 10_000_000)
        .unwrap()
        .early_count_trigger_named("early", 1)
        .unwrap();
    assert!(
        early
            .stateful_set_map_named::<i32, i32>(
                "early-set",
                "tests/early-set",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((SetStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let late = pairs
        .fixed_windows("late-fixed", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 1_000_000)
        .unwrap();
    assert!(
        late.stateful_set_map_named::<i32, i32>(
            "late-set",
            "tests/late-set",
            Version { major: 1, minor: 0 },
            |_, _, _| Ok((SetStateMutation::Keep, 0)),
        )
        .is_err()
    );
    let sessions = pairs.session_windows_named("sessions", 10_000_000).unwrap();
    assert!(
        sessions
            .stateful_set_map_named::<i32, i32>(
                "session-set",
                "tests/session-set",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((SetStateMutation::Keep, 0)),
            )
            .is_err()
    );
    assert!(
        pairs
            .stateful_set_map_named::<Uncoded, i32>(
                "uncoded-set",
                "tests/uncoded-set",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((SetStateMutation::Keep, 0)),
            )
            .is_err()
    );
}

#[test]
fn fixed_windows_keep_independent_set_cells() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 10_i32)])
        .unwrap()
        .fixed_windows("fixed", 10_000_000)
        .unwrap()
        .stateful_set_map_named::<i32, i32>(
            "set",
            "tests/fixed-set-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                Ok((
                    SetStateMutation::Insert(value),
                    i32::from(prior.contains(&value)),
                ))
            },
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let apply_window = |start_ns| {
        let output = registry
            .apply_many(
                "set",
                Envelope {
                    value: Box::new((1_i32, 10_i32)),
                    timestamp_ns: start_ns,
                    window: Window::Fixed {
                        size_ns: 10_000_000,
                    },
                    assigned_window: Some(WindowInterval {
                        start_ns,
                        end_ns: start_ns + 10_000_000,
                    }),
                    pane: PaneInfo::NO_FIRING,
                },
            )
            .unwrap();
        let [row] = output.try_into().ok().unwrap();
        row.value.downcast::<(i32, i32)>().unwrap().1
    };
    assert_eq!(apply_window(0), 0);
    assert_eq!(apply_window(20_000_000), 0);
    assert_eq!(apply_window(0), 1);
    assert_eq!(apply_window(20_000_000), 1);
}
