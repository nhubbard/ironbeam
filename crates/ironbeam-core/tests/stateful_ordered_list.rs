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
                    -8 => OrderedListStateMutation::ClearRange {
                        start_ms: 0,
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
    let wrong_input = Envelope {
        value: Box::new(1_i32),
        ..input(-4)
    };
    assert!(
        registry
            .apply_stateful_value("ordered", wrong_input, None)
            .is_err()
    );
    assert_eq!(apply(&registry, -4), [10]);
}

#[test]
fn ordered_list_clear_range_preserves_entries_below_and_at_upper_bound() {
    let (_, registry) = pipeline().snapshot().unwrap();
    assert_eq!(apply(&registry, -10), Vec::<i32>::new());
    assert_eq!(apply(&registry, 10), [-10]);
    assert_eq!(apply(&registry, 20), [-10, 10]);
    assert_eq!(apply(&registry, -8), [-10, 10, 20]);
    assert_eq!(apply(&registry, -4), [-10, 20]);
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct Uncoded;

impl Element for Uncoded {
    const TYPE_ID: &'static str = "tests/uncoded-ordered-list";
    const CODER_ID: &'static str = "tests/uncoded-ordered-list-v1";
}

#[test]
fn ordered_list_requires_coded_inputs_and_outputs() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    assert!(
        pairs
            .stateful_ordered_list_map_named::<Uncoded, i32>(
                "uncoded-cell",
                "tests/uncoded-ordered-cell",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((OrderedListStateMutation::Keep, 0)),
            )
            .is_err()
    );
    assert!(
        pairs
            .stateful_ordered_list_map_named::<i32, Uncoded>(
                "uncoded-output",
                "tests/uncoded-ordered-output",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((OrderedListStateMutation::Keep, Uncoded)),
            )
            .is_err()
    );
    let key_pairs = pipeline
        .from_vec("uncoded-keys", vec![(Uncoded, 1_i32)])
        .unwrap();
    assert!(
        key_pairs
            .stateful_ordered_list_map_named::<i32, i32>(
                "uncoded-key",
                "tests/uncoded-ordered-key",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((OrderedListStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let value_pairs = pipeline
        .from_vec("uncoded-values", vec![(1_i32, Uncoded)])
        .unwrap();
    assert!(
        value_pairs
            .stateful_ordered_list_map_named::<i32, i32>(
                "uncoded-input",
                "tests/uncoded-ordered-input",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((OrderedListStateMutation::Keep, 0)),
            )
            .is_err()
    );
}

#[test]
fn ordered_list_requires_default_nonmerging_windows() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    assert!(
        pairs
            .stateful_ordered_list_map_named::<i32, i32>(
                "bad-semantic",
                "unversioned",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((OrderedListStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let session = pairs.session_windows_named("sessions", 10_000_000).unwrap();
    assert!(
        session
            .stateful_ordered_list_map_named::<i32, i32>(
                "session-ordered",
                "tests/session-ordered",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((OrderedListStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let early = pairs
        .fixed_windows("early-fixed", 10_000_000)
        .unwrap()
        .early_count_trigger_named("early", 1)
        .unwrap();
    assert!(
        early
            .stateful_ordered_list_map_named::<i32, i32>(
                "early-ordered",
                "tests/early-ordered",
                Version { major: 1, minor: 0 },
                |_, _, _| Ok((OrderedListStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let late = pairs
        .fixed_windows("late-fixed", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 1_000_000)
        .unwrap();
    assert!(
        late.stateful_ordered_list_map_named::<i32, i32>(
            "late-ordered",
            "tests/late-ordered",
            Version { major: 1, minor: 0 },
            |_, _, _| Ok((OrderedListStateMutation::Keep, 0)),
        )
        .is_err()
    );
    let fixed = pairs.fixed_windows("fixed", 10_000_000).unwrap();
    fixed
        .stateful_ordered_list_map_named::<i32, i32>(
            "fixed-ordered",
            "tests/fixed-ordered",
            Version { major: 1, minor: 0 },
            |_, _, _| Ok((OrderedListStateMutation::Keep, 0)),
        )
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let Operation::StatefulParDo { contract, .. } = &graph.node("fixed-ordered").unwrap().operation
    else {
        panic!("fixed ordered-list must be stateful");
    };
    assert_eq!(contract.window_coder_id, "c_iw");
}
