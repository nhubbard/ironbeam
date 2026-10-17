use ironbeam_core::{
    CombiningFunction, CombiningStateMutation, Element, Envelope, Error, Operation, PaneInfo,
    Pipeline, UserStateKind, Version, Window,
};

fn pipeline() -> Pipeline {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("pairs", vec![(1_i32, 1_i32)])
        .unwrap()
        .stateful_combining_map_named::<i32, i32>(
            "combining",
            "tests/combining-state",
            Version { major: 1, minor: 0 },
            CombiningFunction {
                semantic_id: "tests/checked-sum",
                version: Version { major: 2, minor: 0 },
            },
            |prior, value| {
                prior
                    .checked_add(value)
                    .filter(|sum| *sum < 999)
                    .ok_or_else(|| Error::incompatible("combining overflow"))
            },
            |_, value, prior| {
                let mutation = match value {
                    -1 => CombiningStateMutation::Clear,
                    -2 => CombiningStateMutation::Replace(50),
                    -3 => CombiningStateMutation::Keep,
                    -4 => CombiningStateMutation::Add(999),
                    value => CombiningStateMutation::Add(value),
                };
                Ok((mutation, prior.copied().unwrap_or_default()))
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

fn apply(registry: &ironbeam_core::Registry, value: i32) -> i32 {
    let output = registry.apply_many("combining", input(value)).unwrap();
    let [row] = output.try_into().ok().unwrap();
    row.value.downcast::<(i32, i32)>().unwrap().1
}

#[test]
fn combining_add_clear_replace_and_checkpoint_preserve_accumulator() {
    let pipeline = pipeline();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let Operation::StatefulParDo { contract, .. } = &graph.node("combining").unwrap().operation
    else {
        panic!("combining node must be stateful");
    };
    assert!(matches!(
        contract.states.get("combining"),
        Some(UserStateKind::Combining { combine_semantic_id, combine_version, .. })
            if combine_semantic_id == "tests/checked-sum" && *combine_version == Version { major: 2, minor: 0 }
    ));
    assert_eq!(apply(&registry, 10), 0);
    assert_eq!(apply(&registry, 20), 10);
    let snapshot = registry.checkpoint_stateful_value("combining").unwrap();
    let (_, restored) = pipeline.snapshot().unwrap();
    restored
        .restore_stateful_value("combining", &snapshot)
        .unwrap();
    assert_eq!(apply(&restored, -3), 30);
    assert_eq!(apply(&restored, -2), 30);
    assert_eq!(apply(&restored, 5), 50);
    assert_eq!(apply(&restored, -1), 55);
    assert_eq!(apply(&restored, 7), 0);
}

#[test]
fn failed_combining_merge_leaves_cell_unchanged() {
    let (_, registry) = pipeline().snapshot().unwrap();
    assert_eq!(apply(&registry, 7), 0);
    let before = registry.checkpoint_stateful_value("combining").unwrap();
    assert!(registry.apply_many("combining", input(-4)).is_err());
    assert_eq!(
        registry.checkpoint_stateful_value("combining").unwrap(),
        before
    );
    assert_eq!(apply(&registry, -3), 7);
    assert!(
        registry
            .apply_stateful_value("combining", input(1), Some(&[1]))
            .is_err()
    );
}

#[test]
fn combining_merges_multiple_coded_runner_accumulators() {
    let (_, registry) = pipeline().snapshot().unwrap();
    let encoded = [10_i32, 20, 5]
        .into_iter()
        .map(|value| value.encode().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        i32::decode(
            &registry
                .merge_combining_accumulators("combining", &encoded)
                .unwrap()
        )
        .unwrap(),
        35
    );
    assert!(
        registry
            .merge_combining_accumulators("combining", &[])
            .is_err()
    );
    assert!(
        registry
            .merge_combining_accumulators("missing", &encoded)
            .is_err()
    );
    assert!(
        registry
            .merge_combining_accumulators("combining", &[encoded[0].clone(), vec![1]])
            .is_err()
    );
    let overflow = [990_i32, 20]
        .into_iter()
        .map(|value| value.encode().unwrap())
        .collect::<Vec<_>>();
    assert!(
        registry
            .merge_combining_accumulators("combining", &overflow)
            .is_err()
    );
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct Uncoded;

impl Element for Uncoded {
    const TYPE_ID: &'static str = "tests/uncoded-combining";
    const CODER_ID: &'static str = "tests/uncoded-combining-v1";
}

#[test]
fn combining_requires_coded_accumulator_and_versioned_merge() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    assert!(
        pairs
            .stateful_combining_map_named::<Uncoded, i32>(
                "uncoded",
                "tests/uncoded",
                Version { major: 1, minor: 0 },
                CombiningFunction {
                    semantic_id: "tests/merge",
                    version: Version { major: 1, minor: 0 }
                },
                |_, _| Ok(Uncoded),
                |_, _, _| Ok((CombiningStateMutation::Keep, 0)),
            )
            .is_err()
    );
    assert!(
        pairs
            .stateful_combining_map_named::<i32, i32>(
                "bad-merge",
                "tests/bad-merge",
                Version { major: 1, minor: 0 },
                CombiningFunction {
                    semantic_id: "unversioned",
                    version: Version { major: 0, minor: 1 }
                },
                |prior, value| Ok(prior + value),
                |_, _, _| Ok((CombiningStateMutation::Keep, 0)),
            )
            .is_err()
    );
}

#[test]
fn combining_requires_coded_keys_inputs_and_outputs() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    assert!(
        pairs
            .stateful_combining_map_named::<i32, Uncoded>(
                "uncoded-output",
                "tests/uncoded-output",
                Version { major: 1, minor: 0 },
                CombiningFunction {
                    semantic_id: "tests/merge",
                    version: Version { major: 1, minor: 0 }
                },
                |left, right| Ok(left + right),
                |_, _, _| Ok((CombiningStateMutation::Keep, Uncoded)),
            )
            .is_err()
    );
    let uncoded_keys = pipeline
        .from_vec("uncoded-keys", vec![(Uncoded, 1_i32)])
        .unwrap();
    assert!(
        uncoded_keys
            .stateful_combining_map_named::<i32, i32>(
                "uncoded-key",
                "tests/uncoded-key",
                Version { major: 1, minor: 0 },
                CombiningFunction {
                    semantic_id: "tests/merge",
                    version: Version { major: 1, minor: 0 }
                },
                |left, right| Ok(left + right),
                |_, _, _| Ok((CombiningStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let uncoded_values = pipeline
        .from_vec("uncoded-values", vec![(1_i32, Uncoded)])
        .unwrap();
    assert!(
        uncoded_values
            .stateful_combining_map_named::<i32, i32>(
                "uncoded-value",
                "tests/uncoded-value",
                Version { major: 1, minor: 0 },
                CombiningFunction {
                    semantic_id: "tests/merge",
                    version: Version { major: 1, minor: 0 }
                },
                |left, right| Ok(left + right),
                |_, _, _| Ok((CombiningStateMutation::Keep, 0)),
            )
            .is_err()
    );
}

#[test]
fn combining_requires_default_nonmerging_windows() {
    let pipeline = Pipeline::default();
    let pairs = pipeline.from_vec("pairs", vec![(1_i32, 1_i32)]).unwrap();
    let session = pairs.session_windows_named("sessions", 10_000_000).unwrap();
    assert!(
        session
            .stateful_combining_map_named::<i32, i32>(
                "session-combining",
                "tests/session-combining",
                Version { major: 1, minor: 0 },
                CombiningFunction {
                    semantic_id: "tests/merge",
                    version: Version { major: 1, minor: 0 }
                },
                |left, right| Ok(left + right),
                |_, _, _| Ok((CombiningStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let early = pairs
        .fixed_windows("early-window", 10_000_000)
        .unwrap()
        .early_count_trigger_named("early-count", 1)
        .unwrap();
    assert!(
        early
            .stateful_combining_map_named::<i32, i32>(
                "early-combining",
                "tests/early-combining",
                Version { major: 1, minor: 0 },
                CombiningFunction {
                    semantic_id: "tests/merge",
                    version: Version { major: 1, minor: 0 }
                },
                |left, right| Ok(left + right),
                |_, _, _| Ok((CombiningStateMutation::Keep, 0)),
            )
            .is_err()
    );
    let late = pairs
        .fixed_windows("late-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 1_000_000)
        .unwrap();
    assert!(
        late.stateful_combining_map_named::<i32, i32>(
            "late-combining",
            "tests/late-combining",
            Version { major: 1, minor: 0 },
            CombiningFunction {
                semantic_id: "tests/merge",
                version: Version { major: 1, minor: 0 }
            },
            |left, right| Ok(left + right),
            |_, _, _| Ok((CombiningStateMutation::Keep, 0)),
        )
        .is_err()
    );
}
