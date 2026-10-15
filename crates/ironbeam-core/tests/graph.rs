use ironbeam_core::*;

#[test]
fn keyed_batches_reject_zero_and_unrepresentable_portable_sizes() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    for invalid in std::iter::once(0).chain(usize::try_from(i64::MAX).ok()) {
        let error = input
            .group_into_batches_named("batches", invalid)
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Graph);
        assert!(error.message.contains("batch size"));
    }
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(graph.node("batches").is_none());

    if let Ok(largest) = usize::try_from(i64::MAX - 1) {
        input.group_into_batches_named("largest", largest).unwrap();
        let (graph, _) = pipeline.snapshot().unwrap();
        assert!(graph.node("largest").is_some());
    }
}

#[test]
fn encoded_byte_batch_limits_change_graph_identity() {
    let make = |max_bytes| {
        let pipeline = Pipeline::default();
        pipeline
            .from_vec("source", vec![(1_i32, "a".to_owned())])
            .unwrap()
            .group_into_batches_with_limits_named("batches", 2, max_bytes)
            .unwrap();
        pipeline.snapshot().unwrap().0.fingerprint().unwrap()
    };
    assert_ne!(make(4), make(5));
}

struct ScriptedUnbounded {
    polls: std::collections::VecDeque<UnboundedPoll<Timestamped<i32>>>,
}

impl UnboundedSource<Timestamped<i32>> for ScriptedUnbounded {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

struct FailingUnbounded;

impl UnboundedSource<Timestamped<i32>> for FailingUnbounded {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Err(Error::incompatible("source poll failed"))
    }
}

fn unbounded_signal(
    position: Option<Vec<u8>>,
    watermark: Option<i64>,
    completed: bool,
) -> ProgressSignal {
    ProgressSignal {
        source_key: "events".into(),
        split_id: "split-a".into(),
        checkpoint_position: position,
        event_time_ns: watermark,
        completed,
    }
}

#[test]
fn unbounded_source_polling_resumes_and_preserves_time() {
    let opened = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "stream-config-v1".into(),
            -1_000_000,
            {
                let opened = opened.clone();
                move |position| {
                    opened.lock().unwrap().push(position.clone());
                    let polls = if position.is_none() {
                        vec![
                            UnboundedPoll::Pending,
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: Vec::new(),
                                progress: unbounded_signal(Some(vec![0]), None, false),
                            }),
                            UnboundedPoll::Ready(UnboundedBatch {
                                elements: vec![Timestamped {
                                    value: 7_i32,
                                    timestamp_ns: 1_000_000,
                                }],
                                progress: unbounded_signal(Some(vec![1]), Some(500_000), false),
                            }),
                        ]
                    } else {
                        vec![UnboundedPoll::Ready(UnboundedBatch {
                            elements: vec![Timestamped {
                                value: 8_i32,
                                timestamp_ns: 2_000_000,
                            }],
                            progress: unbounded_signal(Some(vec![2]), Some(1_500_000), true),
                        })]
                    };
                    Ok(Box::new(ScriptedUnbounded {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                }
            },
        )
        .unwrap();
    source.output("out").unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("events").unwrap().boundedness,
        Boundedness::Unbounded
    );
    assert_eq!(
        graph.node("events").unwrap().time.initial_timestamp_ns,
        -1_000_000
    );
    assert_eq!(
        registry.source_config_digest("events"),
        Some("stream-config-v1")
    );

    let mut reader = registry.open_unbounded_source("events", None).unwrap();
    assert!(matches!(reader.poll(1).unwrap(), UnboundedPoll::Pending));
    let UnboundedPoll::Ready(progress_only) = reader.poll(1).unwrap() else {
        panic!("expected progress");
    };
    assert!(progress_only.elements.is_empty());
    assert_eq!(progress_only.progress.checkpoint_position, Some(vec![0]));
    let UnboundedPoll::Ready(batch) = reader.poll(1).unwrap() else {
        panic!("expected data");
    };
    assert_eq!(batch.elements.len(), 1);
    assert_eq!(*batch.elements[0].value.downcast_ref::<i32>().unwrap(), 7);
    assert_eq!(batch.elements[0].timestamp_ns, 1_000_000);
    assert_eq!(batch.progress.checkpoint_position, Some(vec![1]));
    let saved_progress = batch.progress;

    let mut resumed = registry
        .open_unbounded_source("events", Some(saved_progress.clone()))
        .unwrap();
    let UnboundedPoll::Ready(batch) = resumed.poll(1).unwrap() else {
        panic!("expected resumed data");
    };
    assert_eq!(*batch.elements[0].value.downcast_ref::<i32>().unwrap(), 8);
    assert!(batch.progress.completed);
    assert!(resumed.poll(1).is_err());
    assert_eq!(*opened.lock().unwrap(), [None, Some(vec![1])]);
    assert!(
        registry
            .open_unbounded_source(
                "events",
                Some(ProgressSignal {
                    source_key: "wrong".into(),
                    ..saved_progress
                })
            )
            .is_err()
    );
}

#[test]
#[allow(clippy::too_many_lines)] // One matrix checks all malformed source-progress shapes.
fn unbounded_source_rejects_invalid_progress_and_capacity() {
    let valid = UnboundedPoll::Ready(UnboundedBatch {
        elements: vec![Timestamped {
            value: 1_i32,
            timestamp_ns: 0,
        }],
        progress: unbounded_signal(Some(vec![1]), Some(1), false),
    });
    let mut bad_key = valid.clone();
    if let UnboundedPoll::Ready(batch) = &mut bad_key {
        batch.progress.source_key = "wrong".into();
    }
    let mut bad_split = valid.clone();
    if let UnboundedPoll::Ready(batch) = &mut bad_split {
        batch.progress.split_id.clear();
    }
    let mut no_position = valid.clone();
    if let UnboundedPoll::Ready(batch) = &mut no_position {
        batch.progress.checkpoint_position = None;
    }
    let mut oversize = valid.clone();
    if let UnboundedPoll::Ready(batch) = &mut oversize {
        batch.progress.checkpoint_position = Some(vec![0; 1024 * 1024 + 1]);
    }
    let mut too_many = valid.clone();
    if let UnboundedPoll::Ready(batch) = &mut too_many {
        batch.elements.push(Timestamped {
            value: 2_i32,
            timestamp_ns: 0,
        });
    }
    let no_progress = UnboundedPoll::Ready(UnboundedBatch {
        elements: Vec::new(),
        progress: unbounded_signal(None, None, false),
    });
    let changed_split = UnboundedPoll::Ready(UnboundedBatch {
        elements: Vec::new(),
        progress: ProgressSignal {
            split_id: "split-b".into(),
            ..unbounded_signal(Some(vec![2]), Some(2), false)
        },
    });
    let regressed = UnboundedPoll::Ready(UnboundedBatch {
        elements: Vec::new(),
        progress: unbounded_signal(Some(vec![2]), Some(0), false),
    });
    for script in [
        vec![bad_key],
        vec![bad_split],
        vec![no_position],
        vec![oversize],
        vec![too_many],
        vec![no_progress],
        vec![valid.clone(), changed_split],
        vec![valid.clone(), regressed],
    ] {
        let pipeline = Pipeline::default();
        pipeline
            .unbounded_source_with_factory(
                "events",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "test-script".into(),
                0,
                move |_| {
                    Ok(Box::new(ScriptedUnbounded {
                        polls: script.clone().into(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap();
        let (_, registry) = pipeline.snapshot().unwrap();
        let mut reader = registry.open_unbounded_source("events", None).unwrap();
        if reader.poll(1).is_ok() {
            assert!(reader.poll(1).is_err());
        }
    }
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "test-script".into(),
            0,
            move |_| {
                Ok(Box::new(ScriptedUnbounded {
                    polls: vec![valid.clone()].into(),
                }) as TypedUnboundedSourceReader<i32>)
            },
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(
        registry
            .open_unbounded_source("events", None)
            .unwrap()
            .poll(0)
            .is_err()
    );
    let mut resumed = registry
        .open_unbounded_source(
            "events",
            Some(unbounded_signal(Some(vec![1]), Some(5), false)),
        )
        .unwrap();
    assert!(resumed.poll(1).is_err());
    assert!(
        registry
            .open_unbounded_source(
                "events",
                Some(unbounded_signal(
                    Some(vec![0; 1024 * 1024 + 1]),
                    None,
                    false
                )),
            )
            .is_err()
    );
    for invalid in [
        ProgressSignal {
            split_id: String::new(),
            ..unbounded_signal(Some(vec![1]), None, false)
        },
        unbounded_signal(Some(vec![1]), None, true),
        unbounded_signal(None, None, false),
    ] {
        assert!(
            registry
                .open_unbounded_source("events", Some(invalid))
                .is_err()
        );
    }
}

#[test]
fn unbounded_source_reports_open_and_poll_failures() {
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: true,
    };
    assert!(
        pipeline
            .unbounded_source_with_factory::<i32>(
                "bad",
                progress.clone(),
                String::new(),
                0,
                |_| Ok(Box::new(FailingUnbounded)),
            )
            .is_err()
    );
    pipeline
        .unbounded_source_with_factory("events", progress, "source-config".into(), 0, |position| {
            if position.is_none() {
                Err(Error::incompatible("source open failed"))
            } else {
                Ok(Box::new(FailingUnbounded) as TypedUnboundedSourceReader<i32>)
            }
        })
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.open_source("events").is_err());
    assert!(registry.open_unbounded_source("missing", None).is_err());
    assert!(registry.open_unbounded_source("events", None).is_err());
    let mut reader = registry
        .open_unbounded_source("events", Some(unbounded_signal(Some(vec![1]), None, false)))
        .unwrap();
    assert!(reader.poll(1).is_err());
}

#[test]
fn typed_source_and_portable_join_registry_reject_malformed_inputs() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    left.join_inner_named("join", &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();

    assert_eq!(
        registry
            .open_typed_source::<i32>("missing")
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
    assert!(registry.open_typed_source::<i32>("left").unwrap().is_none());
    assert!(registry.vec_source_batches::<i32>("left").is_none());
    assert!(registry.vec_source_batches::<(i32, i32)>("left").is_some());

    for tagged in [vec![], vec![2], vec![0, 1]] {
        assert_eq!(
            registry
                .execute_join_group(
                    "join",
                    &1_i32.encode().unwrap(),
                    &[tagged],
                    &Window::Global,
                    None
                )
                .err()
                .unwrap()
                .kind,
            ErrorKind::Incompatible
        );
    }
    assert_eq!(
        registry
            .execute_join_group("missing", &[], &[], &Window::Global, None)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Incompatible
    );
}

#[test]
fn coder_contract_and_direct_error_taxonomy() {
    #[derive(Clone)]
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "test/opaque";
        const CODER_ID: &'static str = "test/opaque-v1";
    }
    assert!(!Opaque::supports_coder());
    assert!(matches!(
        Opaque.encode().unwrap_err().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert!(matches!(
        Opaque::decode(&[]).err().unwrap().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let pipeline = Pipeline::default();
    let (_, registry) = {
        pipeline.from_vec("opaque", vec![Opaque]).unwrap();
        pipeline.snapshot().unwrap()
    };
    assert_eq!(registry.source_len_hint("opaque"), Some(1));
    assert_eq!(registry.source_config_digest("opaque"), None);
    assert!(!bool::decode(&[0]).unwrap());
    assert!(bool::decode(&[1]).unwrap());
    assert!(bool::supports_coder());
    assert_eq!(Error::io("node", "direct", "disk").kind, ErrorKind::Io);
    assert_eq!(
        Error::recovery("node", "bad state").kind,
        ErrorKind::Recovery
    );
    assert_eq!(Error::resource("node", "limit").kind, ErrorKind::Resource);
}

#[test]
fn composite_coders_are_typed_and_reject_truncated_payloads() {
    type Pair = (String, Vec<i32>);
    let value: Pair = ("key".into(), vec![1, 2, 3]);
    assert!(Pair::supports_coder());
    assert_eq!(Pair::decode(&value.encode().unwrap()).unwrap(), value);
    assert!(Pair::type_identity().contains("std/vector<std/i32>"));
    assert!(Pair::coder_identity().contains("ironbeam/i32-le"));
    let mut truncated = value.encode().unwrap();
    truncated.pop();
    assert_eq!(
        Pair::decode(&truncated).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    assert_eq!(
        Vec::<i32>::decode(&[]).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    let pipeline = Pipeline::default();
    pipeline.from_vec("pairs", vec![value]).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(graph.node("pairs").unwrap().type_id, Pair::type_identity());
}

#[test]
fn option_coder_has_distinct_none_and_some_values() {
    assert_eq!(Option::<i32>::decode(&[0]).unwrap(), None);
    assert_eq!(
        Option::<i32>::decode(&Some(7).encode().unwrap()).unwrap(),
        Some(7)
    );
    assert_eq!(
        Option::<i32>::decode(&[0, 1]).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    assert_eq!(
        Option::<i32>::decode(&[2]).unwrap_err().kind,
        ErrorKind::Incompatible
    );
}

#[test]
fn higher_arity_tuple_coders_round_trip_and_check_truncation() {
    type Triple = (i32, String, bool);
    type Ten = (i32, i32, i32, i32, i32, i32, i32, i32, i32, i32);
    let value: Triple = (7, "seven".into(), true);
    let bytes = value.encode().unwrap();
    assert_eq!(Triple::decode(&bytes).unwrap(), value);
    assert_eq!(
        Triple::decode(&bytes[..bytes.len() - 1]).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    let ten: Ten = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assert_eq!(Ten::decode(&ten.encode().unwrap()).unwrap(), ten);
}

#[test]
fn offset_windows_and_interval_coder_handle_signed_time() {
    let window = Window::FixedOffset {
        size_ns: 10,
        offset_ns: 3,
    };
    let interval = window.assign(-1).unwrap().unwrap();
    assert_eq!(
        interval,
        WindowInterval {
            start_ns: -7,
            end_ns: 3
        }
    );
    assert_eq!(
        WindowInterval::decode(&interval.encode().unwrap()).unwrap(),
        interval
    );
    assert_eq!(
        WindowInterval::decode(&[0]).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    let invalid_interval = WindowInterval {
        start_ns: 3,
        end_ns: 3,
    };
    assert_eq!(
        WindowInterval::decode(&invalid_interval.encode().unwrap())
            .unwrap_err()
            .kind,
        ErrorKind::Incompatible
    );
    assert_eq!(
        Window::FixedOffset {
            size_ns: 0,
            offset_ns: 3
        }
        .assign(0)
        .unwrap_err()
        .kind,
        ErrorKind::Graph
    );
    assert!(
        Window::FixedOffset {
            size_ns: 10,
            offset_ns: i64::MAX
        }
        .assign(i64::MIN)
        .is_err()
    );
}

#[test]
fn sliding_windows_assign_overlapping_intervals_across_zero() {
    let window = Window::Sliding {
        size_ns: 4,
        period_ns: 2,
        offset_ns: -1,
    };
    assert_eq!(
        window.assign_all(0).unwrap(),
        vec![
            Some(WindowInterval {
                start_ns: -3,
                end_ns: 1
            }),
            Some(WindowInterval {
                start_ns: -1,
                end_ns: 3
            }),
        ]
    );
    assert_eq!(window.assign(0).unwrap_err().kind, ErrorKind::Graph);
    assert_eq!(
        Window::Sliding {
            size_ns: 1,
            period_ns: 2,
            offset_ns: 0,
        }
        .assign_all(1)
        .unwrap(),
        Vec::<Option<WindowInterval>>::new()
    );
    assert!(
        Window::Sliding {
            size_ns: 2,
            period_ns: 1,
            offset_ns: 0,
        }
        .assign_all(i64::MAX)
        .is_err()
    );
    assert!(
        Window::Sliding {
            size_ns: 2_049,
            period_ns: 2,
            offset_ns: 0,
        }
        .assign_all(0)
        .is_err()
    );
}

#[test]
fn session_windows_validate_gap_and_time_bounds() {
    let session = Window::Session { gap_ns: 5 };
    assert_eq!(
        session.assign(-2).unwrap(),
        Some(WindowInterval {
            start_ns: -2,
            end_ns: 3
        })
    );
    assert_eq!(
        session.assign(i64::MAX - 5).unwrap().unwrap().end_ns,
        i64::MAX
    );
    assert_eq!(
        session.assign(i64::MAX - 4).unwrap_err().kind,
        ErrorKind::Graph
    );
    assert_eq!(
        Window::Session { gap_ns: 0 }.assign(0).unwrap_err().kind,
        ErrorKind::Graph
    );
    let pipeline = Pipeline::default();
    let bounded = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    assert!(bounded.group_by_session_named("bad-gap", 0).is_err());
    assert!(
        bounded
            .session_windows_named("bad-explicit-gap", 0)
            .is_err()
    );
    let explicit = bounded
        .session_windows_named("explicit-session", 5)
        .unwrap();
    explicit.group_by_key_named("explicit-group").unwrap();
    let grouped = bounded.group_by_session_named("session", 5).unwrap();
    grouped
        .join_inner_named("bounded-session-join", &grouped)
        .unwrap();
    cogroup_by_key_2_named("bounded-session-cogroup", &grouped, &grouped).unwrap();
    let fixed = bounded.fixed_windows("fixed-before-session", 10).unwrap();
    assert!(fixed.group_by_session_named("nested-session", 5).is_err());
    assert!(
        fixed
            .session_windows_named("nested-explicit-session", 5)
            .is_err()
    );
    let unbounded = pipeline
        .unbounded_source::<(i32, i32)>(
            "stream-for-session",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            0,
        )
        .unwrap();
    let unbounded_grouped = unbounded
        .group_by_session_named("unbounded-session", 5)
        .unwrap();
    unbounded_grouped
        .join_inner_named("unbounded-session-join", &unbounded_grouped)
        .unwrap();
    cogroup_by_key_2_named(
        "unbounded-session-cogroup",
        &unbounded_grouped,
        &unbounded_grouped,
    )
    .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    graph.validate().unwrap();
    let mut malformed = graph.clone();
    malformed.node_mut_for_test("session/session").time.window = Window::Session { gap_ns: 0 };
    assert_eq!(malformed.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut malformed = graph.clone();
    malformed.node_mut_for_test("session/session").time.window = Window::Global;
    assert_eq!(malformed.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut malformed = graph;
    malformed.node_mut_for_test("session/session").boundedness = Boundedness::Unbounded;
    assert_eq!(malformed.validate().unwrap_err().kind, ErrorKind::Graph);
}

#[test]
fn bounded_session_join_registry_merges_intervals_from_both_inputs() {
    fn session_events(rows: &[(i64, i32, i32)]) -> Vec<Envelope> {
        let window = Window::Session { gap_ns: 5 };
        rows.iter()
            .map(|&(timestamp_ns, key, value)| Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: Box::new((key, value)),
                timestamp_ns,
                assigned_window: window.assign(timestamp_ns).unwrap(),
                window: window.clone(),
            })
            .collect()
    }
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![(1_i32, 1_i32)])
        .unwrap()
        .session_windows_named("left-session", 5)
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![(1_i32, 10_i32)])
        .unwrap()
        .session_windows_named("right-session", 5)
        .unwrap();
    left.join_inner_named("inner", &right).unwrap();
    left.join_left_named("left-join", &right).unwrap();
    left.join_right_named("right-join", &right).unwrap();
    left.join_full_named("full", &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let input = || {
        vec![
            session_events(&[(0, 1, 1), (8, 1, 2), (30, 2, 3)]),
            session_events(&[(4, 1, 10), (20, 1, 11), (31, 2, 12)]),
        ]
    };
    let inner = registry.apply_aggregate("inner", input()).unwrap();
    assert_eq!(inner.len(), 3);
    assert_eq!(
        inner
            .iter()
            .filter(|event| event.assigned_window
                == Some(WindowInterval {
                    start_ns: 0,
                    end_ns: 13
                }))
            .count(),
        2
    );
    assert_eq!(
        registry
            .apply_aggregate("left-join", input())
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        registry
            .apply_aggregate("right-join", input())
            .unwrap()
            .len(),
        4
    );
    let full = registry.apply_aggregate("full", input()).unwrap();
    assert_eq!(full.len(), 4);
    assert!(full.into_iter().any(|event| {
        event.assigned_window
            == Some(WindowInterval {
                start_ns: 20,
                end_ns: 25,
            })
            && *event
                .value
                .downcast::<(i32, (Option<i32>, Option<i32>))>()
                .unwrap()
                == (1, (None, Some(11)))
    }));
    let mut malformed = input();
    malformed[0][0].assigned_window = None;
    assert!(registry.apply_aggregate("inner", malformed).is_err());
    let mut malformed = input();
    malformed[1][0].assigned_window = None;
    assert!(registry.apply_aggregate("inner", malformed).is_err());
    let mut malformed = input();
    malformed[0][0].value = Box::new("wrong type");
    assert!(registry.apply_aggregate("inner", malformed).is_err());
    let mut malformed = input();
    malformed[1][0].value = Box::new("wrong type");
    assert!(registry.apply_aggregate("inner", malformed).is_err());
}

#[test]
fn bounded_session_cogroup_registry_merges_and_rejects_bad_input_metadata() {
    fn event<T: Element>(key: i32, value: T, timestamp_ns: i64) -> Envelope {
        let window = Window::Session { gap_ns: 5 };
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((key, value)),
            timestamp_ns,
            assigned_window: window.assign(timestamp_ns).unwrap(),
            window,
        }
    }
    let pipeline = Pipeline::default();
    let first = pipeline
        .from_vec("first", vec![(1_i32, 1_i32)])
        .unwrap()
        .session_windows_named("first-session", 5)
        .unwrap();
    let second = pipeline
        .from_vec("second", vec![(1_i32, "bridge".to_owned())])
        .unwrap()
        .session_windows_named("second-session", 5)
        .unwrap();
    let third = pipeline
        .from_vec("third", vec![(1_i32, true)])
        .unwrap()
        .session_windows_named("third-session", 5)
        .unwrap();
    cogroup_by_key_3_named("session-cogroup", &first, &second, &third).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let inputs = || {
        vec![
            vec![event(1, 1_i32, 0), event(1, 2_i32, 8)],
            vec![event(1, "bridge".to_owned(), 4)],
            vec![event(1, true, 12)],
        ]
    };
    let output = registry
        .apply_aggregate("session-cogroup", inputs())
        .unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0].assigned_window,
        Some(WindowInterval {
            start_ns: 0,
            end_ns: 17
        })
    );
    let mut malformed = inputs();
    malformed[1][0].value = Box::new((1_i32, 99_i32));
    assert!(
        registry
            .apply_aggregate("session-cogroup", malformed)
            .is_err()
    );
    let mut malformed = inputs();
    malformed[2][0].assigned_window = None;
    assert!(
        registry
            .apply_aggregate("session-cogroup", malformed)
            .is_err()
    );
}

#[test]
fn filter_registry_and_duplicate_sink_failures_are_explicit() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    let filtered = input.filter_named("even", |x| x % 2 == 0).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut source = registry.open_source("source").unwrap();
    assert!(
        registry
            .apply("even", source.next().unwrap().unwrap())
            .unwrap()
            .is_none()
    );
    assert!(
        registry
            .apply("even", source.next().unwrap().unwrap())
            .unwrap()
            .is_some()
    );
    filtered.append_sink("sink", Replay::Idempotent).unwrap();
    assert_eq!(
        filtered
            .append_sink("sink", Replay::Idempotent)
            .unwrap_err()
            .kind,
        ErrorKind::Graph
    );
}

#[test]
fn flatten_declares_typed_fan_in_and_rejects_invalid_edges() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![1_i32]).unwrap();
    let right = pipeline.from_vec("right", vec![2_i32]).unwrap();
    let merged = left.flatten_named("merged", &[right]).unwrap();
    merged.output("out").unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let node = graph.node("merged").unwrap();
    assert_eq!(node.additional_inputs.len(), 1);
    assert_eq!(node.additional_inputs[0].key, "right");
    assert_eq!(
        graph
            .validate_for("other", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "flatten".into()
        }
    );
    let mut malformed = graph.clone();
    malformed.node_mut_for_test("merged").additional_inputs[0].key = "missing".into();
    assert_eq!(malformed.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut malformed = graph.clone();
    malformed.node_mut_for_test("merged").additional_inputs[0]
        .element
        .coder_id = "wrong".into();
    assert_eq!(malformed.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut malformed = graph.clone();
    malformed.node_mut_for_test("merged").additional_inputs[0].key = "out".into();
    assert_eq!(malformed.validate().unwrap_err().kind, ErrorKind::Graph);
}

fn small() -> (Graph, Registry) {
    let p = Pipeline::default();
    let values = p.from_vec("input", vec![1_i32, 2, 3, 4]).unwrap();
    let doubled = values.map_named("double/v1", |x| x * 2).unwrap();
    let even = doubled.filter_named("even/v1", |x| x % 2 == 0).unwrap();
    even.output("result").unwrap();
    p.snapshot().unwrap()
}

#[test]
fn canonical_identity_ignores_local_order_and_rejects_mismatch() {
    let (a, _) = small();
    let mut b = a.clone();
    b.nodes.reverse();
    for (index, node) in b.nodes.iter_mut().enumerate() {
        node.local_id = index + 100;
    }
    assert_eq!(a.canonical_json().unwrap(), b.canonical_json().unwrap());
    assert_eq!(a.fingerprint().unwrap(), b.fingerprint().unwrap());
    b.node_mut_for_test("double/v1").time.initial_timestamp_ns = 1;
    assert_ne!(a.fingerprint().unwrap(), b.fingerprint().unwrap());
    let identity = ExecutionIdentity {
        graph_fingerprint: a.fingerprint().unwrap(),
        binary_build_id: "build-1".into(),
        registry_manifest: "functions-1".into(),
        coder_manifest: "coders-1".into(),
        source_config_digest: "source-1".into(),
        sink_config_digest: "sink-1".into(),
    };
    assert_eq!(identity.verify(&identity), Ok(()));
    let mut worker = identity.clone();
    worker.binary_build_id = "build-2".into();
    assert_eq!(
        identity.verify(&worker).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    let mut worker = identity.clone();
    worker.sink_config_digest = "sink-2".into();
    assert_eq!(
        identity.verify(&worker).unwrap_err().kind,
        ErrorKind::Incompatible
    );
}

trait NodeMut {
    fn node_mut_for_test(&mut self, key: &str) -> &mut Node;
}
impl NodeMut for Graph {
    fn node_mut_for_test(&mut self, key: &str) -> &mut Node {
        self.nodes.iter_mut().find(|n| n.key == key).unwrap()
    }
}

#[test]
fn validates_versions_edges_cycles_and_coders() {
    let (a, _) = small();
    let mut b = a.clone();
    b.version.major += 1;
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Incompatible);
    let mut b = a.clone();
    b.version.minor += 1;
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Incompatible);
    let mut b = a.clone();
    b.checkpoint_version.minor += 1;
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Incompatible);
    let mut b = a.clone();
    b.node_mut_for_test("result").input = Some("missing".into());
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut b = a.clone();
    b.node_mut_for_test("input").input = Some("result".into());
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut b = a.clone();
    b.node_mut_for_test("even/v1").input = Some("result".into());
    assert!(b.validate().unwrap_err().message.contains("cycle"));
    let mut b = a.clone();
    b.node_mut_for_test("even/v1").coder_id = "wrong".into();
    assert!(b.validate().unwrap_err().message.contains("coder"));
    let mut b = a.clone();
    b.node_mut_for_test("result").coder_version.major += 1;
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Incompatible);
    let mut b = a;
    b.nodes.push(b.nodes[0].clone());
    assert_eq!(b.validate().unwrap_err().kind, ErrorKind::Graph);
}

#[test]
fn graph_rejects_invalid_window_trigger_lateness_and_output_time_contracts() {
    let invalid_times = [
        Time {
            window: Window::Sliding {
                size_ns: 0,
                period_ns: 1_000_000,
                offset_ns: 0,
            },
            ..Time::default()
        },
        Time {
            window: Window::Sliding {
                size_ns: 1_000_000,
                period_ns: 0,
                offset_ns: 0,
            },
            ..Time::default()
        },
        Time {
            window: Window::Session { gap_ns: 0 },
            ..Time::default()
        },
        Time {
            trigger: WindowTrigger::EarlyElementCount { count: 0 },
            ..Time::default()
        },
        Time {
            trigger: WindowTrigger::EarlyElementCount {
                count: i32::MAX as u64 + 1,
            },
            ..Time::default()
        },
        Time {
            window: Window::Session { gap_ns: 1_000_000 },
            trigger: WindowTrigger::EarlyElementCount { count: 1 },
            ..Time::default()
        },
        Time {
            trigger: WindowTrigger::LateElementCount { count: 1 },
            ..Time::default()
        },
        Time {
            window: Window::Fixed { size_ns: 1_000_000 },
            allowed_lateness_ns: 1_000_000,
            trigger: WindowTrigger::LateElementCount { count: 0 },
            ..Time::default()
        },
        Time {
            window: Window::Fixed { size_ns: 1_000_000 },
            allowed_lateness_ns: 1_000_000,
            trigger: WindowTrigger::LateElementCount {
                count: i32::MAX as u64 + 1,
            },
            ..Time::default()
        },
        Time {
            window: Window::Fixed { size_ns: 1 },
            allowed_lateness_ns: 1_000_000,
            trigger: WindowTrigger::LateElementCount { count: 1 },
            ..Time::default()
        },
        Time {
            output_time: WindowOutputTime::EndOfWindow,
            ..Time::default()
        },
        Time {
            allowed_lateness_ns: -1,
            ..Time::default()
        },
        Time {
            allowed_lateness_ns: 1,
            ..Time::default()
        },
        Time {
            allowed_lateness_ns: 1_000_000,
            ..Time::default()
        },
    ];
    let (valid, _) = small();
    for time in invalid_times {
        let mut graph = valid.clone();
        graph.node_mut_for_test("input").time = time;
        assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    }
}

#[test]
fn unbounded_metadata_and_capability_are_explicit() {
    let p = Pipeline::default();
    let events = p
        .unbounded_source::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            -1,
        )
        .unwrap();
    assert_eq!(
        events.global_combine_marker("illegal").err().unwrap().kind,
        ErrorKind::Graph
    );
    let mapped = events.map_named("normalize/v1", |x| x + 1).unwrap();
    let windowed = mapped
        .fixed_windows("five-minutes", 300_000_000_000)
        .unwrap();
    windowed.append_sink("out", Replay::Idempotent).unwrap();
    let (graph, _) = p.snapshot().unwrap();
    let source = graph.node("events").unwrap();
    assert_eq!(source.time.initial_timestamp_ns, -1);
    assert_eq!(source.time.window, Window::Global);
    assert_eq!(graph.node("normalize/v1").unwrap().time, source.time);
    assert_eq!(
        graph.node("five-minutes").unwrap().time.window,
        Window::Fixed {
            size_ns: 300_000_000_000
        }
    );
    assert_eq!(
        graph.node("out").unwrap().boundedness,
        Boundedness::Unbounded
    );
    match &source.operation {
        Operation::Source { progress } => {
            assert!(progress.stable_split_id && progress.reports_event_time);
            assert_eq!(progress.replay, Replay::AtLeastOnce);
        }
        _ => panic!("expected source"),
    }
    let error = graph
        .validate_for("batch", &Capabilities::default())
        .unwrap_err();
    assert_eq!(error.node.as_deref(), Some("events"));
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "unbounded-source".into()
        }
    );
    assert_eq!(error.retry, RetryClass::Permanent);
}

#[test]
fn custom_capability_and_builder_rejections() {
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32]).unwrap();
    assert!(p.from_vec("input", vec![2_i32]).is_err());
    assert!(input.fixed_windows("bad-window", 0).is_err());
    assert!(
        input
            .custom_named("bad", "unnamespaced", GRAPH_VERSION)
            .is_err()
    );
    input
        .custom_named("custom", "vendor/op", GRAPH_VERSION)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    let error = graph
        .validate_for("batch", &Capabilities::default())
        .unwrap_err();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "vendor/op".into()
        }
    );
}

#[test]
fn event_time_is_preserved_and_pre_epoch_windows_are_checked() {
    assert_eq!(
        Window::Fixed { size_ns: 100 }.assign(-1).unwrap(),
        Some(WindowInterval {
            start_ns: -100,
            end_ns: 0
        })
    );
    assert!(Window::Fixed { size_ns: 100 }.assign(i64::MAX).is_err());
    let p = Pipeline::default();
    let input = p.from_vec("input", vec![1_i32]).unwrap();
    input
        .map_named("map", |x| x + 1)
        .unwrap()
        .fixed_windows("window", 100)
        .unwrap()
        .output("out")
        .unwrap();
    let (_, registry) = p.snapshot().unwrap();
    let source = registry
        .open_source("input")
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let mapped = registry.apply("map", source).unwrap().unwrap();
    assert_eq!(mapped.timestamp_ns, 0);
    assert_eq!(mapped.window, Window::Global);
    let assigned = registry.apply("window", mapped).unwrap().unwrap();
    assert_eq!(
        assigned.assigned_window,
        Some(WindowInterval {
            start_ns: 0,
            end_ns: 100
        })
    );
    assert_eq!(*assigned.value.downcast::<i32>().unwrap(), 2);
}

#[test]
fn malformed_descriptors_fail_before_lowering() {
    let (baseline, _) = small();
    let mut graph = baseline.clone();
    graph.checkpoint_version.major += 1;
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Incompatible);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("input").key.clear();
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("input").type_id.clear();
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("input").semantic_version.major = 0;
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("input").input = Some("result".into());
    assert_eq!(graph.validate().unwrap_err().kind, ErrorKind::Graph);
    let mut graph = baseline.clone();
    graph.node_mut_for_test("result").boundedness = Boundedness::Unbounded;
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("boundedness")
    );
    let mut graph = baseline.clone();
    graph.node_mut_for_test("result").time.initial_timestamp_ns = 42;
    assert!(graph.validate().unwrap_err().message.contains("event time"));
    let mut graph = baseline.clone();
    graph.node_mut_for_test("even/v1").operation = Operation::Custom {
        semantic_id: "bad".into(),
        version: Version { major: 0, minor: 0 },
    };
    assert!(graph.validate().unwrap_err().message.contains("namespaced"));
    let mut graph = baseline;
    graph.node_mut_for_test("even/v1").time.window = Window::Fixed { size_ns: -1 };
    assert!(graph.validate().unwrap_err().message.contains("positive"));

    let p = Pipeline::default();
    p.from_vec("input", vec![1_i32])
        .unwrap()
        .fixed_windows("window", 100)
        .unwrap()
        .output("result")
        .unwrap();
    let (mut graph, _) = p.snapshot().unwrap();
    graph.node_mut_for_test("window").time.window = Window::Global;
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("fixed-window")
    );

    let p = Pipeline::default();
    p.from_vec("input", vec![1_i32])
        .unwrap()
        .custom_named("custom", "vendor/op", GRAPH_VERSION)
        .unwrap()
        .output("result")
        .unwrap();
    let (mut graph, _) = p.snapshot().unwrap();
    graph.node_mut_for_test("custom").semantic_id = "vendor/other".into();
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("identity differs")
    );
}

#[test]
fn capabilities_validate_each_requested_operation() {
    let p = Pipeline::default();
    p.from_vec("input", vec![1_i32])
        .unwrap()
        .fixed_windows("window", 100)
        .unwrap()
        .append_sink("sink", Replay::Idempotent)
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    let mut caps = Capabilities::default();
    assert_eq!(
        graph
            .validate_for("test", &caps)
            .unwrap_err()
            .node
            .as_deref(),
        Some("window")
    );
    caps.fixed_window = true;
    assert_eq!(
        graph
            .validate_for("test", &caps)
            .unwrap_err()
            .node
            .as_deref(),
        Some("sink")
    );
    caps.append_sink = true;
    graph.validate_for("test", &caps).unwrap();

    let p = Pipeline::default();
    p.from_vec("input", vec![1_i32])
        .unwrap()
        .custom_named("custom", "vendor/known", GRAPH_VERSION)
        .unwrap()
        .output("result")
        .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    caps.custom_operations.insert("vendor/known".into());
    graph.validate_for("test", &caps).unwrap();
}

#[test]
fn map_edge_declares_its_expected_input_type_and_coder() {
    let pipeline = Pipeline::default();
    let numbers = pipeline.from_vec("numbers", vec![1_i32]).unwrap();
    pipeline
        .from_vec("words", vec![String::from("one")])
        .unwrap();
    numbers
        .map_named("map", |value| u32::try_from(value).unwrap())
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let expected = graph.node("map").unwrap().input_element.as_ref().unwrap();
    assert_eq!(expected.type_id, "std/i32");
    assert_eq!(expected.coder_id, "ironbeam/i32-le");
    assert_eq!(graph.node("map").unwrap().type_id, "std/u32");
    assert_eq!(graph.version.major, 3);
    assert_eq!(
        graph.node("map").unwrap().semantic_version,
        BUILTIN_OPERATION_VERSION
    );

    let mut old_descriptor = graph.clone();
    old_descriptor.version.major = 1;
    assert_eq!(
        old_descriptor.validate().unwrap_err().kind,
        ErrorKind::Incompatible
    );

    let mut malformed = graph.clone();
    malformed.node_mut_for_test("map").input = Some("words".into());
    assert!(
        malformed
            .validate()
            .unwrap_err()
            .message
            .contains("input edge")
    );
    let mut malformed = graph.clone();
    malformed
        .node_mut_for_test("map")
        .input_element
        .as_mut()
        .unwrap()
        .coder_id = "wrong".into();
    assert!(
        malformed
            .validate()
            .unwrap_err()
            .message
            .contains("input edge")
    );
    let mut malformed = graph;
    malformed.node_mut_for_test("map").input_element = None;
    assert!(
        malformed
            .validate()
            .unwrap_err()
            .message
            .contains("source/input")
    );
}

#[test]
fn registry_reports_missing_operations_and_wrong_runtime_types() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![1_i32]).unwrap();
    input
        .map_named("map", |value| value + 1)
        .unwrap()
        .filter_named("filter", |value| *value > 0)
        .unwrap()
        .output("out")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert_eq!(
        registry.open_source("missing").err().unwrap().kind,
        ErrorKind::Execution
    );
    let make_wrong = || Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new(String::from("wrong")),
        timestamp_ns: -1,
        window: Window::Global,
        assigned_window: None,
    };
    assert_eq!(
        registry.apply("missing", make_wrong()).err().unwrap().kind,
        ErrorKind::Execution
    );
    assert_eq!(
        registry.apply("map", make_wrong()).err().unwrap().kind,
        ErrorKind::Execution
    );
    assert_eq!(
        registry.apply("filter", make_wrong()).err().unwrap().kind,
        ErrorKind::Execution
    );
    let error = registry.open_source("missing").err().unwrap();
    assert!(error.to_string().contains("missing"));
}

#[test]
fn time_and_progress_controls_are_separate_from_values() {
    assert_eq!(Time::default().window, Window::Global);
    assert_eq!(Window::Global.assign(-1).unwrap(), None);
    assert!(Window::Fixed { size_ns: 0 }.assign(0).is_err());
    let progress = ProgressSignal {
        source_key: "source".into(),
        split_id: "split-a".into(),
        checkpoint_position: Some(vec![1, 2]),
        event_time_ns: Some(-1),
        completed: false,
    };
    let serialized = serde_json::to_string(&progress).unwrap();
    assert_eq!(
        serde_json::from_str::<ProgressSignal>(&serialized).unwrap(),
        progress
    );
}

#[test]
fn bounded_aggregation_marker_and_update_sink_capability() {
    let pipeline = Pipeline::default();
    pipeline
        .from_vec("input", vec![1_i32])
        .unwrap()
        .global_combine_marker("combine")
        .unwrap()
        .append_sink("sink", Replay::None)
        .unwrap();
    let (mut graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("combine").unwrap().semantic_id,
        "ironbeam/global-combine"
    );
    graph.node_mut_for_test("sink").operation = Operation::Sink {
        spec: SinkSpec {
            mode: SinkMode::Update,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
    };
    let caps = Capabilities {
        global_combine: true,
        ..Capabilities::default()
    };
    let error = graph.validate_for("fake", &caps).unwrap_err();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "update-sink".into()
        }
    );
    let caps = Capabilities {
        update_sink: true,
        global_combine: true,
        ..Capabilities::default()
    };
    graph.validate_for("fake", &caps).unwrap();
}

#[test]
fn malformed_aggregation_and_overflow_do_not_reach_a_runner() {
    let pipeline = Pipeline::default();
    let events = pipeline
        .unbounded_source::<i32>(
            "events",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            -1,
        )
        .unwrap();
    let windowed = events.fixed_windows("window", 100).unwrap();
    windowed.global_combine_marker("combine").unwrap();
    let (mut graph, _) = pipeline.snapshot().unwrap();
    graph.node_mut_for_test("combine").time.window = Window::Global;
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("event time/window")
    );

    assert!(
        Window::Fixed { size_ns: i64::MAX }
            .assign(i64::MIN)
            .is_err()
    );
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    source
        .fixed_windows("window", 100)
        .unwrap()
        .output("out")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut event = registry
        .open_source("source")
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    event.timestamp_ns = i64::MAX;
    assert_eq!(
        registry.apply("window", event).err().unwrap().kind,
        ErrorKind::Graph
    );
}

#[test]
fn builder_exposes_keys_and_rejects_unsupported_coder_version() {
    struct FutureCoder;
    impl Element for FutureCoder {
        const TYPE_ID: &'static str = "test/future";
        const CODER_ID: &'static str = "test/future-v2";
        const CODER_VERSION: Version = Version { major: 2, minor: 0 };
    }
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source::<FutureCoder>(
            "future",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::None,
                reports_event_time: false,
            },
            0,
        )
        .unwrap();
    assert_eq!(source.key(), "future");
    let output = source.output("out").unwrap();
    assert_eq!(output.key(), "out");
    assert_eq!(
        output.snapshot().err().unwrap().kind,
        ErrorKind::Incompatible
    );
}

#[test]
fn branching_snapshots_are_canonical_across_build_order() {
    fn build(reverse: bool) -> Graph {
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
        if reverse {
            source
                .filter_named("right", |value| *value > 1)
                .unwrap()
                .output("right-out")
                .unwrap();
            source
                .map_named("left", |value| value * 2)
                .unwrap()
                .output("left-out")
                .unwrap();
        } else {
            source
                .map_named("left", |value| value * 2)
                .unwrap()
                .output("left-out")
                .unwrap();
            source
                .filter_named("right", |value| *value > 1)
                .unwrap()
                .output("right-out")
                .unwrap();
        }
        pipeline.snapshot().unwrap().0
    }
    let a = build(false);
    let b = build(true);
    assert_eq!(a.fingerprint().unwrap(), b.fingerprint().unwrap());
    assert_eq!(a.nodes.len(), 5);
    assert_ne!(
        a.node("left").unwrap().local_id,
        b.node("left").unwrap().local_id
    );
}

#[test]
fn built_in_coders_round_trip_and_source_digest_binds_values() {
    assert_eq!(i32::decode(&(-7_i32).encode().unwrap()).unwrap(), -7);
    assert_eq!(i64::decode(&(-7_i64).encode().unwrap()).unwrap(), -7);
    assert_eq!(u32::decode(&7_u32.encode().unwrap()).unwrap(), 7);
    assert_eq!(u64::decode(&7_u64.encode().unwrap()).unwrap(), 7);
    assert!(!bool::decode(&false.encode().unwrap()).unwrap());
    assert!(bool::decode(&true.encode().unwrap()).unwrap());
    assert_eq!(
        String::decode(&"pre-epoch".to_string().encode().unwrap()).unwrap(),
        "pre-epoch"
    );
    assert_eq!(i32::decode(&[1]).unwrap_err().kind, ErrorKind::Incompatible);
    assert_eq!(
        bool::decode(&[2]).unwrap_err().kind,
        ErrorKind::Incompatible
    );
    assert_eq!(
        String::decode(&[255]).unwrap_err().kind,
        ErrorKind::Incompatible
    );

    let first = Pipeline::default();
    first.from_vec("source", vec![1_i32, 2]).unwrap();
    let (_, first_registry) = first.snapshot().unwrap();
    let second = Pipeline::default();
    second.from_vec("source", vec![1_i32, 3]).unwrap();
    let (_, second_registry) = second.snapshot().unwrap();
    assert_eq!(first_registry.source_len_hint("source"), Some(2));
    assert_ne!(
        first_registry.source_config_digest("source"),
        second_registry.source_config_digest("source")
    );
}

#[test]
fn vector_source_digest_matches_the_existing_coder_byte_contract() {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    fn check<T: Element + Clone + Sync>(values: Vec<T>) {
        let mut hash = Sha256::new();
        hash.update(T::coder_identity().as_bytes());
        hash.update(T::CODER_VERSION.major.to_le_bytes());
        hash.update(T::CODER_VERSION.minor.to_le_bytes());
        for value in &values {
            let bytes = value.encode().unwrap();
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
        let mut expected = String::from("sha256-v1:");
        for byte in hash.finalize() {
            write!(expected, "{byte:02x}").unwrap();
        }
        let pipeline = Pipeline::default();
        pipeline.from_vec("source", values).unwrap();
        let (_, registry) = pipeline.snapshot().unwrap();
        assert_eq!(
            registry.source_config_digest("source"),
            Some(expected.as_str())
        );
    }
    #[derive(Clone)]
    struct Custom(u32);
    impl Element for Custom {
        const TYPE_ID: &'static str = "test/custom-digest";
        const CODER_ID: &'static str = "test/custom-digest-v1";
        fn supports_coder() -> bool {
            true
        }
        fn encode(&self) -> Result<Vec<u8>, Error> {
            Ok(self.0.to_be_bytes().to_vec())
        }
    }
    check(vec![1_u32, 2, 3]);
    check(vec![false, true]);
    check(vec!["é".to_string(), "beam".to_string()]);
    check(vec![(1_u32, 2_u32), (3, 4)]);
    check(vec![Custom(7), Custom(8)]);

    let mut expected = Vec::new();
    expected.extend_from_slice(&4_u64.to_le_bytes());
    expected.extend_from_slice(&7_u32.to_le_bytes());
    expected.extend_from_slice(&4_u64.to_le_bytes());
    expected.extend_from_slice(&9_u32.to_le_bytes());
    let mut reused = vec![255];
    (7_u32, 9_u32).encode_reusing(&mut reused).unwrap();
    assert_eq!(reused, expected);
    let mut appended = vec![255];
    (7_u32, 9_u32).encode_into(&mut appended).unwrap();
    assert_eq!(&appended[1..], expected);
}

#[test]
fn custom_vector_coder_error_is_reported_during_source_registration() {
    #[derive(Clone)]
    struct Failing;
    impl Element for Failing {
        const TYPE_ID: &'static str = "test/failing";
        const CODER_ID: &'static str = "test/failing-v1";
        fn supports_coder() -> bool {
            true
        }
        fn encode(&self) -> Result<Vec<u8>, Error> {
            Err(Error::incompatible("cannot encode source"))
        }
    }

    let pipeline = Pipeline::default();
    let error = pipeline.from_vec("source", vec![Failing]).err().unwrap();
    assert_eq!(error.kind, ErrorKind::Incompatible);
    assert!(pipeline.snapshot().unwrap().0.node("source").is_none());
}

#[test]
#[allow(clippy::too_many_lines)]
fn bounded_source_and_deferred_sink_registry_contract() {
    use std::sync::{Arc, Mutex};
    struct Capture(Arc<Mutex<Vec<i32>>>);
    impl TypedSink<i32> for Capture {
        fn write(&mut self, value: i32) -> Result<(), Error> {
            self.0.lock().unwrap().push(value);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.0.lock().unwrap().clear();
            Ok(())
        }
    }
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: false,
    };
    let pipeline = Pipeline::default();
    assert!(
        pipeline
            .bounded_source::<i32>("bad", progress.clone(), String::new(), None, || Ok(
                Box::new(std::iter::empty())
            ))
            .is_err()
    );
    let source = pipeline
        .bounded_source("input", progress, "digest".into(), Some(1), || {
            Ok(Box::new(
                vec![Ok(7_i32), Err(Error::io("input", "test", "read failed"))].into_iter(),
            ))
        })
        .unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let copy = captured.clone();
    let sink = source
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            "capture-digest".into(),
            move || Ok(Box::new(Capture(copy.clone()))),
        )
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert_eq!(sink.key(), "sink");
    assert_eq!(registry.source_config_digest("input"), Some("digest"));
    assert_eq!(registry.sink_config_digest("sink"), Some("capture-digest"));
    assert_eq!(registry.source_len_hint("input"), Some(1));
    let mut source = registry.open_source("input").unwrap();
    assert_eq!(
        *source
            .next()
            .unwrap()
            .unwrap()
            .value
            .downcast::<i32>()
            .unwrap(),
        7
    );
    assert_eq!(source.next().unwrap().err().unwrap().kind, ErrorKind::Io);
    assert_eq!(
        registry.open_source("missing").err().unwrap().kind,
        ErrorKind::Execution
    );
    let mut session = registry.open_sink("sink").unwrap();
    assert_eq!(
        session
            .write(Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: Box::new("bad".to_string()),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None
            })
            .unwrap_err()
            .kind,
        ErrorKind::Execution
    );
    session
        .write(Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(7_i32),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        })
        .unwrap();
    session.commit().unwrap();
    assert_eq!(*captured.lock().unwrap(), [7]);
    session.abort().unwrap();
    assert!(captured.lock().unwrap().is_empty());
    assert!(registry.open_sink("missing").is_err());
    assert_eq!(
        graph
            .validate_for("fake", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "final-result-sink".into()
        }
    );
    let caps = Capabilities {
        final_result_sink: true,
        ..Capabilities::default()
    };
    graph.validate_for("fake", &caps).unwrap();
}

#[test]
fn sink_session_exposes_bundle_finalization_through_typed_adapter() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Finalizable(Arc<AtomicUsize>);
    impl TypedSink<i32> for Finalizable {
        fn begin_bundle(&mut self, instruction_id: &str) -> Result<(), Error> {
            if instruction_id != "bundle-7" {
                return Err(Error::incompatible("wrong bundle ID"));
            }
            self.0.fetch_add(10, Ordering::SeqCst);
            Ok(())
        }
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn requires_bundle_finalization(&self) -> bool {
            true
        }
        fn finalize_bundle(&mut self) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let completed = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![1_i32]).unwrap();
    let action = Arc::clone(&completed);
    input
        .sink_named_with_config_and_finalization(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            "test-finalizable".into(),
            move || Ok(Box::new(Finalizable(Arc::clone(&action)))),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.sink_requires_bundle_finalization("sink"));
    let mut session = registry.open_sink("sink").unwrap();
    assert!(session.requires_bundle_finalization());
    session.begin_bundle("bundle-7").unwrap();
    session.commit().unwrap();
    session.finalize_bundle().unwrap();
    assert_eq!(completed.load(Ordering::SeqCst), 11);
}

#[test]
fn unregistered_sink_finalization_fails_closed() {
    struct Ordinary;
    impl TypedSink<i32> for Ordinary {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    struct Untyped;
    impl SinkSession for Untyped {
        fn write(&mut self, _: Envelope) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let mut untyped = Untyped;
    assert!(!untyped.requires_bundle_finalization());
    assert!(untyped.finalize_bundle().is_err());
    assert!(untyped.recover_bundle_finalization("bundle-7").is_err());
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![1_i32]).unwrap();
    let spec = SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::AtLeastOnce,
        commit: true,
    };
    assert!(
        input
            .sink_named_with_config("empty", spec.clone(), String::new(), || Ok(Box::new(
                Ordinary
            )))
            .is_err()
    );
    assert!(
        input
            .sink_named_with_config_and_finalization("bad", spec.clone(), String::new(), || Ok(
                Box::new(Ordinary)
            ))
            .is_err()
    );
    let mut uncommitted = spec.clone();
    uncommitted.commit = false;
    assert!(
        input
            .sink_named_with_config_and_finalization(
                "no-commit",
                uncommitted,
                "digest".into(),
                || Ok(Box::new(Ordinary))
            )
            .is_err()
    );
    input
        .sink_named_with_config("ordinary", spec, "digest".into(), || Ok(Box::new(Ordinary)))
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(!registry.sink_requires_bundle_finalization("ordinary"));
    let mut session = registry.open_sink("ordinary").unwrap();
    assert!(!session.requires_bundle_finalization());
    assert!(session.finalize_bundle().is_err());
    assert!(session.recover_bundle_finalization("bundle-7").is_err());
}

#[test]
fn transform_registry_preserves_time_and_runs_all_aggregates() {
    fn events<T: Element>(values: impl IntoIterator<Item = T>) -> Vec<Envelope> {
        values
            .into_iter()
            .map(|value| Envelope {
                pane: ironbeam_core::PaneInfo::NO_FIRING,
                value: Box::new(value),
                timestamp_ns: -1,
                window: Window::Global,
                assigned_window: None,
            })
            .collect()
    }
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec("left", vec![(1_i32, 2_i32), (1, 3)])
        .unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 4_i32)]).unwrap();
    left.group_by_key_named("group").unwrap();
    left.join_inner_named("join", &right).unwrap();
    let values = left.map_named("values", |(_, value)| value).unwrap();
    values
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap();
    values
        .map_with_side_named("side", &right, |value, side| {
            value + i32::try_from(side.len()).unwrap()
        })
        .unwrap();
    values
        .flat_map_named("expand", |value| vec![value, -value])
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let outputs = registry
        .apply_many("expand", events([2_i32]).pop().unwrap())
        .unwrap();
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].timestamp_ns, -1);
    let group = registry
        .apply_aggregate("group", vec![events([(1_i32, 2_i32), (1, 3)])])
        .unwrap();
    assert_eq!(
        *group
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, Vec<i32>)>()
            .unwrap(),
        (1, vec![2, 3])
    );
    let join = registry
        .apply_aggregate(
            "join",
            vec![events([(1_i32, 2_i32)]), events([(1_i32, 4_i32)])],
        )
        .unwrap();
    assert_eq!(
        *join
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<(i32, (i32, i32))>()
            .unwrap(),
        (1, (2, 4))
    );
    let sum = registry
        .apply_aggregate("sum", vec![events([2_i32, 3])])
        .unwrap();
    assert_eq!(
        *sum.into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<i32>()
            .unwrap(),
        5
    );
    let side = registry
        .apply_aggregate("side", vec![events([2_i32]), events([(1_i32, 4_i32)])])
        .unwrap();
    assert_eq!(
        *side
            .into_iter()
            .next()
            .unwrap()
            .value
            .downcast::<i32>()
            .unwrap(),
        3
    );
    assert!(!registry.has_aggregate("missing"));
    assert!(registry.apply_aggregate("missing", vec![]).is_err());
}

#[test]
fn grouped_tuple_coder_exposes_beam_key_and_value_halves() {
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec("pairs", vec![("a".to_owned(), 2_i32)])
        .unwrap();
    input.group_by_key_named("group").unwrap();
    input.latest_per_key_named("latest").unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_group_coder("group"));
    assert!(!registry.has_group_coder("latest"));
    let first = ("a".to_owned(), 2_i32).encode().unwrap();
    let second = ("a".to_owned(), 3_i32).encode().unwrap();
    let (first_key, first_value) = registry.split_group_input("group", &first).unwrap();
    let (second_key, second_value) = registry.split_group_input("group", &second).unwrap();
    assert_eq!(first_key, second_key);
    assert_eq!(
        registry
            .assemble_group_input("group", &first_key, &first_value)
            .unwrap(),
        first
    );
    assert!(
        registry
            .assemble_group_input("group", b"\xff", &first_value)
            .is_err()
    );
    assert!(
        registry
            .assemble_group_input("group", &first_key, b"\xff")
            .is_err()
    );
    assert!(
        registry
            .assemble_group_input("latest", &first_key, &first_value)
            .is_err()
    );
    let grouped = registry
        .assemble_group_output("group", &first_key, &[first_value, second_value])
        .unwrap();
    assert_eq!(
        <(String, Vec<i32>)>::decode(&grouped).unwrap(),
        ("a".to_owned(), vec![2, 3])
    );
    assert!(registry.split_group_input("group", b"bad").is_err());
    assert!(
        registry
            .assemble_group_output("group", b"\xff", &[])
            .is_err()
    );
    assert!(
        registry
            .assemble_group_output("group", &first_key, &[vec![1]])
            .is_err()
    );
    assert!(registry.split_group_input("latest", &first).is_err());
}

#[test]
fn latest_per_key_direct_fold_keeps_one_value_and_restores_checkpoint() {
    fn event(key: &str, timestamp: i64, value: i32) -> Envelope {
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new((key.to_owned(), (timestamp, value))),
            timestamp_ns: timestamp,
            window: Window::Global,
            assigned_window: None,
        }
    }

    let pipeline = Pipeline::default();
    let source = pipeline
        .from_vec("source", Vec::<(String, i32)>::new())
        .unwrap();
    source
        .map_named("ordinary-keyed", |(key, value)| (key, (1_i64, value)))
        .unwrap()
        .group_by_key_named("ordinary-group")
        .unwrap();
    source.latest_per_key_named("latest").unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let mut ordinary = registry.open_fold("ordinary-group").unwrap();
    assert!(!ordinary.accepts_timed_vec_batch(std::any::TypeId::of::<Vec<(String, i32)>>()));
    assert!(
        ordinary
            .add_timed_vec_batch(Box::new(vec![("a".to_owned(), 1_i32)]), &0_i32, "time")
            .is_err()
    );
    ordinary.add(event("a", 1, 7)).unwrap();
    let ordinary_checkpoint = ordinary.checkpoint_state().unwrap().unwrap();
    let mut fold = registry.open_fold("latest/group").unwrap();
    assert!(
        fold.add_vec_batch(Box::new(vec![("a".to_owned(), (1_i64, 7_i32))]))
            .is_err()
    );
    fold.add(event("a", 1, 7)).unwrap();
    fold.add(event("a", 2, 8)).unwrap();
    fold.add(event("b", 1, 9)).unwrap();
    let checkpoint = fold.checkpoint_state().unwrap().unwrap();
    let mut restored = registry.open_fold("latest/group").unwrap();
    assert!(restored.restore_checkpoint_state(b"invalid").is_err());
    assert!(
        restored
            .restore_checkpoint_state(&ordinary_checkpoint)
            .is_err()
    );
    restored.restore_checkpoint_state(&checkpoint).unwrap();
    restored.add(event("a", 3, 10)).unwrap();
    restored.add(event("a", 3, 11)).unwrap();
    let rows = restored
        .finish()
        .unwrap()
        .into_iter()
        .map(|event| *event.value.downcast::<(String, Vec<(i64, i32)>)>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![("a".into(), vec![(3, 10)]), ("b".into(), vec![(1, 9)])]
    );

    let mut streamed = registry.open_fold("latest/group").unwrap();
    streamed.add(event("a", 4, 12)).unwrap();
    let mut emitted = Vec::new();
    streamed
        .finish_into(&mut |event| {
            emitted.push(*event.value.downcast::<(String, Vec<(i64, i32)>)>().unwrap());
            Ok(())
        })
        .unwrap();
    assert_eq!(emitted, vec![("a".into(), vec![(4, 12)])]);
}

#[test]
fn latest_per_key_registry_rejects_bad_groups_and_restores_selected_event_time() {
    fn envelope<T: Element>(value: T, timestamp_ns: i64) -> Envelope {
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(value),
            timestamp_ns,
            window: Window::Global,
            assigned_window: None,
        }
    }

    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![("a".to_owned(), 1_i32)])
        .unwrap()
        .assign_timestamps_named("time", |(_, value)| i64::from(*value))
        .unwrap()
        .latest_per_key_named("latest")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.typed_timestamp_transform("time").is_some());
    assert!(registry.typed_timestamp_transform("source").is_none());
    assert!(registry.is_keyed_timestamp_reification("latest/timestamp"));
    assert!(!registry.is_keyed_timestamp_reification("time"));

    let captured = registry
        .apply("latest/timestamp", envelope(("a".to_owned(), 7_i32), 70))
        .unwrap()
        .unwrap();
    assert_eq!(captured.timestamp_ns, 70);
    assert_eq!(
        *captured.value.downcast::<(String, (i64, i32))>().unwrap(),
        ("a".to_owned(), (70, 7))
    );
    assert!(
        registry
            .apply("latest/timestamp", envelope(9_i32, 70))
            .is_err()
    );
    assert!(registry.apply_aggregate("latest/group", vec![]).is_err());

    let grouped = registry
        .apply_aggregate(
            "latest/group",
            vec![vec![
                envelope(("a".to_owned(), (10_i64, 1_i32)), 10),
                envelope(("a".to_owned(), (30_i64, 3_i32)), 30),
                envelope(("a".to_owned(), (20_i64, 2_i32)), 20),
            ]],
        )
        .unwrap();
    assert_eq!(grouped.len(), 1);
    let selected = registry
        .apply("latest", grouped.into_iter().next().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(selected.timestamp_ns, 30);
    assert_eq!(
        *selected.value.downcast::<(String, i32)>().unwrap(),
        ("a".into(), 3)
    );
    assert!(registry.apply("latest", envelope(7_i32, 0)).is_err());
    assert!(
        registry
            .apply(
                "latest",
                envelope(("a".to_owned(), Vec::<(i64, i32)>::new()), 0)
            )
            .is_err()
    );
    let selected = registry
        .apply(
            "latest",
            envelope(("a".to_owned(), vec![(10_i64, 1_i32), (30, 3), (20, 2)]), 0),
        )
        .unwrap()
        .unwrap();
    assert_eq!(selected.timestamp_ns, 30);
    assert_eq!(
        *selected.value.downcast::<(String, i32)>().unwrap(),
        ("a".into(), 3)
    );
}

#[test]
fn latest_per_key_fold_rejects_mismatched_typed_batches_and_checkpoint_state() {
    fn event(value: impl Element) -> Envelope {
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(value),
            timestamp_ns: 1,
            window: Window::Global,
            assigned_window: None,
        }
    }

    let pipeline = Pipeline::default();
    pipeline
        .from_vec("source", vec![("a".to_owned(), 1_i32)])
        .unwrap()
        .assign_timestamps_named("time", |(_, value)| i64::from(*value))
        .unwrap()
        .latest_per_key_named("latest")
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let timestamp = registry.typed_timestamp_transform("time").unwrap();
    assert!(registry.apply("time", event(7_i32)).is_err());
    let mut fold = registry.open_fold("latest/group").unwrap();
    assert!(fold.accepts_timed_vec_batch(std::any::TypeId::of::<Vec<(String, i32)>>()));
    assert!(!fold.accepts_timed_vec_batch(std::any::TypeId::of::<Vec<i32>>()));
    assert!(fold.add(event(7_i32)).is_err());
    assert!(
        fold.add_timed_vec_batch(Box::new(vec![7_i32]), timestamp, "time")
            .is_err()
    );
    assert!(
        fold.add_timed_vec_batch(
            Box::new(vec![("a".to_owned(), 1_i32)]),
            &"wrong timestamp type".to_owned(),
            "time",
        )
        .is_err()
    );
    fold.add_timed_vec_batch(Box::new(vec![("a".to_owned(), 1_i32)]), timestamp, "time")
        .unwrap();
    let state = fold.checkpoint_state().unwrap().unwrap();
    assert!(fold.restore_checkpoint_state(&state).is_err());

    let mut wrong_version: serde_json::Value = serde_json::from_slice(&state).unwrap();
    wrong_version["version"]["major"] = serde_json::json!(999);
    assert!(
        registry
            .open_fold("latest/group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&wrong_version).unwrap())
            .is_err()
    );

    let mut wrong_coder: serde_json::Value = serde_json::from_slice(&state).unwrap();
    wrong_coder["value_coder"] = serde_json::json!("wrong-coder");
    assert!(
        registry
            .open_fold("latest/group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&wrong_coder).unwrap())
            .is_err()
    );

    let mut duplicate: serde_json::Value = serde_json::from_slice(&state).unwrap();
    let row = duplicate["groups"][0].clone();
    duplicate["groups"].as_array_mut().unwrap().push(row);
    assert!(
        registry
            .open_fold("latest/group")
            .unwrap()
            .restore_checkpoint_state(&serde_json::to_vec(&duplicate).unwrap())
            .is_err()
    );
}

#[test]
fn multi_input_validation_rejects_descriptor_and_topology_corruption() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    let flat = left
        .flatten_named("flat", std::slice::from_ref(&right))
        .unwrap();
    left.join_inner_named("join", &right).unwrap();
    left.map_with_side_named("side", &right, |value, _| value)
        .unwrap();
    flat.group_by_key_named("group").unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let mut altered = graph.clone();
    altered.node_mut_for_test("flat").additional_inputs.clear();
    altered.validate().unwrap();
    let mut altered = graph.clone();
    let duplicated = altered.node("join").unwrap().additional_inputs[0].clone();
    altered
        .node_mut_for_test("join")
        .additional_inputs
        .push(duplicated);
    assert!(
        altered
            .validate()
            .unwrap_err()
            .message
            .contains("exactly two")
    );
    let mut altered = graph.clone();
    altered.node_mut_for_test("right").type_id = "wrong/type".into();
    assert!(
        altered
            .validate()
            .unwrap_err()
            .message
            .contains("additional input")
    );
    let mut altered = graph.clone();
    altered.node_mut_for_test("flat").additional_inputs[0]
        .element
        .type_id = "wrong/type".into();
    assert!(
        altered
            .validate()
            .unwrap_err()
            .message
            .contains("additional input")
    );
    let mut altered = graph.clone();
    altered.node_mut_for_test("right").time.initial_timestamp_ns = 42;
    assert!(
        altered
            .validate()
            .unwrap_err()
            .message
            .contains("time/window")
    );
    let mut altered = graph.clone();
    altered.node_mut_for_test("flat").boundedness = Boundedness::Unbounded;
    assert!(
        altered
            .validate()
            .unwrap_err()
            .message
            .contains("boundedness")
    );
    let mut altered = graph.clone();
    altered.node_mut_for_test("side").additional_inputs[0].key = "missing".into();
    assert!(
        altered
            .validate()
            .unwrap_err()
            .message
            .contains("missing additional")
    );
    let mut altered = graph.clone();
    altered
        .node_mut_for_test("group")
        .additional_inputs
        .push(graph.node("flat").unwrap().additional_inputs[0].clone());
    assert!(altered.validate().unwrap_err().message.contains("forbid"));
    let mut altered = graph;
    altered.node_mut_for_test("join").additional_inputs[0].key = "join".into();
    assert!(altered.validate().is_err());
}

#[test]
fn new_operations_require_individual_backend_capabilities() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    left.flatten_named("flat", std::slice::from_ref(&right))
        .unwrap();
    left.group_by_key_named("group").unwrap();
    left.join_inner_named("join", &right).unwrap();
    left.map_with_side_named("side", &right, |value, _| value)
        .unwrap();
    left.combine_globally_named("combine", |values| i32::try_from(values.len()).unwrap())
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let mut caps = Capabilities::default();
    for (name, enable) in [
        ("flatten", 0),
        ("group-by-key", 1),
        ("join-inner", 2),
        ("side-input-map", 3),
        ("global-combine", 4),
    ] {
        let error = graph.validate_for("fake", &caps).unwrap_err();
        assert_eq!(
            error.kind,
            ErrorKind::UnsupportedCapability {
                capability: name.into()
            }
        );
        match enable {
            0 => caps.flatten = true,
            1 => caps.group_by_key = true,
            2 => caps.join_inner = true,
            3 => caps.side_input_map = true,
            _ => caps.global_combine = true,
        }
    }
    graph.validate_for("fake", &caps).unwrap();
}

#[test]
fn phase4_builder_rejects_incompatible_inputs() {
    let first = Pipeline::default();
    let other = Pipeline::default();
    let a = first.from_vec("a", vec![1_i32]).unwrap();
    let b = other.from_vec("b", vec![2_i32]).unwrap();
    assert!(a.flatten_named("single", &[]).is_ok());
    assert!(
        a.flatten_named("different", std::slice::from_ref(&b))
            .is_err()
    );
    assert!(a.map_with_side_named("side", &b, |value, _| value).is_err());
    assert!(
        a.map_with_windowed_side_named("windowed-side-cross", &b, |value, _| value)
            .is_err()
    );
    let timed = a.fixed_windows("window", 10).unwrap();
    assert!(
        a.flatten_named("time-mismatch", std::slice::from_ref(&timed))
            .is_err()
    );
    assert!(
        a.map_with_side_named("side-time", &timed, |value, _| value)
            .is_err()
    );
    assert!(
        a.map_with_windowed_side_named("windowed-side-time", &timed, |value, _| value)
            .is_err()
    );
    let unbounded = first
        .unbounded_source::<i32>(
            "unbounded",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            0,
        )
        .unwrap();
    assert!(
        a.map_with_side_named("side-unbounded", &unbounded, |value, _| value)
            .is_err()
    );
    assert!(
        a.map_with_windowed_side_named("windowed-side-unbounded", &unbounded, |value, _| value)
            .is_err()
    );
    assert!(
        unbounded
            .combine_globally_named("infinite", |values| i32::try_from(values.len()).unwrap())
            .is_err()
    );
    assert_eq!(
        a.flatten_named("mixed", &[unbounded]).unwrap().key(),
        "mixed"
    );
    let keyed_a = first.from_vec("keyed-a", vec![(1_i32, 2_i32)]).unwrap();
    let keyed_b = other.from_vec("keyed-b", vec![(1_i32, 3_i32)]).unwrap();
    assert!(keyed_a.join_inner_named("cross", &keyed_b).is_err());
    let keyed_timed = keyed_a.fixed_windows("keyed-window", 10).unwrap();
    assert!(keyed_a.join_inner_named("join-time", &keyed_timed).is_err());
    let keyed_unbounded = first
        .unbounded_source::<(i32, i32)>(
            "keyed-stream",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::None,
                reports_event_time: true,
            },
            0,
        )
        .unwrap();
    assert!(
        keyed_a
            .join_inner_named("join-infinite", &keyed_unbounded)
            .is_err()
    );
    assert!(
        keyed_unbounded
            .group_by_key_named("group-infinite")
            .is_err()
    );
}

#[test]
fn cogroup_uses_earliest_event_time_across_all_inputs() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    cogroup_by_key_2_named("cogroup", &left, &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let event = |time, value| Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new((1_i32, value)),
        timestamp_ns: time,
        window: Window::Global,
        assigned_window: None,
    };
    let mut output = registry
        .apply_aggregate(
            "cogroup",
            vec![vec![event(5, 2), event(3, 4)], vec![event(1, 10)]],
        )
        .unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output.pop().unwrap().timestamp_ns, 1);
}

#[test]
fn aggregate_registry_rejects_missing_and_mistyped_runner_inputs() {
    struct Sum;
    impl OrderedFoldFn<i32, i32, i32> for Sum {
        fn create(&self) -> i32 {
            0
        }
        fn add_input(&self, accumulator: &mut i32, value: i32) {
            *accumulator += value;
        }
        fn finish(&self, accumulator: i32) -> i32 {
            accumulator
        }
    }
    fn event<T: Element>(value: T) -> Envelope {
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(value),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        }
    }

    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("values", vec![1_i32]).unwrap();
    values
        .fold_globally_ordered_by_named("ordered", |value| *value, Sum)
        .unwrap();
    values
        .latest_globally_by_named("latest-tie", |value| *value)
        .unwrap();
    pipeline
        .from_vec("pairs", vec![(1_i32, 2_i32)])
        .unwrap()
        .map_values_batches_named("batch", 2, <[i32]>::to_vec)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    for key in ["ordered", "latest-tie", "batch"] {
        assert_eq!(
            registry
                .apply_aggregate(key, Vec::new())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
        assert_eq!(
            registry
                .apply_aggregate(key, vec![vec![event("wrong type".to_owned())]])
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
    }
}

#[test]
fn stateful_global_inputs_require_compatible_finite_windows() {
    let pipeline = Pipeline::default();
    let bounded = pipeline.from_vec("bounded", vec![(1_i32, 2_i32)]).unwrap();
    let foreign = Pipeline::default()
        .from_vec("foreign", vec![(1_i32, 3_i32)])
        .unwrap();
    assert!(cogroup_by_key_2_named("foreign-cogroup", &bounded, &foreign).is_err());
    let windowed = bounded.fixed_windows("windowed", 10).unwrap();
    assert!(cogroup_by_key_2_named("window-cogroup", &bounded, &windowed).is_err());

    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: true,
    };
    let streaming_pairs = pipeline
        .unbounded_source::<(i32, i32)>("streaming-pairs", progress.clone(), 0)
        .unwrap();
    assert!(cogroup_by_key_2_named("global-cogroup", &bounded, &streaming_pairs).is_err());
    assert!(
        streaming_pairs
            .join_full_named("global-join", &bounded)
            .is_err()
    );
    assert!(
        streaming_pairs
            .map_values_batches_named("global-batch", 2, <[i32]>::to_vec)
            .is_err()
    );
    assert!(
        streaming_pairs
            .fold_values_ordered_by_named("global-ordered", |value| *value, SumI32)
            .is_err()
    );
    assert!(
        streaming_pairs
            .latest_per_key_by_named("global-latest", |value| *value)
            .is_err()
    );

    let streaming_values = pipeline
        .unbounded_source::<i32>("streaming-values", progress, 0)
        .unwrap();
    assert!(
        bounded
            .wait_on_named("unbounded-wait", &streaming_values)
            .is_err()
    );
    assert!(
        streaming_values
            .latest_globally_by_named("global-latest-tie", |value| *value)
            .is_err()
    );
}

#[test]
fn wait_on_all_rejects_missing_or_invalid_signals_without_partial_nodes() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let signal = pipeline.from_vec("signal", vec![2_i32]).unwrap();
    let foreign = Pipeline::default()
        .from_vec("foreign", vec![3_i32])
        .unwrap();
    let unbounded = pipeline
        .unbounded_source::<i32>(
            "unbounded",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            0,
        )
        .unwrap();
    let before = pipeline.snapshot().unwrap().0.nodes.len();
    assert!(main.wait_on_all_named::<i32>("empty", &[]).is_err());
    assert!(main.wait_on_signals_named("empty-erased", &[]).is_err());
    assert!(
        main.wait_on_signals_named(
            "foreign-erased",
            &[signal.wait_signal(), foreign.wait_signal()]
        )
        .is_err()
    );
    assert!(
        main.wait_on_signals_named(
            "unbounded-erased",
            &[signal.wait_signal(), unbounded.wait_signal()]
        )
        .is_err()
    );
    assert!(
        main.wait_on_all_named("foreign-wait", &[signal.clone(), foreign])
            .is_err()
    );
    assert!(
        main.wait_on_all_named("unbounded-wait", &[signal, unbounded])
            .is_err()
    );
    assert_eq!(pipeline.snapshot().unwrap().0.nodes.len(), before);
}

struct SumI32;
impl OrderedFoldFn<i32, i32, i32> for SumI32 {
    fn create(&self) -> i32 {
        0
    }
    fn add_input(&self, accumulator: &mut i32, value: i32) {
        *accumulator += value;
    }
    fn finish(&self, accumulator: i32) -> i32 {
        accumulator
    }
}

#[test]
fn unbounded_global_ordered_folds_require_finite_windows() {
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: true,
    };
    let values = pipeline
        .unbounded_source::<i32>("values", progress.clone(), 0)
        .unwrap();
    assert_eq!(
        values
            .fold_globally_ordered_named("global-fold", SumI32)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        values
            .fold_globally_ordered_by_named("global-ordered", |value| *value, SumI32)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    let keyed = pipeline
        .unbounded_source::<(i32, i32)>("keyed", progress, 0)
        .unwrap();
    assert_eq!(
        keyed
            .fold_values_ordered_named("keyed-fold", SumI32)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        keyed
            .fold_values_ordered_by_named("keyed-ordered", |value| *value, SumI32)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
}

#[test]
fn aggregate_registry_reports_bad_arity_and_runtime_types() {
    fn event(value: impl Send + 'static) -> Envelope {
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(value),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        }
    }
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    let values = left.map_named("values", |(_, value)| value).unwrap();
    values
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap();
    values
        .map_with_side_named("side", &right, |value, _| value)
        .unwrap();
    left.group_by_key_named("group").unwrap();
    left.join_inner_named("join", &right).unwrap();
    left.join_left_named("left-join", &right).unwrap();
    left.join_right_named("right-join", &right).unwrap();
    left.join_full_named("full-join", &right).unwrap();
    values.flat_map_named("flat", |value| vec![value]).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.apply_aggregate("sum", vec![]).is_err());
    assert!(
        registry
            .apply_aggregate("sum", vec![vec![event("bad")]])
            .is_err()
    );
    assert!(registry.apply_aggregate("side", vec![]).is_err());
    assert!(
        registry
            .apply_aggregate("side", vec![vec![event(1_i32)], vec![event("bad")]])
            .is_err()
    );
    assert!(
        registry
            .apply_aggregate(
                "side",
                vec![vec![event("bad")], vec![event((1_i32, 2_i32))]]
            )
            .is_err()
    );
    assert!(registry.apply_aggregate("group", vec![]).is_err());
    assert!(
        registry
            .apply_aggregate("group", vec![vec![event("bad")]])
            .is_err()
    );
    assert!(registry.apply_aggregate("join", vec![]).is_err());
    assert!(
        registry
            .apply_aggregate(
                "join",
                vec![vec![event("bad")], vec![event((1_i32, 2_i32))]]
            )
            .is_err()
    );
    assert!(
        registry
            .apply_aggregate(
                "join",
                vec![vec![event((1_i32, 2_i32))], vec![event("bad")]]
            )
            .is_err()
    );
    assert!(registry.apply_many("flat", event("bad")).is_err());

    // Portable workers can receive malformed typed payloads from an input
    // coder. Both index orientations must reject them with the join node's
    // execution error, including outer joins that retain unmatched rows.
    for join in ["join", "left-join", "right-join", "full-join"] {
        for inputs in [
            vec![
                vec![event("bad")],
                vec![event((1_i32, 3_i32)), event((2, 4))],
            ],
            vec![
                vec![event((1_i32, 2_i32))],
                vec![event("bad"), event((2, 4))],
            ],
            vec![
                vec![event((1_i32, 2_i32)), event((2, 4))],
                vec![event("bad")],
            ],
            vec![
                vec![event((1_i32, 2_i32)), event("bad")],
                vec![event((1_i32, 3_i32))],
            ],
        ] {
            let Err(error) = registry.apply_aggregate(join, inputs) else {
                panic!("{join} accepted a malformed typed input");
            };
            assert_eq!(error.kind, ErrorKind::Execution);
            assert_eq!(error.node.as_deref(), Some(join));
            assert!(error.message.contains("type mismatch"), "{join}: {error:?}");
        }
    }
}

#[test]
fn portable_ordered_and_latest_steps_reject_corrupt_shuffle_groups() {
    fn event(value: impl Send + 'static) -> Envelope {
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(value),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        }
    }
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", vec![(1_i32, 2_i32)]).unwrap();
    input
        .fold_values_ordered_by_named("ordered", |value| *value, SumI32)
        .unwrap();
    input
        .latest_per_key_by_named("latest-tie", |value| *value)
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();

    for (key, value) in [
        ("ordered/order", event("bad")),
        ("ordered", event("bad")),
        ("ordered", event((1_i32, Vec::<(i32, (i64, i32))>::new()))),
        (
            "ordered",
            event((1_i32, vec![(7_i32, (0_i64, 1_i32)), (7, (1, 2))])),
        ),
        ("latest-tie/tie", event("bad")),
        ("latest-tie", event("bad")),
        (
            "latest-tie",
            event((1_i32, Vec::<(i64, (i32, i32))>::new())),
        ),
        (
            "latest-tie",
            event((1_i32, vec![(0_i64, (7_i32, 1_i32)), (0, (7, 2))])),
        ),
    ] {
        let Err(error) = registry.apply_many(key, value) else {
            panic!("{key} accepted a corrupt shuffle input");
        };
        assert_eq!(error.kind, ErrorKind::Execution);
        assert_eq!(error.node.as_deref(), Some(key));
    }
}

#[test]
fn single_input_aggregates_reject_extra_runner_inputs() {
    fn event(value: impl Send + 'static) -> Envelope {
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(value),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
        }
    }
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("values", vec![1_i32]).unwrap();
    values
        .combine_globally_named("combine", |rows| rows.into_iter().sum::<i32>())
        .unwrap();
    values
        .fold_globally_ordered_by_named("ordered", |value| *value, SumI32)
        .unwrap();
    let keyed = pipeline.from_vec("keyed", vec![(1_i32, 2_i32)]).unwrap();
    keyed.group_by_key_named("group").unwrap();
    keyed.latest_per_key_named("latest").unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();

    for (key, inputs) in [
        ("combine", vec![vec![event(1_i32)], vec![event(2_i32)]]),
        ("ordered", vec![vec![event(1_i32)], vec![event(2_i32)]]),
        (
            "group",
            vec![vec![event((1_i32, 2_i32))], vec![event((2_i32, 3_i32))]],
        ),
        (
            "latest/group",
            vec![
                vec![event((1_i32, (0_i64, 2_i32)))],
                vec![event((2_i32, (0_i64, 3_i32)))],
            ],
        ),
    ] {
        let Err(error) = registry.apply_aggregate(key, inputs) else {
            panic!("{key} ignored an extra runner input");
        };
        assert_eq!(error.kind, ErrorKind::Execution);
        assert_eq!(error.node.as_deref(), Some(key));
        assert!(error.message.contains("one input"));
    }
}

#[test]
fn built_in_fold_sessions_drain_values_and_propagate_emitter_failure() {
    fn event(value: impl Send + 'static, timestamp_ns: i64) -> Envelope {
        Envelope {
            pane: ironbeam_core::PaneInfo::NO_FIRING,
            value: Box::new(value),
            timestamp_ns,
            window: Window::Global,
            assigned_window: None,
        }
    }
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("values", vec![1_i32, 2]).unwrap();
    values
        .fold_globally_ordered_named("global", SumI32)
        .unwrap();
    let keyed = pipeline.from_vec("keyed", vec![(1_i32, 1_i32)]).unwrap();
    keyed.fold_values_ordered_named("per-key", SumI32).unwrap();
    keyed.group_by_key_named("group").unwrap();
    keyed.latest_per_key_named("latest").unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();

    let empty_global = registry.open_fold("global").unwrap();
    let mut empty = Vec::new();
    empty_global
        .finish_into(&mut |row| {
            empty.push((*row.value.downcast::<i32>().unwrap(), row.timestamp_ns));
            Ok(())
        })
        .unwrap();
    assert_eq!(empty, [(0, 0)]);

    let mut global = registry.open_fold("global").unwrap();
    global.add(event(1_i32, 5)).unwrap();
    global.add(event(2_i32, 10)).unwrap();
    let mut sums = Vec::new();
    global
        .finish_into(&mut |row| {
            sums.push((*row.value.downcast::<i32>().unwrap(), row.timestamp_ns));
            Ok(())
        })
        .unwrap();
    assert_eq!(sums, [(3, 5)]);

    let mut per_key = registry.open_fold("per-key").unwrap();
    per_key.add(event((1_i32, 1_i32), 5)).unwrap();
    per_key.add(event((1_i32, 2_i32), 10)).unwrap();
    let mut keyed_sums = Vec::new();
    per_key
        .finish_into(&mut |row| {
            keyed_sums.push((
                *row.value.downcast::<(i32, i32)>().unwrap(),
                row.timestamp_ns,
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(keyed_sums, [((1, 3), 5)]);

    let mut group = registry.open_fold("group").unwrap();
    group.add(event((1_i32, 1_i32), 5)).unwrap();
    group.add(event((1_i32, 2_i32), 10)).unwrap();
    let mut groups = Vec::new();
    group
        .finish_into(&mut |row| {
            groups.push((
                *row.value.downcast::<(i32, Vec<i32>)>().unwrap(),
                row.timestamp_ns,
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(groups, [((1, vec![1, 2]), 5)]);

    let mut latest = registry.open_fold("latest/group").unwrap();
    latest.add(event((1_i32, (5_i64, 1_i32)), 5)).unwrap();
    latest.add(event((1_i32, (10_i64, 2_i32)), 10)).unwrap();
    let mut latest_groups = Vec::new();
    latest
        .finish_into(&mut |row| {
            latest_groups.push((
                *row.value.downcast::<(i32, Vec<(i64, i32)>)>().unwrap(),
                row.timestamp_ns,
            ));
            Ok(())
        })
        .unwrap();
    assert_eq!(latest_groups, [((1, vec![(10, 2)]), 5)]);

    let mut failing = registry.open_fold("group").unwrap();
    failing.add(event((1_i32, 1_i32), 5)).unwrap();
    failing.add(event((2_i32, 2_i32), 10)).unwrap();
    let mut emitted = 0;
    let error = failing
        .finish_into(&mut |_| {
            emitted += 1;
            Err(Error::io("sink", "test", "write failed"))
        })
        .unwrap_err();
    assert_eq!(emitted, 1);
    assert_eq!(error.kind, ErrorKind::Io);
}

#[test]
fn remaining_phase4_contract_edges_are_explicit() {
    #[derive(Clone)]
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "test/opaque2";
        const CODER_ID: &'static str = "test/opaque2";
    }
    assert!(vec![Opaque].encode().is_err());
    assert!(Vec::<Opaque>::decode(&[]).is_err());
    assert!((Opaque, 1_i32).encode().is_err());
    assert!(<(Opaque, i32)>::decode(&[]).is_err());
    assert!(Vec::<i32>::decode(&[]).is_err());
    let mut encoded = vec![1_i32].encode().unwrap();
    encoded.push(0);
    assert!(Vec::<i32>::decode(&encoded).is_err());
    let mut encoded = (1_i32, 2_i32).encode().unwrap();
    encoded.push(0);
    assert!(<(i32, i32)>::decode(&encoded).is_err());

    let pipeline = Pipeline::default();
    let a = pipeline.from_vec("a", vec![1_i32]).unwrap();
    a.flat_map_named("flat", |value| vec![value, -value])
        .unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    let mut source = registry.open_source("a").unwrap();
    let event = source.next().unwrap().unwrap();
    assert!(registry.apply("flat", event).is_err());
    let _ = graph;
    assert!(!registry.has_sink("missing"));
    let pipeline = Pipeline::default();
    let value = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let terminal = value
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            || Err(Error::io("sink", "test", "cannot open")),
        )
        .unwrap();
    assert!(terminal.snapshot().is_ok());
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.has_sink("sink"));
    assert_eq!(
        registry.open_sink("sink").err().unwrap().kind,
        ErrorKind::Io
    );
}

#[test]
fn unbounded_multi_input_graph_validation_catches_invalid_rewrites() {
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::None,
        reports_event_time: true,
    };
    let first = pipeline
        .unbounded_source::<(i32, i32)>("first", progress.clone(), 0)
        .unwrap()
        .fixed_windows("first-window", 10)
        .unwrap();
    let second = pipeline
        .unbounded_source::<(i32, i32)>("second", progress, 0)
        .unwrap()
        .fixed_windows("second-window", 10)
        .unwrap();
    first.join_inner_named("join", &second).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("join").unwrap().boundedness,
        Boundedness::Unbounded
    );
    let mut changed = graph;
    changed.node_mut_for_test("first-window").time.window = Window::Global;
    changed.node_mut_for_test("second-window").time.window = Window::Global;
    changed.node_mut_for_test("join").time.window = Window::Global;
    assert!(changed.validate().is_err());
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32]).unwrap();
    let side = pipeline.from_vec("side", vec![2_i32]).unwrap();
    main.map_with_side_named("map-side", &side, |value, _| value)
        .unwrap();
    let (mut graph, _) = pipeline.snapshot().unwrap();
    graph.node_mut_for_test("side").boundedness = Boundedness::Unbounded;
    graph.node_mut_for_test("map-side").boundedness = Boundedness::Unbounded;
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("side input must be bounded")
    );
}

#[test]
fn graph_rejects_unbounded_aggregate_and_join_even_after_descriptor_edit() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    left.group_by_key_named("group").unwrap();
    left.join_inner_named("join", &right).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let mut changed = graph.clone();
    changed.node_mut_for_test("left").boundedness = Boundedness::Unbounded;
    changed.node_mut_for_test("group").boundedness = Boundedness::Unbounded;
    changed.node_mut_for_test("join").boundedness = Boundedness::Unbounded;
    assert!(
        changed
            .validate()
            .unwrap_err()
            .message
            .contains("aggregation")
    );
    changed.node_mut_for_test("group").operation = Operation::Map;
    assert!(changed.validate().unwrap_err().message.contains("join"));
    let mut changed = graph;
    changed.node_mut_for_test("right").boundedness = Boundedness::Unbounded;
    changed.node_mut_for_test("join").boundedness = Boundedness::Unbounded;
    assert!(changed.validate().unwrap_err().message.contains("join"));
}

#[test]
fn graph_rejects_flatten_output_type_disagreement() {
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![1_i32]).unwrap();
    let right = pipeline.from_vec("right", vec![2_i32]).unwrap();
    left.flatten_named("flat", &[right]).unwrap();
    let (mut graph, _) = pipeline.snapshot().unwrap();
    graph.node_mut_for_test("right").type_id = "other/type".into();
    graph.node_mut_for_test("flat").additional_inputs[0]
        .element
        .type_id = "other/type".into();
    assert!(
        graph
            .validate()
            .unwrap_err()
            .message
            .contains("flatten input type/coder")
    );
}

#[test]
fn composite_coder_rejects_corrupt_lengths_and_group_join_handles_misses() {
    assert!(<(i32, i32)>::decode(&[]).is_err());
    assert!(<(i32, i32)>::decode(&u64::MAX.to_le_bytes()).is_err());
    assert!(<(i32, i32)>::decode(&0_u64.to_le_bytes()).is_err());
    let mut truncated = 1_u64.to_le_bytes().to_vec();
    truncated.extend_from_slice(&0_u64.to_le_bytes());
    assert!(Vec::<i32>::decode(&truncated).is_err());
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(2_i32, 3_i32)]).unwrap();
    left.join_inner_named("join", &right).unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let event = |value: (i32, i32)| Envelope {
        pane: ironbeam_core::PaneInfo::NO_FIRING,
        value: Box::new(value),
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
    };
    assert!(
        registry
            .apply_aggregate("join", vec![vec![event((1, 2))], vec![event((2, 3))]])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn graph_rejects_each_identity_and_version_component() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    source
        .custom_named("custom", "tests/op", Version { major: 1, minor: 0 })
        .unwrap();
    source.fixed_windows("window", 10).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let mut changed = graph.clone();
    changed.node_mut_for_test("source").coder_id.clear();
    assert!(changed.validate().is_err());
    let mut changed = graph.clone();
    changed.node_mut_for_test("source").semantic_version.major = 0;
    assert!(changed.validate().is_err());
    let mut changed = graph.clone();
    changed.node_mut_for_test("source").coder_version.minor = 1;
    assert_eq!(
        changed.validate().unwrap_err().kind,
        ErrorKind::Incompatible
    );
    let mut changed = graph.clone();
    changed.node_mut_for_test("custom").operation = Operation::Custom {
        semantic_id: "invalid".into(),
        version: Version { major: 1, minor: 0 },
    };
    assert!(
        changed
            .validate()
            .unwrap_err()
            .message
            .contains("namespaced")
    );
    let mut changed = graph.clone();
    changed.node_mut_for_test("custom").operation = Operation::Custom {
        semantic_id: "tests/op".into(),
        version: Version { major: 0, minor: 0 },
    };
    assert!(
        changed
            .validate()
            .unwrap_err()
            .message
            .contains("namespaced")
    );
    let mut changed = graph.clone();
    changed.node_mut_for_test("custom").semantic_id = "tests/other".into();
    assert!(changed.validate().unwrap_err().message.contains("differs"));
    let mut changed = graph;
    changed.node_mut_for_test("window").time.window = Window::Global;
    assert!(
        changed
            .validate()
            .unwrap_err()
            .message
            .contains("fixed-window")
    );
}

#[test]
fn test_stream_registers_replayable_unbounded_events() {
    let pipeline = Pipeline::default();
    let stream = pipeline
        .test_stream_source(
            "events",
            vec![
                TestStreamEvent::Elements(vec![(7_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
                TestStreamEvent::AdvanceProcessingTime(1_000_000),
            ],
        )
        .unwrap();
    stream.output("result").unwrap();
    let (graph, registry) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("events").unwrap().boundedness,
        Boundedness::Unbounded
    );
    assert!(
        registry
            .source_config_digest("events")
            .unwrap()
            .starts_with("sha256-v1:")
    );
    assert_eq!(
        registry.test_stream("events").unwrap(),
        &[
            EncodedTestStreamEvent::Elements(vec![(7_i32.encode().unwrap(), 1_000_000)]),
            EncodedTestStreamEvent::AdvanceWatermark(2_000_000),
            EncodedTestStreamEvent::AdvanceProcessingTime(1_000_000),
        ]
    );
    assert!(registry.open_source("events").is_err());
    assert!(
        pipeline
            .test_stream_source::<i32>("empty", Vec::new())
            .is_err()
    );
    assert!(
        pipeline
            .test_stream_source::<i32>(
                "bad-duration",
                vec![TestStreamEvent::AdvanceProcessingTime(0)]
            )
            .is_err()
    );
    assert!(
        pipeline
            .test_stream_source::<i32>(
                "overflow-duration",
                vec![
                    TestStreamEvent::AdvanceProcessingTime(i64::MAX),
                    TestStreamEvent::AdvanceProcessingTime(1),
                ]
            )
            .is_err()
    );
    assert!(
        pipeline
            .test_stream_source::<i32>(
                "bad",
                vec![
                    TestStreamEvent::AdvanceWatermark(2),
                    TestStreamEvent::AdvanceWatermark(2),
                ]
            )
            .is_err()
    );
}

#[test]
fn test_stream_rejects_an_element_with_a_broken_coder() {
    struct BrokenCoder;
    impl Element for BrokenCoder {
        const TYPE_ID: &'static str = "tests/broken-test-stream-coder";
        const CODER_ID: &'static str = "tests/broken-test-stream-coder-v1";
        fn supports_coder() -> bool {
            true
        }
        fn encode(&self) -> Result<Vec<u8>, Error> {
            Err(Error::incompatible("intentional encoding failure"))
        }
    }
    let pipeline = Pipeline::default();
    assert!(
        pipeline
            .test_stream_source(
                "broken",
                vec![TestStreamEvent::Elements(vec![(BrokenCoder, 0)])]
            )
            .is_err()
    );
}

#[test]
fn external_bounded_source_requires_backend_capability() {
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: false,
    };
    assert!(
        pipeline
            .bounded_source_with_semantic::<i32>(
                "invalid",
                "invalid",
                progress.clone(),
                "digest".into(),
                None,
                || Ok(Box::new(std::iter::empty()))
            )
            .is_err()
    );
    pipeline
        .bounded_source_with_semantic::<i32>(
            "file",
            "ironbeam/io/jsonl-source",
            progress,
            "digest".into(),
            None,
            || Ok(Box::new(std::iter::once(Ok(1_i32)))),
        )
        .unwrap();
    let (mut graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph
            .validate_for("dataflow-preview", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "ironbeam/io/jsonl-source".into()
        }
    );
    graph
        .validate_for(
            "direct",
            &Capabilities {
                external_bounded_source: true,
                ..Capabilities::default()
            },
        )
        .unwrap();
    graph.node_mut_for_test("file").semantic_id = "invalid".into();
    assert!(graph.validate().unwrap_err().message.contains("namespaced"));
}

#[test]
fn reusable_transform_requires_a_positive_semantic_batch_size() {
    struct Identity;
    impl DynOp for Identity {
        fn apply(&self, input: Partition) -> Partition {
            input
        }
    }
    let source = Pipeline::default().from_vec("source", vec![1_i32]).unwrap();
    let error = source
        .apply_dyn_transform_with_semantic_named::<i32>(
            "operator",
            0,
            "ironbeam/tests/identity",
            Version { major: 1, minor: 0 },
            std::sync::Arc::new(Identity),
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Graph);
    assert!(error.message.contains("batch size"));
}

#[test]
fn sliding_windows_reject_nonpositive_size_and_period() {
    let source = Pipeline::default().from_vec("source", vec![1_i32]).unwrap();
    assert!(source.sliding_windows("size", 0, 1, 0).is_err());
    assert!(source.sliding_windows("period", 1, -1, 0).is_err());
}
