use ironbeam_core::{
    BagStateMutation, Element, Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec,
    SourceProgress, TestStreamEvent, TimedStateResult, TypedSink, TypedSourceIterator,
    UserTimerDomain, UserTimerRequest, Version,
};
use ironbeam_direct::{DirectRunner, JobConfig};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const fn spec() -> SinkSpec {
    SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::Idempotent,
        commit: true,
    }
}

#[test]
fn shared_stateful_stage_runs_once_before_two_sink_branches() {
    let calls = Arc::new(AtomicUsize::new(0));
    let first = Arc::new(Mutex::new(Vec::new()));
    let second = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let callback_calls = Arc::clone(&calls);
    let shared = pipeline
        .from_vec("pairs", vec![(1_i32, 10_i32), (1, 20), (2, 30), (1, 40)])
        .unwrap()
        .stateful_bag_map_named::<i32, String>(
            "stateful",
            "tests/multi-sink-stateful-fanout",
            Version { major: 1, minor: 0 },
            move |key, value, prior| {
                callback_calls.fetch_add(1, Ordering::SeqCst);
                Ok((
                    BagStateMutation::Append(vec![value]),
                    format!("{key}:{value}:{}", prior.len()),
                ))
            },
        )
        .unwrap()
        .map_named("shared-label", |(_, label)| label)
        .unwrap();
    let captured = Arc::clone(&first);
    shared
        .sink_named("first", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&captured),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let captured = Arc::clone(&second);
    shared
        .map_named("second-label", |label| format!("copy:{label}"))
        .unwrap()
        .sink_named("second", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&captured),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    DirectRunner
        .run_sinks(&pipeline, &["first", "second"], &JobConfig::default())
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        *first.lock().unwrap(),
        ["1:10:0", "1:20:1", "2:30:0", "1:40:2"]
    );
    assert_eq!(
        *second.lock().unwrap(),
        ["copy:1:10:0", "copy:1:20:1", "copy:2:30:0", "copy:1:40:2"]
    );
    first.lock().unwrap().clear();
    second.lock().unwrap().clear();
    DirectRunner
        .run_sinks(&pipeline, &["first", "second"], &JobConfig::default())
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 8);
    assert_eq!(
        *first.lock().unwrap(),
        ["1:10:0", "1:20:1", "2:30:0", "1:40:2"]
    );
}

#[test]
fn one_stream_branch_can_keep_value_state_after_the_split() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 2_i32), 0)]),
                TestStreamEvent::Elements(vec![((1_i32, 5_i32), 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(2_000_000),
            ],
        )
        .unwrap();
    let left_rows = Arc::new(Mutex::new(Vec::new()));
    let right_rows = Arc::new(Mutex::new(Vec::new()));
    let left_capture = Arc::clone(&left_rows);
    let left = source
        .stateful_value_map_named::<i32, i32>(
            "left-state",
            "tests/branch-state-v1",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let next = prior.unwrap_or(0) + value;
                Ok((Some(next), next))
            },
        )
        .unwrap()
        .map_named("left-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named("left", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&left_capture),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let right_capture = Arc::clone(&right_rows);
    let right = source
        .map_named("right-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named("right", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&right_capture),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    DirectRunner
        .start_stream_sink_pair(&left, &right, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*left_rows.lock().unwrap(), ["1:2", "1:7"]);
    assert_eq!(*right_rows.lock().unwrap(), ["1:2", "1:5"]);
}

#[test]
fn left_stream_branch_routes_its_timer_output_to_only_its_sink() {
    branch_timer_output_reaches_only_its_sink(false);
}

#[test]
fn right_stream_branch_routes_its_timer_output_to_only_its_sink() {
    branch_timer_output_reaches_only_its_sink(true);
}

fn branch_timer_output_reaches_only_its_sink(stateful_on_right: bool) {
    let pipeline = Pipeline::default();
    let source = pipeline
        .test_stream_source(
            "source",
            vec![
                TestStreamEvent::Elements(vec![((1_i32, 3_i32), 0)]),
                TestStreamEvent::AdvanceWatermark(6_000_000),
            ],
        )
        .unwrap();
    let stateful_rows = Arc::new(Mutex::new(Vec::new()));
    let raw_rows = Arc::new(Mutex::new(Vec::new()));
    let stateful_capture = Arc::clone(&stateful_rows);
    let stateful = source
        .stateful_timer_map_named::<i32, i32>(
            "branch-timer",
            "tests/branch-timer-v1",
            Version { major: 1, minor: 0 },
            BTreeMap::from([("event".into(), UserTimerDomain::EventTime)]),
            |_, value, _, _| {
                Ok(TimedStateResult {
                    state: Some(value),
                    outputs: Vec::new(),
                    timers: vec![UserTimerRequest::Set {
                        family_id: "event".into(),
                        dynamic_tag: "flush".into(),
                        fire_timestamp_ns: 5_000_000,
                        output_timestamp_ns: None,
                    }],
                })
            },
            |_, state, _| {
                Ok(TimedStateResult {
                    state: None,
                    outputs: state.into_iter().collect(),
                    timers: Vec::new(),
                })
            },
        )
        .unwrap()
        .map_named("stateful-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named("stateful", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&stateful_capture),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let raw_capture = Arc::clone(&raw_rows);
    let raw = source
        .map_named("raw-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named("raw", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&raw_capture),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let (left, right) = if stateful_on_right {
        (&raw, &stateful)
    } else {
        (&stateful, &raw)
    };
    DirectRunner
        .start_stream_sink_pair(left, right, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*stateful_rows.lock().unwrap(), ["1:3"]);
    assert_eq!(*raw_rows.lock().unwrap(), ["1:3"]);
}

struct Capture<T> {
    rows: Arc<Mutex<Vec<T>>>,
    pending: Vec<T>,
}

struct Probe {
    aborts: Arc<AtomicUsize>,
    commits: Arc<AtomicUsize>,
    fail_write: bool,
    fail_commit: bool,
}
impl TypedSink<i32> for Probe {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        if self.fail_write {
            Err(Error::execution("failing", "direct", "write failed"))
        } else {
            Ok(())
        }
    }
    fn commit(&mut self) -> Result<(), Error> {
        if self.fail_commit {
            return Err(Error::execution("failing", "direct", "commit failed"));
        }
        self.commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl<T: Element> TypedSink<T> for Capture<T> {
    fn write(&mut self, value: T) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.rows.lock().unwrap().extend(self.pending.drain(..));
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

#[test]
fn several_sinks_share_source_and_each_common_stage_once() {
    let opens = Arc::new(AtomicUsize::new(0));
    let common_calls = Arc::new(AtomicUsize::new(0));
    let doubled_calls = Arc::new(AtomicUsize::new(0));
    let numbers = Arc::new(Mutex::new(Vec::new()));
    let labels = Arc::new(Mutex::new(Vec::new()));
    let evens = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let opened = Arc::clone(&opens);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/multi-sink-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            Some(4),
            move || {
                opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new([1, 2, 3, 4].into_iter().map(Ok)) as TypedSourceIterator<i32>)
            },
        )
        .unwrap();
    let common = Arc::clone(&common_calls);
    let shared = source
        .map_named("shared", move |value| {
            common.fetch_add(1, Ordering::SeqCst);
            value + 1
        })
        .unwrap();
    let doubled = Arc::clone(&doubled_calls);
    let pair = shared
        .map_named("doubled", move |value| {
            doubled.fetch_add(1, Ordering::SeqCst);
            value * 2
        })
        .unwrap();
    let captured = Arc::clone(&numbers);
    pair.sink_named("numbers", spec(), move || {
        Ok(Box::new(Capture {
            rows: Arc::clone(&captured),
            pending: Vec::new(),
        }))
    })
    .unwrap();
    let captured = Arc::clone(&labels);
    pair.map_named("label", |value| format!("n={value}"))
        .unwrap()
        .sink_named("labels", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&captured),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let captured = Arc::clone(&evens);
    shared
        .filter_named("even", |value| value % 2 == 0)
        .unwrap()
        .sink_named("evens", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&captured),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let config = JobConfig {
        buffer_budget_elements: 2,
        ..JobConfig::default()
    };
    let job = DirectRunner
        .run_sinks(&pipeline, &["numbers", "labels", "evens"], &config)
        .unwrap();
    assert_eq!(*numbers.lock().unwrap(), [4, 6, 8, 10]);
    assert_eq!(*labels.lock().unwrap(), ["n=4", "n=6", "n=8", "n=10"]);
    assert_eq!(*evens.lock().unwrap(), [2, 4]);
    assert_eq!(opens.load(Ordering::SeqCst), 1);
    assert_eq!(common_calls.load(Ordering::SeqCst), 4);
    assert_eq!(doubled_calls.load(Ordering::SeqCst), 4);
    assert_eq!(job.metrics().source_elements, 4);
    assert_eq!(job.metrics().output_elements, 10);
}

#[test]
fn multiple_sources_flatten_once_into_shared_sink_branches() {
    let left_opens = Arc::new(AtomicUsize::new(0));
    let right_opens = Arc::new(AtomicUsize::new(0));
    let after_merge_calls = Arc::new(AtomicUsize::new(0));
    let numbers = Arc::new(Mutex::new(Vec::new()));
    let labels = Arc::new(Mutex::new(Vec::new()));
    let left_only = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: false,
    };
    let opened = Arc::clone(&left_opens);
    let left = pipeline
        .bounded_source_with_semantic::<i32>(
            "left-source",
            "tests/multi-sink-left",
            progress.clone(),
            "left-v1".into(),
            Some(2),
            move || {
                opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new([1, 2].into_iter().map(Ok)) as TypedSourceIterator<i32>)
            },
        )
        .unwrap()
        .map_named("left-pre", |value| value + 10)
        .unwrap();
    let opened = Arc::clone(&right_opens);
    let right = pipeline
        .bounded_source_with_semantic::<i32>(
            "right-source",
            "tests/multi-sink-right",
            progress,
            "right-v1".into(),
            Some(2),
            move || {
                opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new([3, 4].into_iter().map(Ok)) as TypedSourceIterator<i32>)
            },
        )
        .unwrap()
        .map_named("right-pre", |value| value + 20)
        .unwrap();
    let captured = Arc::clone(&left_only);
    left.sink_named("left-only", spec(), move || {
        Ok(Box::new(Capture {
            rows: Arc::clone(&captured),
            pending: Vec::new(),
        }))
    })
    .unwrap();
    let calls = Arc::clone(&after_merge_calls);
    let merged = left
        .flatten_named("merged", &[right])
        .unwrap()
        .map_named("shared-after-merge", move |value| {
            calls.fetch_add(1, Ordering::SeqCst);
            value * 2
        })
        .unwrap();
    let captured = Arc::clone(&numbers);
    merged
        .sink_named("numbers", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&captured),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let captured = Arc::clone(&labels);
    merged
        .map_named("label", |value| format!("n={value}"))
        .unwrap()
        .sink_named("labels", spec(), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&captured),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let job = DirectRunner
        .run_sinks(
            &pipeline,
            &["numbers", "labels", "left-only"],
            &JobConfig {
                buffer_budget_elements: 2,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(*numbers.lock().unwrap(), [22, 24, 46, 48]);
    assert_eq!(*labels.lock().unwrap(), ["n=22", "n=24", "n=46", "n=48"]);
    assert_eq!(*left_only.lock().unwrap(), [11, 12]);
    assert_eq!(left_opens.load(Ordering::SeqCst), 1);
    assert_eq!(right_opens.load(Ordering::SeqCst), 1);
    assert_eq!(after_merge_calls.load(Ordering::SeqCst), 4);
    assert_eq!(job.metrics().source_elements, 4);
    assert_eq!(job.metrics().output_elements, 10);
}

#[test]
fn flatten_repeated_input_preserves_edge_multiplicity() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    let repeated = source
        .flatten_named("repeated", std::slice::from_ref(&source))
        .unwrap();
    let first = Arc::new(Mutex::new(Vec::new()));
    let second = Arc::new(Mutex::new(Vec::new()));
    for (key, rows) in [
        ("first", Arc::clone(&first)),
        ("second", Arc::clone(&second)),
    ] {
        repeated
            .sink_named(key, spec(), move || {
                Ok(Box::new(Capture {
                    rows: Arc::clone(&rows),
                    pending: Vec::new(),
                }))
            })
            .unwrap();
    }
    DirectRunner
        .run_sinks(&pipeline, &["first", "second"], &JobConfig::default())
        .unwrap();
    for rows in [&first, &second] {
        let mut actual = rows.lock().unwrap().clone();
        actual.sort_unstable();
        assert_eq!(actual, [1, 1, 2, 2]);
    }
}

#[test]
fn non_flatten_fan_in_is_rejected_before_sink_factories_open() {
    let sink_opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![(1_i32, 2_i32)]).unwrap();
    let right = pipeline.from_vec("right", vec![(1_i32, 3_i32)]).unwrap();
    let joined = left.join_inner_named("join", &right).unwrap();
    let opened = Arc::clone(&sink_opens);
    joined
        .sink_named("joined", spec(), move || {
            opened.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Capture {
                rows: Arc::new(Mutex::new(Vec::new())),
                pending: Vec::new(),
            }))
        })
        .unwrap();
    let opened = Arc::clone(&sink_opens);
    left.sink_named("left-only", spec(), move || {
        opened.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(Capture {
            rows: Arc::new(Mutex::new(Vec::new())),
            pending: Vec::new(),
        }))
    })
    .unwrap();
    let error = DirectRunner
        .run_sinks(&pipeline, &["joined", "left-only"], &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(sink_opens.load(Ordering::SeqCst), 0);
}

#[test]
fn invalid_multi_sink_plan_fails_before_opening_source_or_sinks() {
    let opens = Arc::new(AtomicUsize::new(0));
    let sink_opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let opened = Arc::clone(&opens);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/multi-sink-validation",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            Some(1),
            move || {
                opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(1).map(Ok)) as TypedSourceIterator<i32>)
            },
        )
        .unwrap();
    for key in ["a", "b"] {
        let sink_opens = Arc::clone(&sink_opens);
        source
            .sink_named(key, spec(), move || {
                sink_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(Capture {
                    rows: Arc::new(Mutex::new(Vec::new())),
                    pending: Vec::new(),
                }))
            })
            .unwrap();
    }
    assert_eq!(
        DirectRunner
            .run_sinks(&pipeline, &["a", "a"], &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        DirectRunner
            .run_sinks(&pipeline, &["a"], &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        DirectRunner
            .run_sinks(&pipeline, &["a", "missing"], &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    assert_eq!(sink_opens.load(Ordering::SeqCst), 0);
}

#[test]
fn multi_sink_rejects_uncoded_split_and_declared_finalization_before_opening() {
    #[derive(Clone)]
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "tests/multi-sink-opaque";
        const CODER_ID: &'static str = "tests/multi-sink-opaque-v1";
    }
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![Opaque]).unwrap();
    for key in ["left", "right"] {
        let opened = Arc::clone(&opens);
        source
            .sink_named(key, spec(), move || {
                opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(Capture {
                    rows: Arc::new(Mutex::new(Vec::new())),
                    pending: Vec::new(),
                }))
            })
            .unwrap();
    }
    let error = DirectRunner
        .run_sinks(&pipeline, &["left", "right"], &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(opens.load(Ordering::SeqCst), 0);

    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let opened = Arc::clone(&opens);
    source
        .sink_named_with_config_and_finalization(
            "finalizing",
            spec(),
            "sink-v1".into(),
            move || {
                opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(Probe {
                    aborts: Arc::new(AtomicUsize::new(0)),
                    commits: Arc::new(AtomicUsize::new(0)),
                    fail_write: false,
                    fail_commit: false,
                }))
            },
        )
        .unwrap();
    source
        .sink_named("other", spec(), || {
            Ok(Box::new(Probe {
                aborts: Arc::new(AtomicUsize::new(0)),
                commits: Arc::new(AtomicUsize::new(0)),
                fail_write: false,
                fail_commit: false,
            }))
        })
        .unwrap();
    let error = DirectRunner
        .run_sinks(&pipeline, &["finalizing", "other"], &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[test]
fn multi_sink_rejects_non_idempotent_commit_contracts_before_opening() {
    for invalid in [
        SinkSpec {
            mode: SinkMode::Update,
            ..spec()
        },
        SinkSpec {
            commit: false,
            ..spec()
        },
        SinkSpec {
            replay: Replay::AtLeastOnce,
            ..spec()
        },
    ] {
        let opens = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::default();
        let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
        for (key, sink_spec) in [("invalid", invalid), ("valid", spec())] {
            let opened = Arc::clone(&opens);
            source
                .sink_named(key, sink_spec, move || {
                    opened.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(Probe {
                        aborts: Arc::new(AtomicUsize::new(0)),
                        commits: Arc::new(AtomicUsize::new(0)),
                        fail_write: false,
                        fail_commit: false,
                    }))
                })
                .unwrap();
        }
        assert!(matches!(
            DirectRunner
                .run_sinks(&pipeline, &["invalid", "valid"], &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::UnsupportedCapability { .. }
        ));
        assert_eq!(opens.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn multi_sink_cancellation_after_first_write_aborts_every_session() {
    struct CancelOnWrite {
        cancellation: Arc<AtomicBool>,
        aborts: Arc<AtomicUsize>,
        commits: Arc<AtomicUsize>,
    }
    impl TypedSink<i32> for CancelOnWrite {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            self.cancellation.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let cancellation = Arc::new(AtomicBool::new(false));
    let aborts = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let cancel = Arc::clone(&cancellation);
    let aborted = Arc::clone(&aborts);
    let committed = Arc::clone(&commits);
    source
        .sink_named("cancel", spec(), move || {
            Ok(Box::new(CancelOnWrite {
                cancellation: Arc::clone(&cancel),
                aborts: Arc::clone(&aborted),
                commits: Arc::clone(&committed),
            }))
        })
        .unwrap();
    let aborted = Arc::clone(&aborts);
    let committed = Arc::clone(&commits);
    source
        .sink_named("other", spec(), move || {
            Ok(Box::new(Probe {
                aborts: Arc::clone(&aborted),
                commits: Arc::clone(&committed),
                fail_write: false,
                fail_commit: false,
            }))
        })
        .unwrap();
    let error = DirectRunner
        .run_sinks(
            &pipeline,
            &["cancel", "other"],
            &JobConfig {
                cancellation,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
    assert_eq!(commits.load(Ordering::SeqCst), 0);
}

#[test]
fn multi_sink_cancellation_after_source_end_aborts_before_commit() {
    let cancellation = Arc::new(AtomicBool::new(false));
    let aborts = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let token = Arc::clone(&cancellation);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/multi-sink-cancel-before-commit",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            None,
            move || {
                let token = Arc::clone(&token);
                let mut emitted = 0;
                Ok(Box::new(std::iter::from_fn(move || {
                    if emitted == 2 {
                        token.store(true, Ordering::SeqCst);
                        None
                    } else {
                        emitted += 1;
                        Some(Ok(1))
                    }
                })) as TypedSourceIterator<i32>)
            },
        )
        .unwrap();
    for key in ["first", "second"] {
        let aborted = Arc::clone(&aborts);
        let committed = Arc::clone(&commits);
        source
            .sink_named(key, spec(), move || {
                Ok(Box::new(Probe {
                    aborts: Arc::clone(&aborted),
                    commits: Arc::clone(&committed),
                    fail_write: false,
                    fail_commit: false,
                }))
            })
            .unwrap();
    }
    let error = DirectRunner
        .run_sinks(
            &pipeline,
            &["first", "second"],
            &JobConfig {
                buffer_budget_elements: 2,
                cancellation,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
    assert_eq!(commits.load(Ordering::SeqCst), 0);
}

#[test]
fn later_sink_factory_failure_aborts_open_session_before_source_runs() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let source_opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let opened = Arc::clone(&source_opens);
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/multi-sink-factory-failure",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            Some(1),
            move || {
                opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(Ok(1))) as TypedSourceIterator<i32>)
            },
        )
        .unwrap();
    let aborted = Arc::clone(&aborts);
    let committed = Arc::clone(&commits);
    source
        .sink_named("first", spec(), move || {
            Ok(Box::new(Probe {
                aborts: Arc::clone(&aborted),
                commits: Arc::clone(&committed),
                fail_write: false,
                fail_commit: false,
            }))
        })
        .unwrap();
    source
        .sink_named("broken", spec(), || {
            Err(Error::execution("broken", "direct", "factory failed"))
        })
        .unwrap();
    let error = DirectRunner
        .run_sinks(&pipeline, &["first", "broken"], &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
    assert_eq!(commits.load(Ordering::SeqCst), 0);
    assert_eq!(source_opens.load(Ordering::SeqCst), 0);
}

#[test]
fn multi_sink_rejects_undeclared_bundle_finalization_and_oversized_expansion() {
    struct FinalizingProbe(Arc<AtomicUsize>);
    impl TypedSink<i32> for FinalizingProbe {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn requires_bundle_finalization(&self) -> bool {
            true
        }
    }

    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let aborted = Arc::clone(&aborts);
    source
        .sink_named("finalizing", spec(), move || {
            Ok(Box::new(FinalizingProbe(Arc::clone(&aborted))))
        })
        .unwrap();
    source
        .sink_named("other", spec(), || {
            Ok(Box::new(Probe {
                aborts: Arc::new(AtomicUsize::new(0)),
                commits: Arc::new(AtomicUsize::new(0)),
                fail_write: false,
                fail_commit: false,
            }))
        })
        .unwrap();
    assert!(matches!(
        DirectRunner
            .run_sinks(&pipeline, &["finalizing", "other"], &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(aborts.load(Ordering::SeqCst), 1);

    let pipeline = Pipeline::default();
    let expanded = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .flat_map_named("expand", |value| vec![value, value + 1, value + 2])
        .unwrap();
    let aborts = Arc::new(AtomicUsize::new(0));
    for key in ["left", "right"] {
        let aborted = Arc::clone(&aborts);
        expanded
            .sink_named(key, spec(), move || {
                Ok(Box::new(Probe {
                    aborts: Arc::clone(&aborted),
                    commits: Arc::new(AtomicUsize::new(0)),
                    fail_write: false,
                    fail_commit: false,
                }))
            })
            .unwrap();
    }
    assert_eq!(
        DirectRunner
            .run_sinks(
                &pipeline,
                &["left", "right"],
                &JobConfig {
                    buffer_budget_elements: 2,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
}

#[test]
fn write_failure_aborts_every_open_session_before_commit() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    for (key, fail_write) in [("first", false), ("failing", true), ("third", false)] {
        let aborts = Arc::clone(&aborts);
        let commits = Arc::clone(&commits);
        source
            .sink_named(key, spec(), move || {
                Ok(Box::new(Probe {
                    aborts: Arc::clone(&aborts),
                    commits: Arc::clone(&commits),
                    fail_write,
                    fail_commit: false,
                }))
            })
            .unwrap();
    }
    let error = DirectRunner
        .run_sinks(
            &pipeline,
            &["first", "failing", "third"],
            &JobConfig::default(),
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(aborts.load(Ordering::SeqCst), 3);
    assert_eq!(commits.load(Ordering::SeqCst), 0);
}

#[test]
fn later_commit_failure_reports_partial_visibility_for_idempotent_retry() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    for key in ["first", "failing", "third"] {
        let aborts = Arc::clone(&aborts);
        let commits = Arc::clone(&commits);
        let fail_commit = key == "failing";
        source
            .sink_named(key, spec(), move || {
                Ok(Box::new(Probe {
                    aborts: Arc::clone(&aborts),
                    commits: Arc::clone(&commits),
                    fail_write: false,
                    fail_commit,
                }))
            })
            .unwrap();
    }
    let error = DirectRunner
        .run_sinks(
            &pipeline,
            &["first", "failing", "third"],
            &JobConfig::default(),
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert!(
        error
            .message
            .contains("earlier sinks may already be committed")
    );
    assert_eq!(commits.load(Ordering::SeqCst), 1);
    assert_eq!(aborts.load(Ordering::SeqCst), 3);
}

#[test]
fn fan_out_respects_the_configured_buffer_limit() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    for key in ["a", "b", "c"] {
        source
            .sink_named(key, spec(), || {
                Ok(Box::new(Capture {
                    rows: Arc::new(Mutex::new(Vec::new())),
                    pending: Vec::new(),
                }))
            })
            .unwrap();
    }
    let config = JobConfig {
        buffer_budget_elements: 2,
        ..JobConfig::default()
    };
    let error = DirectRunner
        .run_sinks(&pipeline, &["a", "b", "c"], &config)
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Resource);
}

#[test]
fn sink_route_attributes_flat_map_panic_and_aborts_session() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let commits = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .flat_map_named("explode", |_| -> Vec<i32> {
            panic!("expected transform panic")
        })
        .unwrap()
        .sink_named("result", spec(), {
            let aborts = Arc::clone(&aborts);
            let commits = Arc::clone(&commits);
            move || {
                Ok(Box::new(Probe {
                    aborts: Arc::clone(&aborts),
                    commits: Arc::clone(&commits),
                    fail_write: false,
                    fail_commit: false,
                }))
            }
        })
        .unwrap();
    let error = DirectRunner
        .run_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("explode"));
    assert_eq!(commits.load(Ordering::SeqCst), 0);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
}
