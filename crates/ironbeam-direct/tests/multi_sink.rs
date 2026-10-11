use ironbeam_core::{
    Element, Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress, TypedSink,
    TypedSourceIterator,
};
use ironbeam_direct::{DirectRunner, JobConfig};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn spec() -> SinkSpec {
    SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::Idempotent,
        commit: true,
    }
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
                Ok(Box::new([1].into_iter().map(Ok)) as TypedSourceIterator<i32>)
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
