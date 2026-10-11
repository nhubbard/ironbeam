use ironbeam_core::{
    Element, ElementMetadata, Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec,
    SourceProgress, TypedSink, Window, WindowInterval,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

const fn spec(replay: Replay) -> SinkSpec {
    SinkSpec {
        mode: SinkMode::Append,
        replay,
        commit: true,
    }
}

struct Noop;
impl TypedSink<i32> for Noop {
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

#[test]
fn pair_rejects_different_builders_duplicate_sinks_and_checkpointing() {
    let first = Pipeline::default();
    let input = first.from_vec("source", vec![1_i32]).unwrap();
    let left = input
        .sink_named("left", spec(Replay::Idempotent), || Ok(Box::new(Noop)))
        .unwrap();
    let right = input
        .sink_named("right", spec(Replay::Idempotent), || Ok(Box::new(Noop)))
        .unwrap();
    let no_commit = input
        .sink_named(
            "no-commit",
            SinkSpec {
                commit: false,
                ..spec(Replay::Idempotent)
            },
            || Ok(Box::new(Noop)),
        )
        .unwrap();
    let other = Pipeline::default()
        .from_vec("other-source", vec![1_i32])
        .unwrap()
        .sink_named("other", spec(Replay::Idempotent), || Ok(Box::new(Noop)))
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_sink_pair(&left, &other, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        DirectRunner
            .run_sink_pair(&left, &left, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert!(matches!(
        DirectRunner
            .run_sink_pair(&left, &no_commit, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            binary_build_id: "test-binary".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert!(matches!(
        DirectRunner
            .run_sink_pair(&left, &right, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let cancelled = JobConfig::default();
    cancelled.cancellation.store(true, Ordering::Release);
    assert_eq!(
        DirectRunner
            .run_sink_pair(&left, &right, &cancelled)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}

struct Capture<T: Element> {
    rows: Arc<Mutex<Vec<(T, ElementMetadata)>>>,
    commits: Arc<AtomicUsize>,
    aborts: Arc<AtomicUsize>,
}

impl<T: Element> TypedSink<T> for Capture<T> {
    fn write(&mut self, _: T) -> Result<(), Error> {
        Err(Error::incompatible("metadata hook skipped"))
    }
    fn write_with_metadata(&mut self, value: T, metadata: ElementMetadata) -> Result<(), Error> {
        self.rows.lock().unwrap().push((value, metadata));
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

#[test]
#[allow(clippy::too_many_lines)] // The fixture checks both branches and shared-source accounting.
fn two_sinks_share_source_and_transforms_and_keep_branch_metadata() {
    let opens = Arc::new(AtomicUsize::new(0));
    let common_calls = Arc::new(AtomicUsize::new(0));
    let left_rows = Arc::new(Mutex::new(Vec::new()));
    let right_rows = Arc::new(Mutex::new(Vec::new()));
    let left_commits = Arc::new(AtomicUsize::new(0));
    let right_commits = Arc::new(AtomicUsize::new(0));
    let left_aborts = Arc::new(AtomicUsize::new(0));
    let right_aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source_opens = Arc::clone(&opens);
    let source = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            Some(3),
            move || {
                source_opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new([Ok(1_i32), Ok(2), Ok(3)].into_iter()))
            },
        )
        .unwrap();
    let calls = Arc::clone(&common_calls);
    let common = source
        .map_named("common", move |value| {
            assert!(rayon::current_thread_index().is_some());
            calls.fetch_add(1, Ordering::SeqCst);
            value + 1
        })
        .unwrap()
        .assign_timestamps_named("time", |value| i64::from(*value) * 10)
        .unwrap()
        .fixed_windows("window", 20)
        .unwrap();
    let l_rows = Arc::clone(&left_rows);
    let l_commits = Arc::clone(&left_commits);
    let l_aborts = Arc::clone(&left_aborts);
    let left = common
        .map_named("double", |value| value * 2)
        .unwrap()
        .sink_named("left", spec(Replay::Idempotent), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&l_rows),
                commits: Arc::clone(&l_commits),
                aborts: Arc::clone(&l_aborts),
            }))
        })
        .unwrap();
    let r_rows = Arc::clone(&right_rows);
    let r_commits = Arc::clone(&right_commits);
    let r_aborts = Arc::clone(&right_aborts);
    let right = common
        .map_named("label", |value| format!("n={value}"))
        .unwrap()
        .sink_named("right", spec(Replay::Idempotent), move || {
            Ok(Box::new(Capture {
                rows: Arc::clone(&r_rows),
                commits: Arc::clone(&r_commits),
                aborts: Arc::clone(&r_aborts),
            }))
        })
        .unwrap();

    for (run, parallelism) in [(1, 1), (2, 3)] {
        left_rows.lock().unwrap().clear();
        right_rows.lock().unwrap().clear();
        let job = DirectRunner
            .run_sink_pair(
                &left,
                &right,
                &JobConfig {
                    parallelism,
                    buffer_budget_elements: 2,
                    ..JobConfig::default()
                },
            )
            .unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), run);
        assert_eq!(common_calls.load(Ordering::SeqCst), run * 3);
        assert_eq!(job.processed(), 6);
        assert_eq!(job.metrics().source_elements, 3);
        let expected_metadata = [
            ElementMetadata {
                timestamp_ns: 20,
                window: Window::Fixed { size_ns: 20 },
                assigned_window: Some(WindowInterval {
                    start_ns: 20,
                    end_ns: 40,
                }),
            },
            ElementMetadata {
                timestamp_ns: 30,
                window: Window::Fixed { size_ns: 20 },
                assigned_window: Some(WindowInterval {
                    start_ns: 20,
                    end_ns: 40,
                }),
            },
            ElementMetadata {
                timestamp_ns: 40,
                window: Window::Fixed { size_ns: 20 },
                assigned_window: Some(WindowInterval {
                    start_ns: 40,
                    end_ns: 60,
                }),
            },
        ];
        assert_eq!(
            *left_rows.lock().unwrap(),
            vec![
                (4, expected_metadata[0].clone()),
                (6, expected_metadata[1].clone()),
                (8, expected_metadata[2].clone()),
            ]
        );
        assert_eq!(
            *right_rows.lock().unwrap(),
            vec![
                ("n=2".to_owned(), expected_metadata[0].clone()),
                ("n=3".to_owned(), expected_metadata[1].clone()),
                ("n=4".to_owned(), expected_metadata[2].clone()),
            ]
        );
    }
    assert_eq!(left_commits.load(Ordering::SeqCst), 2);
    assert_eq!(right_commits.load(Ordering::SeqCst), 2);
    assert_eq!(left_aborts.load(Ordering::SeqCst), 0);
    assert_eq!(right_aborts.load(Ordering::SeqCst), 0);
}

#[test]
fn pair_rejects_non_idempotent_sink_before_opening_source() {
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let action = Arc::clone(&opens);
    let input = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            Some(1),
            move || {
                action.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new([Ok(1_i32)].into_iter()))
            },
        )
        .unwrap();
    let left = input
        .sink_named("left", spec(Replay::Idempotent), || {
            Ok(Box::new(Capture {
                rows: Arc::new(Mutex::new(Vec::new())),
                commits: Arc::new(AtomicUsize::new(0)),
                aborts: Arc::new(AtomicUsize::new(0)),
            }))
        })
        .unwrap();
    let right = input
        .sink_named("right", spec(Replay::AtLeastOnce), || {
            Ok(Box::new(Capture {
                rows: Arc::new(Mutex::new(Vec::new())),
                commits: Arc::new(AtomicUsize::new(0)),
                aborts: Arc::new(AtomicUsize::new(0)),
            }))
        })
        .unwrap();
    let error = DirectRunner
        .run_sink_pair(&left, &right, &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(opens.load(Ordering::SeqCst), 0);
}

#[test]
fn pair_aborts_first_sink_if_second_factory_fails() {
    struct AbortCounter(Arc<AtomicUsize>);
    impl TypedSink<i32> for AbortCounter {
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
    }
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let action = Arc::clone(&aborts);
    let left = input
        .sink_named("left", spec(Replay::Idempotent), move || {
            Ok(Box::new(AbortCounter(Arc::clone(&action))))
        })
        .unwrap();
    let right = input
        .sink_named("right", spec(Replay::Idempotent), || {
            Err(Error::io("right", "test", "factory failed"))
        })
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_sink_pair(&left, &right, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
}

#[test]
fn pair_aborts_both_if_first_commit_fails() {
    struct CommitFailure {
        fail: bool,
        aborts: Arc<AtomicUsize>,
    }
    impl TypedSink<i32> for CommitFailure {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            if self.fail {
                Err(Error::io("left", "test", "commit failed"))
            } else {
                Ok(())
            }
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let action = Arc::clone(&aborts);
    let left = input
        .sink_named("left", spec(Replay::Idempotent), move || {
            Ok(Box::new(CommitFailure {
                fail: true,
                aborts: Arc::clone(&action),
            }))
        })
        .unwrap();
    let action = Arc::clone(&aborts);
    let right = input
        .sink_named("right", spec(Replay::Idempotent), move || {
            Ok(Box::new(CommitFailure {
                fail: false,
                aborts: Arc::clone(&action),
            }))
        })
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_sink_pair(&left, &right, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
}

#[test]
fn pair_cancellation_during_first_write_aborts_both() {
    use std::sync::atomic::AtomicBool;
    struct CancelOnWrite(Arc<AtomicBool>, Arc<AtomicUsize>);
    impl TypedSink<i32> for CancelOnWrite {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            self.0.store(true, Ordering::Release);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let cancellation = Arc::new(AtomicBool::new(false));
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let c = Arc::clone(&cancellation);
    let a = Arc::clone(&aborts);
    let left = input
        .sink_named("left", spec(Replay::Idempotent), move || {
            Ok(Box::new(CancelOnWrite(Arc::clone(&c), Arc::clone(&a))))
        })
        .unwrap();
    let a = Arc::clone(&aborts);
    let right = input
        .sink_named("right", spec(Replay::Idempotent), move || {
            Ok(Box::new(AbortOnly(Arc::clone(&a))))
        })
        .unwrap();
    let config = JobConfig {
        cancellation,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_sink_pair(&left, &right, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
}

#[test]
fn pair_cancellation_after_right_write_stops_before_commit_or_next_batch() {
    use std::sync::atomic::AtomicBool;
    struct CancelRight(Arc<AtomicBool>, Arc<AtomicUsize>);
    impl TypedSink<i32> for CancelRight {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            self.0.store(true, Ordering::Release);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.1.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    for rows in [1, 2] {
        let cancellation = Arc::new(AtomicBool::new(false));
        let aborts = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::default();
        let input = pipeline
            .from_vec("source", (0..rows).collect::<Vec<i32>>())
            .unwrap();
        let a = Arc::clone(&aborts);
        let left = input
            .sink_named("left", spec(Replay::Idempotent), move || {
                Ok(Box::new(AbortOnly(Arc::clone(&a))))
            })
            .unwrap();
        let c = Arc::clone(&cancellation);
        let a = Arc::clone(&aborts);
        let right = input
            .sink_named("right", spec(Replay::Idempotent), move || {
                Ok(Box::new(CancelRight(Arc::clone(&c), Arc::clone(&a))))
            })
            .unwrap();
        let config = JobConfig {
            cancellation,
            buffer_budget_elements: 1,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner
                .run_sink_pair(&left, &right, &config)
                .err()
                .unwrap()
                .kind,
            ErrorKind::Cancelled
        );
        assert_eq!(aborts.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn pair_cancellation_at_source_completion_aborts_before_commit() {
    use std::sync::atomic::AtomicBool;
    let cancellation = Arc::new(AtomicBool::new(false));
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let action = Arc::clone(&cancellation);
    let input = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "completion-cancel-v1".into(),
            Some(1),
            move || {
                let action = Arc::clone(&action);
                let mut emitted = false;
                Ok(Box::new(std::iter::from_fn(move || {
                    if emitted {
                        action.store(true, Ordering::Release);
                        None
                    } else {
                        emitted = true;
                        Some(Ok(1_i32))
                    }
                })))
            },
        )
        .unwrap();
    let a = Arc::clone(&aborts);
    let left = input
        .sink_named("left", spec(Replay::Idempotent), move || {
            Ok(Box::new(AbortOnly(Arc::clone(&a))))
        })
        .unwrap();
    let a = Arc::clone(&aborts);
    let right = input
        .sink_named("right", spec(Replay::Idempotent), move || {
            Ok(Box::new(AbortOnly(Arc::clone(&a))))
        })
        .unwrap();
    let config = JobConfig {
        cancellation,
        buffer_budget_elements: 1,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_sink_pair(&left, &right, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
}

#[test]
fn pair_writes_each_batch_before_reading_the_next() {
    struct CountingSink(Arc<AtomicUsize>);
    impl TypedSink<i32> for CountingSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let writes = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let read_writes = Arc::clone(&writes);
    let input = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "backpressure-v1".into(),
            Some(2),
            move || {
                let seen = Arc::clone(&read_writes);
                Ok(Box::new((0..2).map(move |value| {
                    if value == 1 {
                        assert_eq!(seen.load(Ordering::SeqCst), 2);
                    }
                    Ok(value)
                })))
            },
        )
        .unwrap();
    let action = Arc::clone(&writes);
    let left = input
        .sink_named("left", spec(Replay::Idempotent), move || {
            Ok(Box::new(CountingSink(Arc::clone(&action))))
        })
        .unwrap();
    let action = Arc::clone(&writes);
    let right = input
        .sink_named("right", spec(Replay::Idempotent), move || {
            Ok(Box::new(CountingSink(Arc::clone(&action))))
        })
        .unwrap();
    DirectRunner
        .run_sink_pair(
            &left,
            &right,
            &JobConfig {
                parallelism: 1,
                buffer_budget_elements: 1,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 4);
}

#[test]
fn parallel_pair_writes_before_transforming_the_next_chunk() {
    struct FirstWriteCheck {
        transformed: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
    }
    impl TypedSink<i32> for FirstWriteCheck {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            if self.writes.fetch_add(1, Ordering::SeqCst) == 0 {
                assert_eq!(self.transformed.load(Ordering::SeqCst), 2);
            }
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let transformed = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2, 3, 4]).unwrap();
    let count = Arc::clone(&transformed);
    let shared = source
        .map_named("count", move |value| {
            count.fetch_add(1, Ordering::SeqCst);
            value
        })
        .unwrap();
    let make_sink = |transformed: Arc<AtomicUsize>, writes: Arc<AtomicUsize>| {
        move || {
            Ok(Box::new(FirstWriteCheck {
                transformed: Arc::clone(&transformed),
                writes: Arc::clone(&writes),
            }) as Box<dyn TypedSink<i32>>)
        }
    };
    let left = shared
        .sink_named(
            "left",
            spec(Replay::Idempotent),
            make_sink(Arc::clone(&transformed), Arc::clone(&writes)),
        )
        .unwrap();
    let right = shared
        .sink_named(
            "right",
            spec(Replay::Idempotent),
            make_sink(Arc::clone(&transformed), Arc::clone(&writes)),
        )
        .unwrap();
    DirectRunner
        .run_sink_pair(
            &left,
            &right,
            &JobConfig {
                parallelism: 2,
                buffer_budget_elements: 4,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(transformed.load(Ordering::SeqCst), 4);
    assert_eq!(writes.load(Ordering::SeqCst), 8);
}

struct AbortOnly(Arc<AtomicUsize>);
impl TypedSink<i32> for AbortOnly {
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
}

#[test]
fn pair_aborts_both_before_commit_when_a_branch_write_fails() {
    struct FailingWrite {
        fail: bool,
        commits: Arc<AtomicUsize>,
        aborts: Arc<AtomicUsize>,
    }
    impl TypedSink<i32> for FailingWrite {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            if self.fail {
                Err(Error::io("right", "test", "injected write failure"))
            } else {
                Ok(())
            }
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
    let commits = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let c = Arc::clone(&commits);
    let a = Arc::clone(&aborts);
    let left = input
        .sink_named("left", spec(Replay::Idempotent), move || {
            Ok(Box::new(FailingWrite {
                fail: false,
                commits: Arc::clone(&c),
                aborts: Arc::clone(&a),
            }))
        })
        .unwrap();
    let c = Arc::clone(&commits);
    let a = Arc::clone(&aborts);
    let right = input
        .sink_named("right", spec(Replay::Idempotent), move || {
            Ok(Box::new(FailingWrite {
                fail: true,
                commits: Arc::clone(&c),
                aborts: Arc::clone(&a),
            }))
        })
        .unwrap();
    let error = DirectRunner
        .run_sink_pair(&left, &right, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Io);
    assert_eq!(commits.load(Ordering::SeqCst), 0);
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
}

#[test]
fn pair_second_commit_failure_preserves_first_commit_for_idempotent_retry() {
    struct CommitSink {
        fail: bool,
        commits: Arc<AtomicUsize>,
        aborts: Arc<AtomicUsize>,
    }
    impl TypedSink<i32> for CommitSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(Error::io("right", "test", "injected commit failure"))
            } else {
                Ok(())
            }
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let left_commits = Arc::new(AtomicUsize::new(0));
    let right_commits = Arc::new(AtomicUsize::new(0));
    let left_aborts = Arc::new(AtomicUsize::new(0));
    let right_aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let c = Arc::clone(&left_commits);
    let a = Arc::clone(&left_aborts);
    let left = input
        .sink_named("left", spec(Replay::Idempotent), move || {
            Ok(Box::new(CommitSink {
                fail: false,
                commits: Arc::clone(&c),
                aborts: Arc::clone(&a),
            }))
        })
        .unwrap();
    let c = Arc::clone(&right_commits);
    let a = Arc::clone(&right_aborts);
    let right = input
        .sink_named("right", spec(Replay::Idempotent), move || {
            Ok(Box::new(CommitSink {
                fail: true,
                commits: Arc::clone(&c),
                aborts: Arc::clone(&a),
            }))
        })
        .unwrap();
    let error = DirectRunner
        .run_sink_pair(&left, &right, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Io);
    assert!(
        error
            .message
            .contains("left sink left is already committed")
    );
    assert_eq!(left_commits.load(Ordering::SeqCst), 1);
    assert_eq!(right_commits.load(Ordering::SeqCst), 1);
    assert_eq!(left_aborts.load(Ordering::SeqCst), 0);
    assert_eq!(right_aborts.load(Ordering::SeqCst), 1);
}
