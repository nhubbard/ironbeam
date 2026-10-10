use ironbeam_core::{Capabilities, ErrorKind, Pipeline};
use ironbeam_direct::{CheckpointConfig, DirectCollectExt, DirectRunner, JobConfig};

#[test]
fn grouping_join_and_combine_execute_from_typed_graph() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec(
            "left",
            vec![(1_i32, "a".to_string()), (1, "b".into()), (2, "c".into())],
        )
        .unwrap();
    let right = pipeline
        .from_vec("right", vec![(1_i32, 10_i32), (1, 20), (3, 30)])
        .unwrap();
    let grouped = left
        .group_by_key_named("group")
        .unwrap()
        .output("groups")
        .unwrap();
    let joined = left
        .join_inner_named("join", &right)
        .unwrap()
        .output("pairs")
        .unwrap();
    let summed = right
        .map_named("values", |(_, v)| v)
        .unwrap()
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("total")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph
            .validate_for("other", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "group-by-key".into()
        }
    );
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            buffer_budget_elements: 2,
            ..JobConfig::default()
        };
        let (groups, _) = DirectRunner.run_collect(&grouped, &config).unwrap();
        assert_eq!(
            groups,
            vec![
                (1, vec!["a".to_string(), "b".into()]),
                (2, vec!["c".into()])
            ]
        );
        let (pairs, _) = DirectRunner.run_collect(&joined, &config).unwrap();
        assert_eq!(
            pairs,
            vec![
                (1, ("a".to_string(), 10)),
                (1, ("b".to_string(), 10)),
                (1, ("a".to_string(), 20)),
                (1, ("b".to_string(), 20))
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&summed, &config).unwrap().0,
            vec![60]
        );
    }
}

#[test]
fn outer_joins_preserve_duplicates_and_missing_sides() {
    let pipeline = Pipeline::default();
    let left = pipeline
        .from_vec(
            "left-outer",
            vec![(1_i32, "a".to_string()), (1, "b".into()), (2, "c".into())],
        )
        .unwrap();
    let right = pipeline
        .from_vec("right-outer", vec![(1_i32, 10_i32), (1, 20), (3, 30)])
        .unwrap();
    let left_out = left
        .join_left_named("left-join", &right)
        .unwrap()
        .output("left-result")
        .unwrap();
    let right_out = left
        .join_right_named("right-join", &right)
        .unwrap()
        .output("right-result")
        .unwrap();
    let full_out = left
        .join_full_named("full-join", &right)
        .unwrap()
        .output("full-result")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph
            .validate_for("preview", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "join-outer".into()
        }
    );
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        assert_eq!(
            DirectRunner.run_collect(&left_out, &config).unwrap().0,
            vec![
                (1, ("a".into(), Some(10))),
                (1, ("b".into(), Some(10))),
                (1, ("a".into(), Some(20))),
                (1, ("b".into(), Some(20))),
                (2, ("c".into(), None))
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&right_out, &config).unwrap().0,
            vec![
                (1, (Some("a".into()), 10)),
                (1, (Some("b".into()), 10)),
                (1, (Some("a".into()), 20)),
                (1, (Some("b".into()), 20)),
                (3, (None, 30))
            ]
        );
        assert_eq!(
            DirectRunner.run_collect(&full_out, &config).unwrap().0,
            vec![
                (1, (Some("a".into()), Some(10))),
                (1, (Some("b".into()), Some(10))),
                (1, (Some("a".into()), Some(20))),
                (1, (Some("b".into()), Some(20))),
                (3, (None, Some(30))),
                (2, (Some("c".into()), None))
            ]
        );
    }
}

#[test]
fn aggregate_checkpoint_and_marker_are_rejected_before_source() {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    let output = input
        .combine_globally_named("sum", |v| v.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "build".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert!(matches!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let marker = input
        .global_combine_marker("marker")
        .unwrap()
        .output("marker-out")
        .unwrap();
    assert!(matches!(
        DirectRunner
            .run_collect(&marker, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
}

#[test]
fn bounded_side_input_is_explicit_and_executes() {
    let pipeline = Pipeline::default();
    let main = pipeline.from_vec("main", vec![1_i32, 2, 3]).unwrap();
    let side = pipeline.from_vec("side", vec![10_i32, 20]).unwrap();
    let output = main
        .map_with_side_named("add-side", &side, |x, values| {
            x + values.iter().sum::<i32>()
        })
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert_eq!(
        graph.node("add-side").unwrap().additional_inputs[0].key,
        "side"
    );
    assert_eq!(
        graph
            .validate_for("other", &Capabilities::default())
            .unwrap_err()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "side-input-map".into()
        }
    );
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![31, 32, 33]
    );
    assert_eq!(output.collect_seq().unwrap(), vec![31, 32, 33]);
    assert_eq!(output.collect_par(2).unwrap(), vec![31, 32, 33]);
}

#[test]
fn deferred_sink_commits_only_after_success_and_aborts_on_write_failure() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    use std::sync::{Arc, Mutex};
    struct RecordingSink {
        events: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }
    impl TypedSink<i32> for RecordingSink {
        fn write(&mut self, value: i32) -> Result<(), Error> {
            self.events.lock().unwrap().push("write");
            if self.fail && value == 2 {
                return Err(Error::io("sink", "test", "injected failure"));
            }
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            self.events.lock().unwrap().push("commit");
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.events.lock().unwrap().push("abort");
            Ok(())
        }
    }
    for fail in [false, true] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
        let cloned = events.clone();
        let sink = input
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::FinalResult,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                move || {
                    Ok(Box::new(RecordingSink {
                        events: cloned.clone(),
                        fail,
                    }))
                },
            )
            .unwrap();
        let result = DirectRunner.run_sink(&sink, &JobConfig::default());
        if fail {
            assert_eq!(result.err().unwrap().kind, ErrorKind::Io);
            assert_eq!(*events.lock().unwrap(), ["write", "write", "abort"]);
        } else {
            assert!(result.is_ok());
            assert_eq!(*events.lock().unwrap(), ["write", "write", "commit"]);
        }
    }
}

#[test]
fn aggregate_sink_runs_and_rejects_checkpoint_before_source() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    use std::sync::{Arc, Mutex};
    struct SumSink(Arc<Mutex<Vec<i32>>>);
    impl TypedSink<i32> for SumSink {
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
    let pipeline = Pipeline::default();
    let count = pipeline
        .from_vec("source", vec![1_i32, 2, 3])
        .unwrap()
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap();
    let values = Arc::new(Mutex::new(Vec::new()));
    let cloned = values.clone();
    let sink = count
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            move || Ok(Box::new(SumSink(cloned.clone()))),
        )
        .unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(*values.lock().unwrap(), [6]);
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: dir.path().to_path_buf(),
            binary_build_id: "test".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_sink(&sink, &config).err().unwrap().kind,
        ErrorKind::UnsupportedCapability {
            capability: "sink-checkpoint".into()
        }
    );
}

#[test]
fn materialized_path_handles_filter_flatten_failure_and_budget() {
    use ironbeam_core::Error;
    let pipeline = Pipeline::default();
    let left = pipeline.from_vec("left", vec![1_i32, 2, 3]).unwrap();
    let right = pipeline.from_vec("right", vec![10_i32]).unwrap();
    let combined = left
        .filter_named("odd", |value| value % 2 == 1)
        .unwrap()
        .flatten_named("flat", &[right])
        .unwrap();
    let result = combined
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&result, &JobConfig::default())
            .unwrap()
            .0,
        [14]
    );
    let budget = JobConfig {
        memory_budget_bytes: 1,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&result, &budget)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );

    let pipeline = Pipeline::default();
    let input = pipeline
        .bounded_source(
            "source",
            ironbeam_core::SourceProgress {
                stable_split_id: true,
                replay: ironbeam_core::Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "source-v1".into(),
            None,
            || {
                Ok(Box::new(
                    vec![Err::<i32, _>(Error::io("source", "test", "read"))].into_iter(),
                ))
            },
        )
        .unwrap();
    let output = input
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );

    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .map_named("panic", |_| -> i32 { panic!("transform") })
        .unwrap()
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );

    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .combine_globally_named("panic", |_| -> i32 { panic!("aggregate") })
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn sink_commit_failures_abort_and_cancelled_job_does_not_open_sink() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct FailingCommit(Arc<AtomicUsize>);
    impl TypedSink<i32> for FailingCommit {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Err(Error::io("sink", "test", "commit failed"))
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let count = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let copy = count.clone();
    let sink = input
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            move || {
                copy.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(FailingCommit(copy.clone())))
            },
        )
        .unwrap();
    let cancelled = JobConfig {
        cancellation: Arc::new(AtomicBool::new(true)),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_sink(&sink, &cancelled).err().unwrap().kind,
        ErrorKind::Cancelled
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[test]
fn sink_panics_and_missing_commit_contract_are_typed_errors() {
    use ironbeam_core::{Error, Replay, SinkMode, SinkSpec, TypedSink};
    struct PanickingSink(bool);
    impl TypedSink<i32> for PanickingSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            assert!(!self.0, "write panic");
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            panic!("commit panic")
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    for write_panic in [true, false] {
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
        let sink = input
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::FinalResult,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                move || Ok(Box::new(PanickingSink(write_panic))),
            )
            .unwrap();
        assert_eq!(
            DirectRunner
                .run_sink(&sink, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
    }
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let sink = input
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            || -> Result<Box<dyn TypedSink<i32>>, Error> { panic!("factory panic") },
        )
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Execution
    );
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let sink = input
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: false,
            },
            || Ok(Box::new(PanickingSink(false))),
        )
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability {
            capability: "sink-commit".into()
        }
    );
}

#[test]
fn aggregate_empty_input_and_mid_job_cancellation_are_defined() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let pipeline = Pipeline::default();
    let output = pipeline
        .from_vec("source", Vec::<i32>::new())
        .unwrap()
        .combine_globally_named("count", |values| i32::try_from(values.len()).unwrap())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [0]
    );
    let cancelled = Arc::new(AtomicBool::new(false));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let flag = cancelled.clone();
    let output = input
        .combine_globally_named("combine", move |values| {
            flag.store(true, Ordering::Release);
            values.into_iter().sum::<i32>()
        })
        .unwrap()
        .output("out")
        .unwrap();
    let config = JobConfig {
        cancellation: cancelled,
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner
            .run_collect(&output, &config)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Cancelled
    );
}

#[test]
fn durable_sink_requires_an_element_coder_before_running() {
    use ironbeam_core::{Element, Error, Replay, SinkMode, SinkSpec, TypedSink};
    #[derive(Clone)]
    struct Opaque;
    impl Element for Opaque {
        const TYPE_ID: &'static str = "tests/opaque";
        const CODER_ID: &'static str = "tests/opaque";
    }
    struct Noop;
    impl TypedSink<Opaque> for Noop {
        fn write(&mut self, _: Opaque) -> Result<(), Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("source", vec![Opaque]).unwrap();
    let sink = input
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::FinalResult,
                replay: Replay::Idempotent,
                commit: true,
            },
            || Ok(Box::new(Noop)),
        )
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = JobConfig {
        spill: Some(ironbeam_direct::SpillConfig {
            directory: dir.path().to_path_buf(),
            threshold_bytes: 1,
        }),
        ..JobConfig::default()
    };
    assert_eq!(
        DirectRunner.run_sink(&sink, &config).err().unwrap().kind,
        ErrorKind::UnsupportedCapability {
            capability: "element-coder-for-durable-output".into()
        }
    );
}

#[test]
fn unrelated_aggregate_marker_does_not_block_selected_output() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32, 2]).unwrap();
    source.global_combine_marker("unrelated-marker").unwrap();
    let output = source
        .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [3]
    );
}

#[test]
fn source_factory_and_iterator_panics_are_typed_execution_errors() {
    use ironbeam_core::{Error, Replay, SourceProgress, TypedSourceIterator};
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: false,
    };
    for materialized in [false, true] {
        let pipeline = Pipeline::default();
        let input = pipeline
            .bounded_source::<i32>(
                "source",
                progress.clone(),
                "source-v1".into(),
                None,
                || -> Result<TypedSourceIterator<i32>, Error> { panic!("factory") },
            )
            .unwrap();
        let output = if materialized {
            input
                .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
                .unwrap()
                .output("out")
                .unwrap()
        } else {
            input.output("out").unwrap()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
        let pipeline = Pipeline::default();
        let input = pipeline
            .bounded_source::<i32>("source", progress.clone(), "source-v1".into(), None, || {
                Ok(Box::new(std::iter::from_fn(
                    || -> Option<Result<i32, Error>> { panic!("iterator") },
                )))
            })
            .unwrap();
        let output = if materialized {
            input
                .combine_globally_named("sum", |values| values.into_iter().sum::<i32>())
                .unwrap()
                .output("out")
                .unwrap()
        } else {
            input.output("out").unwrap()
        };
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Execution
        );
    }
}
