use ironbeam_core::{
    ElementMetadata, Error, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress, TypedSink,
    Window, WindowInterval,
};
use ironbeam_direct::{DirectRunner, JobConfig};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct TimedSink(Arc<Mutex<Vec<(i32, ElementMetadata)>>>);

impl TypedSink<i32> for TimedSink {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        Err(Error::incompatible("sink metadata hook was skipped"))
    }
    fn write_with_metadata(&mut self, value: i32, metadata: ElementMetadata) -> Result<(), Error> {
        self.0.lock().unwrap().push((value, metadata));
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
fn direct_sinks_keep_event_time_and_window_metadata_on_both_routes() {
    for materialized in [false, true] {
        for parallelism in [1, 3] {
            let pipeline = Pipeline::default();
            let values = pipeline
                .from_vec("input", vec![(-2_i64, 7_i32), (12, 9)])
                .unwrap()
                .timestamps_from_pairs_named("event-time")
                .unwrap()
                .fixed_windows("windows", 10)
                .unwrap();
            let values = if materialized {
                values.reshuffle_named("reshuffle").unwrap()
            } else {
                values
            };
            let captured = Arc::new(Mutex::new(Vec::new()));
            let action = Arc::clone(&captured);
            let sink = values
                .sink_named(
                    "capture",
                    SinkSpec {
                        mode: SinkMode::Append,
                        replay: Replay::AtLeastOnce,
                        commit: true,
                    },
                    move || Ok(Box::new(TimedSink(Arc::clone(&action)))),
                )
                .unwrap();
            let job = DirectRunner
                .run_sink(
                    &sink,
                    &JobConfig {
                        parallelism,
                        ..JobConfig::default()
                    },
                )
                .unwrap();
            assert_eq!(job.processed(), 2);
            assert_eq!(
                *captured.lock().unwrap(),
                vec![
                    (
                        7,
                        ElementMetadata {
                            timestamp_ns: -2,
                            window: Window::Fixed { size_ns: 10 },
                            assigned_window: Some(WindowInterval {
                                start_ns: -10,
                                end_ns: 0
                            })
                        }
                    ),
                    (
                        9,
                        ElementMetadata {
                            timestamp_ns: 12,
                            window: Window::Fixed { size_ns: 10 },
                            assigned_window: Some(WindowInterval {
                                start_ns: 10,
                                end_ns: 20
                            })
                        }
                    ),
                ]
            );
        }
    }
}

#[test]
fn direct_sink_writes_before_reading_the_next_source_batch() {
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
    let source = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "streaming-test-v1".into(),
            Some(2),
            move || {
                let writes = Arc::clone(&read_writes);
                Ok(Box::new((0..2).map(move |value| {
                    if value == 1 {
                        assert_eq!(writes.load(Ordering::SeqCst), 1);
                    }
                    Ok(value)
                })))
            },
        )
        .unwrap();
    let action = Arc::clone(&writes);
    let sink = source
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            move || Ok(Box::new(CountingSink(Arc::clone(&action)))),
        )
        .unwrap();
    DirectRunner
        .run_sink(
            &sink,
            &JobConfig {
                buffer_budget_elements: 1,
                parallelism: 3,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 2);
}

#[test]
fn typed_vector_sink_writes_each_batch_before_transforming_the_next() {
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
    let inspected = Arc::clone(&writes);
    let pipeline = Pipeline::default();
    let sink = pipeline
        .from_vec("source", vec![1_i32, 2])
        .unwrap()
        .map_named("inspect-prior-write", move |value| {
            if value == 2 {
                assert_eq!(inspected.load(Ordering::SeqCst), 1);
            }
            value
        })
        .unwrap()
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            {
                let writes = Arc::clone(&writes);
                move || Ok(Box::new(CountingSink(Arc::clone(&writes))))
            },
        )
        .unwrap();
    let job = DirectRunner
        .run_sink(
            &sink,
            &JobConfig {
                buffer_budget_elements: 1,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 2);
    assert_eq!(job.metrics().peak_batch_elements, 1);
}

#[test]
fn direct_sink_counts_filtered_rows_on_both_execution_modes() {
    for parallelism in [1, 3] {
        let pipeline = Pipeline::default();
        let filtered = pipeline
            .from_vec("source", vec![1_i32, 2, 3, 4])
            .unwrap()
            .filter_named("evens", |value| *value % 2 == 0)
            .unwrap();
        let captured = Arc::new(Mutex::new(Vec::new()));
        struct ValueSink(Arc<Mutex<Vec<i32>>>);
        impl TypedSink<i32> for ValueSink {
            fn write(&mut self, value: i32) -> Result<(), Error> {
                self.0.lock().unwrap().push(value);
                Ok(())
            }
            fn commit(&mut self) -> Result<(), Error> {
                Ok(())
            }
            fn abort(&mut self) -> Result<(), Error> {
                Ok(())
            }
        }
        let action = Arc::clone(&captured);
        let sink = filtered
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::AtLeastOnce,
                    commit: true,
                },
                move || Ok(Box::new(ValueSink(Arc::clone(&action)))),
            )
            .unwrap();
        let job = DirectRunner
            .run_sink(
                &sink,
                &JobConfig {
                    parallelism,
                    buffer_budget_elements: 2,
                    ..JobConfig::default()
                },
            )
            .unwrap();
        assert_eq!(*captured.lock().unwrap(), vec![2, 4]);
        assert_eq!(job.processed(), 2);
        assert_eq!(job.metrics().filtered_elements, 2);
    }
}

#[test]
fn direct_sink_stops_before_reading_another_batch_after_cancellation() {
    use std::sync::atomic::AtomicBool;
    struct CancellingSink(Arc<AtomicBool>);
    impl TypedSink<i32> for CancellingSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            self.0.store(true, Ordering::Release);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    for vector_source in [true, false] {
        let cancellation = Arc::new(AtomicBool::new(false));
        let pipeline = Pipeline::default();
        let values = if vector_source {
            pipeline.from_vec("source", vec![1_i32, 2]).unwrap()
        } else {
            pipeline
                .bounded_source(
                    "source",
                    SourceProgress {
                        stable_split_id: true,
                        replay: Replay::AtLeastOnce,
                        reports_event_time: false,
                    },
                    "cancel-source-v1".into(),
                    Some(2),
                    || Ok(Box::new([Ok(1_i32), Ok(2)].into_iter())),
                )
                .unwrap()
        };
        let action = Arc::clone(&cancellation);
        let sink = values
            .sink_named(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::AtLeastOnce,
                    commit: true,
                },
                move || Ok(Box::new(CancellingSink(Arc::clone(&action)))),
            )
            .unwrap();
        let error = DirectRunner
            .run_sink(
                &sink,
                &JobConfig {
                    cancellation,
                    buffer_budget_elements: 1,
                    ..JobConfig::default()
                },
            )
            .err()
            .unwrap();
        assert_eq!(error.kind, ironbeam_core::ErrorKind::Cancelled);
    }
}

#[test]
fn direct_sink_attributes_vector_clone_panics_to_the_source() {
    struct PanickingClone;
    impl Clone for PanickingClone {
        fn clone(&self) -> Self {
            panic!("clone failed")
        }
    }
    impl ironbeam_core::Element for PanickingClone {
        const TYPE_ID: &'static str = "test/panicking-clone";
        const CODER_ID: &'static str = "test/panicking-clone-coder";
    }
    struct NoopSink;
    impl TypedSink<PanickingClone> for NoopSink {
        fn write(&mut self, _: PanickingClone) -> Result<(), Error> {
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
    let sink = pipeline
        .from_vec("source", vec![PanickingClone])
        .unwrap()
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            || Ok(Box::new(NoopSink)),
        )
        .unwrap();
    let error = DirectRunner
        .run_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ironbeam_core::ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("source"));
}

#[test]
fn direct_sink_attributes_transform_panics_to_the_operation() {
    struct NoopSink;
    impl TypedSink<i32> for NoopSink {
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
    let pipeline = Pipeline::default();
    let sink = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .map_named("explode", |_| -> i32 { panic!("transform failed") })
        .unwrap()
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            || Ok(Box::new(NoopSink)),
        )
        .unwrap();
    let error = DirectRunner
        .run_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ironbeam_core::ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("explode"));
}

#[test]
fn direct_sink_stops_between_operations_when_cancelled() {
    use std::sync::atomic::AtomicBool;
    let cancellation = Arc::new(AtomicBool::new(false));
    let pipeline = Pipeline::default();
    let trigger = Arc::clone(&cancellation);
    let sink = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .map_named("cancel", move |value| {
            trigger.store(true, Ordering::Release);
            value
        })
        .unwrap()
        .map_named("unreachable", |_| panic!("ran after cancellation"))
        .unwrap()
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            || Ok(Box::new(TimedSink(Arc::new(Mutex::new(Vec::new()))))),
        )
        .unwrap();
    let error = DirectRunner
        .run_sink(
            &sink,
            &JobConfig {
                cancellation,
                parallelism: 1,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ironbeam_core::ErrorKind::Cancelled);
}
