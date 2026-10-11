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
fn grouped_sink_drains_completed_groups_and_aborts_on_write_failure() {
    type GroupRows = Arc<Mutex<Vec<(i32, Vec<i32>)>>>;
    struct GroupSink {
        rows: GroupRows,
        aborts: Arc<AtomicUsize>,
        fail_first: bool,
    }
    impl TypedSink<(i32, Vec<i32>)> for GroupSink {
        fn write(&mut self, row: (i32, Vec<i32>)) -> Result<(), Error> {
            if self.fail_first {
                return Err(Error::resource("group-sink", "injected write failure"));
            }
            self.rows.lock().unwrap().push(row);
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    for fail_first in [false, true] {
        let rows = Arc::new(Mutex::new(Vec::new()));
        let aborts = Arc::new(AtomicUsize::new(0));
        let pipeline = Pipeline::default();
        let grouped = pipeline
            .from_vec("source", vec![(1_i32, 2_i32), (2, 3), (1, 4), (2, 5)])
            .unwrap()
            .map_named("double", |(key, value)| (key, value * 2))
            .unwrap()
            .group_by_key_named("group")
            .unwrap();
        let sink = grouped
            .sink_named(
                "group-sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::AtLeastOnce,
                    commit: true,
                },
                {
                    let rows = Arc::clone(&rows);
                    let aborts = Arc::clone(&aborts);
                    move || {
                        Ok(Box::new(GroupSink {
                            rows: Arc::clone(&rows),
                            aborts: Arc::clone(&aborts),
                            fail_first,
                        }))
                    }
                },
            )
            .unwrap();
        let result = DirectRunner.run_sink(&sink, &JobConfig::default());
        if fail_first {
            assert_eq!(
                result.err().unwrap().kind,
                ironbeam_core::ErrorKind::Resource
            );
            assert!(rows.lock().unwrap().is_empty());
            assert_eq!(aborts.load(Ordering::SeqCst), 1);
        } else {
            let job = result.unwrap();
            assert_eq!(
                *rows.lock().unwrap(),
                vec![(1, vec![4, 8]), (2, vec![6, 10])]
            );
            assert_eq!(job.metrics().source_elements, 4);
            assert_eq!(job.metrics().output_elements, 2);
            assert_eq!(aborts.load(Ordering::SeqCst), 0);
        }
    }
}

#[test]
fn grouped_sink_drain_preserves_window_metadata() {
    type WindowedGroupRows = Arc<Mutex<Vec<((i32, Vec<i32>), ElementMetadata)>>>;
    struct WindowedGroupSink(WindowedGroupRows);
    impl TypedSink<(i32, Vec<i32>)> for WindowedGroupSink {
        fn write(&mut self, _: (i32, Vec<i32>)) -> Result<(), Error> {
            Err(Error::incompatible("group metadata hook was skipped"))
        }
        fn write_with_metadata(
            &mut self,
            row: (i32, Vec<i32>),
            metadata: ElementMetadata,
        ) -> Result<(), Error> {
            self.0.lock().unwrap().push((row, metadata));
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn abort(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }
    let rows = Arc::new(Mutex::new(Vec::new()));
    let pipeline = Pipeline::default();
    let grouped = pipeline
        .from_vec("source", vec![(2_i64, (1_i32, 7_i32)), (12, (1, 9))])
        .unwrap()
        .timestamps_from_pairs_named("event-time")
        .unwrap()
        .fixed_windows("windows", 10)
        .unwrap()
        .group_by_key_named("group")
        .unwrap();
    let sink = grouped
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            {
                let rows = Arc::clone(&rows);
                move || Ok(Box::new(WindowedGroupSink(Arc::clone(&rows))))
            },
        )
        .unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(
        *rows.lock().unwrap(),
        vec![
            (
                (1, vec![7]),
                ElementMetadata {
                    timestamp_ns: 2,
                    window: Window::Fixed { size_ns: 10 },
                    assigned_window: Some(WindowInterval {
                        start_ns: 0,
                        end_ns: 10,
                    }),
                },
            ),
            (
                (1, vec![9]),
                ElementMetadata {
                    timestamp_ns: 12,
                    window: Window::Fixed { size_ns: 10 },
                    assigned_window: Some(WindowInterval {
                        start_ns: 10,
                        end_ns: 20,
                    }),
                },
            ),
        ]
    );
}

#[test]
fn lifted_global_and_keyed_sinks_drain_fold_results() {
    use ironbeam_core::{CombineFn, Element};

    struct Sum;
    impl CombineFn<i32, i64, i64> for Sum {
        fn create(&self) -> i64 {
            0
        }
        fn add_input(&self, accumulator: &mut i64, value: i32) {
            *accumulator += i64::from(value);
        }
        fn merge(&self, accumulator: &mut i64, other: i64) {
            *accumulator += other;
        }
        fn finish(&self, accumulator: i64) -> i64 {
            accumulator
        }
    }
    struct CollectSink<T: Element>(Arc<Mutex<Vec<T>>>);
    impl<T: Element> TypedSink<T> for CollectSink<T> {
        fn write(&mut self, value: T) -> Result<(), Error> {
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
    let sink_spec = SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::AtLeastOnce,
        commit: true,
    };
    let pipeline = Pipeline::default();
    let global_rows = Arc::new(Mutex::new(Vec::new()));
    let global = pipeline
        .from_vec::<i32>("empty", vec![])
        .unwrap()
        .combine_globally_lifted_named("global", Sum)
        .unwrap()
        .sink_named("global-sink", sink_spec.clone(), {
            let rows = Arc::clone(&global_rows);
            move || Ok(Box::new(CollectSink(Arc::clone(&rows))))
        })
        .unwrap();
    DirectRunner
        .run_sink(&global, &JobConfig::default())
        .unwrap();
    assert_eq!(*global_rows.lock().unwrap(), vec![0_i64]);

    let keyed_rows = Arc::new(Mutex::new(Vec::new()));
    let keyed = pipeline
        .from_vec("keyed-source", vec![(1_i32, 2_i32), (2, 3), (1, 4)])
        .unwrap()
        .map_named("double", |(key, value)| (key, value * 2))
        .unwrap()
        .combine_values_lifted_named("keyed", Sum)
        .unwrap()
        .sink_named("keyed-sink", sink_spec, {
            let rows = Arc::clone(&keyed_rows);
            move || Ok(Box::new(CollectSink(Arc::clone(&rows))))
        })
        .unwrap();
    let job = DirectRunner
        .run_sink(&keyed, &JobConfig::default())
        .unwrap();
    assert_eq!(*keyed_rows.lock().unwrap(), vec![(1, 12_i64), (2, 6)]);
    assert_eq!(job.metrics().source_elements, 3);
    assert_eq!(job.metrics().output_elements, 2);
}

#[test]
fn grouped_sink_writes_before_transforming_the_next_completed_group() {
    struct CountingSink(Arc<AtomicUsize>, Arc<Mutex<Vec<(i32, u32)>>>);
    impl TypedSink<(i32, u32)> for CountingSink {
        fn write(&mut self, row: (i32, u32)) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            self.1.lock().unwrap().push(row);
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
    let rows = Arc::new(Mutex::new(Vec::new()));
    let inspected = Arc::clone(&writes);
    let pipeline = Pipeline::default();
    let sink = pipeline
        .from_vec("source", vec![(1_i32, 7_i32), (2, 8), (3, 9)])
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .map_named("inspect-writes", move |(key, values)| {
            assert_eq!(inspected.load(Ordering::SeqCst), (key - 1) as usize);
            (key, u32::try_from(values.len()).unwrap())
        })
        .unwrap()
        .filter_named("skip-third", |(key, _)| *key != 3)
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
                let rows = Arc::clone(&rows);
                move || {
                    Ok(Box::new(CountingSink(
                        Arc::clone(&writes),
                        Arc::clone(&rows),
                    )))
                }
            },
        )
        .unwrap();
    let job = DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(*rows.lock().unwrap(), vec![(1, 1), (2, 1)]);
    assert_eq!(job.metrics().source_elements, 3);
    assert_eq!(job.metrics().filtered_elements, 1);
    assert_eq!(job.metrics().output_elements, 2);
}

#[test]
fn wait_sink_streams_main_batches_after_signal_completion() {
    use std::sync::atomic::AtomicBool;

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
    let ready = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let signal_ready = Arc::clone(&ready);
    let signal = pipeline
        .from_vec("signal-source", (0_i32..1_000).collect())
        .unwrap()
        .map_named("complete-signal", move |value| {
            if value == 999 {
                signal_ready.store(true, Ordering::SeqCst);
            }
            value
        })
        .unwrap()
        .filter_named("signal-even", |value| *value % 2 == 0)
        .unwrap();
    let main_ready = Arc::clone(&ready);
    let sink = pipeline
        .from_vec("main-source", (0_i32..1_000).collect())
        .unwrap()
        .map_named("check-signal", move |value| {
            assert!(main_ready.load(Ordering::SeqCst));
            value
        })
        .unwrap()
        .wait_on_named("wait", &signal)
        .unwrap()
        .filter_named("evens", |value| value % 2 == 0)
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
                memory_budget_bytes: 256,
                buffer_budget_elements: 8,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 500);
    assert_eq!(job.metrics().source_elements, 2_000);
    assert_eq!(job.metrics().filtered_elements, 1_000);
    assert_eq!(job.metrics().output_elements, 500);
    assert!(job.metrics().peak_batch_elements <= 8);
}

#[test]
fn direct_sink_counts_filtered_rows_on_both_execution_modes() {
    for parallelism in [1, 3] {
        let pipeline = Pipeline::default();
        let filtered = pipeline
            .from_vec("source", vec![1_i32, 2, 3, 4])
            .unwrap()
            .filter_named("evens", |value| *value % 2 == 0)
            .unwrap()
            .map_named("after-filter", |value| value + 10)
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
        assert_eq!(*captured.lock().unwrap(), vec![12, 14]);
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
            .map_named("identity-before-cancelling-sink", |value| value)
            .unwrap()
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
fn direct_sink_aborts_when_a_later_route_stage_sees_cancellation() {
    use std::sync::atomic::AtomicBool;

    struct AbortTrackingSink(Arc<AtomicUsize>);
    impl TypedSink<i32> for AbortTrackingSink {
        fn write(&mut self, _: i32) -> Result<(), Error> {
            panic!("cancelled route reached the sink")
        }
        fn commit(&mut self) -> Result<(), Error> {
            panic!("cancelled route committed")
        }
        fn abort(&mut self) -> Result<(), Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let cancellation = Arc::new(AtomicBool::new(false));
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let values = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "cancel-route-source-v1".into(),
            Some(1),
            || Ok(Box::new(std::iter::once(Ok(1_i32)))),
        )
        .unwrap();
    let signal = Arc::clone(&cancellation);
    let sink_aborts = Arc::clone(&aborts);
    let sink = values
        .map_named("cancel-route", move |value| {
            signal.store(true, Ordering::Release);
            value
        })
        .unwrap()
        .filter_named("must-not-run", |_| panic!("cancelled route continued"))
        .unwrap()
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::AtLeastOnce,
                commit: true,
            },
            move || Ok(Box::new(AbortTrackingSink(Arc::clone(&sink_aborts)))),
        )
        .unwrap();
    let error = DirectRunner
        .run_sink(
            &sink,
            &JobConfig {
                cancellation,
                ..JobConfig::default()
            },
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ironbeam_core::ErrorKind::Cancelled);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
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
