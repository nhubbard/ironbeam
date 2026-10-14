use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, Sink, SinkMode, SinkSpec, SourceProgress,
    Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll,
    UnboundedSource,
};
use ironbeam_direct::{DirectRunner, JobConfig, SpillConfig};

struct PendingSource;

impl UnboundedSource<Timestamped<i32>> for PendingSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(UnboundedPoll::Pending)
    }
}

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

fn grouped_sink(progress: SourceProgress, sink_replay: Replay) -> Sink<i32> {
    let pipeline = Pipeline::default();
    pipeline
        .unbounded_source_with_factory("events", progress, "preflight-source-v1".into(), 0, |_| {
            Ok(Box::new(PendingSource) as TypedUnboundedSourceReader<i32>)
        })
        .unwrap()
        .fixed_windows("window", 10)
        .unwrap()
        .map_named("key", |value| (value % 2, value))
        .unwrap()
        .group_by_key_named("group")
        .unwrap()
        .map_named("sum", |(key, values)| key + values.into_iter().sum::<i32>())
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: sink_replay,
                commit: true,
            },
            "preflight-sink-v1".into(),
            || Ok(Box::new(NoopSink)),
        )
        .unwrap()
}

#[test]
fn grouped_stream_rejects_missing_watermarks_before_source_open() {
    let sink = grouped_sink(
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: false,
        },
        Replay::Idempotent,
    );
    let error = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert!(error.message.contains("stream-watermark"));
}

#[test]
fn grouped_stream_rejects_unreplayable_source_and_sink() {
    let weak_source = grouped_sink(
        SourceProgress {
            stable_split_id: false,
            replay: Replay::AtLeastOnce,
            reports_event_time: true,
        },
        Replay::Idempotent,
    );
    let source_error = DirectRunner
        .start_stream_sink(&weak_source, &JobConfig::default())
        .err()
        .unwrap();
    assert!(source_error.message.contains("resumable-stream-source"));

    let unsafe_sink = grouped_sink(
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: true,
        },
        Replay::AtLeastOnce,
    );
    let sink_error = DirectRunner
        .start_stream_sink(&unsafe_sink, &JobConfig::default())
        .err()
        .unwrap();
    assert!(sink_error.message.contains("idempotent-stream-sink"));
}

#[test]
fn grouped_stream_rejects_spill_before_source_open() {
    let sink = grouped_sink(
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: true,
        },
        Replay::Idempotent,
    );
    let config = JobConfig {
        spill: Some(SpillConfig::default()),
        ..JobConfig::default()
    };
    let error = DirectRunner
        .start_stream_sink(&sink, &config)
        .err()
        .unwrap();
    assert!(error.message.contains("stream-spill"));
}

#[test]
fn dual_source_join_rejects_lateness_before_source_open() {
    let pipeline = Pipeline::default();
    let progress = SourceProgress {
        stable_split_id: true,
        replay: Replay::AtLeastOnce,
        reports_event_time: true,
    };
    let left = pipeline
        .unbounded_source_with_factory("left", progress.clone(), "late-left-v1".into(), 0, |_| {
            panic!("late join source must not open")
        })
        .unwrap()
        .fixed_windows("left-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("left-lateness", 5_000_000)
        .unwrap()
        .map_named("left-key", |value: i32| (value, value))
        .unwrap();
    let right = pipeline
        .unbounded_source_with_factory("right", progress, "late-right-v1".into(), 0, |_| {
            panic!("late join source must not open")
        })
        .unwrap()
        .fixed_windows("right-window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("right-lateness", 5_000_000)
        .unwrap()
        .map_named("right-key", |value: i32| (value, value))
        .unwrap();
    let sink = left
        .join_inner_named("join", &right)
        .unwrap()
        .map_named("sum", |(_, (left, right))| left + right)
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "late-join-preflight-v1".into(),
            || Ok(Box::new(NoopSink)),
        )
        .unwrap();
    let error = DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert!(error.message.contains("allowed-lateness"), "{error:?}");
}

#[derive(Clone, Copy)]
enum GroupFault {
    SourceFactory,
    SourcePoll,
    Transform,
    SinkFactory,
    SinkWrite,
    SinkCommit,
}

struct GroupFaultReader(GroupFault);

impl UnboundedSource<Timestamped<i32>> for GroupFaultReader {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        if matches!(self.0, GroupFault::SourcePoll) {
            panic!("grouped source poll failed");
        }
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: vec![Timestamped {
                value: 2,
                timestamp_ns: 1,
            }],
            progress: ProgressSignal {
                source_key: "events".into(),
                split_id: "split-0".into(),
                checkpoint_position: Some(vec![1]),
                event_time_ns: Some(20),
                completed: true,
            },
        }))
    }
}

struct GroupFaultSink(GroupFault);

impl TypedSink<i32> for GroupFaultSink {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        if matches!(self.0, GroupFault::SinkWrite) {
            panic!("grouped sink write failed");
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if matches!(self.0, GroupFault::SinkCommit) {
            panic!("grouped sink commit failed");
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[test]
fn grouped_stream_attributes_source_transform_and_sink_failures() {
    for fault in [
        GroupFault::SourceFactory,
        GroupFault::SourcePoll,
        GroupFault::Transform,
        GroupFault::SinkFactory,
        GroupFault::SinkWrite,
        GroupFault::SinkCommit,
    ] {
        let pipeline = Pipeline::default();
        let sink = pipeline
            .unbounded_source_with_factory(
                "events",
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                "group-fault-source-v1".into(),
                0,
                move |_| {
                    if matches!(fault, GroupFault::SourceFactory) {
                        panic!("grouped source factory failed");
                    }
                    Ok(Box::new(GroupFaultReader(fault)) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap()
            .fixed_windows("window", 10)
            .unwrap()
            .map_named("key", move |value| {
                if matches!(fault, GroupFault::Transform) {
                    panic!("grouped transform failed");
                }
                (1_i32, value)
            })
            .unwrap()
            .group_by_key_named("group")
            .unwrap()
            .map_named("sum", |(_, values)| values.into_iter().sum::<i32>())
            .unwrap()
            .sink_named_with_config(
                "sink",
                SinkSpec {
                    mode: SinkMode::Append,
                    replay: Replay::Idempotent,
                    commit: true,
                },
                "group-fault-sink-v1".into(),
                move || {
                    if matches!(fault, GroupFault::SinkFactory) {
                        panic!("grouped sink factory failed");
                    }
                    Ok(Box::new(GroupFaultSink(fault)))
                },
            )
            .unwrap();
        let error = DirectRunner
            .start_stream_sink(&sink, &JobConfig::default())
            .unwrap()
            .wait()
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
    }
}
