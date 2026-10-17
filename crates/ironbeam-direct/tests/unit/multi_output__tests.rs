// Unit tests for src/multi_output.rs.
use super::{prepare, prepare_sinks, prepare_stream_sinks, process_event, process_timer_event};
use crate::{JobConfig, metrics::MetricsState};
use ironbeam_core::{
    Envelope, Error, ErrorKind, PaneInfo, Pipeline, Registry, Replay, SinkMode, SinkSpec,
    TypedSink, Version, Window,
};
use std::sync::{Arc, atomic::AtomicBool};

struct NullStringSink;

impl TypedSink<String> for NullStringSink {
    fn write(&mut self, _: String) -> Result<(), Error> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

fn pair_sink_spec() -> SinkSpec {
    SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::Idempotent,
        commit: true,
    }
}

#[test]
fn pair_planner_rejects_terminals_with_the_wrong_operation() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let left = source.output("left").unwrap();
    let right = source
        .map_named("right-map", |value| value + 1)
        .unwrap()
        .output("right")
        .unwrap();
    let (graph, registry) = left.snapshot().unwrap();
    assert_eq!(
        prepare(&graph, &registry, "source", right.key())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        prepare_sinks(&graph, &registry, left.key(), right.key())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    assert_eq!(
        prepare_stream_sinks(&graph, &registry, left.key(), right.key())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
    let error = prepare(&graph, &Registry::default(), left.key(), right.key())
        .err()
        .unwrap();
    assert!(matches!(
        error.kind,
        ErrorKind::UnsupportedCapability { ref capability }
            if capability == "transform-function"
    ));
}

#[test]
fn pair_processor_honors_cancellation_before_applying_shared_work() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let left = source.output("left").unwrap();
    let right = source.output("right").unwrap();
    let (graph, registry) = left.snapshot().unwrap();
    let plan = prepare(&graph, &registry, left.key(), right.key()).unwrap();
    let config = JobConfig {
        cancellation: Arc::new(AtomicBool::new(true)),
        ..JobConfig::default()
    };
    let error = process_event(
        Envelope {
            value: Box::new(1_i32),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
            pane: PaneInfo::NO_FIRING,
        },
        &plan,
        &registry,
        &config,
        &MetricsState::default(),
    )
    .err()
    .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
}

#[test]
fn stream_pair_runs_one_stateful_branch_after_the_split() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    let left = source
        .stateful_value_map_named::<i32, i32>(
            "left-state",
            "tests/left-branch-state",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let next = prior.unwrap_or(0) + value;
                Ok((Some(next), next))
            },
        )
        .unwrap()
        .map_named("left-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named("left", pair_sink_spec(), || Ok(Box::new(NullStringSink)))
        .unwrap();
    let right = source
        .map_named("right-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named("right", pair_sink_spec(), || Ok(Box::new(NullStringSink)))
        .unwrap();
    let (graph, registry) = left.snapshot().unwrap();
    let bounded_error = prepare_sinks(&graph, &registry, left.key(), right.key())
        .err()
        .unwrap();
    assert!(matches!(
        bounded_error.kind,
        ErrorKind::UnsupportedCapability { ref capability }
            if capability == "multi-output-operation"
    ));
    let plan = prepare_stream_sinks(&graph, &registry, left.key(), right.key()).unwrap();
    assert_eq!(plan.stateful_keys, ["left-state"]);
    let (left_rows, right_rows) = process_event(
        Envelope {
            value: Box::new((1_i32, 2_i32)),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
            pane: PaneInfo::NO_FIRING,
        },
        &plan,
        &registry,
        &JobConfig::default(),
        &MetricsState::default(),
    )
    .unwrap();
    assert_eq!(left_rows.len(), 1);
    assert_eq!(right_rows.len(), 1);
    assert_eq!(left_rows[0].value.downcast_ref::<String>().unwrap(), "1:2");
    assert_eq!(right_rows[0].value.downcast_ref::<String>().unwrap(), "1:2");
}

#[test]
fn stream_pair_accepts_two_distinct_stateful_stages() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    let left = source
        .stateful_value_map_named::<i32, i32>(
            "left-state",
            "tests/left-state",
            Version { major: 1, minor: 0 },
            |_, value, _| Ok((Some(value), value)),
        )
        .unwrap()
        .map_named("left-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named("left", pair_sink_spec(), || Ok(Box::new(NullStringSink)))
        .unwrap();
    let right = source
        .stateful_value_map_named::<i32, i32>(
            "right-state",
            "tests/right-state",
            Version { major: 1, minor: 0 },
            |_, value, _| Ok((Some(value), value)),
        )
        .unwrap()
        .map_named("right-label", |(key, value)| format!("{key}:{value}"))
        .unwrap()
        .sink_named("right", pair_sink_spec(), || Ok(Box::new(NullStringSink)))
        .unwrap();
    let (graph, registry) = left.snapshot().unwrap();
    let plan = prepare_stream_sinks(&graph, &registry, left.key(), right.key()).unwrap();
    assert_eq!(plan.stateful_keys, ["left-state", "right-state"]);
    let error = process_timer_event(
        Envelope {
            value: Box::new((1_i32, 2_i32)),
            timestamp_ns: 0,
            window: Window::Global,
            assigned_window: None,
            pane: PaneInfo::NO_FIRING,
        },
        "unknown-state",
        &plan,
        &registry,
        &JobConfig::default(),
        &MetricsState::default(),
    )
    .err()
    .unwrap();
    assert_eq!(error.kind, ErrorKind::Graph);
}
