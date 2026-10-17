use super::{restore, snapshot};
use crate::{
    JobConfig,
    metrics::MetricsState,
    multi_output::{prepare_stream_sinks, process_event},
};
use ironbeam_core::{
    Envelope, Error, ErrorKind, PaneInfo, Pipeline, Replay, SinkMode, SinkSpec, TypedSink, Version,
    Window,
};

struct NullSink;

impl TypedSink<(i32, i32)> for NullSink {
    fn write(&mut self, _: (i32, i32)) -> Result<(), Error> {
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
fn pair_state_roundtrips_and_rejects_corrupt_or_oversized_payloads() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    let spec = SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::Idempotent,
        commit: true,
    };
    let left = source
        .stateful_value_map_named::<i32, i32>(
            "left-state",
            "tests/pair-left-state",
            Version { major: 1, minor: 0 },
            |_, value, _| Ok((Some(value), value)),
        )
        .unwrap()
        .sink_named("left", spec.clone(), || Ok(Box::new(NullSink)))
        .unwrap();
    let right = source
        .stateful_value_map_named::<i32, i32>(
            "right-state",
            "tests/pair-right-state",
            Version { major: 1, minor: 0 },
            |_, value, _| Ok((Some(value), value)),
        )
        .unwrap()
        .sink_named("right", spec, || Ok(Box::new(NullSink)))
        .unwrap();
    let (graph, registry) = left.snapshot().unwrap();
    let plan = prepare_stream_sinks(&graph, &registry, left.key(), right.key()).unwrap();
    let state = snapshot(&plan, &registry, 1024).unwrap().unwrap();
    assert!(state.starts_with(b"IBPAIR01"));
    restore(&plan, &registry, &state).unwrap();
    assert_eq!(
        snapshot(&plan, &registry, state.len()).unwrap().unwrap(),
        state
    );
    assert_eq!(
        snapshot(&plan, &registry, state.len() - 1)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Resource
    );
    for invalid in [&state[1..], &state[..8], &state[..state.len() - 1]] {
        assert_eq!(
            restore(&plan, &registry, invalid).err().unwrap().kind,
            ErrorKind::Incompatible
        );
    }
    let mut trailing = state;
    trailing.push(0);
    assert_eq!(
        restore(&plan, &registry, &trailing).err().unwrap().kind,
        ErrorKind::Incompatible
    );
}

#[test]
fn three_linear_stateful_stages_restore_together() {
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![(1_i32, 2_i32)]).unwrap();
    let spec = SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::Idempotent,
        commit: true,
    };
    let left = source
        .stateful_value_map_named::<i32, i32>(
            "z-left-first",
            "tests/three-state-left-first",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let next = prior.unwrap_or(0) + value;
                Ok((Some(next), next))
            },
        )
        .unwrap()
        .stateful_value_map_named::<i32, i32>(
            "a-left-second",
            "tests/three-state-left-second",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let next = prior.unwrap_or(0) + value * 2;
                Ok((Some(next), next))
            },
        )
        .unwrap()
        .sink_named("left", spec.clone(), || Ok(Box::new(NullSink)))
        .unwrap();
    let right = source
        .stateful_value_map_named::<i32, i32>(
            "m-right-third",
            "tests/three-state-right-third",
            Version { major: 1, minor: 0 },
            |_, value, prior| {
                let next = prior.unwrap_or(0) + value * 10;
                Ok((Some(next), next))
            },
        )
        .unwrap()
        .sink_named("right", spec, || Ok(Box::new(NullSink)))
        .unwrap();
    let (graph, registry) = left.snapshot().unwrap();
    let plan = prepare_stream_sinks(&graph, &registry, left.key(), right.key()).unwrap();
    assert_eq!(plan.stateful_keys.len(), 3);
    assert_eq!(
        plan.stateful_keys,
        ["a-left-second", "m-right-third", "z-left-first"]
    );
    assert_eq!(
        plan.timer_keys,
        ["z-left-first", "a-left-second", "m-right-third"]
    );
    let event = |value| Envelope {
        value: Box::new((1_i32, value)),
        timestamp_ns: 0,
        window: Window::Global,
        assigned_window: None,
        pane: PaneInfo::NO_FIRING,
    };
    let config = JobConfig::default();
    let metrics = MetricsState::default();
    process_event(event(2), &plan, &registry, &config, &metrics).unwrap();
    let saved = snapshot(&plan, &registry, 4096).unwrap().unwrap();
    assert!(saved.starts_with(b"IBPAIR01"));
    process_event(event(3), &plan, &registry, &config, &metrics).unwrap();
    restore(&plan, &registry, &saved).unwrap();
    let (left_rows, right_rows) =
        process_event(event(3), &plan, &registry, &config, &metrics).unwrap();
    assert_eq!(
        left_rows[0].value.downcast_ref::<(i32, i32)>(),
        Some(&(1, 14))
    );
    assert_eq!(
        right_rows[0].value.downcast_ref::<(i32, i32)>(),
        Some(&(1, 50))
    );
}
