use super::{restore, snapshot};
use crate::multi_output::prepare_stream_sinks;
use ironbeam_core::{Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, TypedSink, Version};

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
}
