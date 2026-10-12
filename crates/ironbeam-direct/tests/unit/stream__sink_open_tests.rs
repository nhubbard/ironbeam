// Unit tests for src/stream.rs.
use super::open_sink;
use ironbeam_core::{Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, TypedSink};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct FinalizationProbeSink {
    aborts: Arc<AtomicUsize>,
    panic_on_probe: bool,
}
impl TypedSink<i32> for FinalizationProbeSink {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn requires_bundle_finalization(&self) -> bool {
        assert!(!self.panic_on_probe, "injected finalization probe panic");
        true
    }
}

#[test]
fn unsupported_or_panicking_finalization_probe_aborts_opened_sink() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let panic_on_probe = Arc::new(AtomicBool::new(false));
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let sink_aborts = Arc::clone(&aborts);
    let probe_flag = Arc::clone(&panic_on_probe);
    let sink = source
        .sink_named(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            move || {
                Ok(Box::new(FinalizationProbeSink {
                    aborts: Arc::clone(&sink_aborts),
                    panic_on_probe: probe_flag.load(Ordering::SeqCst),
                }))
            },
        )
        .unwrap();
    let (_, registry) = sink.snapshot().unwrap();
    assert!(matches!(
        open_sink(&registry, sink.key()).err().unwrap().kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
    panic_on_probe.store(true, Ordering::SeqCst);
    assert_eq!(
        open_sink(&registry, sink.key()).err().unwrap().kind,
        ErrorKind::Execution
    );
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
}
