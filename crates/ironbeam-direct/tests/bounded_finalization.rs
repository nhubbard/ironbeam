use ironbeam_core::{
    Error, ErrorKind, Pipeline, Replay, SinkMode, SinkSpec, SourceProgress, TypedSink,
};
use ironbeam_direct::{DirectRunner, JobConfig};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct NoOpSink {
    finalizing: bool,
}

impl TypedSink<i32> for NoOpSink {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn requires_bundle_finalization(&self) -> bool {
        self.finalizing
    }
}

struct UndeclaredFinalizingSink {
    finalizing: bool,
    panic_probe: bool,
    aborts: Arc<AtomicUsize>,
}

impl TypedSink<i32> for UndeclaredFinalizingSink {
    fn write(&mut self, _: i32) -> Result<(), Error> {
        panic!("unsupported sink received input")
    }

    fn commit(&mut self) -> Result<(), Error> {
        panic!("unsupported sink committed")
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn requires_bundle_finalization(&self) -> bool {
        if self.panic_probe {
            panic!("injected finalization probe panic");
        }
        self.finalizing
    }
}

fn idempotent_sink_spec() -> SinkSpec {
    SinkSpec {
        mode: SinkMode::Append,
        replay: Replay::Idempotent,
        commit: true,
    }
}

#[test]
fn bounded_direct_rejects_finalizing_sink_before_opening_source_or_sink() {
    let source_opens = Arc::new(AtomicUsize::new(0));
    let sink_opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let opens = Arc::clone(&source_opens);
    let source = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "finalization-preflight-v1".into(),
            Some(1),
            move || {
                opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(Ok(1_i32))))
            },
        )
        .unwrap();
    let opens = Arc::clone(&sink_opens);
    let sink = source
        .sink_named_with_config_and_finalization(
            "finalized",
            idempotent_sink_spec(),
            "finalized-v1".into(),
            move || {
                opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(NoOpSink { finalizing: true }))
            },
        )
        .unwrap();

    let error = DirectRunner
        .run_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "bundle-finalization".into(),
        }
    );
    assert_eq!(error.node.as_deref(), Some("finalized"));
    assert_eq!(source_opens.load(Ordering::SeqCst), 0);
    assert_eq!(sink_opens.load(Ordering::SeqCst), 0);
}

#[test]
fn bounded_pair_rejects_finalizing_branch_before_opening_either_sink() {
    let source_opens = Arc::new(AtomicUsize::new(0));
    let sink_opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let opens = Arc::clone(&source_opens);
    let source = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "pair-finalization-preflight-v1".into(),
            Some(1),
            move || {
                opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(Ok(1_i32))))
            },
        )
        .unwrap();
    let opens = Arc::clone(&sink_opens);
    let left = source
        .sink_named("left", idempotent_sink_spec(), move || {
            opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(NoOpSink { finalizing: false }))
        })
        .unwrap();
    let opens = Arc::clone(&sink_opens);
    let right = source
        .sink_named_with_config_and_finalization(
            "right",
            idempotent_sink_spec(),
            "right-finalized-v1".into(),
            move || {
                opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(NoOpSink { finalizing: true }))
            },
        )
        .unwrap();

    let error = DirectRunner
        .run_sink_pair(&left, &right, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "bundle-finalization".into(),
        }
    );
    assert_eq!(error.node.as_deref(), Some("right"));
    assert_eq!(source_opens.load(Ordering::SeqCst), 0);
    assert_eq!(sink_opens.load(Ordering::SeqCst), 0);
}

#[test]
fn bounded_direct_aborts_misdeclared_finalizing_session_before_source_open() {
    let source_opens = Arc::new(AtomicUsize::new(0));
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let opens = Arc::clone(&source_opens);
    let source = pipeline
        .bounded_source(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "misdeclared-finalization-v1".into(),
            Some(1),
            move || {
                opens.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(std::iter::once(Ok(1_i32))))
            },
        )
        .unwrap();
    let sink_aborts = Arc::clone(&aborts);
    let sink = source
        .sink_named("misdeclared", idempotent_sink_spec(), move || {
            Ok(Box::new(UndeclaredFinalizingSink {
                finalizing: true,
                panic_probe: false,
                aborts: Arc::clone(&sink_aborts),
            }))
        })
        .unwrap();

    let error = DirectRunner
        .run_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "bundle-finalization".into(),
        }
    );
    assert_eq!(source_opens.load(Ordering::SeqCst), 0);
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
}

#[test]
fn bounded_pair_aborts_both_sessions_when_finalization_probe_fails() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline.from_vec("source", vec![1_i32]).unwrap();
    let sink_aborts = Arc::clone(&aborts);
    let left = source
        .sink_named("left", idempotent_sink_spec(), move || {
            Ok(Box::new(UndeclaredFinalizingSink {
                finalizing: false,
                panic_probe: false,
                aborts: Arc::clone(&sink_aborts),
            }))
        })
        .unwrap();
    let sink_aborts = Arc::clone(&aborts);
    let right = source
        .sink_named("right", idempotent_sink_spec(), move || {
            Ok(Box::new(UndeclaredFinalizingSink {
                finalizing: true,
                panic_probe: false,
                aborts: Arc::clone(&sink_aborts),
            }))
        })
        .unwrap();

    let error = DirectRunner
        .run_sink_pair(&left, &right, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(
        error.kind,
        ErrorKind::UnsupportedCapability {
            capability: "bundle-finalization".into(),
        }
    );
    assert_eq!(error.node.as_deref(), Some("right"));
    assert_eq!(aborts.load(Ordering::SeqCst), 2);
}

#[test]
fn bounded_direct_aborts_session_when_finalization_probe_panics() {
    let aborts = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let sink_aborts = Arc::clone(&aborts);
    let sink = pipeline
        .from_vec("source", vec![1_i32])
        .unwrap()
        .sink_named("panic-probe", idempotent_sink_spec(), move || {
            Ok(Box::new(UndeclaredFinalizingSink {
                finalizing: false,
                panic_probe: true,
                aborts: Arc::clone(&sink_aborts),
            }))
        })
        .unwrap();

    let error = DirectRunner
        .run_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("panic-probe"));
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
}
