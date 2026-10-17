use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SourceProgress, Timestamped,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, UnboundedSource,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct Pending;

impl UnboundedSource<Timestamped<i32>> for Pending {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(UnboundedPoll::Pending)
    }
}

struct Once(Option<UnboundedBatch<Timestamped<i32>>>);

impl UnboundedSource<Timestamped<i32>> for Once {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(UnboundedPoll::Ready(self.0.take().unwrap()))
    }
}

const fn source_progress(replay: Replay) -> SourceProgress {
    SourceProgress {
        stable_split_id: true,
        replay,
        reports_event_time: false,
    }
}

#[test]
fn finalized_source_validates_progress_and_is_reconstructed_in_registry() {
    let pipeline = Pipeline::default();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let callback_seen = Arc::clone(&seen);
    pipeline
        .unbounded_source_with_finalizer(
            "source",
            source_progress(Replay::AtLeastOnce),
            "source-finalization-v1".into(),
            0,
            |_| Ok(Box::new(Pending) as TypedUnboundedSourceReader<i32>),
            move |signal| {
                callback_seen
                    .lock()
                    .unwrap()
                    .push(signal.checkpoint_position.clone().unwrap());
                Ok(())
            },
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    assert!(registry.source_requires_finalization("source"));
    let valid = ProgressSignal {
        source_key: "source".into(),
        split_id: "split-0".into(),
        checkpoint_position: Some(vec![4]),
        event_time_ns: None,
        completed: false,
    };
    registry.finalize_source_progress("source", &valid).unwrap();
    registry.finalize_source_progress("source", &valid).unwrap();
    assert_eq!(seen.lock().unwrap().as_slice(), &[vec![4], vec![4]]);
    let mut invalid = valid.clone();
    invalid.source_key = "other".into();
    assert_eq!(
        registry
            .finalize_source_progress("source", &invalid)
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
    invalid = valid;
    invalid.checkpoint_position = Some(Vec::new());
    assert_eq!(
        registry
            .finalize_source_progress("source", &invalid)
            .unwrap_err()
            .kind,
        ErrorKind::Recovery
    );
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[test]
fn finalized_source_requires_replay_and_catches_callback_panic() {
    let pipeline = Pipeline::default();
    let open = |_| Ok(Box::new(Pending) as TypedUnboundedSourceReader<i32>);
    assert!(
        pipeline
            .unbounded_source_with_finalizer(
                "without-replay",
                source_progress(Replay::None),
                "digest".into(),
                0,
                open,
                |_| Ok(())
            )
            .is_err()
    );
    assert!(
        pipeline
            .unbounded_source_with_finalizer(
                "unstable",
                SourceProgress {
                    stable_split_id: false,
                    ..source_progress(Replay::AtLeastOnce)
                },
                "digest".into(),
                0,
                open,
                |_| Ok(())
            )
            .is_err()
    );
    pipeline
        .unbounded_source_with_finalizer(
            "source",
            source_progress(Replay::AtLeastOnce),
            "digest".into(),
            0,
            open,
            |_| panic!("injected source callback panic"),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let progress = ProgressSignal {
        source_key: "source".into(),
        split_id: "split-0".into(),
        checkpoint_position: Some(vec![1]),
        event_time_ns: None,
        completed: false,
    };
    assert_eq!(
        registry
            .finalize_source_progress("source", &progress)
            .unwrap_err()
            .kind,
        ErrorKind::Execution
    );
}

#[test]
fn finalized_source_rejects_missing_or_empty_cursor_before_output() {
    for cursor in [None, Some(Vec::new())] {
        let pipeline = Pipeline::default();
        let opened = Arc::new(AtomicUsize::new(0));
        let reader_opened = Arc::clone(&opened);
        let progress = ProgressSignal {
            source_key: "source".into(),
            split_id: "split-0".into(),
            checkpoint_position: cursor,
            event_time_ns: None,
            completed: true,
        };
        pipeline
            .unbounded_source_with_finalizer(
                "source",
                source_progress(Replay::AtLeastOnce),
                "empty-cursor-v1".into(),
                0,
                move |_| {
                    reader_opened.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(Once(Some(UnboundedBatch {
                        elements: vec![Timestamped {
                            value: 7,
                            timestamp_ns: 0,
                        }],
                        progress: progress.clone(),
                    }))) as TypedUnboundedSourceReader<i32>)
                },
                |_| Ok(()),
            )
            .unwrap();
        let (_, registry) = pipeline.snapshot().unwrap();
        let mut reader = registry.open_unbounded_source("source", None).unwrap();
        let error = reader.poll(1).err().unwrap();
        assert_eq!(error.kind, ErrorKind::Execution);
        assert!(error.message.contains("nonempty checkpoint position"));
        assert_eq!(opened.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn finalized_source_rejects_empty_resume_cursor_before_open() {
    let pipeline = Pipeline::default();
    let opened = Arc::new(AtomicUsize::new(0));
    let reader_opened = Arc::clone(&opened);
    pipeline
        .unbounded_source_with_finalizer(
            "source",
            source_progress(Replay::AtLeastOnce),
            "empty-resume-v1".into(),
            0,
            move |_| {
                reader_opened.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(Pending) as TypedUnboundedSourceReader<i32>)
            },
            |_| Ok(()),
        )
        .unwrap();
    let (_, registry) = pipeline.snapshot().unwrap();
    let error = registry
        .open_unbounded_source(
            "source",
            Some(ProgressSignal {
                source_key: "source".into(),
                split_id: "split-0".into(),
                checkpoint_position: Some(Vec::new()),
                event_time_ns: None,
                completed: false,
            }),
        )
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(opened.load(Ordering::SeqCst), 0);
}

#[test]
fn finalizer_requirement_changes_source_configuration_identity() {
    let ordinary = Pipeline::default();
    ordinary
        .unbounded_source_with_factory(
            "source",
            source_progress(Replay::AtLeastOnce),
            "same-source-v1".into(),
            0,
            |_| Ok(Box::new(Pending) as TypedUnboundedSourceReader<i32>),
        )
        .unwrap()
        .output("result")
        .unwrap();
    let finalizing = Pipeline::default();
    finalizing
        .unbounded_source_with_finalizer(
            "source",
            source_progress(Replay::AtLeastOnce),
            "same-source-v1".into(),
            0,
            |_| Ok(Box::new(Pending) as TypedUnboundedSourceReader<i32>),
            |_| Ok(()),
        )
        .unwrap()
        .output("result")
        .unwrap();
    let (_, ordinary_registry) = ordinary.snapshot().unwrap();
    let (_, finalizing_registry) = finalizing.snapshot().unwrap();
    assert_ne!(
        ordinary_registry.source_config_digest("source"),
        finalizing_registry.source_config_digest("source")
    );
}
