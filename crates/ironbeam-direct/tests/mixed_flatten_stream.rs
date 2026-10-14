use ironbeam_core::{Error, Pipeline, Replay, SinkMode, SinkSpec, TestStreamEvent, TypedSink};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use std::collections::HashSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Capture {
    rows: Arc<Mutex<Vec<i32>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    cancel_once: Arc<AtomicBool>,
    cancellation: Arc<AtomicBool>,
    bundle: String,
    staged: Vec<i32>,
}

impl TypedSink<i32> for Capture {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        id.clone_into(&mut self.bundle);
        Ok(())
    }

    fn write(&mut self, value: i32) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
            self.rows.lock().unwrap().append(&mut self.staged);
        }
        if self.cancel_once.swap(false, Ordering::SeqCst) {
            self.cancellation.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

#[test]
fn bounded_flatten_rows_resume_by_position_before_unbounded_branch() {
    let directory = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let visits = Arc::new(AtomicUsize::new(0));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let cancellation = Arc::new(AtomicBool::new(false));
    let cancel_once = Arc::new(AtomicBool::new(true));
    let bounded = pipeline
        .from_vec("a-bounded", vec![10_i32, 20])
        .unwrap()
        .map_named("bounded-count", {
            let visits = Arc::clone(&visits);
            move |value| {
                visits.fetch_add(1, Ordering::SeqCst);
                value
            }
        })
        .unwrap();
    let stream = pipeline
        .test_stream_source(
            "z-stream",
            vec![
                TestStreamEvent::Elements(vec![(30_i32, 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
            ],
        )
        .unwrap();
    let sink = bounded
        .flatten_named("mixed-flatten", &[stream])
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "mixed-flatten-sink-v1".into(),
            {
                let rows = Arc::clone(&rows);
                let accepted = Arc::clone(&accepted);
                let cancel_once = Arc::clone(&cancel_once);
                let cancellation = Arc::clone(&cancellation);
                move || {
                    Ok(Box::new(Capture {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        cancel_once: Arc::clone(&cancel_once),
                        cancellation: Arc::clone(&cancellation),
                        bundle: String::new(),
                        staged: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "mixed-flatten-v1".into(),
            ..CheckpointConfig::default()
        }),
        buffer_budget_elements: 1,
        cancellation: Arc::clone(&cancellation),
        ..JobConfig::default()
    };
    assert!(
        DirectRunner
            .start_stream_sink(&sink, &config())
            .unwrap()
            .wait()
            .is_err()
    );
    assert_eq!(*rows.lock().unwrap(), [10]);
    assert_eq!(visits.load(Ordering::SeqCst), 1);
    cancellation.store(false, Ordering::SeqCst);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*rows.lock().unwrap(), [10, 20, 30]);
    assert_eq!(visits.load(Ordering::SeqCst), 2);
}
