use ironbeam_core::{
    CombiningFunction, CombiningStateMutation, ElementMetadata, Error, ErrorKind, Pipeline,
    ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress, Timestamped, TypedSink,
    TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, UnboundedSource, Version,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig, JobStatus};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct IdempotentCapture {
    accepted: Arc<Mutex<HashSet<String>>>,
    pending: Vec<String>,
}

impl TypedSink<String> for IdempotentCapture {
    fn write(&mut self, value: String) -> Result<(), Error> {
        self.pending.push(value);
        Ok(())
    }
    fn write_with_metadata(
        &mut self,
        value: String,
        metadata: ElementMetadata,
    ) -> Result<(), Error> {
        if let Some(window) = metadata.assigned_window {
            self.write(format!("{}:{}:{value}", window.start_ns, window.end_ns))
        } else {
            self.write(value)
        }
    }
    fn commit(&mut self) -> Result<(), Error> {
        self.accepted.lock().unwrap().extend(self.pending.drain(..));
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.pending.clear();
        Ok(())
    }
}

struct SlidingCombiningSource {
    cursor: Option<Vec<u8>>,
    emitted: bool,
}

impl UnboundedSource<Timestamped<(i32, i32)>> for SlidingCombiningSource {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<(i32, i32)>>, Error> {
        if self.emitted {
            return Ok(UnboundedPoll::Pending);
        }
        self.emitted = true;
        let (values, cursor, watermark, completed) = match self.cursor.as_deref() {
            None => (vec![(11, 6_000_000), (12, 7_000_000)], 1, 8_000_000, false),
            Some([1]) => (
                vec![(11, 12_000_000), (11, 18_000_000)],
                2,
                26_000_000,
                true,
            ),
            _ => return Err(Error::incompatible("unexpected sliding combining cursor")),
        };
        Ok(UnboundedPoll::Ready(UnboundedBatch {
            elements: values
                .into_iter()
                .map(|(value, timestamp_ns)| Timestamped {
                    value: (1, value),
                    timestamp_ns,
                })
                .collect(),
            progress: ProgressSignal {
                source_key: "source".into(),
                split_id: "default".into(),
                checkpoint_position: Some(vec![cursor]),
                event_time_ns: Some(watermark),
                completed,
            },
        }))
    }
}

#[test]
#[allow(clippy::too_many_lines)] // The restart fixture keeps both source batches and windows visible.
fn sliding_combining_state_restores_each_window_and_source_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let sink = pipeline
        .unbounded_source_with_factory(
            "source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "sliding-combining-two-batches-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    Ok(Box::new(SlidingCombiningSource {
                        cursor,
                        emitted: false,
                    })
                        as TypedUnboundedSourceReader<(i32, i32)>)
                }
            },
        )
        .unwrap()
        .sliding_windows("windows", 10_000_000, 5_000_000, 0)
        .unwrap()
        .stateful_combining_map_named::<i32, String>(
            "combining",
            "tests/sliding-combining-state",
            Version { major: 1, minor: 0 },
            CombiningFunction {
                semantic_id: "tests/checked-sum",
                version: Version { major: 1, minor: 0 },
            },
            |left, right| {
                left.checked_add(right)
                    .ok_or_else(|| Error::incompatible("combining sum overflow"))
            },
            |key, value, prior| {
                Ok((
                    CombiningStateMutation::Add(value),
                    format!("{key}:{value}:{}", prior.copied().unwrap_or_default()),
                ))
            },
        )
        .unwrap()
        .map_named("label", |(_, label)| label)
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "sliding-combining-capture-v1".into(),
            {
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(IdempotentCapture {
                        accepted: Arc::clone(&accepted),
                        pending: Vec::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            interval_batches: 1,
            max_checkpoints: 2,
            recover: true,
            binary_build_id: "sliding-combining-state-test-build".into(),
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while job
        .progress()
        .and_then(|progress| progress.checkpoint_position)
        != Some(vec![1])
    {
        assert_ne!(job.handle().status(), JobStatus::Failed);
        assert!(
            Instant::now() < deadline,
            "first sliding combining batch was not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(opens.lock().unwrap().as_slice(), &[None, Some(vec![1])]);
    assert_eq!(
        *accepted.lock().unwrap(),
        [
            "0:10000000:1:11:0",
            "0:10000000:1:12:11",
            "5000000:15000000:1:11:0",
            "5000000:15000000:1:12:11",
            "5000000:15000000:1:11:23",
            "10000000:20000000:1:11:0",
            "10000000:20000000:1:11:11",
            "15000000:25000000:1:11:0",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<HashSet<_>>()
    );
}
