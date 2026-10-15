use ironbeam_core::{
    Error, ErrorKind, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress,
    TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader, UnboundedBatch,
    UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Tagged = (i32, u8, i32);

struct Script(VecDeque<UnboundedPoll<Timestamped<Tagged>>>);

impl UnboundedSource<Timestamped<Tagged>> for Script {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<Tagged>>, Error> {
        Ok(self.0.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn batch(
    cursor: u8,
    watermark: i64,
    completed: bool,
    rows: &[(Tagged, i64)],
) -> UnboundedPoll<Timestamped<Tagged>> {
    UnboundedPoll::Ready(UnboundedBatch {
        elements: rows
            .iter()
            .map(|(value, timestamp_ns)| Timestamped {
                value: *value,
                timestamp_ns: *timestamp_ns,
            })
            .collect(),
        progress: ProgressSignal {
            source_key: "shared".into(),
            split_id: "split-0".into(),
            checkpoint_position: Some(vec![cursor]),
            event_time_ns: Some(watermark),
            completed,
        },
    })
}

struct Capture {
    rows: Arc<Mutex<Vec<String>>>,
    accepted: Arc<Mutex<HashSet<String>>>,
    staged: Vec<String>,
    bundle: String,
}

impl TypedSink<String> for Capture {
    fn begin_bundle(&mut self, id: &str) -> Result<(), Error> {
        id.clone_into(&mut self.bundle);
        Ok(())
    }
    fn write(&mut self, row: String) -> Result<(), Error> {
        self.staged.push(row);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        if self.accepted.lock().unwrap().insert(self.bundle.clone()) {
            self.rows.lock().unwrap().append(&mut self.staged);
        }
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the checkpoint restart checks one source cursor, both branches, and late panes"
)]
fn heterogeneous_shared_source_restores_one_cursor_and_late_count_pane() {
    let directory = tempfile::tempdir().unwrap();
    let opens = Arc::new(Mutex::new(Vec::new()));
    let visits = Arc::new(AtomicUsize::new(0));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .unbounded_source_with_factory(
            "shared",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: true,
            },
            "shared-heterogeneous-v1".into(),
            0,
            {
                let opens = Arc::clone(&opens);
                move |cursor| {
                    opens.lock().unwrap().push(cursor.clone());
                    let polls = match cursor.as_deref() {
                        None => vec![batch(
                            1,
                            4_000_000,
                            false,
                            &[((1, 0, 2), 2_000_000), ((1, 1, 4), 4_000_000)],
                        )],
                        Some([1]) => vec![
                            batch(2, 10_000_000, false, &[]),
                            batch(3, 10_000_000, false, &[((1, 0, 3), 3_000_000)]),
                            batch(4, 10_000_000, false, &[((1, 0, 5), 5_000_000)]),
                            batch(5, 15_000_000, true, &[]),
                        ],
                        _ => return Err(Error::incompatible("unexpected shared-source cursor")),
                    };
                    Ok(Box::new(Script(polls.into())) as TypedUnboundedSourceReader<Tagged>)
                }
            },
        )
        .unwrap()
        .map_named("common", {
            let visits = Arc::clone(&visits);
            move |row| {
                visits.fetch_add(1, Ordering::SeqCst);
                row
            }
        })
        .unwrap()
        .fixed_windows("window", 10_000_000)
        .unwrap()
        .allowed_lateness_named("lateness", 5_000_000)
        .unwrap()
        .late_count_trigger_named("late-count", 2)
        .unwrap();
    let main = source
        .filter_named("main-filter", |(_, tag, _)| *tag == 0)
        .unwrap()
        .map_named("main", |(key, _, value)| (key, value))
        .unwrap();
    let side = source
        .filter_named("side-filter", |(_, tag, _)| *tag == 1)
        .unwrap()
        .map_named("side", |(key, _, value)| (key, value.to_string()))
        .unwrap();
    let sink = ironbeam_core::cogroup_by_key_2_named("group", &main, &side)
        .unwrap()
        .reify_panes_named("pane")
        .unwrap()
        .map_named("row", |(pane, (key, buckets))| {
            format!(
                "{key}:{buckets:?}:{:?}:{}:{}:{}:{}",
                pane.timing, pane.is_first, pane.is_last, pane.index, pane.nonspeculative_index
            )
        })
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-heterogeneous-sink-v1".into(),
            {
                let rows = Arc::clone(&rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    let config = || JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().into(),
            binary_build_id: "shared-heterogeneous-build-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let job = DirectRunner.start_stream_sink(&sink, &config()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while job.progress().and_then(|p| p.checkpoint_position) != Some(1_u64.to_le_bytes().to_vec()) {
        assert!(
            Instant::now() < deadline,
            "shared-source checkpoint was not saved"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(rows.lock().unwrap().is_empty());
    job.cancel();
    assert_eq!(job.wait().err().unwrap().kind, ErrorKind::Cancelled);
    DirectRunner
        .start_stream_sink(&sink, &config())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(*opens.lock().unwrap(), [None, Some(vec![1])]);
    assert_eq!(visits.load(Ordering::SeqCst), 4);
    assert_eq!(
        rows.lock().unwrap().as_slice(),
        [
            "1:([2], [\"4\"]):OnTime:true:false:0:0",
            "1:([3, 5], []):Late:false:false:1:1",
        ]
    );
}

#[test]
fn shared_source_can_split_before_window_assignment() {
    let rows = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(HashSet::new()));
    let pipeline = Pipeline::default();
    let source = pipeline
        .test_stream_source(
            "shared",
            vec![
                TestStreamEvent::Elements(vec![
                    ((1_i32, 0_u8, 2_i32), 2_000_000),
                    ((1, 1, 4), 4_000_000),
                ]),
                TestStreamEvent::AdvanceWatermark(10_000_000),
            ],
        )
        .unwrap();
    let main = source
        .filter_named("main-filter", |(_, tag, _)| *tag == 0)
        .unwrap()
        .map_named("main", |(key, _, value)| (key, value))
        .unwrap()
        .fixed_windows("main-window", 10_000_000)
        .unwrap();
    let side = source
        .filter_named("side-filter", |(_, tag, _)| *tag == 1)
        .unwrap()
        .map_named("side", |(key, _, value)| (key, value.to_string()))
        .unwrap()
        .fixed_windows("side-window", 10_000_000)
        .unwrap();
    let sink = ironbeam_core::cogroup_by_key_2_named("group", &main, &side)
        .unwrap()
        .map_named("row", |(key, buckets)| format!("{key}:{buckets:?}"))
        .unwrap()
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "shared-branch-at-source-v1".into(),
            {
                let rows = Arc::clone(&rows);
                let accepted = Arc::clone(&accepted);
                move || {
                    Ok(Box::new(Capture {
                        rows: Arc::clone(&rows),
                        accepted: Arc::clone(&accepted),
                        staged: Vec::new(),
                        bundle: String::new(),
                    }))
                }
            },
        )
        .unwrap();
    DirectRunner
        .start_stream_sink(&sink, &JobConfig::default())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(rows.lock().unwrap().as_slice(), ["1:([2], [\"4\"])"]);
}
