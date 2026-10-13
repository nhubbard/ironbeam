//! A two-source join resumes from a durable checkpoint in a fresh OS process.

use ironbeam_core::{
    Error, Pipeline, ProgressSignal, Replay, SinkMode, SinkSpec, SourceProgress, Timestamped,
    TypedSink, TypedUnboundedSourceReader, UnboundedBatch, UnboundedPoll, UnboundedSource,
};
use ironbeam_direct::{CheckpointConfig, DirectRunner, JobConfig};
use std::collections::VecDeque;
use std::fmt::Write as FmtWrite;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct Reader {
    polls: VecDeque<UnboundedPoll<Timestamped<i32>>>,
}

impl UnboundedSource<Timestamped<i32>> for Reader {
    fn poll(&mut self, _: usize) -> Result<UnboundedPoll<Timestamped<i32>>, Error> {
        Ok(self.polls.pop_front().unwrap_or(UnboundedPoll::Pending))
    }
}

fn ready(
    key: &str,
    cursor: u8,
    watermark: i64,
    completed: bool,
    value: Option<i32>,
) -> UnboundedPoll<Timestamped<i32>> {
    UnboundedPoll::Ready(UnboundedBatch {
        elements: value
            .into_iter()
            .map(|value| Timestamped {
                value,
                timestamp_ns: 1,
            })
            .collect(),
        progress: ProgressSignal {
            source_key: key.into(),
            split_id: "split-0".into(),
            checkpoint_position: Some(vec![cursor]),
            event_time_ns: Some(watermark),
            completed,
        },
    })
}

struct OneResultSink {
    path: PathBuf,
    staged: Vec<i32>,
}

impl TypedSink<i32> for OneResultSink {
    fn write(&mut self, value: i32) -> Result<(), Error> {
        self.staged.push(value);
        Ok(())
    }
    fn commit(&mut self) -> Result<(), Error> {
        if self.staged.is_empty() {
            return Ok(());
        }
        let bytes = self.staged.iter().fold(String::new(), |mut bytes, value| {
            writeln!(bytes, "{value}").unwrap();
            bytes
        });
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)
        {
            Ok(mut file) => {
                file.write_all(bytes.as_bytes())
                    .and_then(|()| file.sync_all())
                    .map_err(|error| Error::incompatible(error.to_string()))?;
                File::open(self.path.parent().expect("output parent"))
                    .and_then(|parent| parent.sync_all())
                    .map_err(|error| Error::incompatible(error.to_string()))?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if std::fs::read_to_string(&self.path)
                    .map_err(|error| Error::incompatible(error.to_string()))?
                    != bytes
                {
                    return Err(Error::incompatible("changed retry output"));
                }
            }
            Err(error) => return Err(Error::incompatible(error.to_string())),
        }
        self.staged.clear();
        Ok(())
    }
    fn abort(&mut self) -> Result<(), Error> {
        self.staged.clear();
        Ok(())
    }
}

#[allow(clippy::too_many_lines)] // Both graph variants share one durable source and sink fixture.
fn pipeline(directory: &Path, stage: &str, fanout: bool) -> ironbeam_core::Sink<i32> {
    let pipeline = Pipeline::default();
    let source = |key: &str, value: i32| {
        let source_key = key.to_owned();
        let log = directory.join("opens.txt");
        let stage = stage.to_owned();
        pipeline
            .unbounded_source_with_factory(
                key,
                SourceProgress {
                    stable_split_id: true,
                    replay: Replay::AtLeastOnce,
                    reports_event_time: true,
                },
                format!("{key}-process-restart-v1"),
                0,
                move |cursor| {
                    let position = cursor
                        .as_deref()
                        .map_or_else(|| "none".to_owned(), |bytes| bytes[0].to_string());
                    let mut file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&log)
                        .map_err(|error| Error::incompatible(error.to_string()))?;
                    writeln!(file, "{source_key}:{position}")
                        .map_err(|error| Error::incompatible(error.to_string()))?;
                    file.sync_all()
                        .map_err(|error| Error::incompatible(error.to_string()))?;
                    let polls = match (source_key.as_str(), cursor.as_deref(), stage.as_str()) {
                        ("main", None, "checkpoint") => vec![
                            ready(&source_key, 1, 0, false, Some(value)),
                            ready(&source_key, 2, 10, true, None),
                        ],
                        ("side", None, "checkpoint") => {
                            vec![ready(&source_key, 1, 0, false, Some(value))]
                        }
                        ("side", Some([1]), "resume") => {
                            vec![ready(&source_key, 2, 10, true, None)]
                        }
                        _ => return Err(Error::incompatible("unexpected process restart cursor")),
                    };
                    Ok(Box::new(Reader {
                        polls: polls.into(),
                    }) as TypedUnboundedSourceReader<i32>)
                },
            )
            .unwrap()
            .fixed_windows(&format!("{key}-window"), 10)
            .unwrap()
            .map_named(&format!("{key}-key"), |value| (1_i32, value))
            .unwrap()
    };
    let main = source("main", 10);
    let side = source("side", 20);
    let path = directory.join("result.txt");
    let inner = main
        .join_inner_named("join", &side)
        .unwrap()
        .map_named("sum", |(key, (left, right))| key * 100 + left + right)
        .unwrap();
    let output = if fanout {
        let left = main
            .join_left_named("left", &side)
            .unwrap()
            .map_named("left-sum", |(key, (left, right))| {
                100 + key * 100 + left + right.unwrap_or_default()
            })
            .unwrap();
        let right = main
            .join_right_named("right", &side)
            .unwrap()
            .map_named("right-sum", |(key, (left, right))| {
                200 + key * 100 + left.unwrap_or_default() + right
            })
            .unwrap();
        let full = main
            .join_full_named("full", &side)
            .unwrap()
            .map_named("full-sum", |(key, (left, right))| {
                300 + key * 100 + left.unwrap_or_default() + right.unwrap_or_default()
            })
            .unwrap();
        inner.flatten_named("merge", &[left, right, full]).unwrap()
    } else {
        inner
    };
    output
        .sink_named_with_config(
            "sink",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            "dual-process-sink-v1".into(),
            move || {
                Ok(Box::new(OneResultSink {
                    path: path.clone(),
                    staged: Vec::new(),
                }))
            },
        )
        .unwrap()
}

fn config(directory: &Path) -> JobConfig {
    JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.join("checkpoint"),
            interval_batches: 1,
            max_checkpoints: 2,
            recover: true,
            binary_build_id: "dual-process-build-v1".into(),
        }),
        ..JobConfig::default()
    }
}

#[test]
fn two_source_join_child() {
    let Ok(directory) = std::env::var("IRONBEAM_DUAL_PROCESS_DIR") else {
        return;
    };
    let stage = std::env::var("IRONBEAM_DUAL_PROCESS_STAGE").unwrap();
    let directory = Path::new(&directory);
    let fanout = std::env::var_os("IRONBEAM_DUAL_PROCESS_FANOUT").is_some();
    let sink = pipeline(directory, &stage, fanout);
    let job = DirectRunner
        .start_stream_sink(&sink, &config(directory))
        .unwrap();
    if stage == "checkpoint" {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if job
                .progress()
                .and_then(|signal| signal.checkpoint_position)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u64::from_le_bytes)
                .is_some_and(|sequence| sequence >= 3)
            {
                assert!(!directory.join("result.txt").exists());
                std::process::exit(0);
            }
            assert!(
                Instant::now() < deadline,
                "open join window was not checkpointed"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    job.wait().unwrap();
}

#[test]
fn two_source_join_recovers_across_worker_processes() {
    run_process_restart(false);
}

#[test]
fn stateful_fanout_recovers_across_worker_processes() {
    run_process_restart(true);
}

fn run_process_restart(fanout: bool) {
    let directory = tempfile::tempdir().unwrap();
    let mut pids = Vec::new();
    for stage in ["checkpoint", "resume", "completed"] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "two_source_join_child", "--nocapture"])
            .env("IRONBEAM_DUAL_PROCESS_DIR", directory.path())
            .env("IRONBEAM_DUAL_PROCESS_STAGE", stage)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if fanout {
            command.env("IRONBEAM_DUAL_PROCESS_FANOUT", "1");
        }
        let child = command.spawn().unwrap();
        pids.push(child.id());
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "child {stage} failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(
        std::fs::read_to_string(directory.path().join("result.txt")).unwrap(),
        if fanout {
            "130\n230\n330\n430\n"
        } else {
            "130\n"
        }
    );
    let mut opens = std::fs::read_to_string(directory.path().join("opens.txt"))
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    opens.sort();
    assert_eq!(opens, ["main:none", "side:1", "side:none"]);
    assert_ne!(pids[0], pids[1]);
    assert_ne!(pids[1], pids[2]);
}
