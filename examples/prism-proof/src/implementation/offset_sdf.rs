use std::collections::BTreeSet;
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use std::time::Duration;

use ironbeam_core::{
    Error, OffsetRange, PCollection, Pipeline, ProgressSignal, Replay, Sink, SinkMode, SinkSpec,
    SourceProgress, TestStreamEvent, Timestamped, TypedSink, TypedUnboundedSourceReader,
    UnboundedBatch, UnboundedPoll, Version,
};

struct OffsetProofSink {
    path: PathBuf,
    values: Vec<i32>,
}

impl TypedSink<i32> for OffsetProofSink {
    fn write(&mut self, value: i32) -> Result<(), Error> {
        self.values.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        file.lock()
            .map_err(|error| Error::incompatible(error.to_string()))?;
        let mut current = String::new();
        file.read_to_string(&mut current)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        let mut rows = current
            .lines()
            .map(|line| {
                line.parse::<i32>()
                    .map_err(|error| Error::incompatible(error.to_string()))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        rows.extend(self.values.drain(..));
        file.set_len(0)
            .map_err(|error| Error::incompatible(error.to_string()))?;
        file.rewind()
            .map_err(|error| Error::incompatible(error.to_string()))?;
        for value in rows {
            writeln!(file, "{value}").map_err(|error| Error::incompatible(error.to_string()))?;
        }
        file.sync_all()
            .map_err(|error| Error::incompatible(error.to_string()))
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.values.clear();
        Ok(())
    }
}

/// A bounded offset callback with two input elements and explicit event times.
pub fn bounded_offset_sdf_fixture(pipeline: &Pipeline, path: PathBuf) -> Result<Sink<i32>, Error> {
    offset_sdf_fixture(
        pipeline.from_vec("offset-input", vec![1_i32, 10])?,
        path,
        None,
    )
}

/// A bounded callback that splits one restriction and resumes its watermark.
pub fn bounded_offset_sdf_residual_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    offset_sdf_fixture(
        pipeline.from_vec("offset-input", vec![1_i32, 10])?,
        path,
        Some(1),
    )
}

/// The same finite offset work from a scripted unbounded input.
pub fn streaming_offset_sdf_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    offset_sdf_fixture(
        pipeline.test_stream_source(
            "offset-events",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![(10_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
            ],
        )?,
        path,
        None,
    )
}

/// The residual callback driven by the same finite TestStream as the plain job.
pub fn streaming_offset_sdf_residual_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    offset_sdf_fixture(
        pipeline.test_stream_source(
            "offset-events",
            vec![
                TestStreamEvent::Elements(vec![(1_i32, 0)]),
                TestStreamEvent::AdvanceWatermark(1_000_000),
                TestStreamEvent::Elements(vec![(10_i32, 1_000_000)]),
                TestStreamEvent::AdvanceWatermark(4_000_000),
            ],
        )?,
        path,
        Some(1),
    )
}

/// A resumable production source followed by a user offset residual.
pub fn production_offset_sdf_residual_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
) -> Result<Sink<i32>, Error> {
    offset_sdf_fixture(
        production_offset_source(pipeline)?
            .map_named("production-offset-value", |value| value * 10)?,
        path,
        Some(70),
    )
}

/// A slow offset callback whose trace distinguishes a runner split from an initial restriction.
pub fn runner_split_offset_sdf_fixture(
    pipeline: &Pipeline,
    path: PathBuf,
    trace: PathBuf,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    pipeline
        .from_vec("runner-split-input", vec![100_i32])?
        .splittable_flat_map_named(
            "runner-split-offsets",
            "prism-proof/runner-split-offsets",
            Version { major: 1, minor: 0 },
            |_| OffsetRange::new(0, 8),
            move |value, context| {
                let restriction = context.tracker().restriction()?;
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&trace)
                    .map_err(|error| Error::incompatible(error.to_string()))?;
                file.lock()
                    .map_err(|error| Error::incompatible(error.to_string()))?;
                writeln!(file, "{}..{}", restriction.start, restriction.stop)
                    .map_err(|error| Error::incompatible(error.to_string()))?;
                drop(file);
                for offset in restriction.start..restriction.stop {
                    if !context.try_claim(offset)? {
                        break;
                    }
                    context.emit(*value + offset as i32, offset * 1_000_000)?;
                    if restriction.start == 0 && offset == 0 {
                        std::thread::sleep(Duration::from_millis(1_500));
                    }
                }
                Ok(())
            },
        )?
        .sink_named_with_config(
            "runner-split-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("runner-split-offset-sdf:{}", path.display()),
            move || {
                Ok(Box::new(OffsetProofSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}

fn production_offset_source(pipeline: &Pipeline) -> Result<PCollection<i32>, Error> {
    pipeline.unbounded_source_with_factory(
        "production-offset-events",
        SourceProgress {
            stable_split_id: true,
            replay: Replay::AtLeastOnce,
            reports_event_time: true,
        },
        "prism-proof/production-offset-source-v1".into(),
        0,
        move |position| {
            let batch = |values: Vec<i32>, cursor: u8, completed| {
                UnboundedPoll::Ready(UnboundedBatch {
                    elements: values
                        .into_iter()
                        .map(|value| Timestamped {
                            value,
                            timestamp_ns: i64::from(cursor) * 1_000_000,
                        })
                        .collect(),
                    progress: ProgressSignal {
                        source_key: "production-offset-events".into(),
                        split_id: "stable-offset-split".into(),
                        checkpoint_position: Some(vec![cursor]),
                        event_time_ns: Some(i64::from(cursor) * 1_000_000),
                        completed,
                    },
                })
            };
            let polls = match position.as_deref() {
                None => vec![
                    UnboundedPoll::Pending,
                    batch(vec![7, 9], 1, false),
                    UnboundedPoll::Pending,
                    batch(vec![8], 2, true),
                ],
                Some([1]) => vec![UnboundedPoll::Pending, batch(vec![8], 2, true)],
                _ => return Err(Error::incompatible("unexpected production offset cursor")),
            };
            Ok(Box::new(super::ProductionSource(polls.into())) as TypedUnboundedSourceReader<i32>)
        },
    )
}

fn offset_sdf_fixture(
    input: PCollection<i32>,
    path: PathBuf,
    split_value: Option<i32>,
) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    input
        .splittable_flat_map_named(
            "offset-expand",
            "prism-proof/offset-expand",
            Version { major: 1, minor: 0 },
            |_| OffsetRange::new(0, 3),
            move |value, context| {
                if Some(*value) == split_value {
                    let restriction = context.tracker().restriction()?;
                    if restriction.start == 0 {
                        context.tracker().try_split(0.5)?;
                        context.update_watermark(1_000_000)?;
                    } else if context.estimator().current()? != Some(1_000_000) {
                        return Err(Error::incompatible(
                            "offset residual lost its watermark estimator state",
                        ));
                    }
                }
                let restriction = context.tracker().restriction()?;
                for offset in restriction.start..restriction.stop {
                    if !context.try_claim(offset)? {
                        break;
                    }
                    context.emit(*value + offset as i32, offset * 1_000_000)?;
                    if *value == 1
                        && offset == 1
                        && std::env::var_os("IRONBEAM_TEST_KILL_OFFSET_SDF_CHILD").is_some()
                    {
                        super::await_supervised_source_replacement(Some(&[1]))?;
                    }
                }
                Ok(())
            },
        )?
        .sink_named_with_config(
            "offset-result",
            SinkSpec {
                mode: SinkMode::Append,
                replay: Replay::Idempotent,
                commit: true,
            },
            format!("offset-sdf:{}", path.display()),
            move || {
                Ok(Box::new(OffsetProofSink {
                    path: sink_path.clone(),
                    values: Vec::new(),
                }))
            },
        )
}
