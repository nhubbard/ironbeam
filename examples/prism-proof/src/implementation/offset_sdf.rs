use std::collections::BTreeSet;
use std::io::{Read, Seek, Write};
use std::path::PathBuf;

use ironbeam_core::{
    Error, OffsetRange, PCollection, Pipeline, Replay, Sink, SinkMode, SinkSpec, TestStreamEvent,
    TypedSink, Version,
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
    offset_sdf_fixture(pipeline.from_vec("offset-input", vec![1_i32, 10])?, path)
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
    )
}

fn offset_sdf_fixture(input: PCollection<i32>, path: PathBuf) -> Result<Sink<i32>, Error> {
    let sink_path = path.clone();
    input
        .splittable_flat_map_named(
            "offset-expand",
            "prism-proof/offset-expand",
            Version { major: 1, minor: 0 },
            |_| OffsetRange::new(0, 3),
            |value, context| {
                if *value == 1
                    && std::env::var_os("IRONBEAM_TEST_OFFSET_SDF_RESIDUAL_WATERMARK").is_some()
                {
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
