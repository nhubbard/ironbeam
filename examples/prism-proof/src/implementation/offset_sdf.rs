use std::collections::BTreeSet;
use std::io::{Read, Seek, Write};
use std::path::PathBuf;

use ironbeam_core::{
    Error, OffsetRange, Pipeline, Replay, Sink, SinkMode, SinkSpec, TypedSink, Version,
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
    let sink_path = path.clone();
    pipeline
        .from_vec("offset-input", vec![1_i32, 10])?
        .splittable_flat_map_named(
            "offset-expand",
            "prism-proof/offset-expand",
            Version { major: 1, minor: 0 },
            |_| OffsetRange::new(0, 3),
            |value, context| {
                for offset in 0..3 {
                    if !context.try_claim(offset)? {
                        break;
                    }
                    context.emit(*value + offset as i32, offset * 1_000_000)?;
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
