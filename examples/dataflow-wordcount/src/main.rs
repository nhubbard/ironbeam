//! V5 WordCount on Dataflow. The same binary rebuilds its graph in the driver
//! and portable worker; `run_v5_from_env` selects the role from worker argv.
//!
//! Set `GOOGLE_CLOUD_PROJECT`, `IRONBEAM_STAGING_LOCATION`, and
//! `IRONBEAM_TEMP_LOCATION`, then run `cargo run -p ironbeam-dataflow-wordcount --release` with
//! an x86_64 Linux worker binary configured in the Dataflow options.

use ironbeam_core::{Error, PCollection, Pipeline, Replay, SinkMode, SinkSpec, TypedSink};
use ironbeam_dataflow::{run_v5_from_env, RunOutcome};

const INPUT_LINES: [&str; 4] = [
    "the quick brown fox",
    "jumps over the lazy dog",
    "the quick brown dog",
    "the lazy fox jumps again",
];

fn counts(pipeline: &Pipeline) -> Result<PCollection<(String, u64)>, Error> {
    let lines = pipeline.from_vec(
        "lines",
        INPUT_LINES.iter().map(|line| (*line).to_owned()).collect(),
    )?;
    lines
        .flat_map_named("words", |line| {
            line.split_whitespace()
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
        })?
        .map_named("word-ones", |word| (word, 1_u64))?
        .group_by_key_named("group-words")?
        .map_named("word-counts", |(word, values)| {
            (word, values.into_iter().sum())
        })
}

#[derive(Default)]
struct LoggingSink(Vec<(String, u64)>);

impl TypedSink<(String, u64)> for LoggingSink {
    fn write(&mut self, value: (String, u64)) -> Result<(), Error> {
        self.0.push(value);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        for (word, count) in self.0.drain(..) {
            tracing::info!(%word, count, "wordcount");
        }
        Ok(())
    }

    fn abort(&mut self) -> Result<(), Error> {
        self.0.clear();
        Ok(())
    }
}

fn build(pipeline: &Pipeline) -> anyhow::Result<()> {
    counts(pipeline)?.sink_named_with_config(
        "log-counts",
        SinkSpec {
            mode: SinkMode::Append,
            replay: Replay::AtLeastOnce,
            commit: true,
        },
        "ironbeam:wordcount:logging:v1".into(),
        || Ok(Box::new(LoggingSink::default())),
    )?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match run_v5_from_env(build, "ironbeam-wordcount-v5").await? {
        RunOutcome::Submitted(outcome) => {
            tracing::info!(job_id = %outcome.job_id, state = %outcome.final_state, "wordcount finished");
        }
        RunOutcome::HarnessExited => tracing::info!("wordcount worker exited"),
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/main__tests.rs"]
mod tests;
