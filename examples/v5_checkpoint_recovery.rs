//! Recover a completed v5 Direct aggregate from a versioned local checkpoint.
//!
//! Run with `cargo run --example v5_checkpoint_recovery`.
//! The checkpoint is scoped to this example run; production jobs should use a
//! stable directory and a build ID that changes when executable code changes.

use ironbeam::v5::{CheckpointConfig, DirectRunner, JobConfig, Output, Pipeline};

fn graph() -> Result<Output<i64>, ironbeam::v5::Error> {
    Pipeline::default()
        .from_vec("numbers", (0..10_000_i64).collect())?
        .map_named("double", |value| value * 2)?
        .filter_named("multiples-of-three", |value| value % 3 == 0)?
        .combine_globally_named("sum", |values| values.into_iter().sum::<i64>())?
        .output("total")
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let config = JobConfig {
        checkpoint: Some(CheckpointConfig {
            directory: directory.path().to_path_buf(),
            interval_batches: 1,
            binary_build_id: "v5-checkpoint-recovery-example-v1".into(),
            ..CheckpointConfig::default()
        }),
        ..JobConfig::default()
    };
    let expected = (0..10_000_i64)
        .map(|value| value * 2)
        .filter(|value| value % 3 == 0)
        .sum::<i64>();

    let (first, initial_job) = DirectRunner.run_collect(&graph()?, &config)?;
    assert_eq!(first, vec![expected]);
    assert_eq!(initial_job.metrics().source_elements, 10_000);
    assert!(initial_job.metrics().checkpoints > 0);

    // Build a new graph to model a restarted process with the same stable
    // node keys, source contents, coder, and executable build identity.
    let (recovered, resumed_job) = DirectRunner.run_collect(&graph()?, &config)?;
    assert_eq!(recovered, first);
    assert_eq!(resumed_job.metrics().source_elements, 0);
    assert_eq!(resumed_job.metrics().recovered_elements, 1);
    println!("Recovered total {expected} without rereading the source.");
    Ok(())
}
