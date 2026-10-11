//! Provider-neutral object storage with an Ironbeam 5 pipeline.
//!
//! Run with `cargo run --example cloud_io_demo`. Both URI schemes use local
//! stores here; hosted providers can be registered under the same API.

use ironbeam::v5::cloud::{LocalObjectIO, ObjectStores};
use ironbeam::v5::{
    DirectRunner, JobConfig, Pipeline, cloud_jsonl_uri_sink, cloud_jsonl_uri_source,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut objects = ObjectStores::new();
    objects.register(
        "file",
        LocalObjectIO::new(directory.path().join("working"))?,
    )?;
    objects.register(
        "archive",
        LocalObjectIO::new(directory.path().join("archive"))?,
    )?;

    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("numbers", vec![1_u32, 2, 3])?;
    let sink = cloud_jsonl_uri_sink(
        &values,
        "write-jsonl",
        &objects,
        "file://batch/jobs/input.jsonl",
    )?;
    DirectRunner.run_sink(&sink, &JobConfig::default())?;

    let reader = Pipeline::default();
    let result = cloud_jsonl_uri_source::<u32>(
        &reader,
        "read-jsonl",
        &objects,
        "file://batch/jobs/input.jsonl",
    )?
    .map_named("double", |value| value * 2)?
    .output("result")?;
    let (rows, _) = DirectRunner.run_collect(&result, &JobConfig::default())?;
    assert_eq!(rows, vec![2, 4, 6]);

    let copied = objects.copy_prefix("file://batch/jobs/", "archive://history/run-1/")?;
    assert_eq!(copied, 1);
    assert_eq!(
        objects.read("file://batch/jobs/input.jsonl")?,
        objects.read("archive://history/run-1/input.jsonl")?
    );
    println!("Processed {rows:?}; archived {copied} object through URI routing");
    Ok(())
}
