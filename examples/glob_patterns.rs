//! Read partitioned JSONL files with stable v5 glob sources.
//!
//! Run with `cargo run --example glob_patterns`.

use anyhow::Result;
use ironbeam::v5::{
    DirectRunner, Element, JobConfig, Pipeline, jsonl_glob_source, jsonl_sink, prelude::*,
};
use serde::{Deserialize, Serialize};
use std::fs::create_dir_all;
use std::path::Path;
use tempfile::TempDir;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct LogEntry {
    timestamp: String,
    level: String,
    message: String,
}

impl Element for LogEntry {
    const TYPE_ID: &'static str = "examples/glob-log-entry";
    const CODER_ID: &'static str = "examples/glob-log-entry-local";
}

fn write_logs(path: &Path, entries: Vec<LogEntry>) -> Result<()> {
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("fixture-rows", entries)?;
    let sink = jsonl_sink(&input, "fixture-write", path)?;
    DirectRunner.run_sink(&sink, &JobConfig::default())?;
    Ok(())
}

fn main() -> Result<()> {
    let temp = TempDir::new()?;
    let base = temp.path();
    println!("Setting up example data in {}", base.display());

    for day in 1..=3 {
        let day_dir = base.join(format!("logs/year=2024/month=01/day={day:02}"));
        create_dir_all(&day_dir)?;
        let log_file = day_dir.join("events.jsonl");
        write_logs(
            &log_file,
            vec![
                LogEntry {
                    timestamp: format!("2024-01-{day:02}T10:00:00"),
                    level: "INFO".into(),
                    message: format!("Day {day} event 1"),
                },
                LogEntry {
                    timestamp: format!("2024-01-{day:02}T11:00:00"),
                    level: "ERROR".into(),
                    message: format!("Day {day} event 2"),
                },
            ],
        )?;
        println!("Created {}", log_file.display());
    }

    let pattern = format!("{}/logs/year=*/month=*/day=*/events.jsonl", base.display());
    println!("\nAll log files: {pattern}");
    let pipeline = Pipeline::default();
    let logs = jsonl_glob_source::<LogEntry>(&pipeline, "all-logs", &pattern)?;
    let all_logs = logs.collect_seq()?;
    assert_eq!(all_logs.len(), 6);
    println!("Total log entries read: {}", all_logs.len());

    let day_one = base.join("logs/year=2024/month=01/day=01/events.jsonl");
    let pipeline = Pipeline::default();
    let day_one_logs =
        jsonl_glob_source::<LogEntry>(&pipeline, "day-one", &day_one.to_string_lossy())?
            .collect_seq()?;
    assert_eq!(day_one_logs.len(), 2);
    println!("Day 1 entries: {}", day_one_logs.len());

    let pipeline = Pipeline::default();
    let error_logs = jsonl_glob_source::<LogEntry>(&pipeline, "error-input", &pattern)?
        .filter_named("errors", |entry| entry.level == "ERROR")?
        .collect_seq()?;
    assert_eq!(error_logs.len(), 3);
    println!("Total ERROR entries: {}", error_logs.len());

    let data_dir = base.join("data");
    create_dir_all(&data_dir)?;
    for batch in 1..=3 {
        let file = data_dir.join(format!("batch_{batch}.jsonl"));
        write_logs(
            &file,
            vec![LogEntry {
                timestamp: format!("2024-01-01T{batch:02}:00:00"),
                level: "INFO".into(),
                message: format!("Batch {batch} processing"),
            }],
        )?;
    }
    let pipeline = Pipeline::default();
    let batch_pattern = format!("{}/data/*.jsonl", base.display());
    let batches =
        jsonl_glob_source::<LogEntry>(&pipeline, "batches", &batch_pattern)?.collect_seq()?;
    assert_eq!(batches.len(), 3);
    println!("Total batch entries: {}", batches.len());
    println!("All examples completed successfully!");
    Ok(())
}
