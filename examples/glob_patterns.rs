//! Example demonstrating file globbing and pattern input for batch ETL operations.
//!
//! This example shows how to use glob patterns to read multiple files at once,
//! which is essential for real-world ETL scenarios with date-based partitions.

use rustflow::*;
use serde::{Deserialize, Serialize};
use std::fs::create_dir_all;
use tempfile::TempDir;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LogEntry {
    timestamp: String,
    level: String,
    message: String,
}

fn main() -> anyhow::Result<()> {
    // Set up a temporary directory with partitioned log files
    let temp = TempDir::new()?;
    let base = temp.path();

    println!("Setting up example data in {}", base.display());

    // Create date-partitioned directory structure
    for day in 1..=3 {
        let day_dir = base.join(format!("logs/year=2024/month=01/day={:02}", day));
        create_dir_all(&day_dir)?;

        let log_file = day_dir.join("events.jsonl");
        let entries = vec![
            LogEntry {
                timestamp: format!("2024-01-{:02}T10:00:00", day),
                level: "INFO".to_string(),
                message: format!("Day {} event 1", day),
            },
            LogEntry {
                timestamp: format!("2024-01-{:02}T11:00:00", day),
                level: "ERROR".to_string(),
                message: format!("Day {} event 2", day),
            },
        ];

        write_jsonl_vec(&log_file, &entries)?;
        println!("Created {}", log_file.display());
    }

    // Example 1: Read all log files using a glob pattern
    println!("\n=== Example 1: Read all logs with glob pattern ===");
    let p = Pipeline::default();
    let pattern = format!("{}/logs/year=*/month=*/day=*/events.jsonl", base.display());
    println!("Pattern: {}", pattern);

    let logs: PCollection<LogEntry> = read_jsonl(&p, &pattern)?;
    let all_logs = logs.collect_seq()?;
    println!("Total log entries read: {}", all_logs.len());

    // Example 2: Read only specific days
    println!("\n=== Example 2: Read specific day range ===");
    let p2 = Pipeline::default();
    let pattern2 = format!(
        "{}/logs/year=2024/month=01/day=01/events.jsonl",
        base.display()
    );
    println!("Pattern: {}", pattern2);

    let day1_logs: PCollection<LogEntry> = read_jsonl(&p2, &pattern2)?;
    let day1_entries = day1_logs.collect_seq()?;
    println!("Day 1 entries: {}", day1_entries.len());

    // Example 3: Process logs - filter and aggregate
    println!("\n=== Example 3: Process and aggregate ===");
    let p3 = Pipeline::default();
    let pattern3 = format!("{}/logs/year=*/month=*/day=*/events.jsonl", base.display());

    let error_logs = read_jsonl::<LogEntry>(&p3, &pattern3)?
        .filter(|entry| entry.level == "ERROR")
        .collect_seq()?;

    println!("Total ERROR entries: {}", error_logs.len());

    // Example 4: Using wildcards for multiple files in same directory
    println!("\n=== Example 4: Multiple files in directory ===");

    // Create multiple files in a single directory
    let data_dir = base.join("data");
    create_dir_all(&data_dir)?;

    for i in 1..=3 {
        let file = data_dir.join(format!("batch_{}.jsonl", i));
        let entries = vec![LogEntry {
            timestamp: format!("2024-01-01T{:02}:00:00", i),
            level: "INFO".to_string(),
            message: format!("Batch {} processing", i),
        }];
        write_jsonl_vec(&file, &entries)?;
    }

    let p4 = Pipeline::default();
    let pattern4 = format!("{}/data/*.jsonl", base.display());
    println!("Pattern: {}", pattern4);

    let batch_logs: PCollection<LogEntry> = read_jsonl(&p4, &pattern4)?;
    let batches = batch_logs.collect_seq()?;
    println!("Total batch entries: {}", batches.len());

    println!("\n✓ All examples completed successfully!");

    Ok(())
}
