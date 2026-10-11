//! Extract sample server logs, aggregate path traffic, and load CSV and JSONL.
//!
//! Run with `cargo run --example etl_pipeline --features io-csv`.

use ironbeam::v5::{
    DirectRunner, Element, JobConfig, Pipeline, csv_sink, csv_source, jsonl_sink, jsonl_source,
    prelude::*,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
struct RawLogEntry {
    path: String,
    status: u16,
    bytes: u64,
    user_agent: String,
}
impl Element for RawLogEntry {
    const TYPE_ID: &'static str = "examples/raw-log";
    const CODER_ID: &'static str = "examples/raw-log-local";
}

#[derive(Clone, Debug)]
struct CleanLogEntry {
    path: String,
    status: u16,
    bytes: u64,
    is_bot: bool,
}
impl Element for CleanLogEntry {
    const TYPE_ID: &'static str = "examples/clean-log";
    const CODER_ID: &'static str = "examples/clean-log-local";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct PathStats {
    path: String,
    total_requests: u64,
    total_bytes: u64,
    avg_bytes: f64,
    error_count: u64,
}
impl Element for PathStats {
    const TYPE_ID: &'static str = "examples/path-stats";
    const CODER_ID: &'static str = "examples/path-stats-local";
}

fn sample_logs() -> Vec<RawLogEntry> {
    [
        ("/api/users", 200, 1024, "Mozilla/5.0"),
        ("/api/users", 201, 512, "curl/7.64.1"),
        ("/api/posts", 200, 2048, "Googlebot/2.1"),
        ("/api/users", 404, 256, "Mozilla/5.0"),
        ("/api/posts", 500, 128, "PostmanRuntime/7.26.8"),
    ]
    .into_iter()
    .map(|(path, status, bytes, user_agent)| RawLogEntry {
        path: path.into(),
        status,
        bytes,
        user_agent: user_agent.into(),
    })
    .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline = Pipeline::default();
    let logs = pipeline.from_vec("raw-logs", sample_logs())?;
    let clean = logs
        .filter_named("valid-logs", |row| row.status > 0 && row.bytes < 10_000_000)?
        .map_named("detect-bots", |row| CleanLogEntry {
            path: row.path,
            status: row.status,
            bytes: row.bytes,
            is_bot: row.user_agent.to_ascii_lowercase().contains("bot"),
        })?;
    let stats = clean
        .key_by_named("by-path", |row| row.path.clone())?
        .map_values_named("path-measures", |row| {
            (row.bytes, u64::from(row.status >= 400))
        })?
        .group_by_key_named("path-groups")?
        .map_named("path-stats", |(path, values)| {
            let total_requests = u64::try_from(values.len()).unwrap();
            let total_bytes = values.iter().map(|(bytes, _)| bytes).sum();
            let error_count = values.iter().map(|(_, errors)| errors).sum();
            // The report is approximate by design; source totals stay exact integers.
            #[allow(clippy::cast_precision_loss)]
            let avg_bytes = total_bytes as f64 / total_requests as f64;
            PathStats {
                path,
                total_requests,
                total_bytes,
                avg_bytes,
                error_count,
            }
        })?;

    let mut all_stats = stats.collect_seq()?;
    all_stats.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(all_stats.len(), 2);
    assert_eq!(all_stats[0].path, "/api/posts");
    assert_eq!(all_stats[0].total_bytes, 2176);
    assert_eq!(all_stats[1].path, "/api/users");
    assert_eq!(all_stats[1].total_requests, 3);
    assert_eq!(all_stats.iter().map(|row| row.error_count).sum::<u64>(), 2);

    let top = stats
        .map_named("traffic-rank", |row| (row.total_bytes, row.path))?
        .top_k_globally_named("top-paths", 1)?
        .collect_seq()?;
    assert_eq!(top, vec![vec![(2176, "/api/posts".to_string())]]);
    let mut bots = clean
        .key_by_named("by-bot", |row| row.is_bot)?
        .count_per_key_named("bot-groups", "bot-counts")?
        .collect_seq()?;
    bots.sort_by_key(|(is_bot, _)| *is_bot);
    assert_eq!(bots, vec![(false, 4), (true, 1)]);

    let dir = tempfile::tempdir()?;
    let csv_path = dir.path().join("path_stats.csv");
    let jsonl_path = dir.path().join("path_stats.jsonl");
    DirectRunner.run_sink(
        &csv_sink(&stats, "csv-output", &csv_path)?,
        &JobConfig::default(),
    )?;
    DirectRunner.run_sink(
        &jsonl_sink(&stats, "jsonl-output", &jsonl_path)?,
        &JobConfig::default(),
    )?;
    let mut csv_rows =
        csv_source::<PathStats>(&Pipeline::default(), "csv-verify", &csv_path)?.collect_seq()?;
    let mut jsonl_rows =
        jsonl_source::<PathStats>(&Pipeline::default(), "jsonl-verify", &jsonl_path)?
            .collect_seq()?;
    csv_rows.sort_by(|a, b| a.path.cmp(&b.path));
    jsonl_rows.sort_by(|a, b| a.path.cmp(&b.path));
    assert_eq!(csv_rows, all_stats);
    assert_eq!(jsonl_rows, all_stats);

    for row in all_stats {
        println!(
            "{}: {} requests, {} bytes",
            row.path, row.total_requests, row.total_bytes
        );
    }
    println!("Bots: {}, humans: {}", bots[1].1, bots[0].1);
    Ok(())
}
