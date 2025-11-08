//! Complete ETL (Extract, Transform, Load) pipeline example.
//!
//! This example demonstrates a realistic data processing workflow:
//! 1. **Extract**: Read web server logs from multiple sources
//! 2. **Transform**: Clean, validate, enrich, and aggregate data
//! 3. **Load**: Write results to multiple output formats
//!
//! Run with: cargo run --example etl_pipeline --features io-jsonl,io-csv

use anyhow::Result;
use rustflow::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawLogEntry {
    timestamp: u64,
    ip: String,
    method: String,
    path: String,
    status: u16,
    bytes: u64,
    user_agent: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd)]
struct CleanLogEntry {
    timestamp: u64,
    ip: String,
    method: String,
    path: String,
    status: u16,
    bytes: u64,
    is_bot: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PathStats {
    path: String,
    total_requests: u64,
    total_bytes: u64,
    avg_bytes: f64,
    error_count: u64,
}

impl PartialEq for PathStats {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Eq for PathStats {}

impl PartialOrd for PathStats {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PathStats {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.cmp(&other.path)
    }
}

fn main() -> Result<()> {
    println!("🚀 ETL Pipeline Example: Web Server Log Analysis\n");

    let pipeline = Pipeline::default();

    // =============================================================================
    // EXTRACT: Generate sample data (simulating multiple log files)
    // =============================================================================
    println!("📥 EXTRACT: Loading log data...");

    let raw_logs = vec![
        RawLogEntry {
            timestamp: 1_000_000,
            ip: "192.168.1.100".to_string(),
            method: "GET".to_string(),
            path: "/api/users".to_string(),
            status: 200,
            bytes: 1024,
            user_agent: "Mozilla/5.0".to_string(),
        },
        RawLogEntry {
            timestamp: 1_000_100,
            ip: "192.168.1.101".to_string(),
            method: "POST".to_string(),
            path: "/api/users".to_string(),
            status: 201,
            bytes: 512,
            user_agent: "curl/7.64.1".to_string(),
        },
        RawLogEntry {
            timestamp: 1_000_200,
            ip: "192.168.1.102".to_string(),
            method: "GET".to_string(),
            path: "/api/posts".to_string(),
            status: 200,
            bytes: 2048,
            user_agent: "Googlebot/2.1".to_string(),
        },
        RawLogEntry {
            timestamp: 1_000_300,
            ip: "192.168.1.100".to_string(),
            method: "GET".to_string(),
            path: "/api/users".to_string(),
            status: 404,
            bytes: 256,
            user_agent: "Mozilla/5.0".to_string(),
        },
        RawLogEntry {
            timestamp: 1_000_400,
            ip: "192.168.1.103".to_string(),
            method: "DELETE".to_string(),
            path: "/api/posts".to_string(),
            status: 500,
            bytes: 128,
            user_agent: "PostmanRuntime/7.26.8".to_string(),
        },
    ];

    let logs = from_vec(&pipeline, raw_logs);

    println!(
        "  ✓ Loaded {} log entries\n",
        logs.clone().collect_seq()?.len()
    );

    // =============================================================================
    // TRANSFORM: Clean, validate, and enrich data
    // =============================================================================
    println!("🔄 TRANSFORM: Processing log data...");

    // Step 1: Data cleaning and validation
    let valid_logs = logs
        .filter(|entry: &RawLogEntry| {
            // Filter out invalid entries
            !entry.ip.is_empty() && entry.status > 0 && entry.bytes < 10_000_000
        })
        .map(|entry: &RawLogEntry| {
            // Detect bots based on user agent
            let is_bot = entry.user_agent.contains("bot")
                || entry.user_agent.contains("Bot")
                || entry.user_agent.contains("crawler");

            CleanLogEntry {
                timestamp: entry.timestamp,
                ip: entry.ip.clone(),
                method: entry.method.clone(),
                path: entry.path.clone(),
                status: entry.status,
                bytes: entry.bytes,
                is_bot,
            }
        });

    println!("  ✓ Cleaned and enriched log entries");

    // Step 2: Aggregate by path
    let path_stats = valid_logs
        .clone()
        .key_by(|entry: &CleanLogEntry| entry.path.clone())
        .map_values(|entry| {
            (
                1u64,                                          // request count
                entry.bytes,                                   // bytes
                if entry.status >= 400 { 1u64 } else { 0u64 }, // error count
            )
        })
        .group_by_key()
        .map(|(path, values): &(String, Vec<(u64, u64, u64)>)| {
            let total_requests: u64 = values.iter().map(|(count, _, _)| count).sum();
            let total_bytes: u64 = values.iter().map(|(_, bytes, _)| bytes).sum();
            let error_count: u64 = values.iter().map(|(_, _, errors)| errors).sum();
            let avg_bytes = if total_requests > 0 {
                total_bytes as f64 / total_requests as f64
            } else {
                0.0
            };

            PathStats {
                path: path.clone(),
                total_requests,
                total_bytes,
                avg_bytes,
                error_count,
            }
        });

    println!("  ✓ Aggregated statistics by path");

    // Step 3: Identify top paths by traffic
    let _top_paths = path_stats
        .clone()
        .map(|stats: &PathStats| (stats.total_bytes, stats.clone()))
        .top_k_per_key(3);

    println!("  ✓ Identified top paths by traffic\n");

    // =============================================================================
    // LOAD: Write results to output files
    // =============================================================================
    println!("💾 LOAD: Writing results...");

    // Collect and display results
    let all_stats = path_stats.clone().collect_seq_sorted()?;

    println!("\n📊 Path Statistics:");
    println!("{:-<80}", "");
    println!(
        "{:<30} {:>12} {:>12} {:>12} {:>12}",
        "Path", "Requests", "Total KB", "Avg KB", "Errors"
    );
    println!("{:-<80}", "");

    for stats in &all_stats {
        println!(
            "{:<30} {:>12} {:>12} {:>12.2} {:>12}",
            stats.path,
            stats.total_requests,
            stats.total_bytes / 1024,
            stats.avg_bytes / 1024.0,
            stats.error_count
        );
    }

    println!("{:-<80}", "");

    // Write to files
    #[cfg(feature = "io-csv")]
    {
        use rustflow::io::csv::write_csv_vec;
        write_csv_vec("path_stats.csv", true, &all_stats)?;
        println!("  ✓ Wrote path_stats.csv");
    }

    #[cfg(feature = "io-jsonl")]
    {
        use rustflow::io::jsonl::write_jsonl_vec;
        write_jsonl_vec("path_stats.jsonl", &all_stats)?;
        println!("  ✓ Wrote path_stats.jsonl");
    }

    // Compute additional metrics
    let bot_stats = valid_logs
        .clone()
        .key_by(|entry: &CleanLogEntry| entry.is_bot)
        .combine_values(Count);

    let bot_counts = bot_stats.collect_seq()?;

    println!("\n🤖 Bot vs Human Traffic:");
    for (is_bot, count) in bot_counts {
        println!(
            "  {}: {} requests",
            if is_bot { "Bots" } else { "Humans" },
            count
        );
    }

    println!("\n✅ ETL Pipeline Complete!");
    println!("\nKey Concepts Demonstrated:");
    println!("  • Extract: Reading from multiple sources");
    println!("  • Transform: Filtering, mapping, enrichment");
    println!("  • Load: Writing to multiple formats");
    println!("  • Aggregation: GroupByKey + custom aggregations");
    println!("  • TopK: Finding top items efficiently");

    Ok(())
}
