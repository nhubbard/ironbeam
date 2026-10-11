//! Run with `cargo run --release --example v5_hotpath_probe -- 100000`.
//! Splits the bounded scalar hot path into source, map, filter, and map/filter.

use ironbeam::v5::{JobConfig, Pipeline, prelude::*};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count: u32 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".into())
        .parse()?;
    assert!(count > 0);
    let batch_size = std::env::var("IRONBEAM_BENCH_BATCH")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(4096);
    assert!(batch_size > 0);
    println!("case,rows,source_ns,graph_ns,run_ns,output_rows,checksum");

    for case in ["source", "map", "filter", "map_filter"] {
        let pipeline = Pipeline::default();
        let started = Instant::now();
        let input = pipeline.from_vec("input", (0..count).collect())?;
        let source_ready = Instant::now();
        let output = match case {
            "source" => input.output("result")?,
            "map" => input.map_named("triple", |n| n.wrapping_mul(3))?.output("result")?,
            "filter" => input.filter_named("even", |n| n % 2 == 0)?.output("result")?,
            _ => input
                .map_named("triple", |n| n.wrapping_mul(3))?
                .filter_named("even", |n| n % 2 == 0)?
                .output("result")?,
        };
        let graph_ready = Instant::now();
        let values = output.collect_with(&JobConfig {
            buffer_budget_elements: batch_size,
            ..JobConfig::default()
        })?.0;
        let run_ready = Instant::now();
        let checksum = values.iter().map(|&value| u64::from(value)).sum::<u64>();
        println!(
            "{case},{count},{},{},{},{},{}",
            source_ready.duration_since(started).as_nanos(),
            graph_ready.duration_since(source_ready).as_nanos(),
            run_ready.duration_since(graph_ready).as_nanos(),
            values.len(),
            checksum
        );
    }
    Ok(())
}
