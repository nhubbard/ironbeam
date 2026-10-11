//! Run with `cargo run --release --example v5_benchmark_corpus -- 100000`.
//! Emits the same workload names and checksums as the v4 baseline example.

use ironbeam::v5::{DirectRunner, JobConfig, Pipeline, jsonl_sink, jsonl_source, prelude::*};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count: u32 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".into())
        .parse()?;
    assert!(count > 0);
    println!("case,rows,elapsed_ns,output_rows,checksum");

    let p = Pipeline::default();
    let start = Instant::now();
    let copied = p.from_vec("copy-input", (0..count).collect())?
        .output("copy-result")?
        .collect_seq()?;
    println!("source_copy,{count},{},{},{}", start.elapsed().as_nanos(),
        copied.len(), copied.iter().map(|&n| u64::from(n)).sum::<u64>());

    let p = Pipeline::default();
    let start = Instant::now();
    let input = p.from_vec("map-input", (0..count).collect())?;
    let source_ready = Instant::now();
    let output = input
        .map_named("map-triple", |n| n.wrapping_mul(3))?
        .filter_named("map-even", |n| n % 2 == 0)?
        .output("map-result")?;
    let graph_ready = Instant::now();
    let mapped = output.collect_seq()?;
    let run_ready = Instant::now();
    if std::env::var_os("IRONBEAM_BENCH_PHASES").is_some() {
        eprintln!("map_filter_phases_ns,source={},graph={},run={}",
            source_ready.duration_since(start).as_nanos(),
            graph_ready.duration_since(source_ready).as_nanos(),
            run_ready.duration_since(graph_ready).as_nanos());
    }
    println!("map_filter,{count},{},{},{}", start.elapsed().as_nanos(),
        mapped.len(), mapped.iter().map(|&n| u64::from(n)).sum::<u64>());

    let p = Pipeline::default();
    let start = Instant::now();
    let groups = p.from_vec("group-input", (0..count).map(|n| (n % 100, n)).collect())?
        .group_by_key_named("group")?
        .output("group-result")?
        .collect_seq()?;
    println!("group_by_key,{count},{},{},{}", start.elapsed().as_nanos(),
        groups.len(), groups.iter().map(|(_, values)| values.len()).sum::<usize>());

    let p = Pipeline::default();
    let start = Instant::now();
    let left = p.from_vec("join-left", (0..count).map(|n| (n, n)).collect())?;
    let right = p.from_vec("join-right", (0..count).map(|n| (n, n.wrapping_add(1))).collect())?;
    let joined = left.join_inner_named("join", &right)?
        .output("join-result")?
        .collect_seq()?;
    println!("join_inner,{count},{},{},{}", start.elapsed().as_nanos(),
        joined.len(), joined.iter().map(|(_, (a, b))| u64::from(*a) + u64::from(*b)).sum::<u64>());

    let temp = tempfile::tempdir()?;
    let path = temp.path().join("rows.jsonl");
    let p = Pipeline::default();
    let start = Instant::now();
    let input = p.from_vec("jsonl-input", (0..count).collect())?;
    DirectRunner.run_sink(&jsonl_sink(&input, "jsonl-write", &path)?, &JobConfig::default())?;
    let read = jsonl_source::<u32>(&Pipeline::default(), "jsonl-read", &path)?
        .output("jsonl-result")?
        .collect_seq()?;
    assert_eq!(read.len(), count as usize);
    println!("jsonl_roundtrip,{count},{},{},{}", start.elapsed().as_nanos(),
        read.len(), read.iter().map(|&n| u64::from(n)).sum::<u64>());
    Ok(())
}
