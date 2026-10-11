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
    let case = std::env::args().nth(2);
    assert!(case.as_deref().is_none_or(|name| matches!(
        name,
        "source_copy"
            | "map_filter"
            | "group_by_key"
            | "latest_per_key"
            | "join_inner"
            | "join_duplicates"
            | "jsonl_roundtrip"
    )));
    let selected = |name: &str| case.as_deref().is_none_or(|chosen| chosen == name);
    println!("case,rows,elapsed_ns,output_rows,checksum");

    if selected("source_copy") {
        let p = Pipeline::default();
        let start = Instant::now();
        let input = p.from_vec("copy-input", (0..count).collect())?;
        let source_ready = Instant::now();
        let output = input.output("copy-result")?;
        let graph_ready = Instant::now();
        let copied = output.collect_seq()?;
        let run_ready = Instant::now();
        if std::env::var_os("IRONBEAM_BENCH_PHASES").is_some() {
            eprintln!(
                "source_copy_phases_ns,source={},graph={},run={}",
                source_ready.duration_since(start).as_nanos(),
                graph_ready.duration_since(source_ready).as_nanos(),
                run_ready.duration_since(graph_ready).as_nanos()
            );
        }
        println!(
            "source_copy,{count},{},{},{}",
            start.elapsed().as_nanos(),
            copied.len(),
            copied.iter().map(|&n| u64::from(n)).sum::<u64>()
        );
    }

    if selected("map_filter") {
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
            eprintln!(
                "map_filter_phases_ns,source={},graph={},run={}",
                source_ready.duration_since(start).as_nanos(),
                graph_ready.duration_since(source_ready).as_nanos(),
                run_ready.duration_since(graph_ready).as_nanos()
            );
        }
        println!(
            "map_filter,{count},{},{},{}",
            start.elapsed().as_nanos(),
            mapped.len(),
            mapped.iter().map(|&n| u64::from(n)).sum::<u64>()
        );
    }

    if selected("group_by_key") {
        let p = Pipeline::default();
        let start = Instant::now();
        let group_input = p.from_vec("group-input", (0..count).map(|n| (n % 100, n)).collect())?;
        let source_ready = Instant::now();
        let group_output = group_input
            .group_by_key_named("group")?
            .output("group-result")?;
        let graph_ready = Instant::now();
        let groups = group_output.collect_seq()?;
        let run_ready = Instant::now();
        if std::env::var_os("IRONBEAM_BENCH_PHASES").is_some() {
            eprintln!(
                "group_by_key_phases_ns,source={},graph={},run={}",
                source_ready.duration_since(start).as_nanos(),
                graph_ready.duration_since(source_ready).as_nanos(),
                run_ready.duration_since(graph_ready).as_nanos()
            );
        }
        println!(
            "group_by_key,{count},{},{},{}",
            start.elapsed().as_nanos(),
            groups.len(),
            groups.iter().map(|(_, values)| values.len()).sum::<usize>()
        );
    }

    if selected("latest_per_key") {
        let p = Pipeline::default();
        let start = Instant::now();
        let input = p.from_vec("latest-input", (0..count).map(|n| (n % 100, n)).collect())?;
        let source_ready = Instant::now();
        let output = input
            .assign_timestamps_named("latest-time", |(_, n)| i64::from(*n) * 1_000_000)?
            .latest_per_key_named("latest")?
            .output("latest-result")?;
        let graph_ready = Instant::now();
        let latest = output.collect_seq()?;
        let run_ready = Instant::now();
        if std::env::var_os("IRONBEAM_BENCH_PHASES").is_some() {
            eprintln!(
                "latest_per_key_phases_ns,source={},graph={},run={}",
                source_ready.duration_since(start).as_nanos(),
                graph_ready.duration_since(source_ready).as_nanos(),
                run_ready.duration_since(graph_ready).as_nanos()
            );
        }
        println!(
            "latest_per_key,{count},{},{},{}",
            start.elapsed().as_nanos(),
            latest.len(),
            latest
                .iter()
                .map(|(_, value)| u64::from(*value))
                .sum::<u64>()
        );
    }

    if selected("join_inner") {
        let p = Pipeline::default();
        let start = Instant::now();
        let left = p.from_vec("join-left", (0..count).map(|n| (n, n)).collect())?;
        let right = p.from_vec(
            "join-right",
            (0..count).map(|n| (n, n.wrapping_add(1))).collect(),
        )?;
        let source_ready = Instant::now();
        let join_output = left
            .join_inner_named("join", &right)?
            .output("join-result")?;
        let graph_ready = Instant::now();
        let joined = join_output.collect_seq()?;
        let run_ready = Instant::now();
        if std::env::var_os("IRONBEAM_BENCH_PHASES").is_some() {
            eprintln!(
                "join_inner_phases_ns,source={},graph={},run={}",
                source_ready.duration_since(start).as_nanos(),
                graph_ready.duration_since(source_ready).as_nanos(),
                run_ready.duration_since(graph_ready).as_nanos()
            );
        }
        println!(
            "join_inner,{count},{},{},{}",
            start.elapsed().as_nanos(),
            joined.len(),
            joined
                .iter()
                .map(|(_, (a, b))| u64::from(*a) + u64::from(*b))
                .sum::<u64>()
        );
    }

    if selected("join_duplicates") {
        let p = Pipeline::default();
        let start = Instant::now();
        let left = p.from_vec("duplicate-left", (0..count).map(|n| (n % 100, n)).collect())?;
        let right = p.from_vec(
            "duplicate-right",
            (0..count.min(100)).map(|n| (n, n + 1)).collect(),
        )?;
        let joined = left
            .join_inner_named("duplicate-join", &right)?
            .output("duplicate-result")?
            .collect_seq()?;
        println!(
            "join_duplicates,{count},{},{},{}",
            start.elapsed().as_nanos(),
            joined.len(),
            joined
                .iter()
                .map(|(_, (a, b))| u64::from(*a) + u64::from(*b))
                .sum::<u64>()
        );
    }

    if selected("jsonl_roundtrip") {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("rows.jsonl");
        let p = Pipeline::default();
        let start = Instant::now();
        let input = p.from_vec("jsonl-input", (0..count).collect())?;
        DirectRunner.run_sink(
            &jsonl_sink(&input, "jsonl-write", &path)?,
            &JobConfig::default(),
        )?;
        let read = jsonl_source::<u32>(&Pipeline::default(), "jsonl-read", &path)?
            .output("jsonl-result")?
            .collect_seq()?;
        assert_eq!(read.len(), count as usize);
        println!(
            "jsonl_roundtrip,{count},{},{},{}",
            start.elapsed().as_nanos(),
            read.len(),
            read.iter().map(|&n| u64::from(n)).sum::<u64>()
        );
    }
    Ok(())
}
