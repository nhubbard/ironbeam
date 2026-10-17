//! Run with `cargo run -p ironbeam-compat --release --example v4_benchmark_corpus -- 100000`.
//! Emits CSV timings. Measure peak RSS externally with `/usr/bin/time -l` on macOS.
use ironbeam_v4::{Pipeline, from_vec};
use std::time::Instant;

#[allow(clippy::too_many_lines)] // Keep the seven reference workload bodies together.
fn main() {
    let count: u32 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".into())
        .parse()
        .expect("positive row count");
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
        let copied = from_vec(&p, (0..count).collect()).collect_seq().unwrap();
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
        let mapped = from_vec(&p, (0..count).collect())
            .map(|n| n.wrapping_mul(3))
            .filter(|n| n % 2 == 0)
            .collect_seq()
            .unwrap();
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
        let groups = from_vec(&p, (0..count).map(|n| (n % 100, n)).collect())
            .group_by_key()
            .collect_seq()
            .unwrap();
        println!(
            "group_by_key,{count},{},{},{}",
            start.elapsed().as_nanos(),
            groups.len(),
            groups.iter().map(|(_, v)| v.len()).sum::<usize>()
        );
    }

    if selected("latest_per_key") {
        let p = Pipeline::default();
        let start = Instant::now();
        let latest = from_vec(
            &p,
            (0..count)
                .map(|n| {
                    (
                        n % 100,
                        ironbeam_v4::window::Timestamped::new(u64::from(n), n),
                    )
                })
                .collect(),
        )
        .latest_per_key()
        .collect_seq()
        .unwrap();
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
        let left = from_vec(&p, (0..count).map(|n| (n, n)).collect());
        let right = from_vec(&p, (0..count).map(|n| (n, n.wrapping_add(1))).collect());
        let joined = left.join_inner(&right).collect_seq().unwrap();
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
        let left = from_vec(&p, (0..count).map(|n| (n % 100, n)).collect());
        let right = from_vec(&p, (0..count.min(100)).map(|n| (n, n + 1)).collect());
        let joined = left.join_inner(&right).collect_seq().unwrap();
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
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rows.jsonl");
        let p = Pipeline::default();
        let start = Instant::now();
        let input = from_vec(&p, (0..count).collect());
        let source_ready = Instant::now();
        let written = input.write_jsonl(&path).unwrap();
        let write_ready = Instant::now();
        let p = Pipeline::default();
        let output = ironbeam_v4::read_jsonl::<u32>(&p, &path).unwrap();
        let read_graph_ready = Instant::now();
        let read = output.collect_seq().unwrap();
        let read_ready = Instant::now();
        assert_eq!(written, read.len());
        if std::env::var_os("IRONBEAM_BENCH_PHASES").is_some() {
            eprintln!(
                "jsonl_phases_ns,source={},write={},read_graph={},read={}",
                source_ready.duration_since(start).as_nanos(),
                write_ready.duration_since(source_ready).as_nanos(),
                read_graph_ready.duration_since(write_ready).as_nanos(),
                read_ready.duration_since(read_graph_ready).as_nanos()
            );
        }
        println!(
            "jsonl_roundtrip,{count},{},{},{}",
            start.elapsed().as_nanos(),
            read.len(),
            read.iter().map(|&n| u64::from(n)).sum::<u64>()
        );
    }
}
