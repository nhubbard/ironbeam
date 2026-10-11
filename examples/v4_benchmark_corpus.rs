//! Run with `cargo run --release --features legacy-v4 --example v4_benchmark_corpus -- 100000`.
//! Emits CSV timings. Measure peak RSS externally with `/usr/bin/time -l` on macOS.
use ironbeam::{Pipeline, from_vec};
use std::time::Instant;

fn main() {
    let count: u32 = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".into())
        .parse()
        .expect("positive row count");
    assert!(count > 0);
    println!("case,rows,elapsed_ns,output_rows,checksum");

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

    #[cfg(feature = "io-jsonl")]
    {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("rows.jsonl");
        let p = Pipeline::default();
        let start = Instant::now();
        let written = from_vec(&p, (0..count).collect())
            .write_jsonl(&path)
            .unwrap();
        let p = Pipeline::default();
        let read = ironbeam::read_jsonl::<u32>(&p, &path)
            .unwrap()
            .collect_seq()
            .unwrap();
        assert_eq!(written, read.len());
        println!(
            "jsonl_roundtrip,{count},{},{},{}",
            start.elapsed().as_nanos(),
            read.len(),
            read.iter().map(|&n| u64::from(n)).sum::<u64>()
        );
    }
}
