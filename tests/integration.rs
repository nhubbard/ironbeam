// tests/integration.rs

use rustflow::*;
use anyhow::Result;
use std::collections::HashMap;

#[cfg(any(feature = "io-jsonl", feature = "io-csv", feature = "io-parquet"))]
use tempfile::tempdir;

#[test]
fn everything_everywhere_all_at_once() -> Result<()> {
    let p = Pipeline::default();

    // ---------- 1) Base input ----------
    let input: Vec<String> = (0..200)
        .map(|i| format!("k{} v{}", i % 7, i))
        .collect();

    // Side inputs
    let side_vec = side_vec::<u32>(vec![2, 3, 5, 7, 11, 13]);
    let side_map = side_hashmap::<String, u32>(vec![
        ("k0".to_string(), 100),
        ("k3".to_string(), 300),
    ]);

    // ---------- 2) Stateless + side inputs ----------
    let words = from_vec(&p, input)
        .map(|s: &String| s.split_whitespace().map(str::to_string).collect::<Vec<_>>())
        .flat_map(|v: &Vec<String>| v.clone())
        .filter(|w: &String| !w.starts_with('v'))
        .map_with_side(side_vec.clone(), |w: &String, primes| {
            if primes.contains(&(w.len() as u32)) { format!("{w}:P") } else { w.clone() }
        })
        .filter_with_side(side_vec.clone(), |w: &String, primes| {
            let d = w.chars().last().unwrap_or('0').to_digit(10).unwrap_or(0);
            primes.contains(&d)
        });

    // ---------- 3) Keyed + value-only + batching ----------
    let keyed = words
        .key_by(|w: &String| w[0..2].to_string())
        .map_values(|w: &String| w.clone())
        .filter_values(|w: &String| w.ends_with('P') || w.ends_with('7'))
        .map_values_batches(32, |batch: &[String]| {
            batch.iter().flat_map(|s| [s.clone(), s.clone()]).collect::<Vec<String>>()
        });

    // try_* ergonomics: clone `keyed` for each branch
    let _try_map = keyed.clone()
        .try_map::<(String, String), String, _>(|kv: &(String, String)| Ok(kv.clone()));

    let _try_flat = keyed.clone()
        .try_flat_map::<(String, String), String, _>(|kv| Ok(vec![kv.clone()]));

    // ---------- 4) GBK + Combine (classic vs lifted) ----------
    // classic: combine_values does its own GBK from (K,V)
    let counts = keyed.clone()
        .map(|kv: &(String, String)| (kv.0.clone(), 1u64))
        .combine_values(Count);

    // lifted: run group_by_key() then combine_values_lifted
    let counts_lifted = keyed.clone()
        .map(|kv: &(String, String)| (kv.0.clone(), 1u64))
        .group_by_key()
        .combine_values_lifted(Count);

    assert_eq!(counts.clone().collect_seq_sorted()?, counts_lifted.clone().collect_seq_sorted()?);

    // ---------- 5) More combiners on keyed numeric stream ----------
    use rustflow::combiners::{Sum, Min, Max, AverageF64, DistinctCount};

    // keep f64 for sum/avg
    let keyed_nums_f64 = from_vec(&p, (0..100u32).collect::<Vec<_>>())
        .key_by(|n: &u32| format!("k{}", n % 5))
        .map_values(|n| *n as f64);

    let _sum = keyed_nums_f64.clone().combine_values(Sum::<f64>::default()).collect_seq()?;
    let _avg = keyed_nums_f64.clone().combine_values(AverageF64).collect_seq()?;

    // separate u64 stream for min/max/distinct (requires Ord)
    let keyed_nums_u64 = from_vec(&p, (0..100u32).collect::<Vec<_>>())
        .key_by(|n: &u32| format!("k{}", n % 5))
        .map_values(|n| *n as u64);

    let _min = keyed_nums_u64.clone().combine_values(Min::<u64>::default()).collect_seq()?;
    let _max = keyed_nums_u64.clone().combine_values(Max::<u64>::default()).collect_seq()?;

    // distinct over u32
    let _dc  = from_vec(&p, (0..100u32).collect::<Vec<_>>())
        .key_by(|n: &u32| format!("k{}", n % 5))
        .map_values(|n| *n % 17)
        .combine_values(DistinctCount::<u32>::default())
        .collect_seq()?;

    // ---------- 6) Joins (borrow right as &PCollection) ----------
    let left = from_vec(&p, vec![
        ("a".to_string(), 1u32), ("a".to_string(), 2u32),
        ("b".to_string(), 3u32)
    ]);
    let right = from_vec(&p, vec![
        ("a".to_string(), "x".to_string()),
        ("c".to_string(), "y".to_string())
    ]);

    let j_inner = left.clone().join_inner(&right);
    let j_left  = left.clone().join_left(&right);
    let j_right = left.clone().join_right(&right);
    let j_full  = left.clone().join_full(&right);

    let _ = j_inner.clone().collect_par_sorted_by_key(None, None)?;
    let _ = j_left.clone().collect_par_sorted_by_key(None, None)?;
    let _ = j_right.clone().collect_par_sorted_by_key(None, None)?;
    let _ = j_full.clone().collect_par_sorted_by_key(None, None)?;

    // ---------- 7) Side map enrichment ----------
    let enriched = counts.clone()  // clone so `counts` is still available later
        .map_with_side_map(side_map.clone(), |(k, v), m: &HashMap<String, u32>| {
            let base = m.get(k).copied().unwrap_or_default() as u64;
            (k.clone(), v + base)
        });
    let _ = enriched.collect_seq()?;

    // ---------- 8) Batching (unkeyed) + deterministic finalization ----------
    let batched = from_vec(&p, (0..256u32).collect::<Vec<_>>())
        .map_batches(40, |chunk: &[u32]| chunk.iter().map(|n| n*n).collect::<Vec<u32>>());
    assert_eq!(
        batched.clone().collect_seq_sorted()?,
        batched.clone().collect_par_sorted(None, None)?
    );

    // ---------- 9) IO blocks (feature-gated) ----------
    #[cfg(any(feature = "io-jsonl", feature = "io-csv", feature = "io-parquet"))]
    {
        let dir = tempdir()?;
        let base = dir.path();

        #[cfg(feature = "io-jsonl")]
        {
            let path = base.join("out.jsonl");
            let out_vec = counts.clone().collect_seq()?;
            let n = from_vec(&p, out_vec.clone()).write_jsonl_par(&path, Some(4))?;
            assert_eq!(n, out_vec.len());
            let streamed = read_jsonl_streaming::<(String, u64)>(&p, &path, 32)?;
            assert_eq!(streamed.collect_seq_sorted()?, out_vec);
        }

        #[cfg(feature = "io-csv")]
        {
            #[derive(serde::Serialize, serde::Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd)]
            struct Row { k: String, v: u64 }

            let path = base.join("out.csv");
            let rows = counts.clone().map(|(k,v)| Row { k: k.clone(), v: *v }).collect_seq()?;
            let n = from_vec(&p, rows.clone()).write_csv(&path, true)?;
            assert_eq!(n, rows.len());

            // vector read
            let _back: Vec<Row> = read_csv_vec(&path, true)?;

            // streaming read
            let stream = read_csv_streaming::<Row>(&p, &path, true, 16)?;
            let _rt = stream.collect_seq_sorted()?; // sorts thanks to Ord derives
        }

        #[cfg(feature = "io-parquet")]
        {
            #[derive(serde::Serialize, serde::Deserialize, Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
            struct Rec { k: String, v: u64 }

            let path = base.join("out.parquet");
            let rows = counts.clone().map(|(k,v)| Rec { k: k.clone(), v: *v }).collect_seq()?;
            let n = from_vec(&p, rows.clone()).write_parquet(&path)?;
            assert_eq!(n, rows.len());

            let stream = read_parquet_streaming::<Rec>(&p, &path, 1)?;
            let _rt = stream.collect_seq_sorted()?;
        }
    }

    // ---------- 10) Planner sanity + fail-fast on a fallible path ----------
    assert_eq!(
        counts.clone().collect_seq_sorted()?,
        counts.clone().collect_par_sorted(None, None)?
    );

    // Use try_map (single item), not try_flat_map (Vec)
    let try_ok = keyed
        .try_map::<(String, String), String, _>(|kv| Ok(kv.clone()));
    let _ok: Vec<(String, String)> = try_ok.collect_fail_fast()?;

    Ok(())
}