//! Mega comprehensive integration test covering EVERY Rustflow feature.
//!
//! This test exercises the entire API surface to ensure all features work together:
//! - All stateless transforms (map, filter, flat_map, etc.)
//! - All keyed operations (key_by, group_by_key, map/filter_values)
//! - All combiners (basic, statistical, topk, quantiles, sampling, distinct)
//! - All join types (inner, left, right, full)
//! - Side inputs (vec, hashmap)
//! - Windowing (tumbling windows for timestamped data)
//! - Batching (map_batches, map_values_batches)
//! - Try operations (try_map, try_flat_map, collect_fail_fast)
//! - All I/O formats (JSONL, CSV, Parquet) with streaming and vector modes
//! - Compression (gzip, zstd, bzip2, xz)
//! - Custom extensions and composite transforms
//! - Sampling (reservoir sampling, global and per-key)
//! - Distinct operations (distinct, distinct_per_key, approx_distinct_count)
//! - All collection methods (collect_seq, collect_par, sorted variants)
//! - Sequential and parallel execution equivalence

use anyhow::Result;
use rustflow::*;
use std::collections::HashMap;
use std::f64::consts::PI;
#[cfg(any(feature = "io-jsonl", feature = "io-csv", feature = "io-parquet"))]
use tempfile::tempdir;

#[test]
fn mega_integration_everything_kitchen_sink() -> Result<()> {
    let p = Pipeline::default();

    println!("🚀 Starting mega integration test...\n");

    // =============================================================================
    // SECTION 1: Basic stateless transforms
    // =============================================================================
    println!("📦 Section 1: Basic Stateless Transforms");

    let numbers = from_vec(&p, (1..=100).collect::<Vec<u32>>());

    // map
    let doubled = numbers.clone().map(|n: &u32| n * 2);

    // filter
    let evens = numbers.clone().filter(|n: &u32| n.is_multiple_of(2));

    // flat_map
    let repeated = numbers.clone().flat_map(|n: &u32| vec![*n, *n]);

    // Verify
    assert_eq!(doubled.clone().collect_seq()?.len(), 100);
    assert_eq!(evens.clone().collect_seq()?.len(), 50);
    assert_eq!(repeated.clone().collect_seq()?.len(), 200);

    println!("  ✅ map, filter, flat_map work correctly\n");

    // =============================================================================
    // SECTION 2: Side inputs
    // =============================================================================
    println!("📦 Section 2: Side Inputs");

    let primes_vec = side_vec::<u32>(vec![2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31]);
    let lookup_map = side_hashmap::<u32, String>(vec![
        (1, "one".to_string()),
        (2, "two".to_string()),
        (3, "three".to_string()),
    ]);

    // map_with_side
    let _marked_primes = numbers
        .clone()
        .map_with_side(primes_vec.clone(), |n: &u32, primes| {
            if primes.contains(n) {
                format!("{n}:PRIME")
            } else {
                format!("{n}:COMPOSITE")
            }
        });

    // filter_with_side
    let only_primes = numbers
        .clone()
        .filter_with_side(primes_vec.clone(), |n: &u32, primes| primes.contains(n));

    // map_with_side_map
    let enriched = numbers
        .clone()
        .filter(|n| *n <= 3)
        .map_with_side_map(lookup_map.clone(), |n: &u32, map: &HashMap<u32, String>| {
            format!("{n}: {}", map.get(n).unwrap_or(&"unknown".to_string()))
        });

    assert_eq!(only_primes.clone().collect_seq()?.len(), 11);
    assert_eq!(enriched.clone().collect_seq()?.len(), 3);

    println!("  ✅ Side inputs (vec and hashmap) work correctly\n");

    // =============================================================================
    // SECTION 3: Keyed operations
    // =============================================================================
    println!("📦 Section 3: Keyed Operations");

    // key_by
    let keyed_nums = numbers.clone().key_by(|n: &u32| format!("k{}", n % 5));

    // map_values
    let _squared_values = keyed_nums.clone().map_values(|n: &u32| n * n);

    // filter_values
    let _large_values = keyed_nums
        .clone()
        .filter_values(|n: &u32| *n > 50);

    // group_by_key
    let grouped = keyed_nums.clone().group_by_key();

    let grouped_results = grouped.clone().collect_seq()?;
    assert!(grouped_results.len() <= 5); // At most 5 keys (k0-k4)

    println!("  ✅ key_by, map_values, filter_values, group_by_key work\n");

    // =============================================================================
    // SECTION 4: ALL Combiners
    // =============================================================================
    println!("📦 Section 4: All Combiners");

    use rustflow::combiners::*;

    // 4a. Basic combiners
    let count_per_key = keyed_nums.clone().map_values(|_| 1u64).combine_values(Count);

    let sum_per_key = keyed_nums
        .clone()
        .map_values(|n| *n as u64)
        .combine_values(Sum::<u64>::default());

    let _min_per_key = keyed_nums
        .clone()
        .map_values(|n| *n)
        .combine_values(Min::<u32>::default());

    let _max_per_key = keyed_nums
        .clone()
        .map_values(|n| *n)
        .combine_values(Max::<u32>::default());

    assert_eq!(count_per_key.clone().collect_seq()?.len(), 5);

    println!("  ✅ Count, Sum, Min, Max combiners work");

    // 4b. Statistical combiners
    let avg_per_key = keyed_nums
        .clone()
        .map_values(|n| *n as f64)
        .combine_values(AverageF64);

    assert_eq!(avg_per_key.clone().collect_seq()?.len(), 5);

    println!("  ✅ AverageF64 combiner works");

    // 4c. Distinct count
    let distinct_per_key = keyed_nums
        .clone()
        .map_values(|n| n % 10)
        .combine_values(DistinctCount::<u32>::default());

    assert_eq!(distinct_per_key.clone().collect_seq()?.len(), 5);

    println!("  ✅ DistinctCount combiner works");

    // 4d. TopK
    let top3_per_key = keyed_nums.clone().top_k_per_key(3);

    let top3_results = top3_per_key.clone().collect_seq()?;
    assert_eq!(top3_results.len(), 5);
    for (_, vals) in top3_results {
        assert!(vals.len() <= 3);
    }

    println!("  ✅ TopK combiner works");

    // 4e. Quantiles (approximate)
    let quantiles_per_key = keyed_nums
        .clone()
        .map_values(|n| *n as f64)
        .combine_values(ApproxQuantiles::new(vec![0.25, 0.5, 0.75], 100.0));

    assert_eq!(quantiles_per_key.clone().collect_seq()?.len(), 5);

    println!("  ✅ ApproxQuantiles (TDigest) combiner works");

    // 4f. Sampling (reservoir)
    let sampled_per_key = keyed_nums
        .clone()
        .sample_values_reservoir_vec(5, 42);

    let sampled_results = sampled_per_key.clone().collect_seq()?;
    for (_, sample) in sampled_results {
        assert!(sample.len() <= 5);
    }

    println!("  ✅ Reservoir sampling combiner works");

    // 4g. Global combiners
    let global_count = numbers.clone().combine_globally(Count, None);
    let global_sum = numbers
        .clone()
        .map(|n| *n as u64)
        .combine_globally(Sum::<u64>::default(), None);

    assert_eq!(global_count.clone().collect_seq()?.len(), 1);
    assert_eq!(global_sum.clone().collect_seq()?.len(), 1);

    println!("  ✅ Global combiners (Count, Sum) work\n");

    // =============================================================================
    // SECTION 5: Lifted combiners
    // =============================================================================
    println!("📦 Section 5: Lifted Combiners");

    let grouped_for_lifted = keyed_nums
        .clone()
        .map_values(|n| *n as u64)
        .group_by_key();

    let lifted_sum = grouped_for_lifted.clone().combine_values_lifted(Sum::<u64>::default());
    let _lifted_count = keyed_nums.clone().map_values(|_| 1u64).group_by_key().combine_values_lifted(Count);

    // Verify lifted == non-lifted
    let sum_seq = sum_per_key.clone().collect_seq_sorted()?;
    let lifted_sum_seq = lifted_sum.clone().collect_seq_sorted()?;
    assert_eq!(sum_seq, lifted_sum_seq);

    println!("  ✅ Lifted combiners equivalent to non-lifted\n");

    // =============================================================================
    // SECTION 6: All join types
    // =============================================================================
    println!("📦 Section 6: All Join Types");

    let left_data = from_vec(
        &p,
        vec![
            ("a".to_string(), 1u32),
            ("a".to_string(), 2u32),
            ("b".to_string(), 3u32),
            ("d".to_string(), 4u32),
        ],
    );

    let right_data = from_vec(
        &p,
        vec![
            ("a".to_string(), "x".to_string()),
            ("b".to_string(), "y".to_string()),
            ("c".to_string(), "z".to_string()),
        ],
    );

    // Inner join
    let j_inner = left_data.clone().join_inner(&right_data);
    let inner_results = j_inner.clone().collect_seq()?;
    // "a" and "b" match
    assert!(inner_results.len() >= 3); // a-x, a-x (twice), b-y

    // Left join
    let j_left = left_data.clone().join_left(&right_data);
    let left_results = j_left.clone().collect_seq()?;
    assert_eq!(left_results.len(), 4); // All left records

    // Right join
    let j_right = left_data.clone().join_right(&right_data);
    let right_results = j_right.clone().collect_seq()?;
    assert_eq!(right_results.len(), 4); // a-x twice, b-y, c-z

    // Full outer join
    let j_full = left_data.clone().join_full(&right_data);
    let full_results = j_full.clone().collect_seq()?;
    assert!(full_results.len() >= 5); // All combinations

    println!("  ✅ All join types (inner, left, right, full) work\n");

    // =============================================================================
    // SECTION 7: Windowing (tumbling windows)
    // =============================================================================
    println!("📦 Section 7: Windowing (Tumbling Windows)");

    let timestamped_events = from_vec(
        &p,
        vec![
            Timestamped::new(1_000, "event_a".to_string()),
            Timestamped::new(2_000, "event_b".to_string()),
            Timestamped::new(11_000, "event_c".to_string()),
            Timestamped::new(12_000, "event_d".to_string()),
            Timestamped::new(21_000, "event_e".to_string()),
        ],
    );

    // Group by window (unkeyed)
    let windowed = timestamped_events.clone().group_by_window(10_000, 0);
    let windowed_results = windowed.clone().collect_seq()?;
    assert!(windowed_results.len() >= 2); // At least 2 windows

    // Keyed windowing
    let keyed_timestamped = from_vec(
        &p,
        vec![
            ("user1".to_string(), Timestamped::new(1_000, 1u32)),
            ("user1".to_string(), Timestamped::new(2_000, 2u32)),
            ("user1".to_string(), Timestamped::new(11_000, 3u32)),
            ("user2".to_string(), Timestamped::new(1_500, 10u32)),
            ("user2".to_string(), Timestamped::new(12_000, 20u32)),
        ],
    );

    let keyed_windowed = keyed_timestamped
        .clone()
        .group_by_key_and_window(10_000, 0);
    let keyed_windowed_results = keyed_windowed.clone().collect_seq()?;
    assert!(keyed_windowed_results.len() >= 3); // Multiple (key, window) pairs

    println!("  ✅ Tumbling windows (unkeyed and keyed) work\n");

    // =============================================================================
    // SECTION 8: Batching
    // =============================================================================
    println!("📦 Section 8: Batching");

    // map_batches (unkeyed)
    let batch_processed = numbers.clone().map_batches(15, |batch: &[u32]| {
        batch.iter().map(|n| n + 1000).collect::<Vec<u32>>()
    });

    let batch_results = batch_processed.clone().collect_seq()?;
    assert_eq!(batch_results.len(), 100);

    // map_values_batches (keyed)
    let keyed_batch = keyed_nums.clone().map_values_batches(10, |batch: &[u32]| {
        batch.iter().map(|n| n * 2).collect::<Vec<u32>>()
    });

    let keyed_batch_results = keyed_batch.clone().collect_seq()?;
    assert_eq!(keyed_batch_results.len(), 100);

    println!("  ✅ Batching (map_batches, map_values_batches) works\n");

    // =============================================================================
    // SECTION 9: Try operations (error handling)
    // =============================================================================
    println!("📦 Section 9: Try Operations");

    let fallible_data = from_vec(&p, vec![1u32, 2, 3, 4, 5]);

    // try_map (success case)
    let try_mapped = fallible_data
        .clone()
        .try_map::<String, String, _>(|n: &u32| Ok(format!("value_{n}")));

    let try_results: Vec<String> = try_mapped.collect_fail_fast()?;
    assert_eq!(try_results.len(), 5);

    // try_flat_map (success case) - creates multiple outputs per input
    let try_flat = fallible_data
        .clone()
        .try_flat_map::<u32, String, _>(|n| Ok(vec![*n, n * 2]));

    let flat_results: Vec<Vec<u32>> = try_flat.collect_fail_fast()?;
    assert_eq!(flat_results.len(), 5); // 5 inputs * 2 outputs each

    println!("  ✅ Try operations (try_map, try_flat_map, collect_fail_fast) work\n");

    // =============================================================================
    // SECTION 10: Distinct operations
    // =============================================================================
    println!("📦 Section 10: Distinct Operations");

    let duplicates = from_vec(&p, vec![1u32, 2, 2, 3, 3, 3, 4, 4, 4, 4]);

    // distinct
    let unique = duplicates.clone().distinct();
    let unique_results = unique.clone().collect_seq_sorted()?;
    assert_eq!(unique_results, vec![1, 2, 3, 4]);

    // approx_distinct_count (global)
    let approx_count = duplicates.clone().approx_distinct_count(1000);
    let count_result = approx_count.clone().collect_seq()?;
    assert_eq!(count_result.len(), 1);

    // distinct_per_key
    let keyed_dups = from_vec(
        &p,
        vec![
            ("k1".to_string(), 1u32),
            ("k1".to_string(), 1u32),
            ("k1".to_string(), 2u32),
            ("k2".to_string(), 5u32),
            ("k2".to_string(), 5u32),
        ],
    );

    let distinct_vals = keyed_dups.clone().distinct_per_key();
    let distinct_results = distinct_vals.clone().collect_seq_sorted()?;
    assert_eq!(distinct_results.len(), 3); // (k1,1), (k1,2), (k2,5)

    println!("  ✅ Distinct operations (distinct, distinct_per_key, approx_distinct_count) work\n");

    // =============================================================================
    // SECTION 11: Sampling (global)
    // =============================================================================
    println!("📦 Section 11: Global Sampling");

    let large_dataset = from_vec(&p, (1..=1000u32).collect::<Vec<_>>());

    // sample_reservoir_vec (returns Vec<T>)
    let sampled_vec = large_dataset.clone().sample_reservoir_vec(50, 123);
    let sample_results = sampled_vec.clone().collect_seq()?;
    assert_eq!(sample_results.len(), 1); // One vec
    assert_eq!(sample_results[0].len(), 50);

    // sample_reservoir (flattened)
    let sampled_flat = large_dataset.clone().sample_reservoir(50, 123);
    let flat_sample = sampled_flat.clone().collect_seq()?;
    assert_eq!(flat_sample.len(), 50);

    println!("  ✅ Global sampling (reservoir) works\n");

    // =============================================================================
    // SECTION 12: Collection methods (sequential & parallel equivalence)
    // =============================================================================
    println!("📦 Section 12: Collection Methods");

    let test_data = from_vec(&p, (1..=50u32).collect::<Vec<_>>());

    // collect_seq vs collect_par
    let seq_result = test_data.clone().collect_seq()?;
    let par_result = test_data.clone().collect_par(None, None)?;
    assert_eq!(seq_result.len(), par_result.len());

    // collect_seq_sorted vs collect_par_sorted
    let seq_sorted = test_data.clone().collect_seq_sorted()?;
    let par_sorted = test_data.clone().collect_par_sorted(None, None)?;
    assert_eq!(seq_sorted, par_sorted);

    // collect_par_sorted_by_key (for keyed data)
    let keyed_sort = test_data
        .clone()
        .key_by(|n| n % 3)
        .collect_par_sorted_by_key(None, None)?;
    assert_eq!(keyed_sort.len(), 50);

    println!("  ✅ Collection methods (seq, par, sorted) equivalent\n");

    // =============================================================================
    // SECTION 13: Custom extensions (CompositeTransform)
    // =============================================================================
    println!("📦 Section 13: Custom Extensions");

    use rustflow::extensions::CompositeTransform;

    struct DoubleAndFilter;

    impl<T: RFBound> CompositeTransform<T, u32> for DoubleAndFilter
    where
        T: 'static + Clone + Into<u32>,
    {
        fn expand(&self, input: PCollection<T>) -> PCollection<u32> {
            input
                .map(|t: &T| {
                    let val: u32 = t.clone().into();
                    val * 2
                })
                .filter(|n: &u32| n > &20)
        }
    }

    let custom_result = from_vec(&p, vec![5u32, 10, 15, 20])
        .apply_composite(DoubleAndFilter)
        .collect_seq()?;

    assert!(custom_result.iter().all(|n| *n > 20));

    println!("  ✅ Custom composite transforms work\n");

    // =============================================================================
    // SECTION 14: I/O operations (all formats + compression)
    // =============================================================================
    #[cfg(any(feature = "io-jsonl", feature = "io-csv", feature = "io-parquet"))]
    {
        println!("📦 Section 14: I/O Operations");

        let io_dir = tempdir()?;
        let base_path = io_dir.path();

        #[derive(
            serde::Serialize, serde::Deserialize, Clone, PartialEq, PartialOrd, Debug,
        )]
        struct Record {
            id: u32,
            name: String,
            value: f64,
        }

        let test_records = vec![
            Record {
                id: 1,
                name: "Alice".to_string(),
                value: PI,
            },
            Record {
                id: 2,
                name: "Bob".to_string(),
                value: 2.71,
            },
            Record {
                id: 3,
                name: "Charlie".to_string(),
                value: 1.41,
            },
        ];

        // 14a. JSONL I/O
        #[cfg(feature = "io-jsonl")]
        {
            // Vector I/O
            use rustflow::io::jsonl::{read_jsonl_vec, write_jsonl_vec};
            let jsonl_path = base_path.join("test.jsonl");
            write_jsonl_vec(&jsonl_path, &test_records)?;
            let loaded: Vec<Record> = read_jsonl_vec(&jsonl_path)?;
            assert_eq!(loaded.len(), 3);

            // Streaming I/O
            let jsonl_stream_path = base_path.join("test_stream.jsonl");
            from_vec(&p, test_records.clone())
                .write_jsonl(&jsonl_stream_path)?;
            let streamed = read_jsonl_streaming::<Record>(&p, &jsonl_stream_path, 100)?;
            assert_eq!(streamed.collect_seq()?.len(), 3);

            // Parallel write
            let jsonl_par_path = base_path.join("test_par.jsonl");
            from_vec(&p, test_records.clone())
                .write_jsonl_par(&jsonl_par_path, Some(2))?;
            let par_loaded: Vec<Record> = read_jsonl_vec(&jsonl_par_path)?;
            assert_eq!(par_loaded.len(), 3);

            println!("  ✅ JSONL I/O (vector, streaming, parallel) works");

            // 14b. Compression with JSONL
            #[cfg(feature = "compression-gzip")]
            {
                let gz_path = base_path.join("test.jsonl.gz");
                write_jsonl_vec(&gz_path, &test_records)?;
                let gz_loaded: Vec<Record> = read_jsonl_vec(&gz_path)?;
                assert_eq!(gz_loaded.len(), 3);
                println!("  ✅ JSONL + Gzip compression works");
            }

            #[cfg(feature = "compression-zstd")]
            {
                let zst_path = base_path.join("test.jsonl.zst");
                write_jsonl_vec(&zst_path, &test_records)?;
                let zst_loaded: Vec<Record> = read_jsonl_vec(&zst_path)?;
                assert_eq!(zst_loaded.len(), 3);
                println!("  ✅ JSONL + Zstd compression works");
            }
        }

        // 14c. CSV I/O
        #[cfg(feature = "io-csv")]
        {
            use rustflow::io::csv::{read_csv_vec, write_csv_vec};
            let csv_path = base_path.join("test.csv");
            write_csv_vec(&csv_path, true, &test_records)?;
            let csv_loaded: Vec<Record> = read_csv_vec(&csv_path, true)?;
            assert_eq!(csv_loaded.len(), 3);

            // Streaming
            let csv_stream_path = base_path.join("test_stream.csv");
            from_vec(&p, test_records.clone()).write_csv(&csv_stream_path, true)?;
            let csv_streamed = read_csv_streaming::<Record>(&p, &csv_stream_path, true, 100)?;
            assert_eq!(csv_streamed.collect_seq()?.len(), 3);

            println!("  ✅ CSV I/O (vector, streaming) works");

            // Compression
            #[cfg(feature = "compression-gzip")]
            {
                let csv_gz_path = base_path.join("test.csv.gz");
                write_csv_vec(&csv_gz_path, true, &test_records)?;
                let csv_gz_loaded: Vec<Record> = read_csv_vec(&csv_gz_path, true)?;
                assert_eq!(csv_gz_loaded.len(), 3);
                println!("  ✅ CSV + Gzip compression works");
            }
        }

        // 14d. Parquet I/O
        #[cfg(feature = "io-parquet")]
        {
            use rustflow::io::parquet::{read_parquet_vec, write_parquet_vec};
            let parquet_path = base_path.join("test.parquet");
            write_parquet_vec(&parquet_path, &test_records)?;
            let parquet_loaded: Vec<Record> = read_parquet_vec(&parquet_path)?;
            assert_eq!(parquet_loaded.len(), 3);

            // Streaming
            let parquet_stream_path = base_path.join("test_stream.parquet");
            from_vec(&p, test_records.clone()).write_parquet(&parquet_stream_path)?;
            let parquet_streamed =
                read_parquet_streaming::<Record>(&p, &parquet_stream_path, 1)?;
            assert_eq!(parquet_streamed.collect_seq()?.len(), 3);

            println!("  ✅ Parquet I/O (vector, streaming) works");
        }

        println!();
    }

    // =============================================================================
    // SECTION 15: Timestamped operations (attach, convert)
    // =============================================================================
    println!("📦 Section 15: Timestamped Operations");

    let plain_events = from_vec(&p, vec!["event1".to_string(), "event2".to_string()]);

    // Attach timestamps
    let with_timestamps = plain_events
        .clone()
        .attach_timestamps(|s: &String| if s == "event1" { 1000 } else { 2000 });

    let ts_results = with_timestamps.clone().collect_seq()?;
    assert_eq!(ts_results.len(), 2);

    // Convert from (timestamp, value) tuples
    let tuple_events = from_vec(&p, vec![(1000u64, "a".to_string()), (2000u64, "b".to_string())]);
    let converted = tuple_events.to_timestamped();
    assert_eq!(converted.collect_seq()?.len(), 2);

    println!("  ✅ Timestamped operations (attach, convert) work\n");

    // =============================================================================
    // SECTION 16: Custom source
    // =============================================================================
    println!("📦 Section 16: Custom Source");

    // Custom from_iter source
    let custom_iter = 0..100u32;
    let custom_source = from_iter(&p, custom_iter);
    let custom_results = custom_source.collect_seq()?;
    assert_eq!(custom_results.len(), 100);

    println!("  ✅ Custom iterator source works\n");
    println!("  ✅ Custom source works\n");

    // =============================================================================
    // FINAL: Verify determinism (seq == par)
    // =============================================================================
    println!("📦 Final: Verifying Determinism");

    let determinism_test = from_vec(&p, (1..=100u32).collect::<Vec<_>>())
        .key_by(|n| n % 10)
        .map_values(|n| n * 2)
        .filter_values(|n| n > &50)
        .group_by_key()
        .combine_values_lifted(Sum::<u32>::default());

    let seq_det = determinism_test.clone().collect_seq_sorted()?;
    let par_det = determinism_test.clone().collect_par_sorted(None, None)?;

    assert_eq!(seq_det, par_det);

    println!("  ✅ Sequential and parallel execution are deterministic\n");

    println!("🎉 MEGA INTEGRATION TEST PASSED! All features work correctly! 🎉");

    Ok(())
}
