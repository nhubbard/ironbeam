use ironbeam_core::{ErrorKind, Pipeline, Replay, SourceProgress};
use ironbeam_direct::{
    DirectFallibleExt, DirectFlatPartitionExt, DirectKeySortedExt, DirectKeyedCollectExt,
    DirectPartitionByExt, DirectPartitionExt, DirectSortedExt, JobConfig, Partition3, Partition4,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn sorted_terminals_and_hashmap_materialization_match_v4() {
    let p = Pipeline::default();
    let source = p.from_iter("numbers", [3_i32, 1, 2]).unwrap();
    assert_eq!(
        source.collect_seq_sorted_named("sequential").unwrap(),
        [1, 2, 3]
    );
    assert_eq!(
        source.collect_par_sorted_named("parallel", 3).unwrap(),
        [1, 2, 3]
    );

    let pairs = p
        .from_vec(
            "pairs",
            vec![("a".to_string(), 1_i32), ("b".into(), 2), ("a".into(), 3)],
        )
        .unwrap();
    let map = pairs
        .to_hashmap_named("map", &JobConfig::default())
        .unwrap();
    assert_eq!(map.get("a"), Some(&3));
    assert_eq!(map.get("b"), Some(&2));
    let sorted = pairs
        .collect_sorted_by_key_named(
            "sorted-keys",
            &JobConfig {
                parallelism: 3,
                ..JobConfig::default()
            },
        )
        .unwrap();
    assert_eq!(
        sorted,
        [
            ("a".to_string(), 1),
            ("a".to_string(), 3),
            ("b".to_string(), 2)
        ]
    );
    assert_eq!(
        pairs
            .collect_seq_sorted_by_key_named("sequential-keys")
            .unwrap(),
        sorted
    );
    assert_eq!(
        pairs
            .collect_par_sorted_by_key_named("parallel-keys", 3)
            .unwrap(),
        sorted
    );
}

#[test]
fn fallible_terminals_preserve_successes_and_report_errors() {
    let pipeline = Pipeline::default();
    let values = pipeline
        .from_vec("fallible", vec![Ok(2_i32), Err("bad".to_string()), Ok(5)])
        .unwrap();
    let (good, errors) = values
        .collect_partitioned_named("partitioned", &JobConfig::default())
        .unwrap();
    assert_eq!(good, [2, 5]);
    assert_eq!(errors, ["bad"]);

    let error = values
        .collect_fail_fast_named("fail-fast", &JobConfig::default())
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("fail-fast"));
    assert!(error.message.contains("bad"));

    let successes = pipeline
        .from_vec("all-good", vec![Ok::<_, String>(3_i32), Ok(7)])
        .unwrap();
    assert_eq!(
        successes
            .collect_fail_fast_named("all-good-out", &JobConfig::default())
            .unwrap(),
        [3, 7]
    );

    let batches = pipeline
        .from_vec(
            "batches",
            vec![Ok(vec![1_i32, 2]), Err("skip".to_string()), Ok(vec![3])],
        )
        .unwrap();
    let (flat, errors) = batches
        .collect_flat_partitioned_named("flat", &JobConfig::default())
        .unwrap();
    assert_eq!(flat, [1, 2, 3]);
    assert_eq!(errors, ["skip"]);
}

#[test]
fn typed_partition_terminals_open_shared_source_once_per_job() {
    let opens = Arc::new(AtomicUsize::new(0));
    let pipeline = Pipeline::default();
    let source = pipeline
        .bounded_source_with_semantic::<i32>(
            "source",
            "tests/partition-source",
            SourceProgress {
                stable_split_id: true,
                replay: Replay::AtLeastOnce,
                reports_event_time: false,
            },
            "partition-source-v1".into(),
            Some(6),
            {
                let opens = Arc::clone(&opens);
                move || {
                    opens.fetch_add(1, Ordering::Relaxed);
                    Ok(Box::new((0..6).map(Ok)))
                }
            },
        )
        .unwrap();

    let (evens, odds, large) = source
        .collect_partitioned3_named("three", &JobConfig::default(), |value| match value {
            0 | 2 => Partition3::First(value.to_string()),
            1 | 3 => Partition3::Second(value),
            _ => Partition3::Third(value > 4),
        })
        .unwrap();
    assert_eq!(evens, ["0", "2"]);
    assert_eq!(odds, [1, 3]);
    assert_eq!(large, [false, true]);
    assert_eq!(opens.load(Ordering::Relaxed), 1);

    let (first, second, third, fourth) = source
        .collect_partitioned4_named("four", &JobConfig::default(), |value| match value {
            0 => Partition4::First(value),
            1 => Partition4::Second(value.to_string()),
            2 => Partition4::Third(value > 0),
            _ => Partition4::Fourth(value),
        })
        .unwrap();
    assert_eq!(first, [0]);
    assert_eq!(second, ["1"]);
    assert_eq!(third, [true]);
    assert_eq!(fourth, [3, 4, 5]);
    assert_eq!(opens.load(Ordering::Relaxed), 2);

    let error = source
        .collect_partitioned3_named("panicking-classifier", &JobConfig::default(), |value| {
            assert!(value != 2, "bad classifier");
            Partition3::<i32, i32, i32>::First(value)
        })
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("panicking-classifier"));

    let error = source
        .collect_partitioned4_named("panicking-four", &JobConfig::default(), |value| {
            assert!(value != 2, "bad classifier");
            Partition4::<i32, i32, i32, i32>::First(value)
        })
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Execution);
    assert_eq!(error.node.as_deref(), Some("panicking-four"));
}
