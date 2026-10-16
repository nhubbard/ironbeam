use ironbeam_core::{Element, Pipeline, Version};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn explicit_shards_split_one_hot_key_into_independent_batches() {
    let pipeline = Pipeline::default();
    let source = pipeline
        .from_vec(
            "pairs",
            vec![
                ("hot".to_owned(), (1_u8, 10_i32)),
                ("hot".to_owned(), (2, 20)),
                ("hot".to_owned(), (1, 30)),
                ("hot".to_owned(), (1, 40)),
                ("hot".to_owned(), (2, 50)),
            ],
        )
        .unwrap();
    let output = source
        .group_into_batches_with_sharded_key_named(
            "batches",
            2,
            "tests/select-shard",
            Version { major: 1, minor: 0 },
            |_, (shard, _)| vec![*shard],
        )
        .unwrap()
        .map_named("read-batches", |(key, values)| {
            let mut values = values
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>();
            values.sort_unstable();
            (key.key, key.shard_id, values)
        })
        .unwrap()
        .output("result")
        .unwrap();
    let mut rows = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("hot".to_owned(), vec![1], vec![10, 30]),
            ("hot".to_owned(), vec![1], vec![40]),
            ("hot".to_owned(), vec![2], vec![20, 50]),
        ]
    );
    let (graph, _) = pipeline.snapshot().unwrap();
    let fingerprint = graph.fingerprint().unwrap();
    assert_eq!(
        graph.node("batches/assign-shard").unwrap().semantic_id,
        "tests/select-shard"
    );
    assert_eq!(
        graph
            .node("batches/assign-shard")
            .unwrap()
            .semantic_version
            .major,
        1
    );
    let invalid = source.group_into_batches_with_sharded_key_named(
        "bad-batches",
        0,
        "tests/select-shard",
        Version { major: 1, minor: 0 },
        |_, _| vec![0],
    );
    assert!(invalid.is_err());
    assert_eq!(
        pipeline.snapshot().unwrap().0.fingerprint().unwrap(),
        fingerprint
    );
}

#[test]
fn explicit_shards_keep_byte_limits_independent() {
    let pipeline = Pipeline::default();
    let short_bytes = (1_u8, "a".to_owned()).encoded_len().unwrap();
    let source = pipeline
        .from_vec(
            "pairs",
            vec![
                ("hot".to_owned(), (1_u8, "a".to_owned())),
                ("hot".to_owned(), (2, "c".to_owned())),
                ("hot".to_owned(), (1, "b".to_owned())),
                ("hot".to_owned(), (2, "d".to_owned())),
                ("hot".to_owned(), (1, "oversized".to_owned())),
            ],
        )
        .unwrap();
    let output = source
        .group_into_batches_with_limits_and_sharded_key_named(
            "byte-batches",
            10,
            short_bytes * 2,
            "tests/byte-shard-selector",
            Version { major: 1, minor: 0 },
            |_, (shard, _)| vec![*shard],
        )
        .unwrap()
        .map_named("read-batches", |(key, values)| {
            (
                key.key,
                key.shard_id,
                values
                    .into_iter()
                    .map(|(_, value)| value)
                    .collect::<Vec<_>>(),
            )
        })
        .unwrap()
        .output("result")
        .unwrap();
    let mut rows = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    rows.sort();
    assert_eq!(
        rows,
        [
            (
                "hot".to_owned(),
                vec![1],
                vec!["a".to_owned(), "b".to_owned()]
            ),
            ("hot".to_owned(), vec![1], vec!["oversized".to_owned()]),
            (
                "hot".to_owned(),
                vec![2],
                vec!["c".to_owned(), "d".to_owned()]
            ),
        ]
    );
    let fingerprint = pipeline.snapshot().unwrap().0.fingerprint().unwrap();
    assert!(
        source
            .group_into_batches_with_limits_and_sharded_key_named(
                "invalid-byte-batches",
                10,
                0,
                "tests/byte-shard-selector",
                Version { major: 1, minor: 0 },
                |_, _| vec![0],
            )
            .is_err()
    );
    assert!(
        source
            .group_into_batches_with_limits_and_sharded_key_named(
                "invalid-byte-shard-selector",
                10,
                short_bytes * 2,
                "",
                Version { major: 1, minor: 0 },
                |_, _| vec![0],
            )
            .is_err()
    );
    assert!(
        source
            .group_into_batches_with_limits_and_sharded_key_named(
                "byte-batches",
                10,
                short_bytes * 2,
                "tests/byte-shard-selector",
                Version { major: 1, minor: 0 },
                |_, _| vec![0],
            )
            .is_err()
    );
    assert_eq!(
        pipeline.snapshot().unwrap().0.fingerprint().unwrap(),
        fingerprint
    );
}
