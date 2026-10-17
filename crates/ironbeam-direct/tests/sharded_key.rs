use ironbeam_core::{BeamShardedKey, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};

#[test]
fn direct_processes_typed_shards_and_preserves_opaque_ids() {
    let pipeline = Pipeline::default();
    let result = pipeline
        .from_vec(
            "shards",
            vec![
                BeamShardedKey::new("key".to_owned(), vec![0, 1]),
                BeamShardedKey::new("key".to_owned(), vec![0, 2]),
            ],
        )
        .unwrap()
        .map_named("read-shard", |value| (value.key, value.shard_id))
        .unwrap()
        .output("result")
        .unwrap();
    let rows = DirectRunner
        .run_collect(&result, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(
        rows,
        vec![
            ("key".to_owned(), vec![0, 1]),
            ("key".to_owned(), vec![0, 2]),
        ]
    );
}
