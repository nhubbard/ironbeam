use ironbeam_core::{Capabilities, Element, ErrorKind, Operation, Pipeline, Replay, SinkMode};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_mongodb::{
    MongoShardSpec, mongodb_parallel_sink, mongodb_sharded_source, mongodb_sink, mongodb_source,
    mongodb_streaming_source,
};
use serde::{Deserialize, Serialize};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::mongo::Mongo;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Doc {
    id: i64,
    name: String,
}
impl Element for Doc {
    const TYPE_ID: &'static str = "tests/mongodb-doc";
    const CODER_ID: &'static str = "tests/mongodb-doc-local";
}

#[test]
fn mongodb_source_reports_open_failure_at_execution() {
    let pipeline = Pipeline::default();
    let output = mongodb_source::<Doc>(
        &pipeline,
        "invalid",
        "not-a-mongodb-uri",
        "db",
        "rows",
        bson::doc! {},
    )
    .unwrap()
    .output("invalid-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    let shards = Pipeline::default();
    let output = mongodb_sharded_source::<Doc>(
        &shards,
        "shards",
        MongoShardSpec {
            uri: "not-a-mongodb-uri",
            database: "db",
            collection: "rows",
            filter: bson::doc! {},
            total_docs: 2,
            docs_per_shard: 1,
            snapshot_id: "fixture-v1",
        },
    )
    .unwrap()
    .output("shards-out")
    .unwrap();
    let (graph, _) = shards.snapshot().unwrap();
    assert!(graph.node("shards/shard-0").is_some());
    assert!(graph.node("shards/shard-1").is_some());
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert!(
        mongodb_sharded_source::<Doc>(
            &shards,
            "bad",
            MongoShardSpec {
                uri: "bad-uri",
                database: "db",
                collection: "rows",
                filter: bson::doc! {},
                total_docs: 2,
                docs_per_shard: 0,
                snapshot_id: "fixture-v1"
            }
        )
        .is_err()
    );
    assert!(
        mongodb_sharded_source::<Doc>(
            &shards,
            "bad-id",
            MongoShardSpec {
                uri: "bad-uri",
                database: "db",
                collection: "rows",
                filter: bson::doc! {},
                total_docs: 2,
                docs_per_shard: 1,
                snapshot_id: ""
            }
        )
        .is_err()
    );
}

#[test]
#[ignore = "requires a Docker-compatible daemon and mongo:5.0.6 image"]
fn mongodb_bounded_sources_and_deferred_sink_round_trip() {
    let container = Mongo::default().start().unwrap();
    let uri = format!(
        "mongodb://{}:{}/",
        container.get_host().unwrap(),
        container.get_host_port_ipv4(27017).unwrap()
    );
    let expected = vec![
        Doc {
            id: 1,
            name: "Ada".into(),
        },
        Doc {
            id: 2,
            name: "Lin".into(),
        },
        Doc {
            id: 3,
            name: "Max".into(),
        },
    ];
    let pipeline = Pipeline::default();
    let values = pipeline.from_vec("values", expected.clone()).unwrap();
    let sink = mongodb_sink(&values, "insert", &uri, "ironbeam_v5", "rows").unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(matches!(&graph.node("insert").unwrap().operation,
        Operation::Sink { spec } if spec.mode == SinkMode::Append && spec.replay == Replay::None && spec.commit));
    assert!(matches!(
        graph
            .validate_for("dataflow-preview", &Capabilities::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    let before = Pipeline::default();
    let empty = mongodb_source::<Doc>(
        &before,
        "before",
        &uri,
        "ironbeam_v5",
        "rows",
        bson::doc! {},
    )
    .unwrap()
    .output("before-out")
    .unwrap();
    assert!(
        DirectRunner
            .run_collect(&empty, &JobConfig::default())
            .unwrap()
            .0
            .is_empty()
    );
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let reads = Pipeline::default();
    let eager = mongodb_source::<Doc>(&reads, "eager", &uri, "ironbeam_v5", "rows", bson::doc! {})
        .unwrap()
        .output("eager-out")
        .unwrap();
    let paged = mongodb_streaming_source::<Doc>(
        &reads,
        "paged",
        &uri,
        "ironbeam_v5",
        "rows",
        bson::doc! {},
        2,
    )
    .unwrap()
    .output("paged-out")
    .unwrap();
    let sharded = mongodb_sharded_source::<Doc>(
        &reads,
        "shards",
        MongoShardSpec {
            uri: &uri,
            database: "ironbeam_v5",
            collection: "rows",
            filter: bson::doc! {},
            total_docs: 3,
            docs_per_shard: 2,
            snapshot_id: "fixture-v1",
        },
    )
    .unwrap()
    .output("shards-out")
    .unwrap();
    let mut eager_rows = DirectRunner
        .run_collect(&eager, &JobConfig::default())
        .unwrap()
        .0;
    let mut paged_rows = DirectRunner
        .run_collect(&paged, &JobConfig::default())
        .unwrap()
        .0;
    eager_rows.sort_by_key(|row| row.id);
    paged_rows.sort_by_key(|row| row.id);
    assert_eq!(eager_rows, expected);
    assert_eq!(paged_rows, expected);
    assert_eq!(
        DirectRunner
            .run_collect(&sharded, &JobConfig::default())
            .unwrap()
            .0,
        expected
    );
    let mismatch = Pipeline::default();
    let output = mongodb_sharded_source::<Doc>(
        &mismatch,
        "mismatch",
        MongoShardSpec {
            uri: &uri,
            database: "ironbeam_v5",
            collection: "rows",
            filter: bson::doc! {},
            total_docs: 4,
            docs_per_shard: 2,
            snapshot_id: "fixture-v1",
        },
    )
    .unwrap()
    .output("mismatch-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    assert!(
        mongodb_parallel_sink(
            &values,
            "bad-parallel",
            &uri,
            "ironbeam_v5",
            "parallel_rows",
            0
        )
        .is_err()
    );
    let parallel =
        mongodb_parallel_sink(&values, "parallel", &uri, "ironbeam_v5", "parallel_rows", 2)
            .unwrap();
    DirectRunner
        .run_sink(&parallel, &JobConfig::default())
        .unwrap();
    let verify = Pipeline::default();
    let output = mongodb_source::<Doc>(
        &verify,
        "parallel-read",
        &uri,
        "ironbeam_v5",
        "parallel_rows",
        bson::doc! {},
    )
    .unwrap()
    .output("parallel-out")
    .unwrap();
    let mut written = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    written.sort_by_key(|row| row.id);
    assert_eq!(written, expected);
}
