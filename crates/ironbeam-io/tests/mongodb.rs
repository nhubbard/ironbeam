#![cfg(feature = "mongodb")]
use ironbeam_core::{Capabilities, Element, ErrorKind, Operation, Pipeline, Replay, SinkMode};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io::{mongodb_sink, mongodb_source, mongodb_streaming_source};
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
}
