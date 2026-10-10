#![cfg(feature = "sql")]
use ironbeam_core::{Capabilities, Element, ErrorKind, Operation, Pipeline, Replay, SinkMode};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io::{sql_sharded_source, sql_sink_with, sql_source, sql_streaming_source};

#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    id: i64,
    name: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/sql-row";
    const CODER_ID: &'static str = "tests/sql-row-local";
}

#[test]
fn sql_sources_and_sink_execute_only_after_runner_validation() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("rows.db").display());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
        sqlx::query("CREATE TABLE rows (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO rows (id, name) VALUES (1, 'Ada'), (2, 'Lin'), (3, 'Max')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE output (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
    });
    let expected = vec![
        Row {
            id: 1,
            name: "Ada".into(),
        },
        Row {
            id: 2,
            name: "Lin".into(),
        },
        Row {
            id: 3,
            name: "Max".into(),
        },
    ];
    let p = Pipeline::default();
    let query = "SELECT id, name FROM rows ORDER BY id";
    let eager = sql_source::<Row>(&p, "sql", &url, query)
        .unwrap()
        .output("eager-out")
        .unwrap();
    let paged = sql_streaming_source::<Row>(&p, "paged", &url, query, 2)
        .unwrap()
        .output("paged-out")
        .unwrap();
    let sharded = sql_sharded_source::<Row>(&p, "shards", &url, query, 3, 2, "fixture-v1")
        .unwrap()
        .output("shards-out")
        .unwrap();
    let input = p.from_vec("input", expected.clone()).unwrap();
    let sink = sql_sink_with(
        &input,
        "insert",
        &url,
        "INSERT INTO output (id, name)",
        |mut separated, row: &Row| {
            separated.push_bind(row.id).push_bind(row.name.clone());
        },
    )
    .unwrap();
    let (graph, _) = p.snapshot().unwrap();
    assert!(graph.node("shards/shard-0").is_some());
    assert!(graph.node("shards/shard-1").is_some());
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
    assert_eq!(
        DirectRunner
            .run_collect(&eager, &JobConfig::default())
            .unwrap()
            .0,
        expected
    );
    assert_eq!(
        DirectRunner
            .run_collect(&paged, &JobConfig::default())
            .unwrap()
            .0,
        expected
    );
    assert_eq!(
        DirectRunner
            .run_collect(&sharded, &JobConfig::default())
            .unwrap()
            .0,
        expected
    );
    assert!(sql_sharded_source::<Row>(&p, "bad-size", &url, query, 3, 0, "fixture-v1").is_err());
    assert!(sql_sharded_source::<Row>(&p, "bad-snapshot", &url, query, 3, 2, "").is_err());
    let mismatch = Pipeline::default();
    let output = sql_sharded_source::<Row>(&mismatch, "mismatch", &url, query, 4, 2, "fixture-v1")
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
    let unwritten = rt.block_on(async {
        let pool = sqlx::SqlitePool::connect(&url).await.unwrap();
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM output")
            .fetch_one(&pool)
            .await
            .unwrap()
    });
    assert_eq!(unwritten, 0);
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let verify = Pipeline::default();
    let written = sql_source::<Row>(
        &verify,
        "written",
        &url,
        "SELECT id, name FROM output ORDER BY id",
    )
    .unwrap()
    .output("written-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&written, &JobConfig::default())
            .unwrap()
            .0,
        expected
    );
    let missing = Pipeline::default();
    let unavailable = format!(
        "sqlite://{}?mode=ro",
        dir.path().join("missing.db").display()
    );
    let missing_output = sql_source::<Row>(&missing, "unavailable", &unavailable, query)
        .unwrap()
        .output("unavailable-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&missing_output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}
