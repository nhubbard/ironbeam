use ironbeam_core::{Element, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_sql::{
    sql_parallel_sink_with, sql_sharded_source, sql_source, sql_streaming_source,
};
use testcontainers::runners::SyncRunner;
use testcontainers_modules::{mysql::Mysql, postgres::Postgres};

#[derive(Clone, Debug, PartialEq, Eq, sqlx::FromRow)]
struct Row {
    id: i64,
    name: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/live-sql-row";
    const CODER_ID: &'static str = "tests/live-sql-row-local";
}

fn exercise(url: &str) {
    sqlx::any::install_default_drivers();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let pool = sqlx::AnyPool::connect(url).await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(
            "CREATE TABLE v5_rows (id BIGINT PRIMARY KEY, name TEXT NOT NULL)".to_owned(),
        ))
        .execute(&pool)
        .await
        .unwrap();
    });
    let expected = (0..7)
        .map(|id| Row {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", expected.clone()).unwrap();
    let sink = sql_parallel_sink_with(
        &input,
        "write",
        url,
        "INSERT INTO v5_rows (id, name)",
        3,
        |mut separated, row: &Row| {
            separated.push_bind(row.id).push_bind(row.name.clone());
        },
    )
    .unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();

    let query = "SELECT id, name FROM v5_rows ORDER BY id";
    let reader = Pipeline::default();
    let eager = sql_source::<Row>(&reader, "eager", url, query)
        .unwrap()
        .output("eager-out")
        .unwrap();
    let paged = sql_streaming_source::<Row>(&reader, "paged", url, query, 3)
        .unwrap()
        .output("paged-out")
        .unwrap();
    let sharded = sql_sharded_source::<Row>(&reader, "shards", url, query, 7, 2, "fixture-frozen")
        .unwrap()
        .output("shards-out")
        .unwrap();
    for output in [&eager, &paged, &sharded] {
        assert_eq!(
            DirectRunner
                .run_collect(output, &JobConfig::default())
                .unwrap()
                .0,
            expected
        );
    }
}

#[test]
#[ignore = "requires a Docker-compatible daemon and postgres:11-alpine image"]
fn postgres_v5_sql_helpers_round_trip() {
    let server = Postgres::default().with_host_auth().start().unwrap();
    let url = format!(
        "postgres://postgres@{}:{}/postgres",
        server.get_host().unwrap(),
        server.get_host_port_ipv4(5432).unwrap()
    );
    exercise(&url);
}

#[test]
#[ignore = "requires a Docker-compatible daemon and mysql:8.1 image"]
fn mysql_v5_sql_helpers_round_trip() {
    let server = Mysql::default().start().unwrap();
    let url = format!(
        "mysql://root@{}:{}/test",
        server.get_host().unwrap(),
        server.get_host_port_ipv4(3306).unwrap()
    );
    exercise(&url);
}
