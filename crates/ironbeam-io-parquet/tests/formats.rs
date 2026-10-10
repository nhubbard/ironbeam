use ironbeam_core::{Boundedness, Capabilities, Element, ErrorKind, Operation, Pipeline, SinkMode};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_parquet::*;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Row {
    id: i32,
    name: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/row";
    const CODER_ID: &'static str = "tests/row-json";
}
fn rows() -> Vec<Row> {
    vec![
        Row {
            id: 1,
            name: "Ada".into(),
        },
        Row {
            id: 2,
            name: "Lin".into(),
        },
    ]
}

type MakeSink = fn(
    &ironbeam_core::PCollection<Row>,
    &str,
    &std::path::Path,
) -> Result<ironbeam_core::Sink<Row>, ironbeam_core::Error>;
type MakeSource = fn(
    &Pipeline,
    &str,
    &std::path::Path,
) -> Result<ironbeam_core::PCollection<Row>, ironbeam_core::Error>;
fn round_trip(extension: &str, make_sink: MakeSink, make_source: MakeSource) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("rows.{extension}"));
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", rows()).unwrap();
    let sink = make_sink(&input, "file", &path).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let node = graph.node("file").unwrap();
    assert!(
        matches!(&node.operation, Operation::Sink { spec } if spec.mode == SinkMode::FinalResult && spec.commit)
    );
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert!(path.exists());
    let next = Pipeline::default();
    let output = make_source(&next, "read", &path)
        .unwrap()
        .output("output")
        .unwrap();
    let (graph, _) = next.snapshot().unwrap();
    let source = graph.node("read").unwrap();
    assert!(source.semantic_id.starts_with("ironbeam/io/"));
    assert!(matches!(
        graph
            .validate_for("dataflow-preview", &Capabilities::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::UnsupportedCapability { .. }
    ));
    assert_eq!(source.boundedness, Boundedness::Bounded);
    assert!(
        matches!(&source.operation, Operation::Source { progress } if progress.stable_split_id && !progress.reports_event_time)
    );
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
    fs::write(&path, b"changed").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    let empty_path = dir.path().join(format!("empty.{extension}"));
    let empty = Pipeline::default();
    let empty_input = empty.from_vec("empty-values", Vec::<Row>::new()).unwrap();
    let empty_sink = make_sink(&empty_input, "empty-file", &empty_path).unwrap();
    DirectRunner
        .run_sink(&empty_sink, &JobConfig::default())
        .unwrap();
    let empty_reader = Pipeline::default();
    let empty_output = make_source(&empty_reader, "empty-read", &empty_path)
        .unwrap()
        .output("empty-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&empty_output, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<Row>::new()
    );
}
#[test]
fn parquet_row_group_shards_read_only_selected_groups() {
    use arrow::datatypes::FieldRef;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("groups.parquet");
    let expected = (0..5)
        .map(|id| Row {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let fields = Vec::<FieldRef>::from_type::<Row>(TracingOptions::default()).unwrap();
    let batch = serde_arrow::to_record_batch(&fields, &expected).unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let file = fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let pipeline = Pipeline::default();
    assert!(parquet_row_group_sharded_source::<Row>(&pipeline, "bad", &path, 0).is_err());
    let output = parquet_row_group_sharded_source::<Row>(&pipeline, "read", &path, 1)
        .unwrap()
        .output("out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(graph.node("read/group-2").is_some());
    for parallelism in [1, 3] {
        let values = DirectRunner
            .run_collect(
                &output,
                &JobConfig {
                    parallelism,
                    ..JobConfig::default()
                },
            )
            .unwrap()
            .0;
        assert_eq!(values, expected);
    }
    fs::write(&path, b"changed").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[test]
fn parquet_contract() {
    round_trip(
        "parquet",
        |i, k, p| parquet_sink(i, k, p),
        |p, k, f| parquet_source(p, k, f),
    );
}
#[test]
fn parquet_lazy_contract() {
    round_trip(
        "parquet",
        |i, k, p| parquet_sink(i, k, p),
        |p, k, f| parquet_streaming_source(p, k, f),
    );
}

#[test]
fn parquet_record_shards_and_glob_preserve_rows() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("a.parquet"), dir.path().join("b.parquet")];
    for (index, path) in paths.iter().enumerate() {
        let pipeline = Pipeline::default();
        let input = pipeline
            .from_vec("values", vec![rows()[index].clone()])
            .unwrap();
        let writer = parquet_sink(&input, "write", path).unwrap();
        DirectRunner
            .run_sink(&writer, &JobConfig::default())
            .unwrap();
    }
    let pipeline = Pipeline::default();
    let glob = parquet_glob_source::<Row>(
        &pipeline,
        "files",
        &format!("{}/*.parquet", dir.path().display()),
    )
    .unwrap()
    .output("glob-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&glob, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
    let pipeline = Pipeline::default();
    let shards = parquet_sharded_source::<Row>(&pipeline, "shards", &paths[0], 1)
        .unwrap()
        .output("shard-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&shards, &JobConfig::default())
            .unwrap()
            .0,
        vec![rows()[0].clone()]
    );
    assert!(parquet_sharded_source::<Row>(&pipeline, "bad", &paths[0], 0).is_err());
}

#[cfg(feature = "parallel-io")]
#[test]
fn parallel_parquet_sink_publishes_one_ordered_container() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("parallel.parquet");
    let expected = (0..7)
        .map(|id| Row {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", expected.clone()).unwrap();
    assert!(parquet_parallel_sink(&input, "bad", &path, 0).is_err());
    let writer = parquet_parallel_sink(&input, "write", &path, 5).unwrap();
    DirectRunner
        .run_sink(&writer, &JobConfig::default())
        .unwrap();
    let pipeline = Pipeline::default();
    let output = parquet_source::<Row>(&pipeline, "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        expected
    );
}

#[test]
fn malformed_parquet_reports_io_without_opening_a_sink() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("broken.parquet");
    fs::write(&path, b"not a Parquet file").unwrap();
    let pipeline = Pipeline::default();
    let output = parquet_source::<Row>(&pipeline, "broken", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    let pipeline = Pipeline::default();
    let output = parquet_streaming_source::<Row>(&pipeline, "lazy-broken", &path)
        .unwrap()
        .output("lazy-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert!(parquet_row_group_sharded_source::<Row>(&pipeline, "groups", &path, 1).is_err());
}
