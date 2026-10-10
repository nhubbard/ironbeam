#![cfg(any(
    feature = "jsonl",
    feature = "csv",
    feature = "xml",
    feature = "avro",
    feature = "parquet"
))]
use ironbeam_core::{Boundedness, Capabilities, Element, ErrorKind, Operation, Pipeline, SinkMode};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io::*;
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
#[cfg(feature = "jsonl")]
#[test]
fn jsonl_contract() {
    round_trip(
        "jsonl",
        |i, k, p| jsonl_sink(i, k, p),
        |p, k, f| jsonl_source(p, k, f),
    );
}
#[cfg(feature = "csv")]
#[test]
fn csv_contract() {
    round_trip(
        "csv",
        |i, k, p| csv_sink(i, k, p),
        |p, k, f| csv_source(p, k, f),
    );
}
#[cfg(feature = "xml")]
#[test]
fn xml_contract() {
    round_trip(
        "xml",
        |i, k, p| xml_sink(i, k, p),
        |p, k, f| xml_source(p, k, f),
    );
}
#[cfg(feature = "avro")]
#[test]
fn avro_contract() {
    round_trip(
        "avro",
        |i, k, p| avro_sink(i, k, p),
        |p, k, f| avro_source(p, k, f),
    );
}
#[cfg(feature = "parquet")]
#[test]
fn parquet_contract() {
    round_trip(
        "parquet",
        |i, k, p| parquet_sink(i, k, p),
        |p, k, f| parquet_source(p, k, f),
    );
}

#[cfg(feature = "jsonl")]
#[test]
fn malformed_source_never_replaces_existing_sink_output() {
    let dir = tempfile::tempdir().unwrap();
    let input_path = dir.path().join("input.jsonl");
    let output_path = dir.path().join("output.jsonl");
    fs::write(&input_path, b"{\"id\":1,\"name\":\"Ada\"}\ninvalid\n").unwrap();
    fs::write(&output_path, b"old result\n").unwrap();
    let pipeline = Pipeline::default();
    let input = jsonl_source::<Row>(&pipeline, "read", &input_path).unwrap();
    let sink = jsonl_sink(&input, "write", &output_path).unwrap();
    let error = DirectRunner
        .run_sink(&sink, &JobConfig::default())
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Io);
    assert_eq!(fs::read(&output_path).unwrap(), b"old result\n");
}

#[cfg(feature = "jsonl")]
#[test]
fn jsonl_blank_lines_and_file_commit_error_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("blank.jsonl");
    fs::write(&source_path, b"\n{\"id\":1,\"name\":\"Ada\"}\n\n").unwrap();
    let pipeline = Pipeline::default();
    let source = jsonl_source::<Row>(&pipeline, "read", &source_path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&source, &JobConfig::default())
            .unwrap()
            .0,
        rows()[..1]
    );
    assert_eq!(
        jsonl_source::<Row>(
            &Pipeline::default(),
            "missing",
            dir.path().join("missing.jsonl")
        )
        .err()
        .unwrap()
        .kind,
        ErrorKind::Io
    );
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", rows()).unwrap();
    let sink = jsonl_sink(&input, "sink", dir.path()).unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert!(dir.path().is_dir());
}

#[cfg(feature = "xml")]
#[test]
fn malformed_xml_is_an_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid.xml");
    fs::write(&path, b"<records><item><id>bad</id></item>").unwrap();
    let pipeline = Pipeline::default();
    let output = xml_source::<Row>(&pipeline, "source", &path)
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
}
