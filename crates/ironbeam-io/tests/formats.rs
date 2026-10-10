#![cfg(any(
    feature = "jsonl",
    feature = "csv",
    feature = "xml",
    feature = "avro",
    feature = "parquet",
    feature = "arrow-ipc",
    feature = "msgpack",
    feature = "cbor",
    feature = "protobuf",
    feature = "tfrecord"
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
#[cfg(feature = "msgpack")]
#[test]
fn msgpack_contract() {
    round_trip(
        "msgpack",
        |i, k, p| msgpack_sink(i, k, p),
        |p, k, f| msgpack_source(p, k, f),
    );
}
#[cfg(feature = "cbor")]
#[test]
fn cbor_contract() {
    round_trip(
        "cbor",
        |i, k, p| cbor_sink(i, k, p),
        |p, k, f| cbor_source(p, k, f),
    );
}

#[cfg(feature = "protobuf")]
#[derive(Clone, PartialEq, prost::Message)]
struct ProtoRow {
    #[prost(int32, tag = "1")]
    id: i32,
    #[prost(string, tag = "2")]
    name: String,
}
#[cfg(feature = "protobuf")]
impl Element for ProtoRow {
    const TYPE_ID: &'static str = "tests/proto-row";
    const CODER_ID: &'static str = "tests/proto-row-protobuf";
}
#[cfg(feature = "protobuf")]
#[test]
fn protobuf_length_delimited_records_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.pb");
    let rows = vec![
        ProtoRow {
            id: 1,
            name: "Ada".into(),
        },
        ProtoRow {
            id: 2,
            name: "Lin".into(),
        },
    ];
    let p = Pipeline::default();
    let input = p.from_vec("input", rows.clone()).unwrap();
    let sink = protobuf_sink(&input, "sink", &path).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let p = Pipeline::default();
    let output = protobuf_source::<ProtoRow>(&p, "source", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );
}
#[cfg(feature = "tfrecord")]
#[test]
fn tfrecord_crc_and_truncation_are_checked() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.tfrecord");
    let rows = vec![b"one".to_vec(), b"two".to_vec()];
    let p = Pipeline::default();
    let input = p.from_vec("input", rows.clone()).unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&input, "sink", &path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let p = Pipeline::default();
    let output = tfrecord_source(&p, "source", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );

    let mut bad = fs::read(&path).unwrap();
    bad[12] ^= 1;
    fs::write(&path, bad).unwrap();
    let p = Pipeline::default();
    let output = tfrecord_source(&p, "bad-source", &path)
        .unwrap()
        .output("bad-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    let bytes = fs::read(&path).unwrap();
    fs::write(&path, &bytes[..7]).unwrap();
    let p = Pipeline::default();
    let output = tfrecord_source(&p, "truncated", &path)
        .unwrap()
        .output("truncated-out")
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
const ROW_AVRO_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
#[cfg(feature = "avro")]
#[test]
fn avro_contract() {
    round_trip(
        "avro",
        |i, k, p| avro_sink(i, k, p, ROW_AVRO_SCHEMA),
        |p, k, f| avro_source(p, k, f),
    );
}
#[cfg(feature = "avro")]
#[test]
fn avro_lazy_contract() {
    round_trip(
        "avro",
        |i, k, p| avro_sink(i, k, p, ROW_AVRO_SCHEMA),
        |p, k, f| avro_streaming_source(p, k, f),
    );
}
#[cfg(feature = "avro")]
#[test]
fn native_avro_interoperates_with_external_records_and_validates_schema() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("native.avro");
    let schema = apache_avro::Schema::parse_str(ROW_AVRO_SCHEMA).unwrap();
    let file = fs::File::create(&path).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, file).unwrap();
    for row in rows() {
        writer.append_ser(row).unwrap();
    }
    writer.flush().unwrap();
    drop(writer);
    let pipeline = Pipeline::default();
    let output = avro_source::<Row>(&pipeline, "external", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );

    let native_path = dir.path().join("written.avro");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", rows()).unwrap();
    let sink = avro_sink(&input, "native", &native_path, ROW_AVRO_SCHEMA).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    let reader = apache_avro::Reader::new(fs::File::open(&native_path).unwrap()).unwrap();
    let decoded: Vec<Row> = reader
        .map(|row| apache_avro::from_value(&row.unwrap()).unwrap())
        .collect();
    assert_eq!(decoded, rows());

    assert_eq!(
        avro_sink(&input, "invalid", dir.path().join("invalid.avro"), "{")
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    let old_path = dir.path().join("old.avro");
    fs::write(&old_path, b"old output").unwrap();
    let incompatible = r#"{"type":"record","name":"Wrong","fields":[{"name":"id","type":"long"},{"name":"missing","type":"string"}]}"#;
    let sink = avro_sink(&input, "incompatible", &old_path, incompatible).unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert_eq!(fs::read(&old_path).unwrap(), b"old output");
}
#[cfg(feature = "avro")]
#[test]
fn avro_json_preview_files_remain_readable() {
    round_trip(
        "avro",
        |i, k, p| avro_json_sink(i, k, p),
        |p, k, f| avro_json_source(p, k, f),
    );
}
#[cfg(feature = "avro")]
#[test]
fn avro_json_serialization_failure_preserves_existing_output() {
    #[derive(Clone)]
    struct Unserializable;
    impl Element for Unserializable {
        const TYPE_ID: &'static str = "tests/unserializable";
        const CODER_ID: &'static str = "tests/unserializable";
    }
    impl Serialize for Unserializable {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom(
                "deliberate serialization failure",
            ))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("existing.avro");
    fs::write(&path, b"old result").unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", vec![Unserializable]).unwrap();
    let sink = avro_json_sink(&input, "file", &path).unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert_eq!(fs::read(&path).unwrap(), b"old result");
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
#[cfg(feature = "parquet")]
#[test]
fn parquet_lazy_contract() {
    round_trip(
        "parquet",
        |i, k, p| parquet_sink(i, k, p),
        |p, k, f| parquet_streaming_source(p, k, f),
    );
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn arrow_ipc_row_contract() {
    round_trip(
        "arrow",
        |i, k, p| arrow_ipc_sink(i, k, p),
        |p, k, f| arrow_ipc_source(p, k, f),
    );
}
#[cfg(feature = "arrow-ipc")]
#[test]
fn arrow_ipc_lazy_row_contract() {
    round_trip(
        "arrow",
        |i, k, p| arrow_ipc_sink(i, k, p),
        |p, k, f| arrow_ipc_streaming_source(p, k, f),
    );
}

#[cfg(feature = "arrow-ipc")]
#[test]
fn arrow_ipc_batch_contract_and_coder() {
    let directory = tempfile::tempdir().unwrap();
    let input_path = directory.path().join("input.arrow");
    let output_path = directory.path().join("output.arrow");
    let write_pipeline = Pipeline::default();
    let row_collection = write_pipeline.from_vec("rows", rows()).unwrap();
    let row_sink = arrow_ipc_sink(&row_collection, "row-sink", &input_path).unwrap();
    DirectRunner
        .run_sink(&row_sink, &JobConfig::default())
        .unwrap();

    let batch_pipeline = Pipeline::default();
    let batches = arrow_ipc_batches_source(&batch_pipeline, "batches", &input_path).unwrap();
    let lazy_batches =
        arrow_ipc_batches_streaming_source(&batch_pipeline, "lazy-batches", &input_path)
            .unwrap()
            .output("lazy-batches-out")
            .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&lazy_batches, &JobConfig::default())
            .unwrap()
            .0[0]
            .0
            .num_rows(),
        2
    );
    let output = batches.output("batches-out").unwrap();
    let collected = DirectRunner
        .run_collect(&output, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].0.num_rows(), 2);
    let encoded = collected[0].encode().unwrap();
    assert_eq!(ArrowBatch::decode(&encoded).unwrap().0.num_rows(), 2);
    assert!(ArrowBatch::decode(&encoded[..encoded.len() - 1]).is_err());
    let serialized = serde_json::to_vec(&collected[0]).unwrap();
    let restored_batch: ArrowBatch = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(restored_batch.0.num_rows(), 2);
    let batch_sink = arrow_ipc_batches_sink(&batches, "batch-sink", &output_path).unwrap();
    DirectRunner
        .run_sink(&batch_sink, &JobConfig::default())
        .unwrap();

    let read_pipeline = Pipeline::default();
    let restored = arrow_ipc_source::<Row>(&read_pipeline, "restored", &output_path)
        .unwrap()
        .output("restored-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&restored, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
}

#[cfg(feature = "jsonl")]
#[test]
fn jsonl_lazy_reader_defers_parse_errors_until_execution() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rows.jsonl");
    fs::write(&path, b"{\"id\":1,\"name\":\"Ada\"}\nmalformed\n").unwrap();
    let pipeline = Pipeline::default();
    let collection = jsonl_streaming_source::<Row>(&pipeline, "lazy", &path).unwrap();
    let output = collection.output("out").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    fs::write(&path, b"{\"id\":1,\"name\":\"Ada\"}\n").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[cfg(feature = "csv")]
#[test]
fn csv_lazy_reader_matches_bounded_rows() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("rows.csv");
    fs::write(&path, b"id,name\n1,Ada\n2,Lin\n").unwrap();
    let pipeline = Pipeline::default();
    let output = csv_streaming_source::<Row>(&pipeline, "lazy", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows()
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
