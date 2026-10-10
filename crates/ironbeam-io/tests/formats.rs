#![cfg(any(
    feature = "jsonl",
    feature = "csv",
    feature = "xml",
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

#[cfg(all(feature = "parallel-io", feature = "jsonl", feature = "csv"))]
#[test]
fn parallel_jsonl_and_csv_sinks_preserve_order_and_single_header() {
    let dir = tempfile::tempdir().unwrap();
    for (extension, make_sink, make_source) in [
        (
            "jsonl",
            (|i, k, p, n| jsonl_parallel_sink(i, k, p, n))
                as fn(&_, &str, &std::path::Path, usize) -> _,
            (|p, k, f| jsonl_source::<Row>(p, k, f)) as fn(&_, &str, &std::path::Path) -> _,
        ),
        (
            "csv",
            (|i, k, p, n| csv_parallel_sink(i, k, p, n))
                as fn(&_, &str, &std::path::Path, usize) -> _,
            (|p, k, f| csv_source::<Row>(p, k, f)) as fn(&_, &str, &std::path::Path) -> _,
        ),
    ] {
        let path = dir.path().join(format!("rows.{extension}"));
        for count in [0, 1, 5] {
            let expected = (0..count)
                .map(|id| Row {
                    id,
                    name: format!("row-{id}"),
                })
                .collect::<Vec<_>>();
            let pipeline = Pipeline::default();
            let input = pipeline.from_vec("input", expected.clone()).unwrap();
            assert!(make_sink(&input, "bad", &path, 0).is_err());
            let sink = make_sink(&input, "write", &path, 4).unwrap();
            DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
            let read_pipeline = Pipeline::default();
            let output = make_source(&read_pipeline, "read", &path)
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
            if extension == "csv" && count > 0 {
                assert_eq!(
                    fs::read_to_string(&path)
                        .unwrap()
                        .matches("id,name")
                        .count(),
                    1
                );
            }
        }
    }
}

#[cfg(all(
    feature = "parallel-io",
    feature = "jsonl",
    feature = "csv",
    feature = "compression-gzip"
))]
#[test]
fn parallel_sinks_compress_after_ordered_shard_assembly() {
    let dir = tempfile::tempdir().unwrap();
    for (extension, make_sink, make_source) in [
        (
            "jsonl.gz",
            (|i, k, p, n| jsonl_parallel_sink(i, k, p, n))
                as fn(&_, &str, &std::path::Path, usize) -> _,
            (|p, k, f| jsonl_source::<Row>(p, k, f)) as fn(&_, &str, &std::path::Path) -> _,
        ),
        (
            "csv.gz",
            (|i, k, p, n| csv_parallel_sink(i, k, p, n))
                as fn(&_, &str, &std::path::Path, usize) -> _,
            (|p, k, f| csv_source::<Row>(p, k, f)) as fn(&_, &str, &std::path::Path) -> _,
        ),
    ] {
        let path = dir.path().join(format!("rows.{extension}"));
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", rows()).unwrap();
        let sink = make_sink(&input, "write", &path, 3).unwrap();
        DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
        assert_eq!(&fs::read(&path).unwrap()[..2], &[0x1f, 0x8b]);
        let read_pipeline = Pipeline::default();
        let output = make_source(&read_pipeline, "read", &path)
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
}

#[cfg(all(feature = "parallel-io", feature = "msgpack", feature = "cbor"))]
#[test]
fn parallel_framed_sinks_keep_record_order() {
    let dir = tempfile::tempdir().unwrap();
    for (extension, make_sink, make_source) in [
        (
            "msgpack",
            (|i, k, p, n| msgpack_parallel_sink(i, k, p, n))
                as fn(&_, &str, &std::path::Path, usize) -> _,
            (|p, k, f| msgpack_source::<Row>(p, k, f)) as fn(&_, &str, &std::path::Path) -> _,
        ),
        (
            "cbor",
            (|i, k, p, n| cbor_parallel_sink(i, k, p, n))
                as fn(&_, &str, &std::path::Path, usize) -> _,
            (|p, k, f| cbor_source::<Row>(p, k, f)) as fn(&_, &str, &std::path::Path) -> _,
        ),
    ] {
        let path = dir.path().join(format!("rows.{extension}"));
        let expected = (0..7)
            .map(|id| Row {
                id,
                name: format!("row-{id}"),
            })
            .collect::<Vec<_>>();
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", expected.clone()).unwrap();
        DirectRunner
            .run_sink(
                &make_sink(&input, "write", &path, 5).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
        let read_pipeline = Pipeline::default();
        let output = make_source(&read_pipeline, "read", &path)
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
}

#[cfg(all(feature = "jsonl", feature = "csv"))]
#[test]
fn file_globs_expand_in_sorted_order_and_reject_empty_matches() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("b.jsonl"), b"{\"id\":2,\"name\":\"Lin\"}\n").unwrap();
    fs::write(dir.path().join("a.jsonl"), b"{\"id\":1,\"name\":\"Ada\"}\n").unwrap();
    let pattern = dir.path().join("*.jsonl").to_string_lossy().to_string();
    let pipeline = Pipeline::default();
    let output = jsonl_glob_source::<Row>(&pipeline, "all-jsonl", &pattern)
        .unwrap()
        .output("jsonl-out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    let original_keys = graph
        .nodes
        .iter()
        .filter(|node| node.key.starts_with("all-jsonl/file-"))
        .map(|node| node.key.clone())
        .collect::<Vec<_>>();
    assert_eq!(original_keys.len(), 2);
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
    fs::write(
        dir.path().join("0.jsonl"),
        b"{\"id\":0,\"name\":\"Zero\"}\n",
    )
    .unwrap();
    let revised = Pipeline::default();
    jsonl_glob_source::<Row>(&revised, "all-jsonl", &pattern).unwrap();
    let (revised_graph, _) = revised.snapshot().unwrap();
    assert!(
        original_keys
            .iter()
            .all(|key| revised_graph.node(key).is_some())
    );
    let single_pattern = dir.path().join("a.jsonl").to_string_lossy().to_string();
    let one = Pipeline::default();
    let output = jsonl_glob_source::<Row>(&one, "one", &single_pattern)
        .unwrap()
        .output("one-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        [rows()[0].clone()]
    );

    fs::write(dir.path().join("b.csv"), b"id,name\n2,Lin\n").unwrap();
    fs::write(dir.path().join("a.csv"), b"id,name\n1,Ada\n").unwrap();
    let csv_pattern = dir.path().join("*.csv").to_string_lossy().to_string();
    let pipeline = Pipeline::default();
    let output = csv_glob_source::<Row>(&pipeline, "all-csv", &csv_pattern)
        .unwrap()
        .output("csv-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
    let empty_pattern = dir.path().join("*.missing").to_string_lossy().to_string();
    assert_eq!(
        jsonl_glob_source::<Row>(&pipeline, "none", &empty_pattern)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}

#[cfg(all(feature = "jsonl", feature = "csv"))]
#[test]
fn jsonl_and_csv_shards_preserve_record_boundaries_and_bind_contents() {
    let dir = tempfile::tempdir().unwrap();
    let jsonl_path = dir.path().join("rows.jsonl");
    fs::write(
        &jsonl_path,
        b"{\"id\":1,\"name\":\"Ada\"}\n\n{\"id\":2,\"name\":\"Lin\"}\n",
    )
    .unwrap();
    let pipeline = Pipeline::default();
    let output = jsonl_sharded_source::<Row>(&pipeline, "jsonl", &jsonl_path, 1)
        .unwrap()
        .output("jsonl-out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    for index in 0..3 {
        assert!(graph.node(&format!("jsonl/shard-{index}")).is_some());
    }
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        rows()
    );
    fs::write(&jsonl_path, b"changed\n").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );

    let csv_path = dir.path().join("rows.csv");
    fs::write(&csv_path, b"id,name\n1,\"Ada\nLovelace\"\n2,Lin\n").unwrap();
    let pipeline = Pipeline::default();
    let output = csv_sharded_source::<Row>(&pipeline, "csv", &csv_path, 1)
        .unwrap()
        .output("csv-out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(graph.node("csv/shard-0").is_some());
    assert!(graph.node("csv/shard-1").is_some());
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![
            Row {
                id: 1,
                name: "Ada\nLovelace".into()
            },
            rows()[1].clone()
        ]
    );
    assert!(csv_sharded_source::<Row>(&pipeline, "bad", &csv_path, 0).is_err());

    let empty_path = dir.path().join("empty.jsonl");
    fs::write(&empty_path, b"").unwrap();
    let pipeline = Pipeline::default();
    let output = jsonl_sharded_source::<Row>(&pipeline, "empty", &empty_path, 2)
        .unwrap()
        .output("empty-out")
        .unwrap();
    assert!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0
            .is_empty()
    );
}

#[cfg(all(feature = "jsonl", feature = "csv", feature = "compression-gzip"))]
#[test]
fn compressed_jsonl_and_csv_shards_match_bounded_readers() {
    let dir = tempfile::tempdir().unwrap();
    for (extension, make_sink, make_source) in [
        (
            "jsonl.gz",
            (|i, k, p| jsonl_sink(i, k, p)) as MakeSink,
            (|p, k, f, n| jsonl_sharded_source::<Row>(p, k, f, n))
                as fn(&Pipeline, &str, &std::path::Path, usize) -> _,
        ),
        (
            "csv.gz",
            (|i, k, p| csv_sink(i, k, p)) as MakeSink,
            (|p, k, f, n| csv_sharded_source::<Row>(p, k, f, n))
                as fn(&Pipeline, &str, &std::path::Path, usize) -> _,
        ),
    ] {
        let path = dir.path().join(format!("rows.{extension}"));
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", rows()).unwrap();
        DirectRunner
            .run_sink(
                &make_sink(&input, "write", &path).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
        let reader = Pipeline::default();
        let output = make_source(&reader, "read", &path, 1)
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
}

#[cfg(all(
    feature = "parquet",
    feature = "arrow-ipc",
    feature = "msgpack",
    feature = "cbor"
))]
#[test]
fn record_format_shards_match_bounded_readers() {
    let dir = tempfile::tempdir().unwrap();
    type Sharded = fn(
        &Pipeline,
        &str,
        &std::path::Path,
        usize,
    ) -> Result<ironbeam_core::PCollection<Row>, ironbeam_core::Error>;
    for (extension, make_sink, make_source) in [
        (
            "parquet",
            (|i, k, p| parquet_sink(i, k, p)) as MakeSink,
            (|p, k, f, n| parquet_sharded_source::<Row>(p, k, f, n)) as Sharded,
        ),
        (
            "arrow",
            (|i, k, p| arrow_ipc_sink(i, k, p)) as MakeSink,
            (|p, k, f, n| arrow_ipc_sharded_source::<Row>(p, k, f, n)) as Sharded,
        ),
        (
            "msgpack",
            (|i, k, p| msgpack_sink(i, k, p)) as MakeSink,
            (|p, k, f, n| msgpack_sharded_source::<Row>(p, k, f, n)) as Sharded,
        ),
        (
            "cbor",
            (|i, k, p| cbor_sink(i, k, p)) as MakeSink,
            (|p, k, f, n| cbor_sharded_source::<Row>(p, k, f, n)) as Sharded,
        ),
    ] {
        let path = dir.path().join(format!("rows.{extension}"));
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", rows()).unwrap();
        DirectRunner
            .run_sink(
                &make_sink(&input, "write", &path).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
        let reader = Pipeline::default();
        let output = make_source(&reader, "read", &path, 1)
            .unwrap()
            .output("out")
            .unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .unwrap()
                .0,
            rows(),
            "{extension}"
        );
    }
}

#[cfg(feature = "parquet")]
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

#[cfg(feature = "arrow-ipc")]
#[test]
fn arrow_ipc_indexed_shards_keep_batch_boundaries_and_rows() {
    use arrow::datatypes::FieldRef;
    use arrow::ipc::writer::FileWriter;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("batches.arrow");
    let fields = Vec::<FieldRef>::from_type::<Row>(TracingOptions::default()).unwrap();
    let batches = (0..3)
        .map(|id| {
            serde_arrow::to_record_batch(
                &fields,
                &vec![Row {
                    id,
                    name: format!("row-{id}"),
                }],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let file = fs::File::create(&path).unwrap();
    let mut writer = FileWriter::try_new(file, &batches[0].schema()).unwrap();
    for batch in &batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();

    let pipeline = Pipeline::default();
    assert!(arrow_ipc_batch_sharded_source::<Row>(&pipeline, "bad", &path, 0).is_err());
    let rows = arrow_ipc_batch_sharded_source::<Row>(&pipeline, "rows", &path, 1)
        .unwrap()
        .output("rows-out")
        .unwrap();
    let batch_output = arrow_ipc_batches_indexed_source(&pipeline, "batches", &path, 2)
        .unwrap()
        .output("batches-out")
        .unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(graph.node("rows/batch-2").is_some());
    assert!(graph.node("batches/batch-1").is_some());
    for parallelism in [1, 3] {
        let config = JobConfig {
            parallelism,
            ..JobConfig::default()
        };
        let actual_rows = DirectRunner.run_collect(&rows, &config).unwrap().0;
        assert_eq!(
            actual_rows,
            (0..3)
                .map(|id| Row {
                    id,
                    name: format!("row-{id}"),
                })
                .collect::<Vec<_>>()
        );
        let actual_batches = DirectRunner.run_collect(&batch_output, &config).unwrap().0;
        assert_eq!(actual_batches.len(), 3);
        assert!(actual_batches.iter().all(|batch| batch.0.num_rows() == 1));
    }
    fs::write(&path, b"changed").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&rows, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
}

#[cfg(all(
    feature = "parallel-io",
    feature = "xml",
    feature = "parquet",
    feature = "arrow-ipc"
))]
#[test]
fn parallel_container_sinks_merge_valid_ordered_files() {
    let dir = tempfile::tempdir().unwrap();
    type ParallelSink = fn(
        &ironbeam_core::PCollection<Row>,
        &str,
        &std::path::Path,
        usize,
    ) -> Result<ironbeam_core::Sink<Row>, ironbeam_core::Error>;
    for (extension, make_sink, make_source) in [
        (
            "xml",
            (|i, k, p, n| xml_parallel_sink(i, k, p, n)) as ParallelSink,
            (|p, k, f| xml_source::<Row>(p, k, f)) as MakeSource,
        ),
        (
            "parquet",
            (|i, k, p, n| parquet_parallel_sink(i, k, p, n)) as ParallelSink,
            (|p, k, f| parquet_source::<Row>(p, k, f)) as MakeSource,
        ),
        (
            "arrow",
            (|i, k, p, n| arrow_ipc_parallel_sink(i, k, p, n)) as ParallelSink,
            (|p, k, f| arrow_ipc_source::<Row>(p, k, f)) as MakeSource,
        ),
    ] {
        let path = dir.path().join(format!("rows.{extension}"));
        let expected = (0..7)
            .map(|id| Row {
                id,
                name: format!("row-{id}"),
            })
            .collect::<Vec<_>>();
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", expected.clone()).unwrap();
        assert!(make_sink(&input, "bad", &path, 0).is_err());
        DirectRunner
            .run_sink(
                &make_sink(&input, "write", &path, 5).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
        let reader = Pipeline::default();
        let output = make_source(&reader, "read", &path)
            .unwrap()
            .output("out")
            .unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .unwrap()
                .0,
            expected,
            "{extension}"
        );
    }
}

#[cfg(all(feature = "jsonl", feature = "csv", feature = "compression-gzip"))]
#[test]
fn compressed_jsonl_and_csv_round_trip_with_lazy_sources() {
    let dir = tempfile::tempdir().unwrap();
    for (extension, make_sink, make_source, make_streaming) in [
        (
            "jsonl.gz",
            (|i, k, p| jsonl_sink(i, k, p)) as MakeSink,
            (|p, k, f| jsonl_source(p, k, f)) as MakeSource,
            (|p, k, f| jsonl_streaming_source(p, k, f)) as MakeSource,
        ),
        (
            "csv.gz",
            (|i, k, p| csv_sink(i, k, p)) as MakeSink,
            (|p, k, f| csv_source(p, k, f)) as MakeSource,
            (|p, k, f| csv_streaming_source(p, k, f)) as MakeSource,
        ),
    ] {
        let path = dir.path().join(format!("rows.{extension}"));
        let pipeline = Pipeline::default();
        let input = pipeline.from_vec("input", rows()).unwrap();
        DirectRunner
            .run_sink(
                &make_sink(&input, "write", &path).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..2], &[0x1f, 0x8b]);
        let read_pipeline = Pipeline::default();
        let eager = make_source(&read_pipeline, "eager", &path)
            .unwrap()
            .output("eager-out")
            .unwrap();
        let streaming = make_streaming(&read_pipeline, "streaming", &path)
            .unwrap()
            .output("streaming-out")
            .unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&eager, &JobConfig::default())
                .unwrap()
                .0,
            rows()
        );
        assert_eq!(
            DirectRunner
                .run_collect(&streaming, &JobConfig::default())
                .unwrap()
                .0,
            rows()
        );
    }
}

#[cfg(all(
    feature = "compression-gzip",
    feature = "xml",
    feature = "msgpack",
    feature = "cbor"
))]
#[test]
fn gzip_round_trips_xml_msgpack_and_cbor() {
    round_trip(
        "xml.gz",
        |i, k, p| xml_sink(i, k, p),
        |p, k, f| xml_streaming_source(p, k, f, 1),
    );
    round_trip(
        "msgpack.gz",
        |i, k, p| msgpack_sink(i, k, p),
        |p, k, f| msgpack_streaming_source(p, k, f),
    );
    round_trip(
        "cbor.gz",
        |i, k, p| cbor_sink(i, k, p),
        |p, k, f| cbor_streaming_source(p, k, f),
    );
}

#[cfg(all(
    feature = "compression-gzip",
    feature = "protobuf",
    feature = "tfrecord"
))]
#[test]
fn gzip_round_trips_protobuf_and_tfrecord_lazy_sources() {
    let dir = tempfile::tempdir().unwrap();
    let proto_path = dir.path().join("rows.pb.gz");
    let proto_rows = vec![
        ProtoRow {
            id: 1,
            name: "Ada".into(),
        },
        ProtoRow {
            id: 2,
            name: "Lin".into(),
        },
    ];
    let pipeline = Pipeline::default();
    let proto_input = pipeline.from_vec("proto", proto_rows.clone()).unwrap();
    DirectRunner
        .run_sink(
            &protobuf_sink(&proto_input, "proto-sink", &proto_path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let proto_output = protobuf_streaming_source::<ProtoRow>(&reader, "proto-read", &proto_path)
        .unwrap()
        .output("proto-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&proto_output, &JobConfig::default())
            .unwrap()
            .0,
        proto_rows.clone()
    );
    let reader = Pipeline::default();
    let output = protobuf_sharded_source::<ProtoRow>(&reader, "shards", &proto_path, 2)
        .unwrap()
        .output("shard-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        proto_rows.clone()
    );

    let tf_path = dir.path().join("rows.tfrecord.gz");
    let tf_rows = vec![b"first".to_vec(), b"second".to_vec()];
    let pipeline = Pipeline::default();
    let tf_input = pipeline.from_vec("tf", tf_rows.clone()).unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_sink(&tf_input, "tf-sink", &tf_path).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let tf_output = tfrecord_streaming_source(&reader, "tf-read", &tf_path)
        .unwrap()
        .output("tf-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&tf_output, &JobConfig::default())
            .unwrap()
            .0,
        tf_rows
    );
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
#[cfg(feature = "msgpack")]
#[test]
fn msgpack_lazy_contract() {
    round_trip(
        "msgpack",
        |i, k, p| msgpack_sink(i, k, p),
        |p, k, f| msgpack_streaming_source(p, k, f),
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
#[cfg(feature = "cbor")]
#[test]
fn cbor_lazy_contract() {
    round_trip(
        "cbor",
        |i, k, p| cbor_sink(i, k, p),
        |p, k, f| cbor_streaming_source(p, k, f),
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
#[cfg(all(feature = "parallel-io", feature = "protobuf", feature = "tfrecord"))]
#[test]
fn parallel_protobuf_and_tfrecord_sinks_keep_records_valid() {
    let dir = tempfile::tempdir().unwrap();
    let proto_rows = (0..7)
        .map(|id| ProtoRow {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let path = dir.path().join("rows.pb");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", proto_rows.clone()).unwrap();
    DirectRunner
        .run_sink(
            &protobuf_parallel_sink(&input, "write", &path, 5).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let output = protobuf_source::<ProtoRow>(&reader, "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        proto_rows.clone()
    );
    let reader = Pipeline::default();
    let output = protobuf_sharded_source::<ProtoRow>(&reader, "shards", &path, 2)
        .unwrap()
        .output("shard-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        proto_rows
    );

    let records = (0..7).map(|id| vec![id as u8; id + 1]).collect::<Vec<_>>();
    let path = dir.path().join("rows.tfrecord");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", records.clone()).unwrap();
    DirectRunner
        .run_sink(
            &tfrecord_parallel_sink(&input, "write", &path, 5).unwrap(),
            &JobConfig::default(),
        )
        .unwrap();
    let reader = Pipeline::default();
    let output = tfrecord_source(&reader, "read", &path)
        .unwrap()
        .output("out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        records.clone()
    );
    let reader = Pipeline::default();
    let output = tfrecord_sharded_source(&reader, "shards", &path, 2)
        .unwrap()
        .output("shard-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        records
    );
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
    let lazy_pipeline = Pipeline::default();
    let lazy_output = protobuf_streaming_source::<ProtoRow>(&lazy_pipeline, "lazy", &path)
        .unwrap()
        .output("lazy-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&lazy_output, &JobConfig::default())
            .unwrap()
            .0,
        rows
    );
    fs::write(&path, [0xff_u8, 0xff, 0xff, 0xff, 0x7f]).unwrap();
    let invalid_pipeline = Pipeline::default();
    let invalid = protobuf_streaming_source::<ProtoRow>(&invalid_pipeline, "invalid", &path)
        .unwrap()
        .output("invalid-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&invalid, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
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
    let lazy_pipeline = Pipeline::default();
    let lazy_output = tfrecord_streaming_source(&lazy_pipeline, "lazy", &path)
        .unwrap()
        .output("lazy-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&lazy_output, &JobConfig::default())
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
    let lazy_bad_pipeline = Pipeline::default();
    let lazy_bad = tfrecord_streaming_source(&lazy_bad_pipeline, "lazy-bad", &path)
        .unwrap()
        .output("lazy-bad-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&lazy_bad, &JobConfig::default())
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
#[cfg(feature = "xml")]
#[test]
fn xml_streaming_spelling_uses_one_whole_file_shard() {
    round_trip(
        "xml",
        |i, k, p| xml_sink(i, k, p),
        |p, k, f| xml_streaming_source(p, k, f, 1),
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
    let shard_pipeline = Pipeline::default();
    let batch_shards =
        arrow_ipc_batches_sharded_source(&shard_pipeline, "batch-shards", &input_path, 1)
            .unwrap()
            .output("batch-shards-out")
            .unwrap();
    let sharded = DirectRunner
        .run_collect(&batch_shards, &JobConfig::default())
        .unwrap()
        .0;
    assert_eq!(sharded.len(), 1);
    assert_eq!(sharded[0].0.num_rows(), 2);
    #[cfg(feature = "parallel-io")]
    {
        let parallel_path = directory.path().join("parallel.arrow");
        let parallel_pipeline = Pipeline::default();
        let input = parallel_pipeline
            .from_vec(
                "input-batches",
                vec![collected[0].clone(), collected[0].clone()],
            )
            .unwrap();
        DirectRunner
            .run_sink(
                &arrow_ipc_batches_parallel_sink(&input, "parallel", &parallel_path, 2).unwrap(),
                &JobConfig::default(),
            )
            .unwrap();
        let reader = Pipeline::default();
        let output = arrow_ipc_batches_source(&reader, "parallel-read", &parallel_path)
            .unwrap()
            .output("parallel-out")
            .unwrap();
        let batches = DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0;
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.0.num_rows())
                .collect::<Vec<_>>(),
            [2, 2]
        );
    }
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
