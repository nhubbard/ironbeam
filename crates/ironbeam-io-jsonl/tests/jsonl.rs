use ironbeam_core::{
    Boundedness, Capabilities, Element, Envelope, ErrorKind, Operation, Pipeline, SinkMode, Window,
};
use ironbeam_direct::{DirectCollectExt, DirectRunner, JobConfig};
use ironbeam_io_jsonl::*;
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

#[test]
fn finalized_jsonl_bundles_publish_one_valid_file() {
    let dir = tempfile::tempdir().unwrap();
    let staging = dir.path().join("staging");
    let target = dir.path().join("result.jsonl");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("input", rows()).unwrap();
    let sink = jsonl_fragment_sink(&input, "final", &staging, "job", "graph").unwrap();
    let (_, registry) = sink.snapshot().unwrap();
    assert!(registry.sink_requires_bundle_finalization("final"));
    let store = jsonl_fragment_store(&staging, "job", "graph", "final").unwrap();
    for (index, row) in rows().into_iter().enumerate() {
        let mut session = registry.open_sink("final").unwrap();
        session.begin_bundle(&format!("bundle-{index}")).unwrap();
        session
            .write(Envelope {
                value: Box::new(row),
                timestamp_ns: 0,
                window: Window::Global,
                assigned_window: None,
            })
            .unwrap();
        session.commit().unwrap();
        assert!(publish_jsonl_fragments::<Row>(&store, 2, &target).is_err());
        session.finalize_bundle().unwrap();
    }
    assert_eq!(store.accepted_fragments().unwrap().len(), 2);
    let result = publish_jsonl_fragments::<Row>(&store, 2, &target).unwrap();
    assert_eq!(result.rows, 2);
    assert_eq!(
        publish_jsonl_fragments::<Row>(&store, 2, &target).unwrap(),
        result
    );
    let output = jsonl_source::<Row>(&Pipeline::default(), "verify", &target)
        .unwrap()
        .output("result")
        .unwrap();
    let mut actual = output.collect_seq().unwrap();
    actual.sort_by_key(|row| row.id);
    assert_eq!(actual, rows());
    assert!(publish_jsonl_fragments::<Row>(&store, 3, &target).is_err());
    assert!(publish_jsonl_fragments::<Row>(&store, 2, dir.path().join("bad.jsonl.gz")).is_err());
}

#[test]
fn malformed_jsonl_fragment_cannot_replace_an_existing_result() {
    for malformed in [
        b"{broken}\n".as_slice(),
        b"{\"id\":1,\"name\":\"Ada\"}".as_slice(),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging");
        let target = dir.path().join("result.jsonl");
        fs::write(&target, b"previous result").unwrap();
        let raw = ironbeam_io::sequence_final::sequence_fragment_store(
            &staging, "job", "graph", "final", "jsonl",
        )
        .unwrap();
        raw.stage("bundle", 1, malformed).unwrap();
        raw.accept("bundle").unwrap();
        let store = jsonl_fragment_store(&staging, "job", "graph", "final").unwrap();
        assert!(publish_jsonl_fragments::<Row>(&store, 1, &target).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"previous result");
    }
}

#[test]
fn append_sink_commits_json_lines_and_marks_replay_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.jsonl");
    fs::write(&path, "{\"id\":0,\"name\":\"old\"}\n").unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", rows()).unwrap();
    let sink = jsonl_append_sink(&input, "append", &path).unwrap();
    let (graph, _) = pipeline.snapshot().unwrap();
    assert!(matches!(
        &graph.node("append").unwrap().operation,
        Operation::Sink { spec } if spec.mode == SinkMode::Append && spec.replay == ironbeam_core::Replay::AtLeastOnce && spec.commit
    ));
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "{\"id\":0,\"name\":\"old\"}\n{\"id\":1,\"name\":\"Ada\"}\n{\"id\":2,\"name\":\"Lin\"}\n"
    );
    assert_eq!(
        jsonl_append_sink(&input, "compressed", dir.path().join("rows.jsonl.gz"))
            .err()
            .unwrap()
            .kind,
        ErrorKind::Graph
    );
}

#[test]
fn append_sink_accepts_a_relative_filename_without_a_parent_component() {
    let temporary = tempfile::Builder::new()
        .prefix("ironbeam-jsonl-append-")
        .tempfile_in(".")
        .unwrap();
    let filename = temporary.path().file_name().unwrap().to_owned();
    let path = std::path::PathBuf::from(filename);
    temporary.close().unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", rows()[..1].to_vec()).unwrap();
    let sink = jsonl_append_sink(&input, "append", &path).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "{\"id\":1,\"name\":\"Ada\"}\n"
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn append_sink_aborts_after_serialization_or_open_failure() {
    #[derive(Clone)]
    struct BrokenRow;
    impl Element for BrokenRow {
        const TYPE_ID: &'static str = "tests/broken-jsonl-row";
        const CODER_ID: &'static str = "tests/broken-jsonl-row";
    }
    impl Serialize for BrokenRow {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("broken.jsonl");
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("broken", vec![BrokenRow]).unwrap();
    let sink = jsonl_append_sink(&input, "append-broken", &path).unwrap();
    assert_eq!(
        DirectRunner
            .run_sink(&sink, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    assert!(path.exists());
    assert_eq!(fs::read(&path).unwrap(), b"");

    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", rows()).unwrap();
    let sink = jsonl_append_sink(&input, "append-directory", dir.path()).unwrap();
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

#[cfg(feature = "parallel-io")]
#[test]
fn parallel_jsonl_sink_preserves_order() {
    let dir = tempfile::tempdir().unwrap();
    let (extension, make_sink, make_source) = (
        "jsonl",
        (|i, k, p, n| jsonl_parallel_sink(i, k, p, n))
            as fn(&_, &str, &std::path::Path, usize) -> _,
        (|p, k, f| jsonl_source::<Row>(p, k, f)) as fn(&_, &str, &std::path::Path) -> _,
    );
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
    }
}

#[cfg(all(feature = "parallel-io", feature = "compression-gzip"))]
#[test]
fn parallel_sinks_compress_after_ordered_shard_assembly() {
    let dir = tempfile::tempdir().unwrap();
    let (extension, make_sink, make_source) = (
        "jsonl.gz",
        (|i, k, p, n| jsonl_parallel_sink(i, k, p, n))
            as fn(&_, &str, &std::path::Path, usize) -> _,
        (|p, k, f| jsonl_source::<Row>(p, k, f)) as fn(&_, &str, &std::path::Path) -> _,
    );
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

    let pipeline = Pipeline::default();
    let empty_pattern = dir.path().join("*.missing").to_string_lossy().to_string();
    assert_eq!(
        jsonl_glob_source::<Row>(&pipeline, "none", &empty_pattern)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}

#[test]
fn jsonl_shards_preserve_record_boundaries_and_bind_contents() {
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

    let empty_path = dir.path().join("empty.jsonl");
    fs::write(&empty_path, b"").unwrap();
    let pipeline = Pipeline::default();
    let output = jsonl_sharded_source::<Row>(&pipeline, "empty", &empty_path, 2)
        .unwrap()
        .output("empty-out")
        .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        Vec::<Row>::new()
    );
}

#[cfg(feature = "compression-gzip")]
#[test]
fn compressed_jsonl_shards_match_bounded_readers() {
    let dir = tempfile::tempdir().unwrap();
    let (extension, make_sink, make_source) = (
        "jsonl.gz",
        (|i, k, p| jsonl_sink(i, k, p)) as MakeSink,
        (|p, k, f, n| jsonl_sharded_source::<Row>(p, k, f, n))
            as fn(&Pipeline, &str, &std::path::Path, usize) -> _,
    );
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

#[cfg(feature = "compression-gzip")]
#[test]
fn compressed_jsonl_round_trip_with_lazy_sources() {
    let dir = tempfile::tempdir().unwrap();
    let (extension, make_sink, make_source, make_streaming) = (
        "jsonl.gz",
        (|i, k, p| jsonl_sink(i, k, p)) as MakeSink,
        (|p, k, f| jsonl_source(p, k, f)) as MakeSource,
        (|p, k, f| jsonl_streaming_source(p, k, f)) as MakeSource,
    );
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

#[test]
fn jsonl_contract() {
    round_trip(
        "jsonl",
        |i, k, p| jsonl_sink(i, k, p),
        |p, k, f| jsonl_source(p, k, f),
    );
}

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

#[test]
fn jsonl_blank_lines_and_file_commit_error_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("blank.jsonl");
    fs::write(
        &source_path,
        "\n\u{2003}\n{\"id\":1,\"name\":\"Ada\"}\r\n\n",
    )
    .unwrap();
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
    fs::write(&source_path, b"\n{\"id\":1,\"name\":\"Ada\"}\nnot-json\n").unwrap();
    let invalid = jsonl_source::<Row>(&Pipeline::default(), "invalid", &source_path)
        .unwrap()
        .output("invalid-out")
        .unwrap()
        .collect_seq()
        .unwrap_err();
    assert_eq!(invalid.kind, ErrorKind::Io);
    assert!(invalid.to_string().contains("line 3"));
    fs::write(&source_path, b"\xff\n").unwrap();
    assert_eq!(
        jsonl_source::<Row>(&Pipeline::default(), "invalid-utf8", &source_path)
            .unwrap()
            .output("invalid-utf8-out")
            .unwrap()
            .collect_seq()
            .unwrap_err()
            .kind,
        ErrorKind::Io
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
