use ironbeam_core::{Boundedness, Capabilities, Element, ErrorKind, Operation, Pipeline, SinkMode};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_avro::*;
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct EvolvedRow {
    id: i32,
    name: String,
    active: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct PromotedRow {
    id: i64,
    name: String,
    active: Option<bool>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct AliasedRow {
    id: i32,
    display_name: String,
}
impl Element for AliasedRow {
    const TYPE_ID: &'static str = "tests/aliased-row";
    const CODER_ID: &'static str = "tests/aliased-row-json";
}
impl Element for PromotedRow {
    const TYPE_ID: &'static str = "tests/promoted-row";
    const CODER_ID: &'static str = "tests/promoted-row-json";
}
impl Element for EvolvedRow {
    const TYPE_ID: &'static str = "tests/evolved-row";
    const CODER_ID: &'static str = "tests/evolved-row-json";
}
const EVOLVED_AVRO_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"active","type":"boolean","default":true}]}"#;
const PROMOTED_AVRO_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"active","type":["null","boolean"],"default":null}]}"#;
const ALIASED_AVRO_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"display_name","type":"string","aliases":["name"]}]}"#;
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
const ROW_AVRO_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
fn write_native_rows(path: &std::path::Path) {
    let schema = apache_avro::Schema::parse_str(ROW_AVRO_SCHEMA).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, fs::File::create(path).unwrap()).unwrap();
    for row in rows() {
        writer.append_ser(row).unwrap();
    }
    writer.flush().unwrap();
}
#[test]
fn avro_contract() {
    round_trip(
        "avro",
        |i, k, p| avro_sink(i, k, p, ROW_AVRO_SCHEMA),
        |p, k, f| avro_source(p, k, f),
    );
}
#[test]
fn avro_lazy_contract() {
    round_trip(
        "avro",
        |i, k, p| avro_sink(i, k, p, ROW_AVRO_SCHEMA),
        |p, k, f| avro_streaming_source(p, k, f),
    );
}
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

#[test]
fn native_avro_reader_schema_resolves_added_and_projected_fields() {
    #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
    struct ProjectedRow {
        id: i32,
    }
    impl Element for ProjectedRow {
        const TYPE_ID: &'static str = "tests/projected-row";
        const CODER_ID: &'static str = "tests/projected-row-json";
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writer-v1.avro");
    write_native_rows(&path);
    let evolved = Pipeline::default();
    let output = avro_source_with_reader_schema::<EvolvedRow>(
        &evolved,
        "evolved",
        &path,
        EVOLVED_AVRO_SCHEMA,
    )
    .unwrap()
    .output("evolved-output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![
            EvolvedRow {
                id: 1,
                name: "Ada".into(),
                active: true
            },
            EvolvedRow {
                id: 2,
                name: "Lin".into(),
                active: true
            },
        ]
    );
    let alternate = Pipeline::default();
    let alternate_schema = EVOLVED_AVRO_SCHEMA.replace("\"default\":true", "\"default\":false");
    avro_source_with_reader_schema::<EvolvedRow>(&alternate, "evolved", &path, &alternate_schema)
        .unwrap()
        .output("evolved-output")
        .unwrap();
    assert_ne!(
        evolved.snapshot().unwrap().0.fingerprint().unwrap(),
        alternate.snapshot().unwrap().0.fingerprint().unwrap()
    );
    let projected = Pipeline::default();
    let projected_schema =
        r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"}]}"#;
    let output = avro_source_with_reader_schema::<ProjectedRow>(
        &projected,
        "projected",
        &path,
        projected_schema,
    )
    .unwrap()
    .output("projected-output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![ProjectedRow { id: 1 }, ProjectedRow { id: 2 }]
    );
}

#[test]
fn native_avro_reader_schema_promotes_ints_and_defaults_nullable_fields() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writer-v1.avro");
    write_native_rows(&path);
    for streaming in [false, true] {
        let pipeline = Pipeline::default();
        let source = if streaming {
            avro_streaming_source_with_reader_schema::<PromotedRow>(
                &pipeline,
                "promoted",
                &path,
                PROMOTED_AVRO_SCHEMA,
            )
        } else {
            avro_source_with_reader_schema::<PromotedRow>(
                &pipeline,
                "promoted",
                &path,
                PROMOTED_AVRO_SCHEMA,
            )
        };
        let output = source.unwrap().output("promoted-output").unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .unwrap()
                .0,
            vec![
                PromotedRow {
                    id: 1,
                    name: "Ada".into(),
                    active: None
                },
                PromotedRow {
                    id: 2,
                    name: "Lin".into(),
                    active: None
                },
            ]
        );
    }
}

#[test]
fn native_avro_reader_schema_resolves_field_aliases() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writer-v1.avro");
    write_native_rows(&path);
    for streaming in [false, true] {
        let pipeline = Pipeline::default();
        let source = if streaming {
            avro_streaming_source_with_reader_schema::<AliasedRow>(
                &pipeline,
                "aliased",
                &path,
                ALIASED_AVRO_SCHEMA,
            )
        } else {
            avro_source_with_reader_schema::<AliasedRow>(
                &pipeline,
                "aliased",
                &path,
                ALIASED_AVRO_SCHEMA,
            )
        };
        let output = source.unwrap().output("aliased-output").unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .unwrap()
                .0,
            vec![
                AliasedRow {
                    id: 1,
                    display_name: "Ada".into()
                },
                AliasedRow {
                    id: 2,
                    display_name: "Lin".into()
                },
            ]
        );
    }
}

#[test]
fn native_avro_reader_schema_rejects_invalid_and_incompatible_requests() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writer-v1.avro");
    write_native_rows(&path);
    assert_eq!(
        avro_source_with_reader_schema::<Row>(&Pipeline::default(), "invalid", &path, "{",)
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    let incompatible = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"string"},{"name":"name","type":"string"}]}"#;
    let pipeline = Pipeline::default();
    let output =
        avro_source_with_reader_schema::<Row>(&pipeline, "incompatible", &path, incompatible)
            .unwrap()
            .output("incompatible-output")
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
fn native_avro_streaming_reader_schema_resolves_records_and_detects_drift() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writer-v1.avro");
    write_native_rows(&path);
    let pipeline = Pipeline::default();
    let output = avro_streaming_source_with_reader_schema::<EvolvedRow>(
        &pipeline,
        "evolved-stream",
        &path,
        EVOLVED_AVRO_SCHEMA,
    )
    .unwrap()
    .output("evolved-stream-output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![
            EvolvedRow {
                id: 1,
                name: "Ada".into(),
                active: true
            },
            EvolvedRow {
                id: 2,
                name: "Lin".into(),
                active: true
            },
        ]
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
    assert_eq!(
        avro_streaming_source_with_reader_schema::<EvolvedRow>(
            &Pipeline::default(),
            "invalid-stream",
            &path,
            "{",
        )
        .err()
        .unwrap()
        .kind,
        ErrorKind::Io
    );
}
#[test]
fn avro_json_preview_files_remain_readable() {
    round_trip(
        "avro",
        |i, k, p| avro_json_sink(i, k, p),
        |p, k, f| avro_json_source(p, k, f),
    );
}
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

#[test]
fn avro_shards_and_glob_preserve_records() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("a.avro"), dir.path().join("b.avro")];
    for (index, path) in paths.iter().enumerate() {
        let pipeline = Pipeline::default();
        let input = pipeline
            .from_vec("values", vec![rows()[index].clone()])
            .unwrap();
        let writer = avro_sink(&input, "write", path, ROW_AVRO_SCHEMA).unwrap();
        DirectRunner
            .run_sink(&writer, &JobConfig::default())
            .unwrap();
    }
    let pipeline = Pipeline::default();
    let glob = avro_glob_source::<Row>(
        &pipeline,
        "files",
        &format!("{}/*.avro", dir.path().display()),
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
    let shards = avro_sharded_source::<Row>(&pipeline, "shards", &paths[0], 1)
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
    assert!(avro_sharded_source::<Row>(&pipeline, "bad", &paths[0], 0).is_err());
}

#[test]
fn avro_glob_and_shards_resolve_reader_schema() {
    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("a.avro");
    let second = dir.path().join("b.avro");
    write_native_rows(&first);
    write_native_rows(&second);
    let expected = [
        EvolvedRow {
            id: 1,
            name: "Ada".into(),
            active: true,
        },
        EvolvedRow {
            id: 2,
            name: "Lin".into(),
            active: true,
        },
    ];

    let pipeline = Pipeline::default();
    let glob = avro_glob_source_with_reader_schema::<EvolvedRow>(
        &pipeline,
        "files",
        &format!("{}/*.avro", dir.path().display()),
        EVOLVED_AVRO_SCHEMA,
    )
    .unwrap()
    .output("glob-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&glob, &JobConfig::default())
            .unwrap()
            .0,
        [expected.as_slice(), expected.as_slice()].concat()
    );

    let pipeline = Pipeline::default();
    let shards = avro_sharded_source_with_reader_schema::<EvolvedRow>(
        &pipeline,
        "shards",
        &first,
        1,
        EVOLVED_AVRO_SCHEMA,
    )
    .unwrap()
    .output("shard-out")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&shards, &JobConfig::default())
            .unwrap()
            .0,
        expected
    );
    fs::write(&first, b"changed").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&shards, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Recovery
    );
    assert!(
        avro_sharded_source_with_reader_schema::<EvolvedRow>(
            &Pipeline::default(),
            "bad",
            &second,
            0,
            EVOLVED_AVRO_SCHEMA
        )
        .is_err()
    );
}

#[cfg(feature = "parallel-io")]
#[test]
fn parallel_avro_sink_publishes_one_ordered_container() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("parallel.avro");
    let expected = (0..7)
        .map(|id| Row {
            id,
            name: format!("row-{id}"),
        })
        .collect::<Vec<_>>();
    let pipeline = Pipeline::default();
    let input = pipeline.from_vec("values", expected.clone()).unwrap();
    assert!(avro_parallel_sink(&input, "bad", &path, ROW_AVRO_SCHEMA, 0).is_err());
    let writer = avro_parallel_sink(&input, "write", &path, ROW_AVRO_SCHEMA, 5).unwrap();
    DirectRunner
        .run_sink(&writer, &JobConfig::default())
        .unwrap();
    let pipeline = Pipeline::default();
    let output = avro_source::<Row>(&pipeline, "read", &path)
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

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_avro_round_trips_through_lazy_source() {
    round_trip(
        "avro.gz",
        |i, k, p| avro_sink(i, k, p, ROW_AVRO_SCHEMA),
        |p, k, f| avro_streaming_source(p, k, f),
    );
}
