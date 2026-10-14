#![cfg(feature = "cloud")]
use ironbeam_cloud_objects::{LocalObjectIO, ObjectStores};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
use ironbeam_io_avro::{cloud_avro_uri_source, cloud_avro_uri_source_with_reader_schema};
use serde::Deserialize;
use std::fs;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct Row {
    id: i32,
    name: String,
}
impl Element for Row {
    const TYPE_ID: &'static str = "tests/cloud-avro-row";
    const CODER_ID: &'static str = "tests/cloud-avro-row-json";
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct EvolvedRow {
    id: i32,
    name: String,
    active: bool,
}
impl Element for EvolvedRow {
    const TYPE_ID: &'static str = "tests/cloud-avro-evolved-row";
    const CODER_ID: &'static str = "tests/cloud-avro-evolved-row-json";
}

const WRITER_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
const READER_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"active","type":"boolean","default":true}]}"#;

fn object_store() -> (tempfile::TempDir, ObjectStores) {
    let dir = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();
    (dir, stores)
}

fn put_avro(stores: &ObjectStores, dir: &std::path::Path) {
    let path = dir.join("writer.avro");
    let schema = apache_avro::Schema::parse_str(WRITER_SCHEMA).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, fs::File::create(&path).unwrap()).unwrap();
    writer
        .append_value(apache_avro::types::Value::Record(vec![
            ("id".into(), apache_avro::types::Value::Int(1)),
            (
                "name".into(),
                apache_avro::types::Value::String("Ada".into()),
            ),
        ]))
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    stores
        .write("file://bucket/rows.avro", &fs::read(path).unwrap())
        .unwrap();
}

#[test]
fn object_uri_resolves_avro_schema_and_rejects_drift() {
    let (dir, stores) = object_store();
    put_avro(&stores, dir.path());
    let pipeline = Pipeline::default();
    let output = cloud_avro_uri_source_with_reader_schema::<EvolvedRow>(
        &pipeline,
        "read",
        &stores,
        "file://bucket/rows.avro",
        READER_SCHEMA,
    )
    .unwrap()
    .output("output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![EvolvedRow {
            id: 1,
            name: "Ada".into(),
            active: true,
        }]
    );
    let alternate = Pipeline::default();
    let alternate_schema = READER_SCHEMA.replace("\"default\":true", "\"default\":false");
    assert_ne!(alternate_schema, READER_SCHEMA);
    cloud_avro_uri_source_with_reader_schema::<EvolvedRow>(
        &alternate,
        "read",
        &stores,
        "file://bucket/rows.avro",
        &alternate_schema,
    )
    .unwrap()
    .output("output")
    .unwrap();
    assert_ne!(
        pipeline.snapshot().unwrap().0.fingerprint().unwrap(),
        alternate.snapshot().unwrap().0.fingerprint().unwrap()
    );
    let same_bytes = stores.read("file://bucket/rows.avro").unwrap();
    stores
        .write("file://bucket/other.avro", &same_bytes)
        .unwrap();
    let other_location = Pipeline::default();
    cloud_avro_uri_source_with_reader_schema::<EvolvedRow>(
        &other_location,
        "read",
        &stores,
        "file://bucket/other.avro",
        READER_SCHEMA,
    )
    .unwrap();
    assert_ne!(
        pipeline.snapshot().unwrap().1.source_config_digest("read"),
        other_location
            .snapshot()
            .unwrap()
            .1
            .source_config_digest("read")
    );
    stores.write("file://bucket/rows.avro", b"changed").unwrap();
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
fn plain_object_uri_reads_native_avro_and_checks_inputs() {
    let (dir, stores) = object_store();
    put_avro(&stores, dir.path());
    let pipeline = Pipeline::default();
    let output =
        cloud_avro_uri_source::<Row>(&pipeline, "read", &stores, "file://bucket/rows.avro")
            .unwrap()
            .output("output")
            .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![Row {
            id: 1,
            name: "Ada".into(),
        }]
    );
    assert!(cloud_avro_uri_source::<Row>(&Pipeline::default(), "bad", &stores, "bad").is_err());
    assert!(
        cloud_avro_uri_source::<Row>(
            &Pipeline::default(),
            "unregistered",
            &stores,
            "gs://bucket/rows.avro",
        )
        .is_err()
    );
    assert!(
        cloud_avro_uri_source::<Row>(
            &Pipeline::default(),
            "missing",
            &stores,
            "file://bucket/missing.avro",
        )
        .is_err()
    );
    assert!(
        cloud_avro_uri_source_with_reader_schema::<EvolvedRow>(
            &Pipeline::default(),
            "invalid",
            &stores,
            "file://bucket/rows.avro",
            "{",
        )
        .is_err()
    );
}

#[test]
fn object_uri_reports_decode_errors_and_disappearing_objects() {
    let (dir, stores) = object_store();
    put_avro(&stores, dir.path());
    let incompatible = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"string"}]}"#;
    let invalid_schema = Pipeline::default();
    let output = cloud_avro_uri_source_with_reader_schema::<Row>(
        &invalid_schema,
        "incompatible",
        &stores,
        "file://bucket/rows.avro",
        incompatible,
    )
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
    let missing_field = Pipeline::default();
    let output = cloud_avro_uri_source::<EvolvedRow>(
        &missing_field,
        "missing-field",
        &stores,
        "file://bucket/rows.avro",
    )
    .unwrap()
    .output("missing-field-output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
    stores
        .write("file://bucket/corrupt.avro", b"not an Avro container")
        .unwrap();
    for with_schema in [false, true] {
        let pipeline = Pipeline::default();
        let source = if with_schema {
            cloud_avro_uri_source_with_reader_schema::<EvolvedRow>(
                &pipeline,
                "corrupt",
                &stores,
                "file://bucket/corrupt.avro",
                READER_SCHEMA,
            )
        } else {
            cloud_avro_uri_source::<EvolvedRow>(
                &pipeline,
                "corrupt",
                &stores,
                "file://bucket/corrupt.avro",
            )
        };
        let output = source.unwrap().output("corrupt-output").unwrap();
        assert_eq!(
            DirectRunner
                .run_collect(&output, &JobConfig::default())
                .err()
                .unwrap()
                .kind,
            ErrorKind::Io
        );
    }
    let late = Pipeline::default();
    let late_output =
        cloud_avro_uri_source::<Row>(&late, "missing-at-run", &stores, "file://bucket/rows.avro")
            .unwrap()
            .output("missing-at-run-output")
            .unwrap();
    stores.delete("file://bucket/rows.avro").unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&late_output, &JobConfig::default())
            .err()
            .unwrap()
            .kind,
        ErrorKind::Io
    );
}
