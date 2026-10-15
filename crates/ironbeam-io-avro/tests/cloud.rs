#![cfg(feature = "cloud")]
use ironbeam_cloud_objects::{
    CloudIOError, CloudResult, ErrorKind as CloudErrorKind, LocalObjectIO, ObjectIO,
    ObjectMetadata, ObjectStores,
};
use ironbeam_core::{Element, ErrorKind, Pipeline};
use ironbeam_direct::{DirectRunner, JobConfig};
#[cfg(feature = "compression-gzip")]
use ironbeam_io_avro::avro_sink;
use ironbeam_io_avro::{
    cloud_avro_streaming_uri_prefix_source,
    cloud_avro_streaming_uri_prefix_source_with_reader_schema, cloud_avro_streaming_uri_source,
    cloud_avro_streaming_uri_source_with_reader_schema, cloud_avro_uri_prefix_source,
    cloud_avro_uri_prefix_source_with_reader_schema, cloud_avro_uri_source,
    cloud_avro_uri_source_with_reader_schema,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{Cursor, ErrorKind as IoErrorKind, Read};

#[derive(Clone, Copy)]
enum FirstReadFailure {
    Interrupted,
    Broken,
}

struct FirstReadFails {
    content: Cursor<Vec<u8>>,
    first: bool,
    mode: FirstReadFailure,
}

impl Read for FirstReadFails {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.first {
            self.first = false;
            let kind = match self.mode {
                FirstReadFailure::Interrupted => IoErrorKind::Interrupted,
                FirstReadFailure::Broken => IoErrorKind::BrokenPipe,
            };
            return Err(std::io::Error::new(kind, "injected object read failure"));
        }
        self.content.read(buffer)
    }
}

struct ReadFaultObjectIO {
    content: Vec<u8>,
    mode: FirstReadFailure,
}

fn unused<T>() -> CloudResult<T> {
    Err(CloudIOError::new(
        CloudErrorKind::InvalidInput,
        "unused test operation",
    ))
}

impl ObjectIO for ReadFaultObjectIO {
    fn put_object(&self, _: &str, _: &str, _: &[u8]) -> CloudResult<()> {
        unused()
    }
    fn open_object(&self, _: &str, _: &str) -> CloudResult<Box<dyn Read + Send>> {
        Ok(Box::new(FirstReadFails {
            content: Cursor::new(self.content.clone()),
            first: true,
            mode: self.mode,
        }))
    }
    fn get_object(&self, _: &str, _: &str) -> CloudResult<Vec<u8>> {
        Ok(self.content.clone())
    }
    fn delete_object(&self, _: &str, _: &str) -> CloudResult<()> {
        unused()
    }
    fn list_objects(&self, _: &str, _: Option<&str>) -> CloudResult<Vec<ObjectMetadata>> {
        unused()
    }
    fn object_exists(&self, _: &str, _: &str) -> CloudResult<bool> {
        unused()
    }
    fn get_metadata(&self, _: &str, _: &str) -> CloudResult<ObjectMetadata> {
        unused()
    }
    fn copy_object(&self, _: &str, _: &str, _: &str, _: &str) -> CloudResult<()> {
        unused()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
struct Row {
    id: i32,
    name: String,
}

#[cfg(feature = "compression-gzip")]
#[test]
fn gzip_object_uri_decodes_native_avro() {
    let (dir, stores) = object_store();
    let path = dir.path().join("rows.avro.gz");
    let writer = Pipeline::default();
    let input = writer
        .from_vec(
            "rows",
            vec![Row {
                id: 7,
                name: "Gzip".into(),
            }],
        )
        .unwrap();
    let sink = avro_sink(&input, "gzip-write", &path, WRITER_SCHEMA).unwrap();
    DirectRunner.run_sink(&sink, &JobConfig::default()).unwrap();
    stores
        .write("file://bucket/rows.avro.gz", &fs::read(path).unwrap())
        .unwrap();
    let pipeline = Pipeline::default();
    let output = cloud_avro_uri_source_with_reader_schema::<EvolvedRow>(
        &pipeline,
        "gzip-read",
        &stores,
        "file://bucket/rows.avro.gz",
        READER_SCHEMA,
    )
    .unwrap()
    .output("gzip-output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![EvolvedRow {
            id: 7,
            name: "Gzip".into(),
            active: true
        }]
    );
    let streamed = Pipeline::default();
    let streamed_output = cloud_avro_streaming_uri_source_with_reader_schema::<EvolvedRow>(
        &streamed,
        "gzip-streamed",
        &stores,
        "file://bucket/rows.avro.gz",
        READER_SCHEMA,
    )
    .unwrap()
    .output("gzip-streamed-output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&streamed_output, &JobConfig::default())
            .unwrap()
            .0,
        vec![EvolvedRow {
            id: 7,
            name: "Gzip".into(),
            active: true
        }]
    );
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
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
struct AliasedRow {
    id: i32,
    display_name: String,
}
impl Element for AliasedRow {
    const TYPE_ID: &'static str = "tests/cloud-avro-aliased-row";
    const CODER_ID: &'static str = "tests/cloud-avro-aliased-row-json";
}
impl Element for EvolvedRow {
    const TYPE_ID: &'static str = "tests/cloud-avro-evolved-row";
    const CODER_ID: &'static str = "tests/cloud-avro-evolved-row-json";
}

const WRITER_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}"#;
const READER_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"active","type":"boolean","default":true}]}"#;
const ALIASED_READER_SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"display_name","type":"string","aliases":["name"]}]}"#;

fn object_store() -> (tempfile::TempDir, ObjectStores) {
    let dir = tempfile::tempdir().unwrap();
    let mut stores = ObjectStores::new();
    stores
        .register("file", LocalObjectIO::new(dir.path()).unwrap())
        .unwrap();
    (dir, stores)
}

fn put_avro(stores: &ObjectStores, dir: &std::path::Path) {
    put_avro_row(stores, dir, "file://bucket/rows.avro", 1, "Ada");
}

fn put_avro_row(stores: &ObjectStores, dir: &std::path::Path, uri: &str, id: i32, name: &str) {
    let path = dir.join("writer.avro");
    let schema = apache_avro::Schema::parse_str(WRITER_SCHEMA).unwrap();
    let mut writer = apache_avro::Writer::new(&schema, fs::File::create(&path).unwrap()).unwrap();
    writer
        .append_value(apache_avro::types::Value::Record(vec![
            ("id".into(), apache_avro::types::Value::Int(id)),
            (
                "name".into(),
                apache_avro::types::Value::String(name.into()),
            ),
        ]))
        .unwrap();
    writer.flush().unwrap();
    drop(writer);
    stores.write(uri, &fs::read(path).unwrap()).unwrap();
}

#[test]
fn streaming_object_uri_resolves_schema_and_rejects_changed_bytes() {
    let (dir, stores) = object_store();
    put_avro(&stores, dir.path());
    let pipeline = Pipeline::default();
    let output = cloud_avro_streaming_uri_source_with_reader_schema::<EvolvedRow>(
        &pipeline,
        "streamed",
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
    put_avro_row(&stores, dir.path(), "file://bucket/rows.avro", 2, "Lin");
    let Err(error) = DirectRunner.run_collect(&output, &JobConfig::default()) else {
        panic!("changed Avro bytes should reject the source");
    };
    assert_eq!(error.kind, ErrorKind::Recovery);
    assert!(
        cloud_avro_streaming_uri_source_with_reader_schema::<EvolvedRow>(
            &Pipeline::default(),
            "invalid-schema",
            &stores,
            "file://bucket/rows.avro",
            "not JSON",
        )
        .is_err()
    );
}

#[test]
fn streaming_object_prefix_binds_sorted_members_and_detects_loss() {
    let (dir, stores) = object_store();
    put_avro_row(&stores, dir.path(), "file://bucket/input/b.avro", 2, "Lin");
    put_avro_row(&stores, dir.path(), "file://bucket/input/a.avro", 1, "Ada");
    let pipeline = Pipeline::default();
    let output = cloud_avro_streaming_uri_prefix_source_with_reader_schema::<EvolvedRow>(
        &pipeline,
        "streamed-prefix",
        &stores,
        "file://bucket/input/",
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
    put_avro_row(&stores, dir.path(), "file://bucket/input/c.avro", 3, "New");
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0
            .len(),
        2
    );
    stores.delete("file://bucket/input/a.avro").unwrap();
    assert!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .is_err()
    );
    assert!(
        cloud_avro_streaming_uri_prefix_source::<Row>(
            &Pipeline::default(),
            "empty",
            &stores,
            "file://bucket/empty/",
        )
        .is_err()
    );
}

#[test]
fn streaming_single_object_without_reader_schema_decodes_rows() {
    let (dir, stores) = object_store();
    put_avro(&stores, dir.path());
    let pipeline = Pipeline::default();
    let output = cloud_avro_streaming_uri_source::<Row>(
        &pipeline,
        "streamed",
        &stores,
        "file://bucket/rows.avro",
    )
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
            name: "Ada".into()
        }]
    );
}

#[test]
fn streaming_source_handles_extensionless_objects_and_singleton_prefixes() {
    let (dir, stores) = object_store();
    put_avro_row(&stores, dir.path(), "file://bucket/single/rows", 9, "NoExt");
    let pipeline = Pipeline::default();
    let output = cloud_avro_streaming_uri_prefix_source::<Row>(
        &pipeline,
        "single-prefix",
        &stores,
        "file://bucket/single/",
    )
    .unwrap()
    .output("output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![Row {
            id: 9,
            name: "NoExt".into()
        }]
    );
    assert!(
        cloud_avro_streaming_uri_source::<Row>(
            &Pipeline::default(),
            "invalid-uri",
            &stores,
            "invalid-uri",
        )
        .is_err()
    );
    assert!(
        cloud_avro_streaming_uri_source::<Row>(
            &Pipeline::default(),
            "missing-object",
            &stores,
            "file://bucket/missing.avro",
        )
        .is_err()
    );
}

#[test]
fn streaming_object_retries_interruption_and_reports_broken_reads() {
    let (dir, local) = object_store();
    put_avro(&local, dir.path());
    let content = local.read("file://bucket/rows.avro").unwrap();
    for (scheme, mode) in [
        ("interrupted", FirstReadFailure::Interrupted),
        ("broken", FirstReadFailure::Broken),
    ] {
        let mut stores = ObjectStores::new();
        stores
            .register(
                scheme,
                ReadFaultObjectIO {
                    content: content.clone(),
                    mode,
                },
            )
            .unwrap();
        let result = cloud_avro_streaming_uri_source::<Row>(
            &Pipeline::default(),
            "faulty-read",
            &stores,
            &format!("{scheme}://bucket/rows.avro"),
        );
        match mode {
            FirstReadFailure::Interrupted => {
                let output = result.unwrap().output("faulty-output").unwrap();
                assert_eq!(
                    DirectRunner
                        .run_collect(&output, &JobConfig::default())
                        .unwrap()
                        .0,
                    vec![Row {
                        id: 1,
                        name: "Ada".into()
                    }]
                );
            }
            FirstReadFailure::Broken => assert!(result.is_err()),
        }
    }
}

#[test]
fn object_uri_prefix_uses_sorted_stable_children() {
    let (dir, stores) = object_store();
    put_avro_row(&stores, dir.path(), "file://bucket/input/b.avro", 2, "Lin");
    put_avro_row(&stores, dir.path(), "file://bucket/input/a.avro", 1, "Ada");
    let pipeline = Pipeline::default();
    let output = cloud_avro_uri_prefix_source_with_reader_schema::<EvolvedRow>(
        &pipeline,
        "files",
        &stores,
        "file://bucket/input/",
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
    let plain = Pipeline::default();
    let plain_output =
        cloud_avro_uri_prefix_source::<Row>(&plain, "plain-files", &stores, "file://bucket/input/")
            .unwrap()
            .output("plain-output")
            .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&plain_output, &JobConfig::default())
            .unwrap()
            .0,
        vec![
            Row {
                id: 1,
                name: "Ada".into()
            },
            Row {
                id: 2,
                name: "Lin".into()
            },
        ]
    );
    let single = Pipeline::default();
    let single_output = cloud_avro_uri_prefix_source::<Row>(
        &single,
        "single-file",
        &stores,
        "file://bucket/input/a.avro",
    )
    .unwrap()
    .output("single-output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&single_output, &JobConfig::default())
            .unwrap()
            .0,
        vec![Row {
            id: 1,
            name: "Ada".into()
        }]
    );
}

#[test]
fn object_uri_prefix_freezes_its_set_and_validates_matches() {
    let (dir, stores) = object_store();
    put_avro_row(&stores, dir.path(), "file://bucket/input/a.avro", 1, "Ada");
    let pipeline = Pipeline::default();
    let output =
        cloud_avro_uri_prefix_source::<Row>(&pipeline, "files", &stores, "file://bucket/input/")
            .unwrap()
            .output("output")
            .unwrap();
    put_avro_row(&stores, dir.path(), "file://bucket/input/b.avro", 2, "New");
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![Row {
            id: 1,
            name: "Ada".into()
        }]
    );
    assert!(
        cloud_avro_uri_prefix_source_with_reader_schema::<EvolvedRow>(
            &Pipeline::default(),
            "empty",
            &stores,
            "file://bucket/none/",
            READER_SCHEMA
        )
        .is_err()
    );
    assert!(
        cloud_avro_uri_prefix_source::<Row>(
            &Pipeline::default(),
            "bad-prefix",
            &stores,
            "invalid-uri"
        )
        .is_err()
    );
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
fn object_uri_resolves_reader_field_alias() {
    let (dir, stores) = object_store();
    put_avro(&stores, dir.path());
    let pipeline = Pipeline::default();
    let output = cloud_avro_uri_source_with_reader_schema::<AliasedRow>(
        &pipeline,
        "aliased",
        &stores,
        "file://bucket/rows.avro",
        ALIASED_READER_SCHEMA,
    )
    .unwrap()
    .output("output")
    .unwrap();
    assert_eq!(
        DirectRunner
            .run_collect(&output, &JobConfig::default())
            .unwrap()
            .0,
        vec![AliasedRow {
            id: 1,
            display_name: "Ada".into()
        }]
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
