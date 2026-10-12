// Unit tests for src/fragment.rs.
use super::*;
use ironbeam_core::Pipeline;

const SCHEMA: &str = r#"{"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#;

#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
struct Row {
    id: i32,
    label: String,
}

impl Element for Row {
    const TYPE_ID: &'static str = "test/avro-fragment-row";
    const CODER_ID: &'static str = "test/avro-fragment-row-v1";
    fn supports_coder() -> bool {
        true
    }
    fn encode(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(self).map_err(|error| Error::incompatible(error.to_string()))
    }
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        serde_json::from_slice(bytes).map_err(|error| Error::incompatible(error.to_string()))
    }
}

fn session(store: &AvroFragmentStore) -> AvroFragmentSink<Row> {
    AvroFragmentSink {
        store: store.clone(),
        bundle_id: None,
        spool: None,
        rows: 0,
        staged: false,
        marker: std::marker::PhantomData,
    }
}

#[test]
fn two_native_schema_fragments_merge_into_one_container() {
    let temp = tempfile::tempdir().unwrap();
    let store =
        avro_fragment_store(temp.path().join("stage"), "job", "graph", "sink", SCHEMA).unwrap();
    let output = temp.path().join("final.avro");
    let mut first = session(&store);
    assert!(
        first
            .write(Row {
                id: 0,
                label: "early".into()
            })
            .is_err()
    );
    assert!(first.begin_bundle("").is_err());
    assert!(first.finalize_bundle().is_err());
    first.begin_bundle("bundle-1").unwrap();
    assert!(first.finalize_bundle().is_err());
    assert!(first.begin_bundle("duplicate").is_err());
    first
        .write(Row {
            id: 1,
            label: "a".into(),
        })
        .unwrap();
    first
        .write(Row {
            id: 2,
            label: "b".into(),
        })
        .unwrap();
    first.commit().unwrap();
    assert!(publish_avro_fragments(&store, 2, &output).is_err());
    assert!(!output.exists());
    let mut overlap = session(&store);
    overlap.begin_bundle("bundle-1").unwrap();
    overlap
        .write(Row {
            id: 9,
            label: "wrong".into(),
        })
        .unwrap();
    assert!(overlap.commit().is_err());
    overlap.abort().unwrap();
    first.finalize_bundle().unwrap();
    first.finalize_bundle().unwrap();
    let mut second = session(&store);
    second.begin_bundle("bundle-2").unwrap();
    second
        .write(Row {
            id: 3,
            label: "c".into(),
        })
        .unwrap();
    second.commit().unwrap();
    let mut restarted = session(&store);
    assert!(restarted.recover_bundle_finalization("missing").is_err());
    restarted.recover_bundle_finalization("bundle-2").unwrap();
    second.finalize_bundle().unwrap();
    let store = store
        .require_bundle_attempts(["bundle-1", "bundle-2"])
        .unwrap();
    let published = publish_avro_fragments(&store, 3, &output).unwrap();
    assert_eq!(published.rows, 3);
    assert_eq!(published.fragments, 2);
    assert_eq!(
        publish_avro_fragments(&store, 3, &output).unwrap(),
        published
    );
    let reader = apache_avro::Reader::new(std::fs::File::open(&output).unwrap()).unwrap();
    assert_eq!(
        reader.writer_schema().canonical_form(),
        apache_avro::Schema::parse_str(SCHEMA)
            .unwrap()
            .canonical_form()
    );
    let mut rows = reader
        .map(|value| apache_avro::from_value::<Row>(&value.unwrap()).unwrap())
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row.id);
    assert_eq!(
        rows,
        [
            Row {
                id: 1,
                label: "a".into()
            },
            Row {
                id: 2,
                label: "b".into()
            },
            Row {
                id: 3,
                label: "c".into()
            }
        ]
    );
}

#[test]
fn builder_scope_abort_and_invalid_schema_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let pipeline = Pipeline::default();
    let input = pipeline
        .from_vec(
            "input",
            vec![Row {
                id: 1,
                label: "a".into(),
            }],
        )
        .unwrap();
    assert!(avro_fragment_sink(&input, "bad", "relative", "job", "graph", SCHEMA).is_err());
    assert!(avro_fragment_sink(&input, "bad", temp.path(), "job", "graph", "{").is_err());
    let staging = temp.path().join("stage");
    let sink = avro_fragment_sink(&input, "sink", &staging, "job", "graph", SCHEMA).unwrap();
    let (_, registry) = sink.snapshot().unwrap();
    assert!(registry.sink_requires_bundle_finalization("sink"));
    assert!(
        registry
            .sink_config_digest("sink")
            .unwrap()
            .contains("avro-fragments-v1")
    );
    let mut opened = registry.open_sink("sink").unwrap();
    assert!(opened.requires_bundle_finalization());
    opened.begin_bundle("aborted").unwrap();
    opened.commit().unwrap();
    opened.abort().unwrap();
    let store = avro_fragment_store(&staging, "job", "graph", "sink", SCHEMA).unwrap();
    assert!(store.accepted_fragments().unwrap().is_empty());
    assert!(std::fs::read_dir(&staging).unwrap().next().is_none());
    let different = r#"{"type":"record","name":"Other","fields":[{"name":"id","type":"long"}]}"#;
    let mut accepted = session(&store);
    accepted.begin_bundle("schema-bound").unwrap();
    accepted
        .write(Row {
            id: 1,
            label: "a".into(),
        })
        .unwrap();
    accepted.commit().unwrap();
    accepted.finalize_bundle().unwrap();
    let wrong = avro_fragment_store(&staging, "job", "graph", "sink", different).unwrap();
    assert!(wrong.accepted_fragments().is_err());
    let metadata_only = r#"{"type":"record","name":"Row","doc":"another contract","fields":[{"name":"id","type":"int"},{"name":"label","type":"string"}]}"#;
    let wrong_metadata =
        avro_fragment_store(&staging, "job", "graph", "sink", metadata_only).unwrap();
    assert!(wrong_metadata.accepted_fragments().is_err());
    assert!(avro_fragment_store(&staging, "job", "", "sink", SCHEMA).is_err());
}

#[test]
fn malformed_datum_and_row_metadata_never_publish() {
    let temp = tempfile::tempdir().unwrap();
    let store =
        avro_fragment_store(temp.path().join("stage"), "job", "graph", "sink", SCHEMA).unwrap();
    store
        .local
        .stage("bad-length", 1, &100_u64.to_le_bytes())
        .unwrap();
    store.local.accept("bad-length").unwrap();
    let output = temp.path().join("result.avro");
    assert!(publish_avro_fragments(&store, 1, &output).is_err());
    assert!(!output.exists());
    let other =
        avro_fragment_store(temp.path().join("other"), "job", "graph", "sink", SCHEMA).unwrap();
    let value = Row {
        id: 1,
        label: "a".into(),
    };
    let datum = GenericDatumWriter::builder(&other.schema)
        .build()
        .unwrap()
        .write_ser_to_vec(&value)
        .unwrap();
    let mut framed = (datum.len() as u64).to_le_bytes().to_vec();
    framed.extend_from_slice(&datum);
    other.local.stage("wrong-count", 2, &framed).unwrap();
    other.local.accept("wrong-count").unwrap();
    assert!(publish_avro_fragments(&other, 2, &output).is_err());
    assert!(!output.exists());
    assert!(publish_avro_fragments(&other, 2, temp.path().join("result.avro.gz")).is_err());
    let truncated = avro_fragment_store(
        temp.path().join("truncated"),
        "job",
        "graph",
        "sink",
        SCHEMA,
    )
    .unwrap();
    truncated.local.stage("truncated", 1, &[1, 2, 3]).unwrap();
    truncated.local.accept("truncated").unwrap();
    assert!(publish_avro_fragments(&truncated, 1, temp.path().join("truncated.avro")).is_err());
    let trailing =
        avro_fragment_store(temp.path().join("trailing"), "job", "graph", "sink", SCHEMA).unwrap();
    let mut framed = ((datum.len() + 1) as u64).to_le_bytes().to_vec();
    framed.extend_from_slice(&datum);
    framed.push(0);
    trailing.local.stage("trailing", 1, &framed).unwrap();
    trailing.local.accept("trailing").unwrap();
    assert!(publish_avro_fragments(&trailing, 1, temp.path().join("trailing.avro")).is_err());
    let invalid =
        avro_fragment_store(temp.path().join("invalid"), "job", "graph", "sink", SCHEMA).unwrap();
    let mut framed = 1_u64.to_le_bytes().to_vec();
    framed.push(0xff);
    invalid.local.stage("invalid", 1, &framed).unwrap();
    invalid.local.accept("invalid").unwrap();
    assert!(publish_avro_fragments(&invalid, 1, temp.path().join("invalid.avro")).is_err());
}
